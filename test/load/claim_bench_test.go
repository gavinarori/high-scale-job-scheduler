package load

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
)

// seedJobs inserts n already-due jobs and returns once all are committed.
// Uses direct InsertMany rather than repo.Create in a loop — this
// benchmark is measuring ClaimDueJobs, not Create, so seeding shouldn't
// count against the timer or dominate setup time for large n.
func seedJobs(b *testing.B, repo *store.JobsRepo, n int, prefix string) {
	b.Helper()
	ctx := context.Background()
	now := time.Now().Add(-time.Minute)

	for i := 0; i < n; i++ {
		_, err := repo.Create(ctx, models.CreateJobRequest{
			IdempotencyKey: fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), i),
			TenantID:       "load-test",
			JobType:        "noop",
			ScheduledAt:    now,
			MaxAttempts:    3,
		})
		if err != nil {
			b.Fatalf("seed job %d failed: %v", i, err)
		}
	}
}

// BenchmarkClaimDueJobs_SingleClaimer measures raw claim throughput with no
// contention — the ceiling any concurrent scenario is bounded by. Run with:
//
//	go test ./test/load/... -bench BenchmarkClaimDueJobs_SingleClaimer -benchtime=1x -run ^$
func BenchmarkClaimDueJobs_SingleClaimer(b *testing.B) {
	db, cleanup := setupMongo(b)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	if err := repo.EnsureIndexes(ctx); err != nil {
		b.Fatalf("index creation failed: %v", err)
	}

	const poolSize = 5000
	seedJobs(b, repo, poolSize, "single")

	b.ResetTimer()
	start := time.Now()
	totalClaimed := 0
	for totalClaimed < poolSize {
		claimed, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), 100)
		if err != nil {
			b.Fatalf("claim failed: %v", err)
		}
		if len(claimed) == 0 {
			break
		}
		totalClaimed += len(claimed)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	b.ReportMetric(float64(totalClaimed)/elapsed.Seconds(), "jobs/sec")
	b.Logf("claimed %d jobs in %s (%.0f jobs/sec)", totalClaimed, elapsed, float64(totalClaimed)/elapsed.Seconds())
}

// BenchmarkClaimDueJobs_Concurrent measures throughput and — critically —
// correctness under N concurrent claimers hammering the same job pool, at
// increasing concurrency levels. This is the number that actually answers
// "how many scheduler instances can I run before ClaimDueJobs itself
// becomes the bottleneck," which matters once Phase 3's leader election
// stops being the only thing bounding scheduler instance count (e.g. if a
// future change allows sharded/multi-leader dispatch per tenant).
func BenchmarkClaimDueJobs_Concurrent(b *testing.B) {
	concurrencyLevels := []int{1, 5, 20, 50}

	for _, n := range concurrencyLevels {
		n := n
		b.Run(fmt.Sprintf("concurrency-%d", n), func(b *testing.B) {
			db, cleanup := setupMongo(b)
			defer cleanup()

			ctx := context.Background()
			repo := store.NewJobsRepo(db)
			if err := repo.EnsureIndexes(ctx); err != nil {
				b.Fatalf("index creation failed: %v", err)
			}

			const poolSize = 5000
			seedJobs(b, repo, poolSize, fmt.Sprintf("concurrent-%d", n))

			var (
				mu            sync.Mutex
				claimed       = make(map[string]int)
				totalDuration time.Duration
			)

			b.ResetTimer()
			start := time.Now()

			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						callStart := time.Now()
						jobs, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), 20)
						callDur := time.Since(callStart)
						if err != nil {
							b.Errorf("claim failed: %v", err)
							return
						}
						mu.Lock()
						totalDuration += callDur
						for _, j := range jobs {
							claimed[j.ID.Hex()]++
						}
						done := len(claimed) >= poolSize
						mu.Unlock()
						if len(jobs) == 0 || done {
							return
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)
			b.StopTimer()

			// This assertion is the actual point of the benchmark, not an
			// afterthought — a throughput number next to a silent
			// correctness violation is worse than no number at all.
			for id, count := range claimed {
				if count != 1 {
					b.Fatalf("job %s claimed %d times at concurrency %d — atomicity violated under load", id, count, n)
				}
			}

			throughput := float64(len(claimed)) / elapsed.Seconds()
			b.ReportMetric(throughput, "jobs/sec")
			b.Logf("concurrency=%d: claimed %d/%d jobs in %s (%.0f jobs/sec, avg call latency %s)",
				n, len(claimed), poolSize, elapsed, throughput, totalDuration/time.Duration(n))
		})
	}
}
