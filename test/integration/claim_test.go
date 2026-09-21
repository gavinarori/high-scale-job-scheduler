package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
	"github.com/stretchr/testify/require"
)

// TestClaimDueJobs_NoDoubleClaim is the test that actually justifies using
// Mongo's findOneAndUpdate over an application-level lock. We create N due
// jobs, then fire many concurrent goroutines — simulating multiple
// scheduler instances mid-election-handoff, or a single instance with
// overlapping ticks — all racing to claim from the same pool. If the
// atomicity guarantee doesn't hold, this test will flake with duplicate
// job IDs across claim batches; a single clean run is not sufficient
// evidence on its own, which is why the goroutine count and job count
// below are both large enough to force contention on the same documents.
func TestClaimDueJobs_NoDoubleClaim(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	const numJobs = 50
	const numConcurrentClaimers = 10

	now := time.Now().UTC()
	for i := 0; i < numJobs; i++ {
		_, err := repo.Create(ctx, models.CreateJobRequest{
			IdempotencyKey: idempKey(i),
			TenantID:       "tenant-a",
			JobType:        "noop",
			ScheduledAt:    now.Add(-time.Minute), // already due
			MaxAttempts:    3,
		})
		require.NoError(t, err)
	}

	var (
		mu      sync.Mutex
		claimed = make(map[string]int) // jobID -> number of times it was returned
		wg      sync.WaitGroup
	)

	for i := 0; i < numConcurrentClaimers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			jobs, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), numJobs)
			if err != nil {
				t.Errorf("claim failed: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, j := range jobs {
				claimed[j.ID.Hex()]++
			}
		}()
	}
	wg.Wait()

	require.Len(t, claimed, numJobs, "every job should have been claimed exactly once across all goroutines")
	for id, count := range claimed {
		require.Equal(t, 1, count, "job %s was claimed %d times — atomicity violated", id, count)
	}
}

func TestClaimDueJobs_IgnoresNotYetDue(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	_, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "future-job",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now().Add(time.Hour), // not due yet
		MaxAttempts:    3,
	})
	require.NoError(t, err)

	claimed, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), 10)
	require.NoError(t, err)
	require.Empty(t, claimed, "a job scheduled in the future must not be claimed")
}

func TestClaimDueJobs_RespectsPriorityOrder(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	now := time.Now().Add(-time.Minute)
	low, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "low-priority", TenantID: "t", JobType: "noop",
		ScheduledAt: now, Priority: 1, MaxAttempts: 3,
	})
	require.NoError(t, err)
	high, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "high-priority", TenantID: "t", JobType: "noop",
		ScheduledAt: now, Priority: 9, MaxAttempts: 3,
	})
	require.NoError(t, err)

	claimed, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), 2)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	require.Equal(t, high.ID, claimed[0].ID, "higher priority job should be claimed first")
	require.Equal(t, low.ID, claimed[1].ID)
}

func idempKey(i int) string {
	return fmt.Sprintf("concurrent-claim-%d-%d", time.Now().UnixNano(), i)
}
