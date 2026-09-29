package load

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arori/job-scheduler/internal/api/handlers"
	"github.com/arori/job-scheduler/internal/store"
)

// newTestAPIServer builds the same routing cmd/api/main.go does, against a
// real (containerized) Mongo, so this benchmark exercises the actual
// handler + repo + index path rather than a stand-in.
func newTestAPIServer(b *testing.B) (*httptest.Server, func()) {
	b.Helper()
	db, cleanup := setupMongo(b)

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	if err := repo.EnsureIndexes(ctx); err != nil {
		b.Fatalf("index creation failed: %v", err)
	}

	jobsHandler := handlers.NewJobsHandler(repo)
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			jobsHandler.CreateJob(w, r)
			return
		}
		jobsHandler.ListJobs(w, r)
	})

	server := httptest.NewServer(mux)
	return server, func() {
		server.Close()
		cleanup()
	}
}

// BenchmarkCreateJob_Concurrent measures POST /jobs throughput at
// increasing client concurrency. This is the number to compare against
// expected production request volume before deciding the API service
// needs more replicas or the unique-idempotency-key index needs
// revisiting (write contention on a single unique index is the most
// likely ceiling here, not CPU).
func BenchmarkCreateJob_Concurrent(b *testing.B) {
	concurrencyLevels := []int{1, 10, 50}

	for _, n := range concurrencyLevels {
		n := n
		b.Run(fmt.Sprintf("concurrency-%d", n), func(b *testing.B) {
			server, cleanup := newTestAPIServer(b)
			defer cleanup()

			const requestsPerWorker = 100
			totalRequests := n * requestsPerWorker

			var (
				successCount int64
				errorCount   int64
				wg           sync.WaitGroup
			)

			client := &http.Client{Timeout: 10 * time.Second}
			runID := time.Now().UnixNano()

			b.ResetTimer()
			start := time.Now()

			for w := 0; w < n; w++ {
				w := w
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < requestsPerWorker; i++ {
						body, _ := json.Marshal(map[string]any{
							"idempotencyKey": fmt.Sprintf("load-%d-%d-%d", runID, w, i),
							"tenantId":       "load-test",
							"jobType":        "noop",
							"scheduledAt":    time.Now().Add(time.Hour),
							"maxAttempts":    3,
						})
						resp, err := client.Post(server.URL+"/jobs", "application/json", bytes.NewReader(body))
						if err != nil {
							atomic.AddInt64(&errorCount, 1)
							continue
						}
						resp.Body.Close()
						if resp.StatusCode == http.StatusCreated {
							atomic.AddInt64(&successCount, 1)
						} else {
							atomic.AddInt64(&errorCount, 1)
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)
			b.StopTimer()

			throughput := float64(successCount) / elapsed.Seconds()
			b.ReportMetric(throughput, "req/sec")
			b.Logf("concurrency=%d: %d/%d succeeded in %s (%.0f req/sec, %d errors)",
				n, successCount, totalRequests, elapsed, throughput, errorCount)

			if errorCount > 0 {
				b.Errorf("concurrency=%d: %d requests failed unexpectedly", n, errorCount)
			}
		})
	}
}
