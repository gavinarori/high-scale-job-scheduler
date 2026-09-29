// Command loadtest drives load against a *running* job-scheduler stack
// over HTTP and measures what the Go benchmarks in test/load cannot: real
// end-to-end latency from job creation through scheduler dispatch, Kafka
// delivery, and executor completion. Requires the full stack to be up
// (docker compose up), not just Mongo.
//
// This complements, not replaces, test/load's benchmarks:
//   - test/load measures the claim query and API handler in isolation,
//     with assertions on correctness under contention.
//   - This tool measures the whole pipeline's throughput and latency as
//     experienced by a client, the way a real caller would.
//
// Usage:
//
//	go run ./cmd/loadtest -rate 20 -duration 30s
//	go run ./cmd/loadtest -api http://localhost:8080 -rate 50 -duration 1m -jobtype slow
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type createdJob struct {
	ID        string
	CreatedAt time.Time
}

type jobRecord struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func main() {
	apiURL := flag.String("api", "http://localhost:8080", "base URL of the API service")
	rate := flag.Float64("rate", 20, "jobs created per second during the send phase")
	duration := flag.Duration("duration", 30*time.Second, "how long to send jobs at the target rate")
	jobType := flag.String("jobtype", "log-message", "job type to create (must have a running executor consuming it)")
	tenant := flag.String("tenant", fmt.Sprintf("loadtest-%d", time.Now().Unix()), "tenant ID to tag every created job with, so this run's jobs are queryable in isolation")
	maxWait := flag.Duration("maxwait", 2*time.Minute, "how long to poll for completion after the send phase ends before giving up")
	concurrency := flag.Int("concurrency", 20, "max in-flight HTTP requests during the send phase")
	flag.Parse()

	client := &http.Client{Timeout: 10 * time.Second}

	fmt.Printf("=== Send phase: %.1f jobs/sec for %s (jobType=%s, tenant=%s) ===\n", *rate, *duration, *jobType, *tenant)
	created, sendLatencies, errCount := sendPhase(client, *apiURL, *jobType, *tenant, *rate, *duration, *concurrency)

	fmt.Printf("\nSent %d jobs (%d errors)\n", len(created), errCount)
	printLatencyStats("Send (HTTP ack) latency", sendLatencies)

	if len(created) == 0 {
		log.Fatal("no jobs were successfully created — check -api is reachable and the stack is running")
	}

	fmt.Printf("\n=== Completion phase: polling until all %d jobs finish or %s elapses ===\n", len(created), *maxWait)
	completionPhase(client, *apiURL, *tenant, created, *maxWait)
}

// sendPhase fires jobs at the target rate, bounded by a concurrency
// semaphore so a slow API doesn't cause unbounded goroutine growth if the
// requested rate outpaces what the server can actually accept.
func sendPhase(client *http.Client, apiURL, jobType, tenant string, rate float64, duration time.Duration, concurrency int) ([]createdJob, []time.Duration, int64) {
	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.Now().Add(duration)
	sem := make(chan struct{}, concurrency)

	var (
		mu        sync.Mutex
		created   []createdJob
		latencies []time.Duration
		errCount  int64
		wg        sync.WaitGroup
		seq       int
	)

	for time.Now().Before(deadline) {
		<-ticker.C
		sem <- struct{}{}
		wg.Add(1)
		seq++
		i := seq
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			body, _ := json.Marshal(map[string]any{
				"idempotencyKey": fmt.Sprintf("loadtest-%s-%d", tenant, i),
				"tenantId":       tenant,
				"jobType":        jobType,
				"payload":        map[string]any{"message": fmt.Sprintf("loadtest job %d", i), "sleepMs": 300.0},
				"scheduledAt":    time.Now(),
				"maxAttempts":    3,
			})

			start := time.Now()
			resp, err := client.Post(apiURL+"/jobs", "application/json", bytes.NewReader(body))
			latency := time.Since(start)

			if err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusCreated {
				atomic.AddInt64(&errCount, 1)
				return
			}

			var rec jobRecord
			if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}

			mu.Lock()
			created = append(created, createdJob{ID: rec.ID, CreatedAt: start})
			latencies = append(latencies, latency)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return created, latencies, errCount
}

// completionPhase polls each created job by ID until it reaches a terminal
// status (completed or dlq) or maxWait elapses. Polling by individual ID
// rather than listing by status keeps this correct regardless of how many
// *other* jobs (seed data, previous runs) share the same tenant.
func completionPhase(client *http.Client, apiURL, tenant string, created []createdJob, maxWait time.Duration) {
	type outcome struct {
		status  string
		e2eTime time.Duration
	}
	results := make(map[string]outcome)

	deadline := time.Now().Add(maxWait)
	pollInterval := 2 * time.Second

	for time.Now().Before(deadline) && len(results) < len(created) {
		time.Sleep(pollInterval)

		var wg sync.WaitGroup
		var mu sync.Mutex
		sem := make(chan struct{}, 30)

		for _, job := range created {
			if _, done := results[job.ID]; done {
				continue
			}
			job := job
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()

				resp, err := client.Get(apiURL + "/jobs/" + job.ID)
				if err != nil {
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					return
				}
				var rec jobRecord
				if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
					return
				}
				if rec.Status == "completed" || rec.Status == "dlq" {
					mu.Lock()
					results[job.ID] = outcome{status: rec.Status, e2eTime: time.Since(job.CreatedAt)}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		fmt.Printf("  ...%d/%d jobs reached a terminal status\n", len(results), len(created))
	}

	var (
		completedLatencies []time.Duration
		dlqCount           int
	)
	for _, o := range results {
		if o.status == "completed" {
			completedLatencies = append(completedLatencies, o.e2eTime)
		} else {
			dlqCount++
		}
	}
	leftover := len(created) - len(results)

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Completed: %d\nDLQ'd:     %d\nStill pending/running (timed out waiting): %d\n",
		len(completedLatencies), dlqCount, leftover)
	printLatencyStats("End-to-end latency (creation -> completion)", completedLatencies)

	if leftover > 0 {
		fmt.Printf("\n%d jobs didn't finish within -maxwait — check executor logs and Kafka consumer lag before assuming this is a scheduler problem; it may just need a longer -maxwait for slow-job payloads.\n", leftover)
	}
}

func printLatencyStats(label string, latencies []time.Duration) {
	if len(latencies) == 0 {
		fmt.Printf("%s: no data\n", label)
		return
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	pct := func(p float64) time.Duration {
		idx := int(p * float64(len(sorted)))
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	avg := sum / time.Duration(len(sorted))

	fmt.Printf("%s (n=%d): min=%s avg=%s p50=%s p95=%s p99=%s max=%s\n",
		label, len(sorted), sorted[0], avg, pct(0.50), pct(0.95), pct(0.99), sorted[len(sorted)-1])
}
