// Command seed populates the jobs collection with a realistic mix of
// one-off jobs (immediate and future-scheduled), recurring cron templates,
// and intentionally flaky/slow jobs — so a freshly started stack has
// something to look at immediately: dashboards show non-zero metrics,
// the DLQ has entries, retries are visible, without needing to hand-craft
// curl commands for every scenario.
//
// Writes directly via the repo layer (bypassing the API/Kafka) since
// seeding thousands of jobs through HTTP would be needlessly slow for a
// setup step — this is not a substitute for test/load, which specifically
// exercises the API and dispatch path under load.
//
// Usage:
//
//	go run ./cmd/seed
//	go run ./cmd/seed -count 500 -tenants 5 -flaky-rate 0.3
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/arori/job-scheduler/internal/config"
	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
)

func main() {
	count := flag.Int("count", 200, "number of one-off jobs to create")
	tenants := flag.Int("tenants", 3, "number of distinct tenant IDs to spread jobs across")
	cronCount := flag.Int("cron", 3, "number of recurring (cron) job templates to create")
	flakyRate := flag.Float64("flaky-rate", 0.4, "fraction of one-off jobs that use the flaky handler (to populate retries/DLQ)")
	flag.Parse()

	cfg := config.Load()
	ctx := context.Background()

	client, err := store.Connect(ctx, cfg.MongoURI)
	if err != nil {
		log.Fatalf("mongo connect failed: %v", err)
	}
	defer client.Disconnect(ctx)

	repo := store.NewJobsRepo(client.Database(cfg.MongoDB))
	if err := repo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("index creation failed: %v", err)
	}

	// Go 1.20+ auto-seeds the global math/rand source; no explicit Seed call needed.
	runID := time.Now().Unix()

	tenantIDs := make([]string, *tenants)
	for i := range tenantIDs {
		tenantIDs[i] = fmt.Sprintf("tenant-%d", i)
	}

	var (
		created      int
		immediate    int
		future       int
		flakyCreated int
	)

	for i := 0; i < *count; i++ {
		tenant := tenantIDs[i%len(tenantIDs)]
		isFlaky := rand.Float64() < *flakyRate
		isFuture := rand.Float64() < 0.25 // 25% scheduled minutes/hours out

		var jobType string
		var payload map[string]any
		switch {
		case isFlaky:
			jobType = "flaky"
			payload = map[string]any{"failRate": 0.6}
			flakyCreated++
		case i%3 == 0:
			jobType = "slow"
			payload = map[string]any{"sleepMs": float64(200 + rand.Intn(2000))}
		default:
			jobType = "log-message"
			payload = map[string]any{"message": fmt.Sprintf("seeded job #%d for %s", i, tenant)}
		}

		var scheduledAt time.Time
		if isFuture {
			scheduledAt = time.Now().Add(time.Duration(1+rand.Intn(60)) * time.Minute)
			future++
		} else {
			scheduledAt = time.Now().Add(-time.Duration(rand.Intn(30)) * time.Second) // already due, staggered
			immediate++
		}

		_, err := repo.Create(ctx, models.CreateJobRequest{
			IdempotencyKey: fmt.Sprintf("seed-%d-%d", runID, i),
			TenantID:       tenant,
			JobType:        jobType,
			Payload:        payload,
			Priority:       rand.Intn(10),
			ScheduledAt:    scheduledAt,
			MaxAttempts:    3,
		})
		if err != nil {
			log.Printf("failed to create job %d: %v", i, err)
			continue
		}
		created++
	}

	// A handful of recurring templates across a spread of intervals, so
	// the scheduler's cron path (SpawnCronInstance/RescheduleCronTemplate)
	// is exercised continuously rather than only in integration tests.
	cronExprs := []string{"* * * * *", "*/5 * * * *", "0 * * * *"} // every min, every 5 min, hourly
	cronCreated := 0
	for i := 0; i < *cronCount; i++ {
		tenant := tenantIDs[i%len(tenantIDs)]
		expr := cronExprs[i%len(cronExprs)]

		_, err := repo.Create(ctx, models.CreateJobRequest{
			IdempotencyKey: fmt.Sprintf("seed-cron-%d-%d", runID, i),
			TenantID:       tenant,
			JobType:        "log-message",
			Payload:        map[string]any{"message": fmt.Sprintf("recurring job #%d (%s) for %s", i, expr, tenant)},
			Priority:       5,
			ScheduledAt:    time.Now(), // overwritten below to the expression's own first occurrence
			Cron:           expr,
			MaxAttempts:    3,
		})
		if err != nil {
			log.Printf("failed to create cron template %d: %v", i, err)
			continue
		}
		cronCreated++
	}

	log.Printf("seed complete: %d one-off jobs created (%d immediate, %d future-scheduled, %d flaky), %d recurring templates, across %d tenants",
		created, immediate, future, flakyCreated, cronCreated, *tenants)
	log.Printf("watch it work: docker compose logs -f scheduler-a scheduler-b executor-log-message executor-noop")
}
