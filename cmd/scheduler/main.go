package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/arori/job-scheduler/internal/config"
	"github.com/arori/job-scheduler/internal/dispatch"
	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/scheduler"
	"github.com/arori/job-scheduler/internal/store"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := store.Connect(ctx, cfg.MongoURI)
	if err != nil {
		log.Fatalf("mongo connect failed: %v", err)
	}
	defer client.Disconnect(context.Background())

	db := client.Database(cfg.MongoDB)
	jobsRepo := store.NewJobsRepo(db)

	brokers := strings.Split(requireEnv("KAFKA_BROKERS"), ",")
	producer, err := dispatch.NewProducer(brokers)
	if err != nil {
		log.Fatalf("kafka producer init failed: %v", err)
	}
	defer producer.Close()

	// Phase 2: dispatch = publish to Kafka instead of running in-process.
	// The scheduler's only job is now "decide + hand off" — actual
	// execution lives entirely in cmd/executor.
	dispatchFn := func(ctx context.Context, job models.Job) {
		msg := dispatch.DispatchMessage{
			JobID:    job.ID.Hex(),
			JobType:  job.JobType,
			Priority: job.Priority,
		}
		if err := producer.PublishDispatch(ctx, msg); err != nil {
			log.Printf("job %s: failed to publish dispatch message: %v", job.ID.Hex(), err)
			// Note: the job stays "queued" in Mongo on publish failure.
			// The reaper's stuck-job sweep (status=queued past timeout)
			// will reset it to pending for a retry on the next tick.
		}
	}

	loop := scheduler.NewLoop(jobsRepo, cfg.SchedulerTick, 20, dispatchFn)
	loop.Run(ctx)
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}
