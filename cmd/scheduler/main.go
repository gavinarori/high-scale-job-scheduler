package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/arori/job-scheduler/internal/config"
	"github.com/arori/job-scheduler/internal/dispatch"
	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/observability"
	"github.com/arori/job-scheduler/internal/scheduler"
	"github.com/arori/job-scheduler/internal/store"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics := observability.NewMetrics()
	go observability.ServeMetrics(":" + getEnv("METRICS_PORT", "9100"))

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
			metrics.DispatchErrorsTotal.Inc()
			log.Printf("job %s: failed to publish dispatch message: %v", job.ID.Hex(), err)
			// Note: the job stays "queued" in Mongo on publish failure.
			// The reaper's stuck-job sweep (status=queued past timeout)
			// will reset it to pending for a retry on the next tick.
		}
	}

	loop := scheduler.NewLoop(jobsRepo, cfg.SchedulerTick, 20, dispatchFn, metrics)

	etcdEndpoints := strings.Split(requireEnv("ETCD_ENDPOINTS"), ",")
	elector, err := scheduler.NewLeaderElector(etcdEndpoints, cfg.WorkerID, 10)
	if err != nil {
		log.Fatalf("etcd leader elector init failed: %v", err)
	}
	defer elector.Close()

	// Multiple scheduler instances can run this binary safely: only the
	// one holding the etcd election lock actually ticks the dispatch loop.
	// Standbys sit in Campaign() (inside RunAsLeader) doing nothing until
	// the current leader's session dies, at which point one of them takes
	// over — this is what removes the single-scheduler-instance
	// constraint from Phase 1/2.
	for {
		if ctx.Err() != nil {
			return
		}
		err := elector.RunAsLeader(ctx, func(leaderCtx context.Context) {
			loop.Run(leaderCtx)
		})
		if err != nil && ctx.Err() == nil {
			log.Printf("leader election error, retrying: %v", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
