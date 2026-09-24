package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arori/job-scheduler/internal/config"
	"github.com/arori/job-scheduler/internal/observability"
	"github.com/arori/job-scheduler/internal/store"
)

const sweepInterval = 30 * time.Second

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

	jobsRepo := store.NewJobsRepo(client.Database(cfg.MongoDB))

	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	log.Printf("reaper started (sweep every %s, claim timeout %s)", sweepInterval, cfg.ClaimTimeout)

	for {
		select {
		case <-ctx.Done():
			log.Println("reaper stopping")
			return
		case <-ticker.C:
			n, err := jobsRepo.SweepStuckJobs(ctx, cfg.ClaimTimeout)
			if err != nil {
				log.Printf("execution sweep failed: %v", err)
			} else if n > 0 {
				metrics.ReaperRecoveredTotal.WithLabelValues("execution").Add(float64(n))
				log.Printf("reaper recovered %d stuck claimed/running job(s)", n)
			}

			qn, err := jobsRepo.SweepStuckQueuedJobs(ctx, cfg.QueuedTimeout)
			if err != nil {
				log.Printf("queued sweep failed: %v", err)
			} else if qn > 0 {
				metrics.ReaperRecoveredTotal.WithLabelValues("queued").Add(float64(qn))
				log.Printf("reaper recovered %d stuck queued job(s)", qn)
			}
		}
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
