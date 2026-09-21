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
	"github.com/arori/job-scheduler/internal/executor"
	exhandlers "github.com/arori/job-scheduler/internal/executor/handlers"
	"github.com/arori/job-scheduler/internal/store"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// JOB_TYPE and KAFKA_BROKERS select which dispatch topic this executor
// instance's consumer group joins. Run one deployment per job type (see
// deployments/k8s/executor-deployment.yaml) so each type scales on its
// own topic's lag independently.
func main() {
	cfg := config.Load()
	jobType := requireEnv("JOB_TYPE")
	brokers := strings.Split(requireEnv("KAFKA_BROKERS"), ",")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := store.Connect(ctx, cfg.MongoURI)
	if err != nil {
		log.Fatalf("mongo connect failed: %v", err)
	}
	defer client.Disconnect(context.Background())

	jobsRepo := store.NewJobsRepo(client.Database(cfg.MongoDB))

	registry := executor.NewRegistry()
	exhandlers.RegisterAll(registry)

	producer, err := dispatch.NewProducer(brokers)
	if err != nil {
		log.Fatalf("kafka producer init failed: %v", err)
	}
	defer producer.Close()

	runner := executor.NewRunner(jobsRepo, registry, cfg.WorkerID)
	runner.Producer = producer

	handler := func(ctx context.Context, msg dispatch.DispatchMessage) error {
		id, err := primitive.ObjectIDFromHex(msg.JobID)
		if err != nil {
			return err
		}
		job, err := jobsRepo.GetByID(ctx, id)
		if err != nil {
			// Job may have already been reaped/reprocessed by another
			// consumer if this message is a redelivery — not fatal.
			log.Printf("job %s not found, skipping: %v", msg.JobID, err)
			return nil
		}
		runner.Execute(ctx, *job)
		return nil
	}

	consumer, err := dispatch.NewConsumer(brokers, jobType, "executor-"+jobType, handler)
	if err != nil {
		log.Fatalf("kafka consumer init failed: %v", err)
	}

	log.Printf("executor started for jobType=%s worker=%s", jobType, cfg.WorkerID)
	consumer.Run(ctx)
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}
