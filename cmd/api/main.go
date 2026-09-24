package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/arori/job-scheduler/internal/api/handlers"
	"github.com/arori/job-scheduler/internal/config"
	"github.com/arori/job-scheduler/internal/observability"
	"github.com/arori/job-scheduler/internal/store"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	// Metrics on their own port/listener — job traffic and Prometheus
	// scrape traffic shouldn't share a mux, so an unusual scrape pattern
	// or an outage in one can't affect the other.
	go observability.ServeMetrics(":" + getEnv("METRICS_PORT", "9100"))

	client, err := store.Connect(ctx, cfg.MongoURI)
	if err != nil {
		log.Fatalf("mongo connect failed: %v", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database(cfg.MongoDB)
	jobsRepo := store.NewJobsRepo(db)

	if err := jobsRepo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("index creation failed: %v", err)
	}

	jobsHandler := handlers.NewJobsHandler(jobsRepo)

	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			jobsHandler.CreateJob(w, r)
		case http.MethodGet:
			jobsHandler.ListJobs(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/jobs/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/jobs/")
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		jobsHandler.GetJob(w, r, id)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	addr := ":" + cfg.HTTPPort
	log.Printf("api service listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
