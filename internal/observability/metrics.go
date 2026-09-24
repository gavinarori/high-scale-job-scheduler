package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every counter/histogram the system exposes. Each binary
// (api, scheduler, executor, reaper) registers only what it touches — a
// nil-safe struct isn't needed since every binary calls NewMetrics() once
// at startup and passes the whole thing around.
type Metrics struct {
	// Scheduler
	SchedulerTickDuration prometheus.Histogram
	JobsClaimedTotal      prometheus.Counter
	DispatchErrorsTotal   prometheus.Counter
	CronInstancesSpawned  prometheus.Counter
	CronInvalidTotal      prometheus.Counter

	// Executor
	JobsProcessedTotal *prometheus.CounterVec   // labels: jobType, result (success|failure)
	JobDuration        *prometheus.HistogramVec // labels: jobType

	// Reaper
	ReaperRecoveredTotal *prometheus.CounterVec // labels: sweep (queued|execution)
}

// NewMetrics registers every metric against the default Prometheus
// registry. Safe to call once per process; calling it twice in the same
// process will panic on duplicate registration, which is intentional —
// it catches accidental double-init during development.
func NewMetrics() *Metrics {
	return &Metrics{
		SchedulerTickDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "scheduler_tick_duration_seconds",
			Help:    "Time taken to run one scheduler tick (claim + dispatch).",
			Buckets: prometheus.DefBuckets,
		}),
		JobsClaimedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_jobs_claimed_total",
			Help: "Total number of jobs claimed by the scheduler across all ticks.",
		}),
		DispatchErrorsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_dispatch_errors_total",
			Help: "Total number of failed Kafka publish attempts during dispatch.",
		}),
		CronInstancesSpawned: promauto.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_cron_instances_spawned_total",
			Help: "Total number of concrete run-instances spawned from recurring job templates.",
		}),
		CronInvalidTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "scheduler_cron_invalid_total",
			Help: "Total number of recurring templates dead-lettered due to an unparseable cron expression.",
		}),
		JobsProcessedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "executor_jobs_processed_total",
			Help: "Total number of jobs processed by executors, by job type and result.",
		}, []string{"job_type", "result"}),
		JobDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "executor_job_duration_seconds",
			Help:    "Time taken to execute a job's handler, by job type.",
			Buckets: prometheus.DefBuckets,
		}, []string{"job_type"}),
		ReaperRecoveredTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "reaper_recovered_jobs_total",
			Help: "Total number of jobs reset to pending by the reaper, by sweep type.",
		}, []string{"sweep"}),
	}
}

// ServeMetrics starts a /metrics and /healthz HTTP endpoint on addr,
// blocking until the process exits. Call this in a goroutine from main()
// in every binary — the scheduler, executor, and reaper have no other
// HTTP server, and the API service exposes this alongside its own routes
// on a separate port so job traffic and scrape traffic don't share a
// listener.
func ServeMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	http.ListenAndServe(addr, mux)
}
