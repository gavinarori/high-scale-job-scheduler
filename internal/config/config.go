package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all environment-derived settings for every binary in the
// system (api, scheduler, executor, reaper). Each binary reads only the
// fields it needs.
type Config struct {
	MongoURI       string
	MongoDB        string
	HTTPPort       string
	SchedulerTick  time.Duration
	ClaimTimeout   time.Duration
	QueuedTimeout  time.Duration
	NearTermWindow time.Duration
	WorkerID       string
}

func Load() Config {
	return Config{
		MongoURI:       getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDB:        getEnv("MONGO_DB", "job_scheduler"),
		HTTPPort:       getEnv("HTTP_PORT", "8080"),
		SchedulerTick:  getDuration("SCHEDULER_TICK", 500*time.Millisecond),
		ClaimTimeout:   getDuration("CLAIM_TIMEOUT", 5*time.Minute),
		QueuedTimeout:  getDuration("QUEUED_TIMEOUT", 30*time.Second),
		NearTermWindow: getDuration("NEAR_TERM_WINDOW", 2*time.Minute),
		WorkerID:       getEnv("WORKER_ID", "worker-"+randSuffix()),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func randSuffix() string {
	return strconv.FormatInt(time.Now().UnixNano()%100000, 36)
}
