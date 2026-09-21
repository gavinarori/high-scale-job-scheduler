package dispatch

import "fmt"

// DispatchTopic returns the topic name for a given job type. Partitioning
// by job type keeps each job type's parallelism independently scalable —
// a slow job type's backlog doesn't starve a fast one's partitions.
func DispatchTopic(jobType string) string {
	return fmt.Sprintf("jobs.dispatch.%s", jobType)
}

// RetryTopic holds jobs that failed but have retries remaining. A separate
// consumer (or the same executor with a delay-aware handler) reads this
// topic and reinserts jobs into Mongo as "pending" once their backoff
// window has elapsed — see internal/dispatch/consumer.go.
const RetryTopic = "jobs.retry"

// DLQTopic holds jobs that exhausted all retry attempts. Nothing consumes
// this automatically in Phase 2 — it's for manual inspection or a future
// alerting/reprocessing tool.
const DLQTopic = "jobs.dlq"

// DispatchMessage is the small payload sent over Kafka. It intentionally
// carries only the job ID plus enough metadata for routing/prioritization —
// the executor fetches the full payload from Mongo by ID. This keeps
// Kafka messages tiny regardless of how large a job's payload is.
type DispatchMessage struct {
	JobID    string `json:"jobId"`
	JobType  string `json:"jobType"`
	Priority int    `json:"priority"`
}
