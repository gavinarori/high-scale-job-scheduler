package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// JobStatus represents the lifecycle state of a job.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusQueued    JobStatus = "queued"
	StatusClaimed   JobStatus = "claimed"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusDLQ       JobStatus = "dlq"
)

// Job is the persisted representation of a scheduled unit of work.
type Job struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	IdempotencyKey string             `bson:"idempotencyKey" json:"idempotencyKey"`
	TenantID       string             `bson:"tenantId" json:"tenantId"`
	JobType        string             `bson:"jobType" json:"jobType"`
	Payload        map[string]any     `bson:"payload" json:"payload"`
	Priority       int                `bson:"priority" json:"priority"`
	Status         JobStatus          `bson:"status" json:"status"`
	ScheduledAt    time.Time          `bson:"scheduledAt" json:"scheduledAt"`
	Cron           string             `bson:"cron,omitempty" json:"cron,omitempty"`
	ClaimedBy      string             `bson:"claimedBy,omitempty" json:"claimedBy,omitempty"`
	ClaimedAt      *time.Time         `bson:"claimedAt,omitempty" json:"claimedAt,omitempty"`
	Attempts       int                `bson:"attempts" json:"attempts"`
	MaxAttempts    int                `bson:"maxAttempts" json:"maxAttempts"`
	LastError      string             `bson:"lastError,omitempty" json:"lastError,omitempty"`
	CreatedAt      time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt      time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// CreateJobRequest is the client-facing payload for job creation.
type CreateJobRequest struct {
	IdempotencyKey string         `json:"idempotencyKey"`
	TenantID       string         `json:"tenantId"`
	JobType        string         `json:"jobType"`
	Payload        map[string]any `json:"payload"`
	Priority       int            `json:"priority"`
	ScheduledAt    time.Time      `json:"scheduledAt"`
	Cron           string         `json:"cron,omitempty"`
	MaxAttempts    int            `json:"maxAttempts"`
}

// ExecutionRecord captures one attempt at running a job.
type ExecutionRecord struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	JobID      primitive.ObjectID `bson:"jobId" json:"jobId"`
	Attempt    int                `bson:"attempt" json:"attempt"`
	StartedAt  time.Time          `bson:"startedAt" json:"startedAt"`
	FinishedAt time.Time          `bson:"finishedAt" json:"finishedAt"`
	Status     string             `bson:"status" json:"status"` // "success" | "failure"
	Error      string             `bson:"error,omitempty" json:"error,omitempty"`
	WorkerID   string             `bson:"workerId" json:"workerId"`
}
