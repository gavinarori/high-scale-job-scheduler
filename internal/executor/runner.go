package executor

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/arori/job-scheduler/internal/dispatch"
	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
)

const defaultExecutionTimeout = 2 * time.Minute

type Runner struct {
	Repo     *store.JobsRepo
	Registry *Registry
	WorkerID string
	Timeout  time.Duration
	// Producer is optional — nil in Phase 1 (in-process dispatch, no DLQ
	// topic needed). Set it in Phase 2 so exhausted-retry jobs also land
	// on the DLQ Kafka topic for external inspection, in addition to the
	// "dlq" status Mongo already records.
	Producer *dispatch.Producer
}

func NewRunner(repo *store.JobsRepo, registry *Registry, workerID string) *Runner {
	return &Runner{
		Repo:     repo,
		Registry: registry,
		WorkerID: workerID,
		Timeout:  defaultExecutionTimeout,
	}
}

// Execute runs a single job end to end: claim -> run -> mark result.
// Called directly by the Phase 1 scheduler dispatch func; in Phase 2 this
// is called from the Kafka consumer's message handler instead.
func (rn *Runner) Execute(ctx context.Context, job models.Job) {
	if err := rn.Repo.MarkClaimed(ctx, job.ID, rn.WorkerID); err != nil {
		log.Printf("job %s: failed to mark claimed: %v", job.ID.Hex(), err)
		return
	}
	if err := rn.Repo.MarkRunning(ctx, job.ID); err != nil {
		log.Printf("job %s: failed to mark running: %v", job.ID.Hex(), err)
		return
	}

	handler, err := rn.Registry.Get(job.JobType)
	if err != nil {
		rn.fail(ctx, job, err)
		return
	}

	runCtx, cancel := context.WithTimeout(ctx, rn.Timeout)
	defer cancel()

	if err := handler(runCtx, job.Payload); err != nil {
		rn.fail(ctx, job, err)
		return
	}

	if err := rn.Repo.MarkCompleted(ctx, job.ID); err != nil {
		log.Printf("job %s: failed to mark completed: %v", job.ID.Hex(), err)
		return
	}
	log.Printf("job %s (%s): completed", job.ID.Hex(), job.JobType)
}

func (rn *Runner) fail(ctx context.Context, job models.Job, cause error) {
	backoff := exponentialBackoff(job.Attempts)
	exhausted := job.Attempts+1 >= job.MaxAttempts

	if err := rn.Repo.MarkFailed(ctx, job.ID, cause.Error(), backoff); err != nil {
		log.Printf("job %s: failed to mark failed: %v", job.ID.Hex(), err)
		return
	}
	log.Printf("job %s (%s): failed (attempt %d): %v", job.ID.Hex(), job.JobType, job.Attempts+1, cause)

	if exhausted && rn.Producer != nil {
		msg := dispatch.DispatchMessage{JobID: job.ID.Hex(), JobType: job.JobType, Priority: job.Priority}
		if err := rn.Producer.PublishDLQ(ctx, msg); err != nil {
			log.Printf("job %s: failed to publish to DLQ topic: %v", job.ID.Hex(), err)
		}
	}
}

// exponentialBackoff returns 2^attempt seconds, capped at 5 minutes.
func exponentialBackoff(attempt int) time.Duration {
	seconds := math.Min(math.Pow(2, float64(attempt)), 300)
	return time.Duration(seconds) * time.Second
}

// Example handler signature for reference — real implementations live in
// internal/executor/handlers/.
var _ = fmt.Sprintf
