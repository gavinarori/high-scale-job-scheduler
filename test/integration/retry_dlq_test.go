package integration

import (
	"context"
	"testing"
	"time"

	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
	"github.com/stretchr/testify/require"
)

func TestMarkFailed_RetriesUntilMaxAttemptsThenDLQs(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	job, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "retry-then-dlq",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now(),
		MaxAttempts:    3,
	})
	require.NoError(t, err)

	// Attempt 1 fails — should go back to pending with a future scheduledAt.
	require.NoError(t, repo.MarkFailed(ctx, job.ID, "boom 1", 10*time.Second))
	after1, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, models.StatusPending, after1.Status)
	require.Equal(t, 1, after1.Attempts)
	require.True(t, after1.ScheduledAt.After(time.Now()), "backoff should push scheduledAt into the future")

	// Attempt 2 fails — still under maxAttempts, still pending.
	require.NoError(t, repo.MarkFailed(ctx, job.ID, "boom 2", 10*time.Second))
	after2, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, models.StatusPending, after2.Status)
	require.Equal(t, 2, after2.Attempts)

	// Attempt 3 fails — exhausts maxAttempts (3), should route to DLQ.
	require.NoError(t, repo.MarkFailed(ctx, job.ID, "boom 3", 10*time.Second))
	after3, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, models.StatusDLQ, after3.Status, "job should be dead-lettered once attempts reach maxAttempts")
	require.Equal(t, 3, after3.Attempts)
	require.Equal(t, "boom 3", after3.LastError)
}

func TestMarkFailed_DLQJobIsNeverReclaimedByScheduler(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	job, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "dlq-not-reclaimed",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now().Add(-time.Minute),
		MaxAttempts:    1,
	})
	require.NoError(t, err)

	require.NoError(t, repo.MarkFailed(ctx, job.ID, "fatal", 0))
	after, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, models.StatusDLQ, after.Status)

	// A scheduler tick right now must not pick this job back up —
	// ClaimDueJobs only ever matches status "pending".
	claimed, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), 10)
	require.NoError(t, err)
	require.Empty(t, claimed, "DLQ'd jobs must never be reclaimed by the scheduler")
}
