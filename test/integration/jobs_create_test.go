package integration

import (
	"context"
	"testing"
	"time"

	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
	"github.com/stretchr/testify/require"
)

func TestCreate_EnforcesIdempotencyKey(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	req := models.CreateJobRequest{
		IdempotencyKey: "dup-key-1",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now(),
		MaxAttempts:    3,
	}

	first, err := repo.Create(ctx, req)
	require.NoError(t, err)
	require.Equal(t, models.StatusPending, first.Status)

	_, err = repo.Create(ctx, req)
	require.ErrorIs(t, err, store.ErrDuplicateIdempotencyKey,
		"second create with the same idempotency key must be rejected, not silently duplicated")
}

func TestCreate_DefaultsMaxAttempts(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	job, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "no-max-attempts",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now(),
		// MaxAttempts intentionally omitted
	})
	require.NoError(t, err)
	require.Equal(t, 3, job.MaxAttempts, "should default to 3 attempts when unset")
}
