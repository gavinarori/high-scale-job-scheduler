package integration

import (
	"context"
	"testing"
	"time"

	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/scheduler"
	"github.com/arori/job-scheduler/internal/store"
	"github.com/stretchr/testify/require"
)

func TestNextOccurrence_ParsesStandardCron(t *testing.T) {
	// Every day at 09:00.
	base := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	next, err := scheduler.NextOccurrence("0 9 * * *", base)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC), next)
}

func TestNextOccurrence_RejectsInvalidExpression(t *testing.T) {
	_, err := scheduler.NextOccurrence("not a cron expr", time.Now())
	require.Error(t, err)
}

func TestSpawnCronInstance_CreatesRunWithoutCronField(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	template, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "nightly-report",
		TenantID:       "tenant-a",
		JobType:        "log-message",
		Payload:        map[string]any{"message": "nightly"},
		ScheduledAt:    time.Now(),
		Cron:           "0 2 * * *",
		MaxAttempts:    3,
	})
	require.NoError(t, err)

	instance, err := repo.SpawnCronInstance(ctx, *template)
	require.NoError(t, err)

	require.NotEqual(t, template.ID, instance.ID, "instance must be a separate document from the template")
	require.Empty(t, instance.Cron, "spawned instance must not itself be recurring")
	require.Equal(t, models.StatusQueued, instance.Status, "instance should be created ready-to-dispatch")
	require.Equal(t, template.JobType, instance.JobType)
	require.Equal(t, template.Payload["message"], instance.Payload["message"])
	require.NotEqual(t, template.IdempotencyKey, instance.IdempotencyKey,
		"each run needs its own idempotency key, distinct from the template's")
}

func TestRescheduleCronTemplate_MovesTemplateToNextOccurrenceAsPending(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	template, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "hourly-sync",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now().Add(-time.Minute),
		Cron:           "0 * * * *",
		MaxAttempts:    3,
	})
	require.NoError(t, err)

	// Simulate ClaimDueJobs having already flipped it to queued.
	claimed, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	next := time.Now().Add(time.Hour).UTC()
	require.NoError(t, repo.RescheduleCronTemplate(ctx, template.ID, next))

	after, err := repo.GetByID(ctx, template.ID)
	require.NoError(t, err)
	require.Equal(t, models.StatusPending, after.Status, "template should return to pending, ready for its next occurrence")
	require.WithinDuration(t, next, after.ScheduledAt, time.Second)
}

func TestCronTemplate_IsNeverDoubleFiredWithinOneTick(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	_, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "every-minute-job",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now().Add(-time.Minute),
		Cron:           "* * * * *",
		MaxAttempts:    3,
	})
	require.NoError(t, err)

	// First claim picks up the template (flips pending -> queued).
	firstClaim, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), 10)
	require.NoError(t, err)
	require.Len(t, firstClaim, 1)

	// A second claim attempt in the same tick window must find nothing —
	// the template is "queued", not "pending", until it's explicitly
	// rescheduled by handleCronJob. This is what prevents the same minute
	// from firing the job twice if the tick interval is shorter than a
	// minute.
	secondClaim, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), 10)
	require.NoError(t, err)
	require.Empty(t, secondClaim, "a queued cron template must not be claimable again before it's rescheduled")
}
