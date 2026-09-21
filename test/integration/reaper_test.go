package integration

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
	"github.com/stretchr/testify/require"
)

func TestSweepStuckJobs_RecoversTimedOutClaimedJob(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	job, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "stuck-claimed",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now(),
		MaxAttempts:    3,
	})
	require.NoError(t, err)

	// Simulate a claim that happened well in the past (executor crashed
	// mid-job) by backdating updatedAt directly — MarkClaimed always sets
	// it to "now", so we bypass it here to construct the stale state.
	backdated := time.Now().Add(-10 * time.Minute)
	_, err = db.Collection("jobs").UpdateOne(ctx,
		bson.M{"_id": job.ID},
		bson.M{"$set": bson.M{
			"status":    models.StatusRunning,
			"claimedAt": backdated,
			"updatedAt": backdated,
		}},
	)
	require.NoError(t, err)

	n, err := repo.SweepStuckJobs(ctx, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	after, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, models.StatusPending, after.Status, "stuck running job should be reset to pending")
}

func TestSweepStuckJobs_DoesNotTouchRecentlyClaimedJobs(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	job, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "legit-in-flight",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now(),
		MaxAttempts:    3,
	})
	require.NoError(t, err)
	require.NoError(t, repo.MarkClaimed(ctx, job.ID, "worker-1"))
	require.NoError(t, repo.MarkRunning(ctx, job.ID))

	// This is the case that actually matters: a job that is genuinely
	// still executing (claimed seconds ago) must survive a sweep with a
	// long timeout — otherwise the reaper would race a slow-but-healthy
	// executor and duplicate work by resetting a job that's about to
	// complete normally.
	n, err := repo.SweepStuckJobs(ctx, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, int64(0), n, "a job claimed moments ago must not be swept")

	after, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, models.StatusRunning, after.Status)
}

func TestSweepStuckQueuedJobs_RecoversPublishFailure(t *testing.T) {
	db, cleanup := setupMongo(t)
	defer cleanup()

	ctx := context.Background()
	repo := store.NewJobsRepo(db)
	require.NoError(t, repo.EnsureIndexes(ctx))

	job, err := repo.Create(ctx, models.CreateJobRequest{
		IdempotencyKey: "stuck-queued",
		TenantID:       "tenant-a",
		JobType:        "noop",
		ScheduledAt:    time.Now().Add(-time.Minute),
		MaxAttempts:    3,
	})
	require.NoError(t, err)

	// ClaimDueJobs flips it to queued (simulating the scheduler claiming
	// it, then hypothetically failing to publish to Kafka).
	claimed, err := repo.ClaimDueJobs(ctx, time.Now().UTC(), 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	// Backdate updatedAt to simulate the publish failure being 1 minute
	// old — well past the short queued-timeout.
	backdated := time.Now().Add(-time.Minute)
	_, err = db.Collection("jobs").UpdateOne(ctx,
		bson.M{"_id": job.ID},
		bson.M{"$set": bson.M{"updatedAt": backdated}},
	)
	require.NoError(t, err)

	n, err := repo.SweepStuckQueuedJobs(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	after, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, models.StatusPending, after.Status,
		"a job stuck in queued past the short timeout should return to pending for redispatch")
}
