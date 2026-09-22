package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arori/job-scheduler/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ErrNotFound = errors.New("job not found")
var ErrDuplicateIdempotencyKey = errors.New("job with this idempotency key already exists")

type JobsRepo struct {
	coll *mongo.Collection
}

func NewJobsRepo(db *mongo.Database) *JobsRepo {
	return &JobsRepo{coll: db.Collection("jobs")}
}

// EnsureIndexes creates the indexes the scheduler and API depend on.
// Safe to call on every startup — Mongo no-ops on existing indexes.
func (r *JobsRepo) EnsureIndexes(ctx context.Context) error {
	_, err := r.coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "status", Value: 1}, {Key: "scheduledAt", Value: 1}},
		},
		{
			Keys:    bson.D{{Key: "idempotencyKey", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "tenantId", Value: 1}, {Key: "status", Value: 1}},
		},
	})
	return err
}

// Create inserts a new job in "pending" status. Returns ErrDuplicateIdempotencyKey
// if a job with the same idempotency key already exists.
func (r *JobsRepo) Create(ctx context.Context, req models.CreateJobRequest) (*models.Job, error) {
	now := time.Now().UTC()
	maxAttempts := req.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	job := models.Job{
		IdempotencyKey: req.IdempotencyKey,
		TenantID:       req.TenantID,
		JobType:        req.JobType,
		Payload:        req.Payload,
		Priority:       req.Priority,
		Status:         models.StatusPending,
		ScheduledAt:    req.ScheduledAt,
		Cron:           req.Cron,
		Attempts:       0,
		MaxAttempts:    maxAttempts,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	res, err := r.coll.InsertOne(ctx, job)
	if mongo.IsDuplicateKeyError(err) {
		return nil, ErrDuplicateIdempotencyKey
	}
	if err != nil {
		return nil, err
	}
	job.ID = res.InsertedID.(primitive.ObjectID)
	return &job, nil
}

// ClaimDueJobs atomically flips up to `limit` due jobs from "pending" to
// "queued", returning the claimed jobs. This is the core concurrency-safe
// operation the scheduler loop calls every tick — findOneAndUpdate is
// single-document atomic in Mongo, so no two scheduler instances (even
// mid-election-handoff) can double-claim the same job.
func (r *JobsRepo) ClaimDueJobs(ctx context.Context, now time.Time, limit int) ([]models.Job, error) {
	var claimed []models.Job

	for i := 0; i < limit; i++ {
		filter := bson.M{
			"status":      models.StatusPending,
			"scheduledAt": bson.M{"$lte": now},
		}
		update := bson.M{
			"$set": bson.M{
				"status":    models.StatusQueued,
				"updatedAt": time.Now().UTC(),
			},
		}
		opts := options.FindOneAndUpdate().
			SetReturnDocument(options.After).
			SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "scheduledAt", Value: 1}})

		var job models.Job
		err := r.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&job)
		if errors.Is(err, mongo.ErrNoDocuments) {
			break // nothing left due right now
		}
		if err != nil {
			return claimed, err
		}
		claimed = append(claimed, job)
	}
	return claimed, nil
}

// MarkClaimed transitions a queued job to claimed by a specific worker,
// called by the executor right before it starts running the job.
func (r *JobsRepo) MarkClaimed(ctx context.Context, id primitive.ObjectID, workerID string) error {
	now := time.Now().UTC()
	filter := bson.M{"_id": id, "status": models.StatusQueued}
	update := bson.M{"$set": bson.M{
		"status":    models.StatusClaimed,
		"claimedBy": workerID,
		"claimedAt": now,
		"updatedAt": now,
	}}
	res, err := r.coll.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkRunning transitions a claimed job to running.
func (r *JobsRepo) MarkRunning(ctx context.Context, id primitive.ObjectID) error {
	_, err := r.coll.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"status": models.StatusRunning, "updatedAt": time.Now().UTC()}},
	)
	return err
}

// MarkCompleted finalizes a successful job.
func (r *JobsRepo) MarkCompleted(ctx context.Context, id primitive.ObjectID) error {
	_, err := r.coll.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"status": models.StatusCompleted, "updatedAt": time.Now().UTC()}},
	)
	return err
}

// MarkFailed increments the attempt count and either re-queues the job for
// retry (status -> pending, scheduledAt pushed out by backoff) or routes it
// to the DLQ if attempts are exhausted.
func (r *JobsRepo) MarkFailed(ctx context.Context, id primitive.ObjectID, errMsg string, backoff time.Duration) error {
	var job models.Job
	if err := r.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&job); err != nil {
		return err
	}

	newAttempts := job.Attempts + 1
	now := time.Now().UTC()
	update := bson.M{
		"attempts":  newAttempts,
		"lastError": errMsg,
		"updatedAt": now,
	}

	if newAttempts >= job.MaxAttempts {
		update["status"] = models.StatusDLQ
	} else {
		update["status"] = models.StatusPending
		update["scheduledAt"] = now.Add(backoff)
	}

	_, err := r.coll.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": update})
	return err
}

// SweepStuckJobs resets jobs stuck in "claimed" or "running" past the given
// execution timeout back to "pending" for redispatch — covers executors
// that crashed or hung mid-job. Called periodically by the reaper.
func (r *JobsRepo) SweepStuckJobs(ctx context.Context, timeout time.Duration) (int64, error) {
	return r.sweepByStatus(ctx, timeout, models.StatusClaimed, models.StatusRunning)
}

// SweepStuckQueuedJobs resets jobs stuck in "queued" past a short timeout
// back to "pending". A job lands here if the scheduler claimed it but then
// failed to publish the dispatch message to Kafka — this should recover
// much faster than an execution hang, hence the separate, shorter timeout.
func (r *JobsRepo) SweepStuckQueuedJobs(ctx context.Context, timeout time.Duration) (int64, error) {
	return r.sweepByStatus(ctx, timeout, models.StatusQueued)
}

// SpawnCronInstance creates a concrete, one-off run of a recurring job
// template. The instance carries no cron field — it's a normal job from
// here on, going through the same dispatch/execute/retry path as anything
// created via the API. Created directly in "queued" status since it's
// meant to fire immediately (the scheduler loop calls this only once the
// template's own scheduledAt is already due).
func (r *JobsRepo) SpawnCronInstance(ctx context.Context, template models.Job) (*models.Job, error) {
	now := time.Now().UTC()
	instance := models.Job{
		IdempotencyKey: fmt.Sprintf("%s-run-%d", template.IdempotencyKey, now.UnixNano()),
		TenantID:       template.TenantID,
		JobType:        template.JobType,
		Payload:        template.Payload,
		Priority:       template.Priority,
		Status:         models.StatusQueued,
		ScheduledAt:    now,
		Attempts:       0,
		MaxAttempts:    template.MaxAttempts,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	res, err := r.coll.InsertOne(ctx, instance)
	if err != nil {
		return nil, fmt.Errorf("spawn cron instance: %w", err)
	}
	instance.ID = res.InsertedID.(primitive.ObjectID)
	return &instance, nil
}

// RescheduleCronTemplate moves a recurring job's template back to "pending"
// at its next occurrence. Called right after SpawnCronInstance so the
// template becomes eligible for claiming again at the right future time —
// the template itself is never dispatched or executed, only its spawned
// instances are.
func (r *JobsRepo) RescheduleCronTemplate(ctx context.Context, id primitive.ObjectID, next time.Time) error {
	_, err := r.coll.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{
			"status":      models.StatusPending,
			"scheduledAt": next,
			"updatedAt":   time.Now().UTC(),
		}},
	)
	return err
}

// MarkCronInvalid dead-letters a recurring template whose cron expression
// can no longer be parsed (e.g. hand-edited directly in Mongo). Without
// this, a bad expression would leave the template permanently stuck in
// "queued" — claimed but never rescheduled — silently halting that
// recurring job forever with no visible error.
func (r *JobsRepo) MarkCronInvalid(ctx context.Context, id primitive.ObjectID, reason string) error {
	_, err := r.coll.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{
			"status":    models.StatusDLQ,
			"lastError": reason,
			"updatedAt": time.Now().UTC(),
		}},
	)
	return err
}

func (r *JobsRepo) sweepByStatus(ctx context.Context, timeout time.Duration, statuses ...models.JobStatus) (int64, error) {
	cutoff := time.Now().UTC().Add(-timeout)
	filter := bson.M{
		"status":    bson.M{"$in": statuses},
		"updatedAt": bson.M{"$lte": cutoff},
	}
	update := bson.M{"$set": bson.M{
		"status":    models.StatusPending,
		"updatedAt": time.Now().UTC(),
	}}
	res, err := r.coll.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, err
	}
	return res.ModifiedCount, nil
}

// GetByID fetches a single job by its ID.
func (r *JobsRepo) GetByID(ctx context.Context, id primitive.ObjectID) (*models.Job, error) {
	var job models.Job
	err := r.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&job)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// ListByTenant returns jobs for a tenant, optionally filtered by status.
func (r *JobsRepo) ListByTenant(ctx context.Context, tenantID string, status models.JobStatus, limit int64) ([]models.Job, error) {
	filter := bson.M{"tenantId": tenantID}
	if status != "" {
		filter["status"] = status
	}
	opts := options.Find().SetLimit(limit).SetSort(bson.D{{Key: "createdAt", Value: -1}})

	cur, err := r.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var jobs []models.Job
	if err := cur.All(ctx, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}
