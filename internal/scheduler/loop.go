package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
)

// DispatchFunc is called for every job the scheduler claims (or for a
// cron template's spawned instance). Phase 2 wires this to a Kafka
// producer call; the loop itself is agnostic to what dispatch does.
type DispatchFunc func(ctx context.Context, job models.Job)

// Loop polls Mongo on a fixed tick and hands claimed jobs to a dispatch
// function. Recurring (cron) jobs are handled specially — see
// handleCronJob — spawning a concrete instance rather than dispatching the
// template itself. Leader election (internal/scheduler/election.go) wraps
// this loop so only one scheduler instance runs it at a time.
type Loop struct {
	repo      *store.JobsRepo
	tick      time.Duration
	batchSize int
	dispatch  DispatchFunc
}

func NewLoop(repo *store.JobsRepo, tick time.Duration, batchSize int, dispatch DispatchFunc) *Loop {
	return &Loop{repo: repo, tick: tick, batchSize: batchSize, dispatch: dispatch}
}

// Run blocks, ticking until ctx is cancelled.
func (l *Loop) Run(ctx context.Context) {
	ticker := time.NewTicker(l.tick)
	defer ticker.Stop()

	log.Printf("scheduler loop started (tick=%s, batch=%d)", l.tick, l.batchSize)

	for {
		select {
		case <-ctx.Done():
			log.Println("scheduler loop stopping")
			return
		case <-ticker.C:
			l.runOnce(ctx)
		}
	}
}

func (l *Loop) runOnce(ctx context.Context) {
	jobs, err := l.repo.ClaimDueJobs(ctx, time.Now().UTC(), l.batchSize)
	if err != nil {
		log.Printf("claim due jobs failed: %v", err)
		return
	}

	dispatched := 0
	for _, job := range jobs {
		if job.Cron != "" {
			l.handleCronJob(ctx, job)
			continue
		}
		go l.dispatch(ctx, job)
		dispatched++
	}
	if dispatched > 0 {
		log.Printf("dispatched %d job(s)", dispatched)
	}
}

// handleCronJob fires one occurrence of a recurring job template: it spawns
// a concrete run-instance (which goes through normal dispatch) and
// reschedules the template to its next occurrence. The template itself is
// never dispatched — only its spawned instances are, which keeps retry
// history, idempotency keys, and DLQ semantics scoped to individual runs
// rather than muddying the recurring template's own status.
func (l *Loop) handleCronJob(ctx context.Context, template models.Job) {
	instance, err := l.repo.SpawnCronInstance(ctx, template)
	if err != nil {
		log.Printf("cron template %s: failed to spawn instance: %v", template.ID.Hex(), err)
		return
	}

	next, err := NextOccurrence(template.Cron, time.Now().UTC())
	if err != nil {
		log.Printf("cron template %s: invalid cron expression, dead-lettering: %v", template.ID.Hex(), err)
		if err := l.repo.MarkCronInvalid(ctx, template.ID, err.Error()); err != nil {
			log.Printf("cron template %s: failed to mark invalid: %v", template.ID.Hex(), err)
		}
		return
	}

	if err := l.repo.RescheduleCronTemplate(ctx, template.ID, next); err != nil {
		log.Printf("cron template %s: failed to reschedule: %v", template.ID.Hex(), err)
		// The template is left in "queued" here — SweepStuckQueuedJobs
		// will pick it back up to pending shortly, same recovery path as
		// a failed Kafka publish.
	}

	log.Printf("cron template %s: spawned instance %s, next occurrence %s", template.ID.Hex(), instance.ID.Hex(), next)
	go l.dispatch(ctx, *instance)
}
