package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/arori/job-scheduler/internal/models"
	"github.com/arori/job-scheduler/internal/store"
)

// DispatchFunc is called for every job the scheduler claims. In Phase 1
// this executes the job in-process; in Phase 2 this becomes a Kafka
// producer call instead — the loop itself doesn't change.
type DispatchFunc func(ctx context.Context, job models.Job)

// Loop is the Phase 1 scheduler: single-instance, polls Mongo on a fixed
// tick, and hands claimed jobs to a dispatch function. Leader election
// (Phase 3) wraps this same loop so only one instance runs it at a time.
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
	for _, job := range jobs {
		go l.dispatch(ctx, job)
	}
	if len(jobs) > 0 {
		log.Printf("dispatched %d job(s)", len(jobs))
	}
}
