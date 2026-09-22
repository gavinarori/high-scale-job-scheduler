package scheduler

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Standard 5-field cron (minute hour dom month dow) — no seconds field,
// since sub-minute recurring jobs should use the scheduler's own tick
// resolution via repeated one-off jobs, not cron.
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// NextOccurrence returns the next time the given cron expression fires
// after the given time. Used both at job-creation time (to compute the
// first occurrence) and by the scheduler loop after each firing (to
// compute the next one).
func NextOccurrence(expr string, after time.Time) (time.Time, error) {
	sched, err := cronParser.Parse(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", expr, err)
	}
	return sched.Next(after), nil
}
