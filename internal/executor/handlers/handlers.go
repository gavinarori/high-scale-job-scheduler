package handlers

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/arori/job-scheduler/internal/executor"
)

// RegisterAll wires every job-type implementation into the registry.
// Add a line here each time you implement a new job type.
func RegisterAll(r *executor.Registry) {
	r.Register("noop", noopHandler)
	r.Register("log-message", logMessageHandler)
	r.Register("flaky", flakyHandler)
	r.Register("slow", slowHandler)
}

// noopHandler is a placeholder for wiring/testing the pipeline end to end.
func noopHandler(ctx context.Context, payload map[string]any) error {
	return nil
}

// logMessageHandler demonstrates reading a typed field out of the payload map.
func logMessageHandler(ctx context.Context, payload map[string]any) error {
	msg, ok := payload["message"].(string)
	if !ok {
		return fmt.Errorf("payload missing required 'message' field")
	}
	log.Printf("[log-message job] %s", msg)
	return nil
}

// flakyHandler fails a configurable fraction of the time (payload
// "failRate", default 0.5). Exists purely so seed data and load tests
// exercise the retry/backoff/DLQ path — without a handler that actually
// fails sometimes, that whole code path only ever gets touched by unit
// tests calling MarkFailed directly, never by a real run through the
// scheduler → Kafka → executor pipeline.
func flakyHandler(ctx context.Context, payload map[string]any) error {
	failRate := 0.5
	if fr, ok := payload["failRate"].(float64); ok {
		failRate = fr
	}
	if rand.Float64() < failRate {
		return fmt.Errorf("flaky job intentionally failed (failRate=%.2f)", failRate)
	}
	return nil
}

// slowHandler sleeps for a configurable duration (payload "sleepMs",
// default 500ms) before succeeding. Useful for load-testing/demoing
// executor throughput under realistic (non-instant) handler latency, and
// for exercising the claim-timeout reaper sweep if sleepMs is set above
// CLAIM_TIMEOUT.
func slowHandler(ctx context.Context, payload map[string]any) error {
	sleepMs := 500.0
	if ms, ok := payload["sleepMs"].(float64); ok {
		sleepMs = ms
	}
	select {
	case <-time.After(time.Duration(sleepMs) * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
