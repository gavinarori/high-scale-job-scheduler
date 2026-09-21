package handlers

import (
	"context"
	"fmt"
	"log"

	"github.com/arori/job-scheduler/internal/executor"
)

// RegisterAll wires every job-type implementation into the registry.
// Add a line here each time you implement a new job type.
func RegisterAll(r *executor.Registry) {
	r.Register("noop", noopHandler)
	r.Register("log-message", logMessageHandler)
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
