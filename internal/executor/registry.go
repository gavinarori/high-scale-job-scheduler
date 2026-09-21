package executor

import (
	"context"
	"fmt"
	"sync"
)

// Handler is the function signature every job-type implementation must satisfy.
type Handler func(ctx context.Context, payload map[string]any) error

// Registry maps jobType -> Handler. Register your job-type implementations
// here at startup (see internal/executor/handlers/).
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(jobType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	if !ok {
		return nil, fmt.Errorf("no handler registered for jobType %q", jobType)
	}
	return h, nil
}
