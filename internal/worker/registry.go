package worker

import (
	"sync"

	"sidekiq/internal/middleware"
)

// Handler is an alias for middleware.Handler, not a separate type - this
// keeps worker and middleware referring to exactly the same function type,
// so a registered handler can be passed straight into middleware.Chain
// without any conversion.
type Handler = middleware.Handler

type Registry struct {
	jobType map[string]Handler
	mu      sync.Mutex
}

func NewRegistry() *Registry {
	return &Registry{
		jobType: make(map[string]Handler),
	}
}

func (r *Registry) Register(jobType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobType[jobType] = h
}

func (r *Registry) Get(jobType string) (Handler, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	value, ok := r.jobType[jobType]
	return value, ok
}
