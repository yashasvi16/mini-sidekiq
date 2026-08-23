package sidekiq

import (
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusActive    JobStatus = "active"
	StatusRetry     JobStatus = "retry"
	StatusDead      JobStatus = "dead"
	StatusCompleted JobStatus = "completed"
)

type Job struct {
	ID          string          `json:"id"`       // UUID
	Queue       string          `json:"queue"`    // "critical", "default", "low"
	Type        string          `json:"type"`     // Handler name, e.g. "SendEmail"
	Payload     json.RawMessage `json:"payload"`  // Arbitrary job args
	Priority    int             `json:"priority"` // 1 (highest) - 10 (lowest)
	CreatedAt   time.Time       `json:"created_at"`
	EnqueuedAt  time.Time       `json:"enqueued_at"`
	ProcessAt   time.Time       `json:"process_at"`   // For delayed/scheduled jobs
	Attempts    int             `json:"attempts"`     // Retry counter
	MaxAttempts int             `json:"max_attempts"` // Default: 3 (0, 1, 2, 3)
	LastError   string          `json:"last_error"`
	UniqueKey   string          `json:"unique_key"` // For idempotency (optional)
	Status      JobStatus       `json:"status"`
}

func NewJob(queue, jobType string, payload any) (*Job, error) {

	p, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Job{
		ID:          uuid.New().String(),
		Queue:       queue,
		Type:        jobType,
		Payload:     p,
		CreatedAt:   time.Now(),
		ProcessAt:   time.Now(),
		Attempts:    0,
		MaxAttempts: 3,
		Status:      StatusPending,
	}, nil
}
