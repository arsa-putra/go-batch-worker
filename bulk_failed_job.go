package worker

import "time"

type FailedJob[T any] struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Error     string    `json:"error"`
	Status    string    `json:"status"`

	RetryCount  int       `json:"retry_count"`
	LastRetryAt time.Time `json:"last_retry_at"`

	IsDead bool       `json:"is_dead"`
	DeadAt *time.Time `json:"dead_at,omitempty"`

	Job T `json:"job"`
}
