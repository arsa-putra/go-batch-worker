package worker

import (
	"sync/atomic"
	"time"
)

type WorkerMetrics struct {
	Processed atomic.Uint64
	Dropped   atomic.Uint64
	Failed    atomic.Uint64
	Batches   atomic.Uint64
}

type WorkerStats struct {
	Name          string    `json:"name"`
	LastFlushAt   time.Time `json:"last_flush_at"`
	LastBatchSize int       `json:"last_batch_size"`
}

type FailedBatch[T any] struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Error     string    `json:"error"`
	Status    string    `json:"status"`

	RetryCount  int       `json:"retry_count"`
	LastRetryAt time.Time `json:"last_retry_at"`

	IsDead bool       `json:"is_dead"`
	DeadAt *time.Time `json:"dead_at,omitempty"`

	Logs []T `json:"logs"`
}
