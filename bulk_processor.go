package worker

import "context"

type BulkProcessor[T any] interface {
	Name() string
	Process(ctx context.Context, job T) error
}
