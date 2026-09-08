package worker

import "context"

type Processor[T any] interface {
	Process(ctx context.Context, items []T) error
	Name() string
}
