package worker

import "golang.org/x/sync/semaphore"

type ResourceLimiter struct {
	DB *semaphore.Weighted
}
