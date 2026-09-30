package worker

import (
	"errors"
	"time"
)

var ErrDeadJob = errors.New("already moved to DLQ")
var ErrSkipRetry = errors.New("retry skipped")

// RetryConfig controls worker retry behavior. MaxRetries is the maximum number
// of attempts made from the failed-job queue. ProcessorAttempts is the number
// of immediate attempts made by BatchWorker before it stores a failed batch.
// Zero values keep the framework defaults. Use SkipRetry for an error that
// must not be retried.
type RetryConfig struct {
	MaxRetries        int
	ProcessorAttempts int
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:        3,
		ProcessorAttempts: 3,
	}
}

func normalizeRetryConfig(configs []RetryConfig) RetryConfig {
	config := DefaultRetryConfig()
	if len(configs) == 0 {
		return config
	}

	provided := configs[0]
	if provided.MaxRetries > 0 {
		config.MaxRetries = provided.MaxRetries
	}
	if provided.ProcessorAttempts > 0 {
		config.ProcessorAttempts = provided.ProcessorAttempts
	}
	return config
}

type skipRetryError struct {
	cause error
}

func (e skipRetryError) Error() string {
	if e.cause == nil {
		return ErrSkipRetry.Error()
	}
	return e.cause.Error()
}

func (e skipRetryError) Unwrap() error { return e.cause }

func (e skipRetryError) Is(target error) bool { return target == ErrSkipRetry }

// SkipRetry marks an error as permanent. Workers still record it as failed,
// but do not schedule another attempt. The original error message is retained.
func SkipRetry(err error) error {
	if err == nil {
		return ErrSkipRetry
	}
	if errors.Is(err, ErrSkipRetry) {
		return err
	}
	return skipRetryError{cause: err}
}

func retry(attempts int, fn func() error) error {
	var err error

	backoff := time.Second

	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrSkipRetry) {
			return err
		}

		time.Sleep(backoff)
		backoff *= 2
	}

	return err
}
