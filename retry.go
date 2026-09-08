package worker

import (
	"errors"
	"time"
)

var ErrDeadJob = errors.New("already moved to DLQ")

func retry(attempts int, fn func() error) error {
	var err error

	backoff := time.Second

	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}

		time.Sleep(backoff)
		backoff *= 2
	}

	return err
}
