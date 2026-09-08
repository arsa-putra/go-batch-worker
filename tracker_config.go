package worker

import (
	"errors"
	"time"
)

type TrackerConfig struct {
	ResultTTL time.Duration
}

func DefaultTrackerConfig() TrackerConfig {
	return TrackerConfig{
		ResultTTL: 7 * 24 * time.Hour,
	}
}

func (c TrackerConfig) Validate() error {
	if c.ResultTTL <= 0 {
		return errors.New(
			"result ttl must be greater than zero",
		)
	}

	return nil
}
