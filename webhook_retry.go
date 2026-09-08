package worker

import (
	"math/rand"
	"time"

	redis "gopkg.in/redis.v5"
)

func retryDelay(
	attempt int,
) time.Duration {

	if attempt < 1 {
		attempt = 1
	}

	base := time.Duration(
		1<<uint(attempt-1),
	) * time.Minute

	jitter := time.Duration(
		rand.Intn(30),
	) * time.Second

	return base + jitter
}

func ScheduleWebhookRetry(
	rdb *redis.Client,
	batchID string,
	attempt int,
) error {

	state := &WebhookRetryState{
		BatchID: batchID,
		Attempt: attempt,
		NextRetryAt: time.Now().
			Add(
				retryDelay(
					attempt,
				),
			).
			Unix(),
	}

	return SaveWebhookRetryState(
		rdb,
		state,
	)
}
