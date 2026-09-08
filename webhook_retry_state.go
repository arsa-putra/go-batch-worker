package worker

import (
	"encoding/json"
	"time"

	redis "gopkg.in/redis.v5"
)

type WebhookRetryState struct {
	BatchID string `json:"batch_id"`

	Attempt int `json:"attempt"`

	NextRetryAt int64 `json:"next_retry_at"`
}

func webhookRetryStateKey(
	batchID string,
) string {

	return "worker:webhook:retry:" +
		batchID
}

func SaveWebhookRetryState(
	rdb *redis.Client,
	state *WebhookRetryState,
) error {

	payload, err := json.Marshal(
		state,
	)

	if err != nil {
		return err
	}

	return rdb.Set(
		webhookRetryStateKey(
			state.BatchID,
		),
		payload,
		0,
	).Err()
}

func LoadWebhookRetryState(
	rdb *redis.Client,
	batchID string,
) (*WebhookRetryState, error) {

	raw, err := rdb.Get(
		webhookRetryStateKey(
			batchID,
		),
	).Result()

	if err != nil {
		return nil, err
	}

	var state WebhookRetryState

	if err := json.Unmarshal(
		[]byte(raw),
		&state,
	); err != nil {
		return nil, err
	}

	return &state, nil
}

func DeleteWebhookRetryState(
	rdb *redis.Client,
	batchID string,
) error {

	return rdb.Del(
		webhookRetryStateKey(
			batchID,
		),
	).Err()
}

func FindWebhookRetryStates(
	rdb *redis.Client,
) ([]*WebhookRetryState, error) {

	keys, err := rdb.Keys(
		"worker:webhook:retry:*",
	).Result()

	if err != nil {
		return nil, err
	}

	states := make(
		[]*WebhookRetryState,
		0,
		len(keys),
	)

	for _, key := range keys {

		raw, err := rdb.Get(
			key,
		).Result()

		if err != nil {
			continue
		}

		var state WebhookRetryState

		if err := json.Unmarshal(
			[]byte(raw),
			&state,
		); err != nil {
			continue
		}

		states = append(
			states,
			&state,
		)
	}

	return states, nil
}

func webhookRetryLockKey(
	batchID string,
) string {

	return "worker:webhook:retry:lock:" +
		batchID
}

func ClaimWebhookRetry(
	rdb *redis.Client,
	batchID string,
) (bool, error) {

	return rdb.SetNX(
		webhookRetryLockKey(
			batchID,
		),
		"1",
		time.Minute,
	).Result()
}

func ReleaseWebhookRetry(
	rdb *redis.Client,
	batchID string,
) error {

	return rdb.Del(
		webhookRetryLockKey(
			batchID,
		),
	).Err()
}
