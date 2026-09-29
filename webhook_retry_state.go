package worker

import (
	"encoding/json"
	"time"
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
	rdb interface{},
	state *WebhookRetryState,
) error {

	payload, err := json.Marshal(
		state,
	)

	if err != nil {
		return err
	}

	return newRedisAdapter(rdb).Set(
		webhookRetryStateKey(
			state.BatchID,
		),
		payload,
		0,
	).Err()
}

func LoadWebhookRetryState(
	rdb interface{},
	batchID string,
) (*WebhookRetryState, error) {

	raw, err := newRedisAdapter(rdb).Get(
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
	rdb interface{},
	batchID string,
) error {

	return newRedisAdapter(rdb).Del(
		webhookRetryStateKey(
			batchID,
		),
	).Err()
}

func FindWebhookRetryStates(
	rdb interface{},
) ([]*WebhookRetryState, error) {

	keys, err := newRedisAdapter(rdb).Keys(
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

		raw, err := newRedisAdapter(rdb).Get(
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
	rdb interface{},
	batchID string,
) (bool, error) {

	return newRedisAdapter(rdb).SetNX(
		webhookRetryLockKey(
			batchID,
		),
		"1",
		time.Minute,
	).Result()
}

func ReleaseWebhookRetry(
	rdb interface{},
	batchID string,
) error {

	return newRedisAdapter(rdb).Del(
		webhookRetryLockKey(
			batchID,
		),
	).Err()
}
