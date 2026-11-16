package worker

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

const (
	webhookRetryKeyPrefix  = "worker:webhook:retry:"
	webhookRetryLockPrefix = webhookRetryKeyPrefix + "lock:"
	webhookRetryScanCount  = 100
	webhookRetryIndexKey   = "worker:webhook:retry-index"
	webhookRetryIndexReady = "worker:webhook:retry-index:ready"
	webhookRetryIndexLock  = "worker:webhook:retry-index:lock"
	webhookRetryIndexBatch = 100
)

var errWebhookRetryIndexBuilding = errors.New("webhook retry index is being built")

type WebhookRetryState struct {
	BatchID string `json:"batch_id"`

	Attempt int `json:"attempt"`

	NextRetryAt int64 `json:"next_retry_at"`
}

func webhookRetryStateKey(
	batchID string,
) string {

	return webhookRetryKeyPrefix +
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

	redis := newRedisAdapter(rdb)
	pipe := redis.Pipeline()
	pipe.Set(webhookRetryStateKey(state.BatchID), payload, 0)
	pipe.ZAdd(webhookRetryIndexKey, float64(state.NextRetryAt), state.BatchID)
	if err := pipe.Exec(); err != nil {
		// Force a rescan on the next worker pass to recover the state/index gap.
		_ = redis.Del(webhookRetryIndexReady).Err()
		return err
	}
	return nil
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

	redis := newRedisAdapter(rdb)
	pipe := redis.Pipeline()
	pipe.Del(webhookRetryStateKey(batchID))
	pipe.ZRem(webhookRetryIndexKey, batchID)
	if err := pipe.Exec(); err != nil {
		_ = redis.Del(webhookRetryIndexReady).Err()
		return err
	}
	return nil
}

func FindWebhookRetryStates(
	rdb interface{},
) ([]*WebhookRetryState, error) {
	if err := ensureWebhookRetryIndex(rdb); err != nil {
		if !errors.Is(err, errWebhookRetryIndexBuilding) {
			return nil, err
		}

		states := make([]*WebhookRetryState, 0)
		if err := forEachWebhookRetryState(rdb, func(state *WebhookRetryState) error {
			states = append(states, state)
			return nil
		}); err != nil {
			return nil, err
		}
		return states, nil
	}

	redis := newRedisAdapter(rdb)
	ids, err := redis.ZRange(webhookRetryIndexKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return loadWebhookRetryStates(rdb, ids)
}

func loadWebhookRetryStates(rdb interface{}, batchIDs []string) ([]*WebhookRetryState, error) {
	const retryReadBatchSize = 100
	redis := newRedisAdapter(rdb)
	var states []*WebhookRetryState
	for start := 0; start < len(batchIDs); start += retryReadBatchSize {
		end := start + retryReadBatchSize
		if end > len(batchIDs) {
			end = len(batchIDs)
		}
		ids := batchIDs[start:end]
		pipe := redis.Pipeline()
		commands := make([]redisPipelineStringCmd, len(ids))
		for i, batchID := range ids {
			commands[i] = pipe.Get(webhookRetryStateKey(batchID))
		}
		if err := pipe.Exec(); err != nil && !errors.Is(err, errRedisNil) {
			return nil, err
		}

		cleanup := redis.Pipeline()
		cleanupNeeded := false
		for i, batchID := range ids {
			raw, err := commands[i].Result()
			if errors.Is(err, errRedisNil) {
				cleanup.ZRem(webhookRetryIndexKey, batchID)
				cleanupNeeded = true
				continue
			}
			if err != nil {
				return nil, err
			}
			var state WebhookRetryState
			if err := json.Unmarshal([]byte(raw), &state); err != nil {
				cleanup.ZRem(webhookRetryIndexKey, batchID)
				cleanupNeeded = true
				continue
			}
			states = append(states, &state)
		}
		if cleanupNeeded {
			if err := cleanup.Exec(); err != nil {
				return nil, err
			}
		}
	}
	return states, nil
}

func countWebhookRetryStates(rdb interface{}) (int, error) {
	redis := newRedisAdapter(rdb)
	if err := ensureWebhookRetryIndex(rdb); err != nil {
		if !errors.Is(err, errWebhookRetryIndexBuilding) {
			return 0, err
		}

		count := 0
		if err := forEachWebhookRetryState(rdb, func(*WebhookRetryState) error {
			count++
			return nil
		}); err != nil {
			return 0, err
		}
		return count, nil
	}

	count, err := redis.ZCard(webhookRetryIndexKey).Result()
	return int(count), err
}

func ensureWebhookRetryIndex(rdb interface{}) error {
	redis := newRedisAdapter(rdb)
	if _, err := redis.Get(webhookRetryIndexReady).Result(); err == nil {
		return nil
	} else if !errors.Is(err, errRedisNil) {
		return err
	}

	locked, err := redis.SetNX(webhookRetryIndexLock, "1", 10*time.Minute).Result()
	if err != nil {
		return err
	}
	if !locked {
		return errWebhookRetryIndexBuilding
	}
	defer redis.Del(webhookRetryIndexLock)

	// Another worker may have completed the migration before this lock was acquired.
	if _, err := redis.Get(webhookRetryIndexReady).Result(); err == nil {
		return nil
	} else if !errors.Is(err, errRedisNil) {
		return err
	}

	if err := redis.Del(webhookRetryIndexKey).Err(); err != nil {
		return err
	}
	indexBatch := make([]*WebhookRetryState, 0, webhookRetryIndexBatch)
	flushIndexBatch := func() error {
		if len(indexBatch) == 0 {
			return nil
		}
		pipe := redis.Pipeline()
		for _, state := range indexBatch {
			pipe.ZAdd(webhookRetryIndexKey, float64(state.NextRetryAt), state.BatchID)
		}
		if err := pipe.Exec(); err != nil {
			return err
		}
		indexBatch = indexBatch[:0]
		return nil
	}
	if err := forEachWebhookRetryState(rdb, func(state *WebhookRetryState) error {
		indexBatch = append(indexBatch, state)
		if len(indexBatch) == cap(indexBatch) {
			return flushIndexBatch()
		}
		return nil
	}); err != nil {
		return err
	}
	if err := flushIndexBatch(); err != nil {
		return err
	}
	return redis.Set(webhookRetryIndexReady, "1", 0).Err()
}

func findDueWebhookRetryStates(rdb interface{}, now int64, limit int64) ([]*WebhookRetryState, error) {
	redis := newRedisAdapter(rdb)
	ids, err := redis.ZRangeByScore(
		webhookRetryIndexKey,
		"-inf",
		strconv.FormatInt(now, 10),
		0,
		limit,
	).Result()
	if err != nil {
		return nil, err
	}

	readPipe := redis.Pipeline()
	commands := make([]redisPipelineStringCmd, len(ids))
	for i, batchID := range ids {
		commands[i] = readPipe.Get(webhookRetryStateKey(batchID))
	}
	if err := readPipe.Exec(); err != nil && !errors.Is(err, errRedisNil) {
		return nil, err
	}

	states := make([]*WebhookRetryState, 0, len(ids))
	indexPipe := redis.Pipeline()
	indexChanges := false
	for i, batchID := range ids {
		raw, err := commands[i].Result()
		if errors.Is(err, errRedisNil) {
			indexPipe.ZRem(webhookRetryIndexKey, batchID)
			indexChanges = true
			continue
		}
		if err != nil {
			return nil, err
		}

		var state WebhookRetryState
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			indexPipe.ZRem(webhookRetryIndexKey, batchID)
			indexChanges = true
			continue
		}
		if state.NextRetryAt > now {
			// Repair stale scores if the state was updated outside the index path.
			indexPipe.ZAdd(webhookRetryIndexKey, float64(state.NextRetryAt), batchID)
			indexChanges = true
			continue
		}
		states = append(states, &state)
	}
	if indexChanges {
		if err := indexPipe.Exec(); err != nil {
			return nil, err
		}
	}
	return states, nil
}

// forEachWebhookRetryState incrementally reads states for public full-listing
// and for the one-time migration from pre-index retry keys.
func forEachWebhookRetryState(rdb interface{}, visit func(*WebhookRetryState) error) error {
	redis := newRedisAdapter(rdb)
	var cursor uint64
	seen := make(map[string]struct{})

	for {
		keys, nextCursor, err := redis.Scan(
			cursor,
			webhookRetryKeyPrefix+"*",
			webhookRetryScanCount,
		).Result()
		if err != nil {
			return err
		}

		stateKeys := make([]string, 0, len(keys))
		for _, key := range keys {
			if strings.HasPrefix(key, webhookRetryLockPrefix) {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			stateKeys = append(stateKeys, key)
		}

		if len(stateKeys) > 0 {
			pipe := redis.Pipeline()
			commands := make([]redisPipelineStringCmd, len(stateKeys))
			for i, key := range stateKeys {
				commands[i] = pipe.Get(key)
			}
			if err := pipe.Exec(); err != nil && !errors.Is(err, errRedisNil) {
				return err
			}

			for i := range stateKeys {
				raw, err := commands[i].Result()
				if errors.Is(err, errRedisNil) {
					// A state may be deleted while SCAN is in progress.
					continue
				}
				if err != nil {
					return err
				}

				var state WebhookRetryState
				if err := json.Unmarshal([]byte(raw), &state); err != nil {
					continue
				}
				if err := visit(&state); err != nil {
					return err
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			return nil
		}
	}
}

func webhookRetryLockKey(
	batchID string,
) string {

	return webhookRetryLockPrefix +
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
