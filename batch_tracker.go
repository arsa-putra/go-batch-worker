package worker

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	redis "gopkg.in/redis.v5"
)

type BatchTracker interface {
	RegisterBatch(batchID string, total int) error
	Get(batchID string) (*BatchState, error)
	MarkSuccess(batchID string) error
	MarkFailed(batchID string) error
	IsCompleted(batchID string) (bool, error)
	TryComplete(batchID string) (bool, error)
	SaveResult(batchID string, result BatchItemResult) error
	CompleteSuccess(batchID string, result BatchItemResult) error
	CompleteFailed(batchID string, result BatchItemResult) error
	Complete(batchID string) (*BatchResult, bool, error)
	BuildResult(batchID string) (*BatchResult, error)
	SaveMetadata(batchID string, metadata *BatchMetadata) error
	LoadMetadata(batchID string) (*BatchMetadata, error)
	BuildSummary(batchID string) (*BatchSummary, error)
	CancelBatch(batchID string) error
	IsCancelled(batchID string) (bool, error)
	CompleteCancelled(batchID string, result BatchItemResult) error
	ListBatches() ([]*BatchState, error)
}

type RedisBatchTracker struct {
	redis   *redis.Client
	options BatchOptions
	config  TrackerConfig
}

func NewRedisBatchTracker(rdb *redis.Client, opts BatchOptions, config TrackerConfig) *RedisBatchTracker {
	if err := config.Validate(); err != nil {
		panic(err)
	}

	return &RedisBatchTracker{
		redis:   rdb,
		options: opts,
		config:  config,
	}
}

func (t *RedisBatchTracker) stateKey(batchID string) string {
	return fmt.Sprintf(
		"worker:batch:%s",
		batchID,
	)
}

func (t *RedisBatchTracker) RegisterBatch(
	batchID string,
	total int,
) error {

	key := t.stateKey(batchID)
	fields := map[string]string{
		"batch_id": batchID,
		"total":    strconv.Itoa(total),
		"done":     "0",
		"success":  "0",
		"failed":   "0",
	}

	err := t.redis.HMSet(
		key,
		fields,
	).Err()

	if err == nil {
		batchCreatedTotal.Inc()
	}

	return err
}

func (t *RedisBatchTracker) Get(
	batchID string,
) (*BatchState, error) {

	key := t.stateKey(batchID)

	m, err := t.redis.HGetAll(
		key,
	).Result()

	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, fmt.Errorf(
			"batch %s not found",
			batchID,
		)
	}
	state := &BatchState{
		BatchID: m["batch_id"],
	}

	total, _ := strconv.ParseInt(m["total"], 10, 64)
	done, _ := strconv.ParseInt(m["done"], 10, 64)
	success, _ := strconv.ParseInt(m["success"], 10, 64)
	failed, _ := strconv.ParseInt(m["failed"], 10, 64)

	state.Total = total
	state.Done = done
	state.Success = success
	state.Failed = failed

	return state, nil
}

func (t *RedisBatchTracker) MarkSuccess(
	batchID string,
) error {

	key := t.stateKey(batchID)

	pipe := t.redis.TxPipeline()

	pipe.HIncrBy(
		key,
		"success",
		1,
	)

	pipe.HIncrBy(
		key,
		"done",
		1,
	)

	_, err := pipe.Exec()
	if err == nil {
		batchItemsSuccessTotal.Inc()
	}

	return err
}

func (t *RedisBatchTracker) MarkFailed(
	batchID string,
) error {

	key := t.stateKey(batchID)

	pipe := t.redis.TxPipeline()

	pipe.HIncrBy(
		key,
		"failed",
		1,
	)

	pipe.HIncrBy(
		key,
		"done",
		1,
	)

	_, err := pipe.Exec()
	if err == nil {
		batchItemsFailedTotal.Inc()
	}

	return err
}

func (t *RedisBatchTracker) IsCompleted(
	batchID string,
) (bool, error) {

	state, err := t.Get(
		batchID,
	)

	if err != nil {
		return false, err
	}

	return state.Done >= state.Total, nil
}

func (t *RedisBatchTracker) completedKey(
	batchID string,
) string {
	return fmt.Sprintf(
		"worker:batch:%s:completed",
		batchID,
	)
}
func (t *RedisBatchTracker) TryComplete(
	batchID string,
) (bool, error) {

	completed, err := t.IsCompleted(
		batchID,
	)

	if err != nil {
		return false, err
	}

	if !completed {
		return false, nil
	}

	ok, err := t.redis.SetNX(
		t.completedKey(batchID),
		"1",
		t.config.ResultTTL,
	).Result()

	if err != nil {
		return false, err
	}
	if ok {
		batchCompletedTotal.Inc()

		t.redis.HSet(
			t.stateKey(batchID),
			"finished_at",
			time.Now().Unix(),
		)
	}
	return ok, nil
}

func (t *RedisBatchTracker) successKey(
	batchID string,
) string {
	return fmt.Sprintf(
		"worker:batch:%s:success",
		batchID,
	)
}

func (t *RedisBatchTracker) failedKey(
	batchID string,
) string {
	return fmt.Sprintf(
		"worker:batch:%s:failed",
		batchID,
	)
}

func (t *RedisBatchTracker) SaveResult(
	batchID string,
	result BatchItemResult,
) error {
	if result.Success && !t.options.StoreSuccessItems {
		return nil
	}

	if !result.Success && !t.options.StoreFailedItems {
		return nil
	}

	if result.Success &&
		t.options.MaxStoredSuccesses > 0 {

		stored, err := t.getStoredSuccessCount(
			batchID,
		)

		if err != nil {
			return err
		}

		if stored >= int64(
			t.options.MaxStoredSuccesses,
		) {
			return nil
		}
	}

	payload, err := json.Marshal(
		result,
	)

	if err != nil {
		return err
	}

	if !result.Success &&
		t.options.MaxStoredErrors > 0 {

		stored, err := t.getStoredErrorCount(
			batchID,
		)

		if err != nil {
			return err
		}

		if stored >= int64(
			t.options.MaxStoredErrors,
		) {

			return nil
		}
	}

	key := t.failedKey(batchID)

	if result.Success {
		key = t.successKey(batchID)
	}

	if err := t.redis.RPush(
		key,
		string(payload),
	).Err(); err != nil {
		return err
	}
	if result.Success &&
		t.options.MaxStoredSuccesses > 0 {

		if err := t.incrementStoredSuccess(
			batchID,
		); err != nil {
			return err
		}
	}
	if !result.Success &&
		t.options.MaxStoredErrors > 0 {

		if err := t.incrementStoredError(
			batchID,
		); err != nil {
			return err
		}
	}
	return nil
}
func (t *RedisBatchTracker) loadResults(
	key string,
) ([]BatchItemResult, error) {

	rows, err := t.redis.LRange(
		key,
		0,
		-1,
	).Result()

	if err != nil {
		return nil, err
	}

	result := make(
		[]BatchItemResult,
		0,
		len(rows),
	)

	for _, row := range rows {
		var item BatchItemResult

		if err := json.Unmarshal(
			[]byte(row),
			&item,
		); err != nil {
			continue
		}

		result = append(
			result,
			item,
		)
	}

	return result, nil
}
func (t *RedisBatchTracker) BuildResult(
	batchID string,
) (*BatchResult, error) {

	state, err := t.Get(
		batchID,
	)

	if err != nil {
		return nil, err
	}

	successItems, err := t.loadResults(
		t.successKey(batchID),
	)

	if err != nil {
		return nil, err
	}

	failedItems, err := t.loadResults(
		t.failedKey(batchID),
	)

	if err != nil {
		return nil, err
	}

	// --- REMOVE DUPLICATES & CROSS-CONTAMINATION ---
	// Filter out duplicate failures
	seenFailed := make(map[string]bool)
	var cleanFailed []BatchItemResult
	for _, item := range failedItems {
		if !seenFailed[item.Key] {
			seenFailed[item.Key] = true
			cleanFailed = append(cleanFailed, item)
		}
	}

	seenSuccess := make(map[string]bool)
	var cleanSuccess []BatchItemResult
	for _, item := range successItems {
		// Filter out duplicates and ensure items present in failed do not appear in success
		if !seenSuccess[item.Key] && !seenFailed[item.Key] {
			seenSuccess[item.Key] = true
			cleanSuccess = append(cleanSuccess, item)
		}
	}

	finishedAt, _ := t.redis.HGet(
		t.stateKey(batchID),
		"finished_at",
	).Int64()

	storedErrors, _ := t.getStoredErrorCount(
		batchID,
	)

	storedSuccesses, _ :=
		t.getStoredSuccessCount(
			batchID,
		)
	return &BatchResult{
		BatchID: batchID,

		Total: state.Total,

		Success:    int64(len(cleanSuccess)),
		Failed:     int64(len(cleanFailed)),
		FinishedAt: finishedAt,

		SuccessItems: cleanSuccess,
		FailedItems:  cleanFailed,

		StoredSuccesses: storedSuccesses,
		StoredErrors:    storedErrors,
	}, nil
}

func (t *RedisBatchTracker) CompleteSuccess(
	batchID string,
	result BatchItemResult,
) error {

	result.Success = true

	if err := t.SaveResult(
		batchID,
		result,
	); err != nil {
		return err
	}

	return t.MarkSuccess(
		batchID,
	)
}

func (t *RedisBatchTracker) CompleteFailed(
	batchID string,
	result BatchItemResult,
) error {

	result.Success = false

	if err := t.SaveResult(
		batchID,
		result,
	); err != nil {
		return err
	}

	return t.MarkFailed(
		batchID,
	)
}

func (t *RedisBatchTracker) Complete(
	batchID string,
) (*BatchResult, bool, error) {

	completed, err := t.TryComplete(
		batchID,
	)

	if err != nil {
		return nil, false, err
	}

	if !completed {
		return nil, false, nil
	}

	result, err := t.BuildResult(
		batchID,
	)

	if err != nil {
		return nil, false, err
	}

	return result, true, nil
}

func (t *RedisBatchTracker) metadataKey(
	batchID string,
) string {
	return "worker:batch:" + batchID + ":metadata"
}

func (t *RedisBatchTracker) SaveMetadata(
	batchID string,
	metadata *BatchMetadata,
) error {

	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	return t.redis.Set(
		t.metadataKey(batchID),
		data,
		t.config.ResultTTL,
	).Err()
}

func (t *RedisBatchTracker) LoadMetadata(
	batchID string,
) (*BatchMetadata, error) {

	raw, err := t.redis.Get(
		t.metadataKey(batchID),
	).Result()

	if err != nil {
		return nil, err
	}

	var metadata BatchMetadata

	if err := json.Unmarshal(
		[]byte(raw),
		&metadata,
	); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (t *RedisBatchTracker) failedCountKey(
	batchID string,
) string {

	return "worker:batch:" +
		batchID +
		":failed:stored"
}

func (t *RedisBatchTracker) successCountKey(
	batchID string,
) string {

	return "worker:batch:" +
		batchID +
		":success:stored"
}
func (t *RedisBatchTracker) getStoredSuccessCount(
	batchID string,
) (int64, error) {

	count, err := t.redis.Get(
		t.successCountKey(batchID),
	).Int64()

	if err == redis.Nil {
		return 0, nil
	}

	return count, err
}
func (t *RedisBatchTracker) incrementStoredSuccess(
	batchID string,
) error {

	return t.redis.Incr(
		t.successCountKey(batchID),
	).Err()
}
func (t *RedisBatchTracker) getStoredErrorCount(
	batchID string,
) (int64, error) {

	count, err := t.redis.Get(
		t.failedCountKey(batchID),
	).Int64()

	if err == redis.Nil {
		return 0, nil
	}

	return count, err
}

func (t *RedisBatchTracker) incrementStoredError(
	batchID string,
) error {

	return t.redis.Incr(
		t.failedCountKey(batchID),
	).Err()
}

func (t *RedisBatchTracker) FinalizeBatch(
	batchID string,
) error {

	keys := []string{
		t.stateKey(batchID),
		t.completedKey(batchID),
		t.metadataKey(batchID),
		t.successKey(batchID),
		t.successCountKey(batchID),
		t.failedKey(batchID),
		t.failedCountKey(batchID),
	}

	pipe := t.redis.TxPipeline()

	for _, key := range keys {
		pipe.Expire(
			key,
			t.config.ResultTTL,
		)
	}

	_, err := pipe.Exec()

	return err
}

func (t *RedisBatchTracker) ListBatches() ([]*BatchState, error) {
	var cursor uint64
	var batchIDs []string
	seen := make(map[string]bool)

	for {
		var keys []string
		var err error

		keys, cursor, err = t.redis.Scan(cursor, "worker:batch:*", 100).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			// Split the key by colon ":"
			// worker:batch:{id} has length of 3
			// worker:batch:{id}:success has length of 4 (skip sub-keys)
			parts := strings.Split(key, ":")
			if len(parts) == 3 {
				batchID := parts[2]
				if !seen[batchID] {
					seen[batchID] = true
					batchIDs = append(batchIDs, batchID)
				}
			}
		}

		// If cursor returns to 0, the Redis scan is complete
		if cursor == 0 {
			break
		}
	}

	// Loop through all valid batch IDs and retrieve their respective states
	var batches []*BatchState
	for _, id := range batchIDs {
		state, err := t.Get(id)
		if err == nil {
			batches = append(batches, state)
		}
	}

	return batches, nil
}
