package worker

import "fmt"

func (t *RedisBatchTracker) cancelledKey(
	batchID string,
) string {

	return fmt.Sprintf(
		"worker:batch:%s:cancelled",
		batchID,
	)
}

func (t *RedisBatchTracker) CancelBatch(
	batchID string,
) error {
	batchCancelledTotal.Inc()

	return t.redis.Set(
		t.cancelledKey(batchID),
		"1",
		t.config.ResultTTL,
	).Err()
}

func (t *RedisBatchTracker) IsCancelled(
	batchID string,
) (bool, error) {

	exists, err := t.redis.Exists(
		t.cancelledKey(batchID),
	).Result()

	if err != nil {
		return false, err
	}

	return exists, nil
}

func (t *RedisBatchTracker) CompleteCancelled(
	batchID string,
	result BatchItemResult,
) error {

	result.Success = false
	result.Message = "batch cancelled"

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

func (w *BulkWorker[T]) markCancelled(
	job T,
) {

	if w.tracker == nil {
		return
	}

	batchID, ok := GetBatchID(job)

	if !ok {
		return
	}

	if trackable, ok := any(job).(ResultTrackable); ok {

		result := trackable.FailedResult(nil)
		result.Message = "batch cancelled"

		_ = w.tracker.CompleteCancelled(
			batchID,
			result,
		)
		batchCancelledTotal.Inc()

		return
	}

	_ = w.tracker.MarkFailed(
		batchID,
	)
}
