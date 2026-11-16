package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const maxRecentErrors = 10

type BulkWorker[T any] struct {
	ch chan T

	wg sync.WaitGroup

	workerCount int
	retryConfig RetryConfig

	processor BulkProcessor[T]

	completion CompletionHandler

	tracker *RedisBatchTracker

	redis *redisAdapter

	closed atomic.Bool

	processed atomic.Int64
	success   atomic.Int64
	failed    atomic.Int64

	lastError    atomic.Value
	recentErrors []WorkerError
	errorMu      sync.RWMutex
	errorCounts  map[string]int64
	errorCountMu sync.RWMutex

	limiter *ResourceLimiter
}

type ItemIdentifiable interface {
	GetBatchID() string
	GetItemID() string
}

func NewBulkWorker[T any](
	redisClient interface{},
	workerCount int,
	bufferSize int,
	processor BulkProcessor[T],
	retryConfigs ...RetryConfig,
) *BulkWorker[T] {

	if workerCount <= 0 {
		workerCount = 1
	}

	if bufferSize <= 0 {
		bufferSize = 1000
	}

	return &BulkWorker[T]{
		ch: make(
			chan T,
			bufferSize,
		),

		workerCount: workerCount,
		retryConfig: normalizeRetryConfig(retryConfigs),
		processor:   processor,
		redis:       newRedisAdapter(redisClient),
		errorCounts: make(
			map[string]int64,
		),
	}
}

func (w *BulkWorker[T]) Name() string {
	return w.processor.Name()
}

func (w *BulkWorker[T]) SetResourceLimiter(
	limiter *ResourceLimiter,
) {
	w.limiter = limiter
}

var ErrQueueFull = errors.New("worker queue full")
var ErrWorkerClosed = errors.New("worker is closed")

func (w *BulkWorker[T]) Submit(
	job T,
) error {

	if w.closed.Load() {
		return ErrWorkerClosed
	}

	if w.tracker != nil {
		if identifiable, ok := any(job).(ItemIdentifiable); ok {
			batchID := identifiable.GetBatchID()

			// Batch pending data and the job index in one Redis pipeline.
			_ = w.tracker.trackPendingPayloadAndLinkJob(
				batchID,
				identifiable.GetItemID(),
				w.Name(),
				job,
			)
		}
	}

	select {

	case w.ch <- job:
		workerQueueSize.
			WithLabelValues(w.Name()).
			Set(float64(len(w.ch)))
		return nil

	default:
		return ErrQueueFull
	}
}

func (w *BulkWorker[T]) Start(
	ctx context.Context,
) {
	log.Printf(
		"[%s] starting %d worker(s)...",
		w.Name(),
		w.workerCount,
	)
	workerActiveJobs.
		WithLabelValues(w.Name()).
		Set(0)

	workerQueueSize.
		WithLabelValues(w.Name()).
		Set(0)

	workerFailedJobs.
		WithLabelValues(w.Name()).
		Set(0)

	workerQueueCapacity.
		WithLabelValues(w.Name()).
		Set(float64(cap(w.ch)))

	workerWorkers.
		WithLabelValues(w.Name()).
		Set(float64(w.workerCount))

	for i := 0; i < w.workerCount; i++ {
		w.wg.Add(1)

		go w.run(
			ctx,
			i+1,
		)
	}
}

func (w *BulkWorker[T]) run(
	ctx context.Context,
	workerID int,
) {
	defer w.wg.Done()
	// Log worker startup to identify which worker ID is currently running
	log.Printf("[%s-%d] started", w.Name(), workerID)
	for {
		select {

		case <-ctx.Done():
			return

		case job, ok := <-w.ch:

			if !ok {
				log.Printf(
					"[%s-%d] channel closed",
					w.Name(),
					workerID,
				)
				return
			}
			workerQueueSize.
				WithLabelValues(w.Name()).
				Set(float64(len(w.ch)))
			batchID, ok := GetBatchID(job)
			if ok && w.tracker != nil {
				w.updatePendingPayloadStatus(job, "processing", 0)
				cancelled, err :=
					w.tracker.IsCancelled(
						batchID,
					)

				if err == nil && cancelled {
					w.markCancelled(
						job,
					)

					w.tryComplete(
						job,
					)
					continue
				}
			}

			start := time.Now()
			err := func() (err error) {
				if w.limiter != nil &&
					w.limiter.DB != nil {

					if err := w.limiter.DB.Acquire(ctx, 1); err != nil {
						return err
					}

					workerDBLimiterActive.Inc()

					defer func() {
						workerDBLimiterActive.Dec()
						w.limiter.DB.Release(1)
					}()
				}

				workerActiveJobs.WithLabelValues(w.Name()).Inc()

				defer func() {
					workerDuration.
						WithLabelValues(w.Name()).Observe(time.Since(start).Seconds())

					workerActiveJobs.WithLabelValues(w.Name()).Dec()

					if r := recover(); r != nil {

						workerPanicTotal.WithLabelValues(w.Name()).Inc()

						log.Printf(
							"[%s] worker panic worker_id=%d panic=%v",
							w.Name(),
							workerID,
							r,
						)
						err = fmt.Errorf("panic: %v", r)
					}
				}()

				return w.processor.Process(ctx, job)
			}()

			if err != nil {
				if errors.Is(err, ErrSkipRetry) {
					w.markFailed(job, err)
					w.tryComplete(job)
					continue
				}

				// Leave retryable items in processing until a retry reaches a
				// terminal result so completion callbacks run only after retries.
				w.markFailedAttempt(job, err)
				w.saveFailedJob(
					job,
					err,
				)
				w.updatePendingPayloadStatus(job, "waiting_retry", 1)

				continue
			}
			w.markSuccess(
				job,
			)
			w.tryComplete(
				job,
			)
		}
	}
}

func (w *BulkWorker[T]) Close() {
	if w.closed.Load() {
		return
	}

	w.closed.Store(true)

	close(w.ch)

	log.Printf(
		"[%s] intake channel closed",
		w.Name(),
	)
}

func (w *BulkWorker[T]) Wait() {
	w.wg.Wait()
}

func (w *BulkWorker[T]) saveFailedJob(
	job T,
	err error,
) {
	batchID, _ := GetBatchID(job)
	id := fmt.Sprintf(
		"%d",
		time.Now().UnixNano(),
	)

	data := FailedJob[T]{
		ID:          id,
		BatchID:     batchID,
		CreatedAt:   time.Now(),
		Error:       err.Error(),
		Status:      "pending",
		RetryCount:  0,
		LastRetryAt: time.Time{},
		IsDead:      false,
		Job:         job,
	}

	payload, _ := json.Marshal(
		data,
	)

	listKey := fmt.Sprintf(
		"worker:%s:failed:list",
		w.Name(),
	)

	dataKey := fmt.Sprintf(
		"worker:%s:failed:data:%s",
		w.Name(),
		id,
	)

	_ = w.redis.Set(
		dataKey,
		payload,
		0,
	).Err()

	_ = w.redis.LPush(
		listKey,
		id,
	).Err()

	total, _ := w.redis.LLen(
		listKey,
	).Result()

	workerFailedJobs.
		WithLabelValues(w.Name()).
		Set(float64(total))
}

func (w *BulkWorker[T]) FailedJobsPaginated(
	page int,
	size int,
) ([]FailedJob[T], int) {
	page, size = normalizeFailedPage(page, size)

	listKey := fmt.Sprintf(
		"worker:%s:failed:list",
		w.Name(),
	)

	total, _ := w.redis.LLen(
		listKey,
	).Result()

	if total <= 0 {
		return []FailedJob[T]{}, int(total)
	}
	pageOffset := int64(page - 1)
	pageSize := int64(size)
	if pageOffset > (total-1)/pageSize {
		return []FailedJob[T]{}, int(total)
	}
	start := pageOffset * pageSize
	end := start + pageSize - 1

	ids, _ := w.redis.LRange(
		listKey,
		start,
		end,
	).Result()

	return loadFailedRecords[FailedJob[T]](w.redis, w.Name(), ids), int(total)
}

func normalizeFailedPage(page, size int) (int, int) {
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 10
	}
	return page, size
}

func loadFailedRecords[T any](redis *redisAdapter, workerName string, ids []string) []T {
	if len(ids) == 0 {
		return []T{}
	}

	result := make([]T, 0, len(ids))
	const pipelineBatchSize = 100
	for start := 0; start < len(ids); start += pipelineBatchSize {
		end := start + pipelineBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		batchIDs := ids[start:end]
		pipe := redis.Pipeline()
		commands := make([]redisPipelineStringCmd, len(batchIDs))
		for i, id := range batchIDs {
			key := fmt.Sprintf("worker:%s:failed:data:%s", workerName, id)
			commands[i] = pipe.Get(key)
		}
		_ = pipe.Exec()

		for _, command := range commands {
			raw, err := command.Result()
			if err != nil {
				continue
			}
			var item T
			if json.Unmarshal([]byte(raw), &item) == nil {
				result = append(result, item)
			}
		}
	}
	return result
}

func (w *BulkWorker[T]) RetryFailedJob(
	id string,
) error {
	ctx := context.Background()

	workerRetryTotal.
		WithLabelValues(w.Name()).
		Inc()
	retryFailedMetric :=
		workerRetryFailedTotal.
			WithLabelValues(w.Name())
	dataKey := fmt.Sprintf(
		"worker:%s:failed:data:%s",
		w.Name(),
		id,
	)

	listKey := fmt.Sprintf(
		"worker:%s:failed:list",
		w.Name(),
	)

	raw, err := w.redis.Get(
		dataKey,
	).Result()

	if err != nil {
		retryFailedMetric.Inc()
		return err
	}

	var job FailedJob[T]

	if err := json.Unmarshal(
		[]byte(raw),
		&job,
	); err != nil {
		retryFailedMetric.Inc()
		return err
	}

	if job.IsDead {
		return ErrDeadJob
	}
	batchID := job.BatchID
	if batchID == "" {
		batchID, _ = GetBatchID(job.Job)
	}
	if w.limiter != nil &&
		w.limiter.DB != nil {

		if err := w.limiter.DB.Acquire(ctx, 1); err != nil {
			return err
		}

		workerDBLimiterActive.Inc()

		defer func() {
			workerDBLimiterActive.Dec()
			w.limiter.DB.Release(1)
		}()
	}
	w.updatePendingPayloadStatusForBatch(job.Job, batchID, "retrying", job.RetryCount+1)

	workerActiveJobs.WithLabelValues(w.Name()).Inc()
	defer workerActiveJobs.WithLabelValues(w.Name()).Dec()

	err = w.processor.Process(ctx, job.Job)

	if err != nil {
		// w.markFailed(
		// 	job.Job,
		// 	err,
		// )
		// w.tryComplete(
		// 	job.Job,
		// )
		retryFailedMetric.Inc()

		job.RetryCount++
		job.LastRetryAt = time.Now()
		if errors.Is(err, ErrSkipRetry) {
			now := time.Now()
			job.IsDead = true
			job.SkipRetry = true
			job.DeadAt = &now
			job.Status = "failed_skip_retry"

			payload, _ := json.Marshal(job)
			_ = w.redis.Set(dataKey, payload, 0).Err()
			w.recordFailedForBatch(job.Job, batchID, err)
			w.tryCompleteBatch(batchID)
			return ErrDeadJob
		}

		if job.RetryCount >= w.retryConfig.MaxRetries {

			now := time.Now()

			job.IsDead = true
			job.DeadAt = &now
			job.Status = "dead"
			payload, _ := json.Marshal(job)
			_ = w.redis.Set(dataKey, payload, 0).Err()
			w.recordFailedForBatch(job.Job, batchID, err)
			w.tryCompleteBatch(batchID)
			return ErrDeadJob
		} else {

			job.Status = "pending"
		}

		payload, _ := json.Marshal(job)

		_ = w.redis.Set(
			dataKey,
			payload,
			0,
		).Err()
		w.updatePendingPayloadStatusForBatch(job.Job, batchID, "waiting_retry", job.RetryCount+1)
		return err
	}

	_ = w.redis.Del(
		dataKey,
	).Err()

	_ = w.redis.LRem(
		listKey,
		1,
		id,
	).Err()
	total, _ := w.redis.LLen(
		listKey,
	).Result()

	workerFailedJobs.
		WithLabelValues(w.Name()).
		Set(float64(total))

	w.markSuccessForBatch(job.Job, batchID)
	w.tryCompleteBatch(batchID)
	workerRetrySuccessTotal.
		WithLabelValues(w.Name()).
		Inc()
	return nil
}

func (w *BulkWorker[T]) QueueSize() int {
	return len(w.ch)
}

func (w *BulkWorker[T]) updatePendingPayloadStatus(
	job T,
	status string,
	retryAttempt int,
) {
	batchID, _ := GetBatchID(job)
	w.updatePendingPayloadStatusForBatch(job, batchID, status, retryAttempt)
}

func (w *BulkWorker[T]) updatePendingPayloadStatusForBatch(
	job T,
	batchID string,
	status string,
	retryAttempt int,
) {
	if w.tracker == nil || batchID == "" {
		return
	}
	identifiable, ok := any(job).(ItemIdentifiable)
	if !ok {
		return
	}
	_ = w.tracker.UpdatePendingPayloadStatus(
		batchID,
		identifiable.GetItemID(),
		status,
		retryAttempt,
	)
}

func (w *BulkWorker[T]) QueueCapacity() int {
	return cap(w.ch)
}

func (w *BulkWorker[T]) SetCompletionHandler(handler CompletionHandler) {
	w.completion = handler
}

func (w *BulkWorker[T]) SetTracker(tracker *RedisBatchTracker) {
	w.tracker = tracker
}

func (w *BulkWorker[T]) markSuccess(job T) {
	batchID, _ := GetBatchID(job)
	w.markSuccessForBatch(job, batchID)
}

func (w *BulkWorker[T]) markSuccessForBatch(job T, batchID string) {
	w.success.Add(1)
	w.processed.Add(1)

	// prometheus counter
	workerProcessed.WithLabelValues(w.Name()).Inc()
	workerSuccess.WithLabelValues(w.Name()).Inc()
	workerLastSuccessTimestamp.WithLabelValues(w.Name()).Set(float64(time.Now().Unix()))

	if w.tracker == nil || batchID == "" {
		return
	}
	if trackable, ok := any(job).(ResultTrackable); ok {

		_ = w.tracker.CompleteSuccess(
			batchID,
			trackable.SuccessResult(),
		)

		return
	}
	w.tracker.MarkSuccess(
		batchID,
	)
}

func (w *BulkWorker[T]) markFailed(job T, err error) {
	w.markFailedAttempt(job, err)
	w.recordFailed(job, err)
}

func (w *BulkWorker[T]) markFailedAttempt(job T, err error) {
	w.failed.Add(1)
	w.processed.Add(1)

	w.setLastError(err)

	// promtehus counter
	workerProcessed.WithLabelValues(w.Name()).Inc()
	workerFailed.WithLabelValues(w.Name()).Inc()
	workerLastFailureTimestamp.WithLabelValues(w.Name()).Set(float64(time.Now().Unix()))
}

func (w *BulkWorker[T]) recordFailed(job T, err error) {
	batchID, _ := GetBatchID(job)
	w.recordFailedForBatch(job, batchID, err)
}

func (w *BulkWorker[T]) recordFailedForBatch(job T, batchID string, err error) {
	if w.tracker == nil || batchID == "" {
		return
	}
	if trackable, ok := any(job).(ResultTrackable); ok {
		result := trackable.FailedResult(err)
		if errors.Is(err, ErrSkipRetry) {
			_ = w.tracker.CompleteFailedWithSkipRetry(batchID, result)
		} else {
			_ = w.tracker.CompleteFailed(batchID, result)
		}

		return
	}
	w.tracker.MarkFailed(
		batchID,
	)
}

func (w *BulkWorker[T]) tryComplete(
	job T,
) {
	batchID, _ := GetBatchID(job)
	w.tryCompleteBatch(batchID)
}

func (w *BulkWorker[T]) tryCompleteBatch(batchID string) {
	if w.tracker == nil || batchID == "" {
		return
	}

	completed, err := w.tracker.TryComplete(
		batchID,
	)

	if err != nil {
		log.Printf(
			"[%s] try complete error batch=%s err=%v",
			w.Name(),
			batchID,
			err,
		)

		return
	}

	if !completed {
		return
	}

	if w.completion == nil {
		return
	}

	err = w.completion.OnCompleted(
		batchID,
	)

	if err != nil {
		log.Printf(
			"[%s] completion handler error batch=%s err=%v",
			w.Name(),
			batchID,
			err,
		)
	}
}

func (w *BulkWorker[T]) Dashboard() map[string]interface{} {
	var lastErr any

	if v := w.lastError.Load(); v != nil {
		lastErr = v
	}
	w.errorMu.RLock()

	recentErrors := append(
		[]WorkerError(nil),
		w.recentErrors...,
	)

	w.errorMu.RUnlock()

	return map[string]interface{}{
		"name":           w.Name(),
		"queue_size":     w.QueueSize(),
		"queue_capacity": w.QueueCapacity(),
		"workers":        w.workerCount,

		"processed":     w.processed.Load(),
		"success":       w.success.Load(),
		"failed":        w.failed.Load(),
		"last_error":    lastErr,
		"recent_errors": recentErrors,
		"top_errors":    w.TopErrors(defaultTopErrors),
	}
}
