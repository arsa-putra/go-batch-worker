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

	redis "gopkg.in/redis.v5"
)

const maxRecentErrors = 10

type BulkWorker[T any] struct {
	ch chan T

	wg sync.WaitGroup

	workerCount int

	processor BulkProcessor[T]

	completion CompletionHandler

	tracker *RedisBatchTracker

	redis *redis.Client

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

func NewBulkWorker[T any](
	redisClient *redis.Client,
	workerCount int,
	bufferSize int,
	processor BulkProcessor[T],
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
		processor:   processor,
		redis:       redisClient,
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
	// Tambahkan baris ini biar keliatan worker ID berapa yang udah nyala
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
				w.markFailed(
					job,
					err,
				)
				w.tryComplete(
					job,
				)
				w.saveFailedJob(
					job,
					err,
				)

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
	id := fmt.Sprintf(
		"%d",
		time.Now().UnixNano(),
	)

	data := FailedJob[T]{
		ID:          id,
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

	if page <= 0 {
		page = 1
	}

	if size <= 0 {
		size = 10
	}

	listKey := fmt.Sprintf(
		"worker:%s:failed:list",
		w.Name(),
	)

	total, _ := w.redis.LLen(
		listKey,
	).Result()

	start := int64((page - 1) * size)
	end := start + int64(size) - 1

	ids, _ := w.redis.LRange(
		listKey,
		start,
		end,
	).Result()

	result := make(
		[]FailedJob[T],
		0,
	)

	for _, id := range ids {

		dataKey := fmt.Sprintf(
			"worker:%s:failed:data:%s",
			w.Name(),
			id,
		)

		raw, err := w.redis.Get(
			dataKey,
		).Result()

		if err != nil {
			continue
		}

		var item FailedJob[T]

		if err := json.Unmarshal(
			[]byte(raw),
			&item,
		); err == nil {

			result = append(
				result,
				item,
			)
		}
	}

	return result, int(total)
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

		if job.RetryCount >= MaxRetryCount {

			now := time.Now()

			job.IsDead = true
			job.DeadAt = &now
			job.Status = "dead"

		} else {

			job.Status = "pending"
		}

		payload, _ := json.Marshal(job)

		_ = w.redis.Set(
			dataKey,
			payload,
			0,
		).Err()
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

	w.markSuccess(
		job.Job,
	)
	w.tryComplete(
		job.Job,
	)
	workerRetrySuccessTotal.
		WithLabelValues(w.Name()).
		Inc()
	return nil
}

func (w *BulkWorker[T]) QueueSize() int {
	return len(w.ch)
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
	w.success.Add(1)
	w.processed.Add(1)

	// prometheus counter
	workerProcessed.WithLabelValues(w.Name()).Inc()
	workerSuccess.WithLabelValues(w.Name()).Inc()
	workerLastSuccessTimestamp.WithLabelValues(w.Name()).Set(float64(time.Now().Unix()))

	if w.tracker == nil {
		return
	}

	batchID, ok := GetBatchID(job)
	if !ok {
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
	w.failed.Add(1)
	w.processed.Add(1)

	w.setLastError(err)

	// promtehus counter
	workerProcessed.WithLabelValues(w.Name()).Inc()
	workerFailed.WithLabelValues(w.Name()).Inc()
	workerLastFailureTimestamp.WithLabelValues(w.Name()).Set(float64(time.Now().Unix()))

	if w.tracker == nil {
		return
	}

	batchID, ok := GetBatchID(job)
	if !ok {
		return
	}
	if trackable, ok := any(job).(ResultTrackable); ok {

		_ = w.tracker.CompleteFailed(
			batchID,
			trackable.FailedResult(err),
		)

		return
	}
	w.tracker.MarkFailed(
		batchID,
	)
}

func (w *BulkWorker[T]) tryComplete(
	job T,
) {

	if w.tracker == nil {
		return
	}

	batchID, ok := GetBatchID(job)
	if !ok {
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
