package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	redis "gopkg.in/redis.v5"
)

type BatchWorker[T any] struct {
	ch chan T

	wg sync.WaitGroup

	workerCount int
	batchSize   int
	flushEvery  time.Duration

	processor Processor[T]
	tracker   BatchTracker
	redis     *redis.Client

	closed atomic.Bool

	metrics WorkerMetrics
	stats   WorkerStats

	limiter *ResourceLimiter
}

func NewBatchWorker[T any](
	redisClient *redis.Client,
	workerCount int,
	bufferSize int,
	batchSize int,
	flushEvery time.Duration,
	processor Processor[T],
	tracker BatchTracker,
) *BatchWorker[T] {
	if workerCount <= 0 {
		workerCount = 1
	}

	if bufferSize <= 0 {
		bufferSize = 1000
	}

	if batchSize <= 0 {
		batchSize = 100
	}

	if flushEvery <= 0 {
		flushEvery = 2 * time.Second
	}

	return &BatchWorker[T]{
		ch:          make(chan T, bufferSize),
		workerCount: workerCount,
		batchSize:   batchSize,
		flushEvery:  flushEvery,
		processor:   processor,
		tracker:     tracker,
		redis:       redisClient,
		stats: WorkerStats{
			Name: processor.Name(),
		},
	}
}

func (w *BatchWorker[T]) Name() string {
	return w.processor.Name()
}

func (w *BatchWorker[T]) SetResourceLimiter(
	limiter *ResourceLimiter,
) {
	w.limiter = limiter
}

func (w *BatchWorker[T]) Info() map[string]interface{} {
	return map[string]interface{}{
		"name":         w.Name(),
		"worker_count": w.workerCount,
		"batch_size":   w.batchSize,
		"buffer_size":  cap(w.ch),
		"flush_every":  w.flushEvery.String(),
	}
}

func (w *BatchWorker[T]) Start(ctx context.Context) {
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

	batchPendingItems.
		WithLabelValues(w.Name()).
		Set(0)

	workerFailedBatches.
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

		go w.run(ctx, i+1)
	}
}

func (w *BatchWorker[T]) run(
	ctx context.Context,
	workerID int,
) {
	defer w.wg.Done()

	log.Printf(
		"[%s-%d] started",
		w.Name(),
		workerID,
	)

	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()

	batch := make([]T, 0, w.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		start := time.Now()

		cp := make([]T, len(batch))
		copy(cp, batch)

		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {

					workerPanicTotal.
						WithLabelValues(w.Name()).
						Inc()

					log.Printf(
						"[%s-%d] batch panic=%v",
						w.Name(),
						workerID,
						r,
					)

					err = fmt.Errorf(
						"panic: %v",
						r,
					)
				}
			}()

			return w.saveBatch(
				ctx,
				cp,
			)
		}()
		batchFlushDuration.
			WithLabelValues(w.Name()).
			Observe(
				time.Since(start).Seconds(),
			)

		batchFlushTotal.
			WithLabelValues(w.Name()).
			Inc()
		w.stats.LastFlushAt = time.Now()
		w.stats.LastBatchSize = len(cp)

		if err != nil {
			w.metrics.Failed.Add(1)

			log.Printf(
				"[%s-%d] batch failed size=%d err=%v",
				w.Name(),
				workerID,
				len(cp),
				err,
			)
		} else {
			w.metrics.Processed.Add(
				uint64(len(cp)),
			)

			w.metrics.Batches.Add(1)

			log.Printf(
				"[%s-%d] batch saved size=%d",
				w.Name(),
				workerID,
				len(cp),
			)
		}

		batch = batch[:0]
		batchPendingItems.
			WithLabelValues(w.Name()).
			Set(0)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf(
				"[%s-%d] shutting down flush=%d",
				w.Name(),
				workerID,
				len(batch),
			)

			flush()

			log.Printf(
				"[%s-%d] stopped",
				w.Name(),
				workerID,
			)

			return

		case item, ok := <-w.ch:
			if !ok {
				flush()

				log.Printf(
					"[%s-%d] channel closed",
					w.Name(),
					workerID,
				)

				return
			}

			batch = append(batch, item)
			batchPendingItems.
				WithLabelValues(w.Name()).
				Set(float64(len(batch)))
			if len(batch) >= w.batchSize {
				flush()
			}
			workerQueueSize.
				WithLabelValues(w.Name()).
				Set(float64(len(w.ch)))
		case <-ticker.C:
			flush()
		}
	}
}

func (w *BatchWorker[T]) Push(item T) {
	if w.closed.Load() {
		return
	}

	select {
	case w.ch <- item:
		workerQueueSize.
			WithLabelValues(w.Name()).
			Set(float64(len(w.ch)))
	default:
		w.metrics.Dropped.Add(1)

		log.Printf(
			"[%s] buffer full dropping item",
			w.Name(),
		)
	}
}

func (w *BatchWorker[T]) Close() {
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

func (w *BatchWorker[T]) Wait() {
	w.wg.Wait()

	log.Printf(
		"[%s] all workers stopped",
		w.Name(),
	)
}

func (w *BatchWorker[T]) saveBatch(
	ctx context.Context,
	items []T,
) error {
	err := retry(3, func() error {
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

		workerActiveJobs.
			WithLabelValues(w.Name()).
			Inc()

		defer workerActiveJobs.
			WithLabelValues(w.Name()).
			Dec()

		return w.processor.Process(
			ctx,
			items,
		)
	})

	if err != nil {
		w.saveFailedBatch(
			items,
			err,
		)

		return err
	}

	workerLastSuccessTimestamp.WithLabelValues(w.Name()).Set(float64(time.Now().Unix()))

	return nil
}

func (w *BatchWorker[T]) saveFailedBatch(
	items []T,
	err error,
) {
	id := fmt.Sprintf(
		"%d",
		time.Now().UnixNano(),
	)

	batch := FailedBatch[T]{
		ID:          id,
		CreatedAt:   time.Now(),
		Error:       err.Error(),
		Status:      "pending",
		RetryCount:  0,
		LastRetryAt: time.Time{},
		IsDead:      false,
		Logs:        items,
	}

	payload, _ := json.Marshal(batch)

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

	workerFailedBatches.
		WithLabelValues(w.Name()).
		Set(float64(total))

	workerLastFailureTimestamp.WithLabelValues(w.Name()).Set(float64(time.Now().Unix()))

}

func (w *BatchWorker[T]) FailedBatchesPaginated(
	page int,
	size int,
) ([]FailedBatch[T], int) {
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
		[]FailedBatch[T],
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

		var batch FailedBatch[T]

		if err := json.Unmarshal(
			[]byte(raw),
			&batch,
		); err == nil {
			result = append(
				result,
				batch,
			)
		}
	}

	return result, int(total)
}

func (w *BatchWorker[T]) RetryFailedBatch(id string) error {
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

	var batch FailedBatch[T]

	if err := json.Unmarshal(
		[]byte(raw),
		&batch,
	); err != nil {
		retryFailedMetric.Inc()
		return err
	}

	if batch.IsDead {
		return ErrDeadJob
	}

	workerActiveJobs.
		WithLabelValues(w.Name()).
		Inc()

	err = w.processor.Process(
		context.Background(),
		batch.Logs,
	)

	workerActiveJobs.
		WithLabelValues(w.Name()).
		Dec()

	if err != nil {
		retryFailedMetric.Inc()
		batch.RetryCount++
		batch.LastRetryAt = time.Now()

		if batch.RetryCount >= MaxRetryCount {

			now := time.Now()

			batch.IsDead = true
			batch.DeadAt = &now
			batch.Status = "dead"

		} else {

			batch.Status = "pending"
		}

		payload, _ := json.Marshal(batch)

		_ = w.redis.Set(
			dataKey,
			payload,
			0,
		).Err()
		return err
	}

	workerRetrySuccessTotal.
		WithLabelValues(w.Name()).
		Inc()

	// cleanup redis
	_ = w.redis.Del(dataKey).Err()
	_ = w.redis.LRem(listKey, 1, id).Err()

	total, _ := w.redis.LLen(
		listKey,
	).Result()

	workerFailedBatches.
		WithLabelValues(w.Name()).
		Set(float64(total))
	return nil
}

func (w *BatchWorker[T]) Dashboard() map[string]interface{} {
	totalFailed, _ := w.redis.LLen(
		fmt.Sprintf(
			"worker:%s:failed:list",
			w.Name(),
		),
	).Result()

	return map[string]interface{}{
		"name": w.stats.Name,
		"queue": map[string]interface{}{
			"current":       len(w.ch),
			"capacity":      cap(w.ch),
			"usage_percent": float64(len(w.ch)) / float64(cap(w.ch)) * 100,
		},
		"config": map[string]interface{}{
			"worker_count": w.workerCount,
			"batch_size":   w.batchSize,
			"flush_every":  w.flushEvery.String(),
		},
		"metrics": map[string]interface{}{
			"processed":              w.metrics.Processed.Load(),
			"dropped":                w.metrics.Dropped.Load(),
			"failed":                 w.metrics.Failed.Load(),
			"batches":                w.metrics.Batches.Load(),
			"pending_failed_batches": totalFailed,
		},
		"stats": w.stats,
	}
}
