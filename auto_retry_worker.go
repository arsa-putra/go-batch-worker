package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	redis "gopkg.in/redis.v5"
)

const MaxRetryCount = 3

type AutoRetryWorker struct {
	redis *redis.Client

	interval time.Duration

	bulkWorkers  []RetryableJobWorker
	batchWorkers []RetryableBatchWorker

	wg sync.WaitGroup
}

type RetryableJobWorker interface {
	Name() string
	RetryFailedJob(id string) error
}

type RetryableBatchWorker interface {
	Name() string
	RetryFailedBatch(id string) error
}

func NewAutoRetryWorker(
	redisClient *redis.Client,
	interval time.Duration,
) *AutoRetryWorker {

	if interval <= 0 {
		interval = time.Minute
	}

	return &AutoRetryWorker{
		redis:    redisClient,
		interval: interval,
	}
}

func (w *AutoRetryWorker) RegisterBulk(
	worker RetryableJobWorker,
) {
	w.bulkWorkers = append(
		w.bulkWorkers,
		worker,
	)
}

func (w *AutoRetryWorker) RegisterBatch(
	worker RetryableBatchWorker,
) {
	w.batchWorkers = append(
		w.batchWorkers,
		worker,
	)
}

func (w *AutoRetryWorker) Name() string {
	return "auto_retry"
}

func (w *AutoRetryWorker) Start(ctx context.Context) {
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()

		ticker := time.NewTicker(
			w.interval,
		)
		defer ticker.Stop()

		for {
			select {

			case <-ctx.Done():
				return

			case <-ticker.C:
				// scan failed jobs

				w.retryBulkWorkers()

				w.retryBatchWorkers()
			}
		}
	}()
}

func (w *AutoRetryWorker) Close() {}

func (w *AutoRetryWorker) Wait() {
	w.wg.Wait()
}

func (w *AutoRetryWorker) retryBulkWorkers() {

	for _, worker := range w.bulkWorkers {

		listKey := fmt.Sprintf(
			"worker:%s:failed:list",
			worker.Name(),
		)

		ids, err := w.redis.LRange(
			listKey,
			0,
			10,
		).Result()

		if err != nil {
			continue
		}

		for _, id := range ids {
			workerAutoRetryTotal.
				WithLabelValues(worker.Name()).
				Inc()

			err := worker.RetryFailedJob(id)

			switch {

			case err == nil:

				workerAutoRetrySuccessTotal.
					WithLabelValues(worker.Name()).
					Inc()

			case errors.Is(err, ErrDeadJob):

				// job sudah masuk DLQ
				// keluarkan dari retry list
				_ = w.redis.LRem(
					listKey,
					1,
					id,
				).Err()

				continue

			default:

				workerAutoRetryFailedTotal.
					WithLabelValues(worker.Name()).
					Inc()

				log.Printf(
					"[auto-retry] worker=%s id=%s error=%v",
					worker.Name(),
					id,
					err,
				)
			}
		}
	}
}

func (w *AutoRetryWorker) retryBatchWorkers() {

	for _, worker := range w.batchWorkers {

		listKey := fmt.Sprintf(
			"worker:%s:failed:list",
			worker.Name(),
		)

		ids, err := w.redis.LRange(
			listKey,
			0,
			10,
		).Result()

		if err != nil {
			continue
		}

		for _, id := range ids {

			workerAutoRetryTotal.
				WithLabelValues(worker.Name()).
				Inc()

			err := worker.RetryFailedBatch(id)

			switch {

			case err == nil:

				workerAutoRetrySuccessTotal.
					WithLabelValues(worker.Name()).
					Inc()

			case errors.Is(err, ErrDeadJob):

				// job sudah masuk DLQ
				// keluarkan dari retry list
				_ = w.redis.LRem(
					listKey,
					1,
					id,
				).Err()

				continue

			default:

				workerAutoRetryFailedTotal.
					WithLabelValues(worker.Name()).
					Inc()

				log.Printf(
					"[auto-retry-batch] worker=%s retry id=%s",
					worker.Name(),
					id,
				)
			}
		}
	}
}
