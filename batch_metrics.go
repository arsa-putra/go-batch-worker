package worker

import "github.com/prometheus/client_golang/prometheus"

var (
	batchCreatedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "batch_created_total",
			Help:      "Total batches created",
		},
	)

	batchCompletedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "batch_completed_total",
			Help:      "Total batches completed",
		},
	)

	batchCancelledTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "batch_cancelled_total",
			Help:      "Total batches cancelled",
		},
	)

	batchItemsSuccessTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "batch_items_success_total",
			Help:      "Total successful batch items",
		},
	)

	batchItemsFailedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "batch_items_failed_total",
			Help:      "Total failed batch items",
		},
	)

	batchFlushTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "batch_flush_total",
			Help:      "Total batch flushes",
		},
		[]string{"worker"},
	)

	batchFlushFailedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "batch_flush_failed_total",
			Help:      "Total failed batch flushes",
		},
		[]string{"worker"},
	)

	batchFlushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dsaapi",
			Name:      "batch_flush_duration_seconds",
			Help:      "Batch flush duration",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"worker"},
	)

	batchPendingItems = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "batch_pending_items",
			Help:      "Items waiting in current batch buffer",
		},
		[]string{"worker"},
	)

	workerFailedBatches = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "worker_failed_batches",
			Help:      "Current failed batches stored",
		},
		[]string{"worker"},
	)
)

func init() {
	prometheus.MustRegister(
		batchCreatedTotal,
		batchCompletedTotal,
		batchCancelledTotal,
		batchItemsSuccessTotal,
		batchItemsFailedTotal,
		batchFlushTotal,
		batchFlushFailedTotal,
		batchFlushDuration,
		batchPendingItems,
		workerFailedBatches,
	)
}
