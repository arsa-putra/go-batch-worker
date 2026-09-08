package worker

import "github.com/prometheus/client_golang/prometheus"

var (
	workerProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_processed_total",
			Help:      "Total processed jobs",
		},
		[]string{"worker"},
	)

	workerSuccess = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_success_total",
			Help:      "Total successful jobs",
		},
		[]string{"worker"},
	)

	workerFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_failed_total",
			Help:      "Total failed jobs",
		},
		[]string{"worker"},
	)

	workerQueueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "worker_queue_size",
			Help:      "Current queue size",
		},
		[]string{"worker"},
	)

	workerActiveJobs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "worker_active_jobs",
			Help:      "Current active jobs",
		},
		[]string{"worker"},
	)

	workerWorkers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "worker_workers",
			Help:      "Worker count",
		},
		[]string{"worker"},
	)
	workerQueueCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "worker_queue_capacity",
			Help:      "Queue capacity",
		},
		[]string{"worker"},
	)
	workerDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dsaapi",
			Name:      "worker_duration_seconds",
			Help:      "Job processing duration",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"worker"},
	)

	workerRetryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_retry_total",
			Help:      "Total retry attempts",
		},
		[]string{"worker"},
	)

	workerRetrySuccessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_retry_success_total",
			Help:      "Total successful retries",
		},
		[]string{"worker"},
	)

	workerRetryFailedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_retry_failed_total",
			Help:      "Total failed retries",
		},
		[]string{"worker"},
	)

	workerFailedJobs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "worker_failed_jobs",
			Help:      "Current failed jobs stored",
		},
		[]string{"worker"},
	)

	workerPanicTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_panic_total",
			Help:      "Total worker panics recovered",
		},
		[]string{"worker"},
	)

	workerLastSuccessTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "worker_last_success_timestamp_seconds",
			Help:      "Unix timestamp of last successful job",
		},
		[]string{"worker"},
	)

	workerLastFailureTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "worker_last_failure_timestamp_seconds",
			Help:      "Unix timestamp of last failed job",
		},
		[]string{"worker"},
	)

	workerAutoRetryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_auto_retry_total",
			Help:      "Total automatic retry attempts",
		},
		[]string{"worker"},
	)

	workerAutoRetrySuccessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_auto_retry_success_total",
			Help:      "Total successful automatic retries",
		},
		[]string{"worker"},
	)

	workerAutoRetryFailedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dsaapi",
			Name:      "worker_auto_retry_failed_total",
			Help:      "Total failed automatic retries",
		},
		[]string{"worker"},
	)

	workerDBLimiterActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dsaapi",
			Name:      "worker_db_limiter_active",
			Help:      "Current DB limiter slots in use",
		},
	)
)

func init() {

	prometheus.MustRegister(
		workerProcessed,
		workerSuccess,
		workerFailed,
		workerQueueSize,
		workerActiveJobs,
		workerWorkers,
		workerQueueCapacity,
		workerDuration,
		workerRetryTotal,
		workerRetrySuccessTotal,
		workerRetryFailedTotal,
		workerFailedJobs,
		workerPanicTotal,
		workerLastSuccessTimestamp,
		workerLastFailureTimestamp,
		workerAutoRetryTotal,
		workerAutoRetrySuccessTotal,
		workerAutoRetryFailedTotal,
		workerDBLimiterActive,
	)
}
