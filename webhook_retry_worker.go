package worker

import "sync"

type WebhookRetryWorker struct {
	tracker *RedisBatchTracker
	client  *WebhookClient
	config  WebhookConfig

	stop chan struct{}
	wg   sync.WaitGroup
}

func NewWebhookRetryWorker(
	tracker *RedisBatchTracker,
	config WebhookConfig,
) *WebhookRetryWorker {

	return &WebhookRetryWorker{
		tracker: tracker,
		client:  NewWebhookClient(),
		config:  config,
		stop:    make(chan struct{}),
	}
}

func (w *WebhookRetryWorker) Name() string {
	return "webhook_retry"
}

func (w *WebhookRetryWorker) Process(
	job *WebhookRetryState,
) error {

	metadata, err :=
		w.tracker.LoadMetadata(
			job.BatchID,
		)

	if err != nil {
		return err
	}

	if metadata.CallbackURL == "" {
		return DeleteWebhookRetryState(
			w.tracker.redis,
			job.BatchID,
		)
	}

	result, err :=
		w.tracker.BuildResult(
			job.BatchID,
		)

	if err != nil {
		return err
	}

	payload :=
		BuildWebhookPayload(
			result,
		)

	err = w.client.Send(
		metadata,
		payload,
	)

	if err == nil {
		_ = w.tracker.FinalizeBatch(
			job.BatchID,
		)
		return DeleteWebhookRetryState(
			w.tracker.redis,
			job.BatchID,
		)
	}

	job.Attempt++

	if job.Attempt >
		w.config.MaxAttempts {
		_ = w.tracker.FinalizeBatch(
			job.BatchID,
		)
		return DeleteWebhookRetryState(
			w.tracker.redis,
			job.BatchID,
		)
	}

	return ScheduleWebhookRetry(
		w.tracker.redis,
		job.BatchID,
		job.Attempt,
	)
}

func (w *WebhookRetryWorker) Dashboard() map[string]interface{} {

	states, _ :=
		FindWebhookRetryStates(
			w.tracker.redis,
		)

	return map[string]interface{}{
		"name": "webhook_retry",

		"pending_retries": len(states),

		"max_attempts": w.config.MaxAttempts,
	}
}
