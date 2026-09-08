package worker

type WebhookCompletion struct {
	tracker *RedisBatchTracker
	client  *WebhookClient
}

func NewWebhookCompletion(
	tracker *RedisBatchTracker,
) *WebhookCompletion {

	return &WebhookCompletion{
		tracker: tracker,
		client:  NewWebhookClient(),
	}
}

func BuildWebhookPayload(
	result *BatchResult,
) BatchResult {

	return BatchResult{
		BatchID: result.BatchID,

		Total:   result.Total,
		Success: result.Success,
		Failed:  result.Failed,

		FinishedAt: result.FinishedAt,

		StoredSuccesses: result.StoredSuccesses,
		StoredErrors:    result.StoredErrors,

		SuccessItems: result.SuccessItems,
		FailedItems:  result.FailedItems,
	}
}

func (h *WebhookCompletion) OnCompleted(
	batchID string,
) error {

	metadata, err :=
		h.tracker.LoadMetadata(batchID)

	if err != nil {
		return err
	}

	if metadata.CallbackURL == "" {
		return h.tracker.FinalizeBatch(
			batchID,
		)
	}

	result, err :=
		h.tracker.BuildResult(batchID)

	if err != nil {
		return err
	}

	payload := BuildWebhookPayload(result)

	err = h.client.Send(
		metadata,
		payload,
	)

	if err != nil {
		_ = ScheduleWebhookRetry(
			h.tracker.redis,
			batchID,
			1,
		)
		return err
	}

	return h.tracker.FinalizeBatch(
		batchID,
	)
}
