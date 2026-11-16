package worker

import (
	"context"
	"errors"
	"time"
)

func (w *WebhookRetryWorker) Start(
	ctx context.Context,
) {

	w.wg.Add(1)

	go func() {

		defer w.wg.Done()

		ticker := time.NewTicker(
			w.config.PollInterval,
		)

		defer ticker.Stop()

		for {

			select {

			case <-ctx.Done():
				return

			case <-w.stop:
				return

			case <-ticker.C:

				w.runOnce()
			}
		}
	}()
}

func (w *WebhookRetryWorker) runOnce() {
	now := time.Now().Unix()
	if err := ensureWebhookRetryIndex(w.tracker.redis); err != nil {
		if errors.Is(err, errWebhookRetryIndexBuilding) {
			_ = forEachWebhookRetryState(w.tracker.redis, func(state *WebhookRetryState) error {
				w.processRetryState(state, now)
				return nil
			})
		}
		return
	}

	states, err := findDueWebhookRetryStates(w.tracker.redis, now, webhookRetryIndexBatch)
	if err != nil {
		return
	}
	for _, state := range states {
		w.processRetryState(state, now)
	}
}

func (w *WebhookRetryWorker) processRetryState(state *WebhookRetryState, now int64) {
	if state.NextRetryAt > now {
		return
	}

	ok, err := ClaimWebhookRetry(
		w.tracker.redis,
		state.BatchID,
	)

	if err != nil {
		return
	}

	if !ok {
		return
	}

	func() {

		defer ReleaseWebhookRetry(
			w.tracker.redis,
			state.BatchID,
		)

		_ = w.Process(
			state,
		)

	}()
}

func (w *WebhookRetryWorker) Close() {

	select {

	case <-w.stop:
		return

	default:
		close(
			w.stop,
		)
	}
}

func (w *WebhookRetryWorker) Wait() {
	w.wg.Wait()
}
