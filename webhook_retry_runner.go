package worker

import (
	"context"
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

	states, err :=
		FindWebhookRetryStates(
			w.tracker.redis,
		)

	if err != nil {
		return
	}

	now := time.Now().Unix()

	for _, state := range states {

		if state.NextRetryAt > now {
			continue
		}

		ok, err := ClaimWebhookRetry(
			w.tracker.redis,
			state.BatchID,
		)

		if err != nil {
			continue
		}

		if !ok {
			continue
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
