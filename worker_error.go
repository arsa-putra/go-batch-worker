package worker

import "time"

type WorkerError struct {
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}

func (w *BulkWorker[T]) setLastError(
	err error,
) {
	if err == nil {
		return
	}

	workerErr := WorkerError{
		Message: err.Error(),
		At:      time.Now(),
	}

	w.lastError.Store(workerErr)

	w.addRecentError(workerErr)
	w.incrementErrorCount(err)
}

func (w *BulkWorker[T]) addRecentError(
	err WorkerError,
) {
	if err.Message == "" {
		return
	}

	w.errorMu.Lock()
	defer w.errorMu.Unlock()

	w.recentErrors = append(w.recentErrors, err)

	if len(w.recentErrors) > maxRecentErrors {
		w.recentErrors =
			w.recentErrors[len(w.recentErrors)-maxRecentErrors:]
	}
}

func (w *BulkWorker[T]) incrementErrorCount(
	err error,
) {

	if err == nil {
		return
	}

	w.errorCountMu.Lock()
	defer w.errorCountMu.Unlock()

	w.errorCounts[err.Error()]++
}
