package worker

type BatchItemResult struct {
	Key     string      `json:"key"`
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type BatchResult struct {
	BatchID string `json:"batch_id"`

	Total int64 `json:"total"`

	Success    int64 `json:"success"`
	Failed     int64 `json:"failed"`
	FinishedAt int64 `json:"finished_at"`

	SuccessItems []BatchItemResult `json:"success_items"`
	FailedItems  []BatchItemResult `json:"failed_items"`

	StoredErrors    int64 `json:"stored_errors"`
	StoredSuccesses int64 `json:"stored_successes"`
}

type ResultTrackable interface {
	SuccessResult() BatchItemResult
	FailedResult(err error) BatchItemResult
}
