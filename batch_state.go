package worker

type BatchState struct {
	BatchID string `json:"batch_id"`

	Total int64 `json:"total"`

	Done int64 `json:"done"`

	Success int64 `json:"success"`

	Failed int64 `json:"failed"`
}
