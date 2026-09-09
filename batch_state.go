package worker

type BatchState struct {
	BatchID string `json:"batch_id"`

	Total int64 `json:"total"`

	Done int64 `json:"done"`

	Success int64 `json:"success"`

	Failed int64 `json:"failed"`

	Processing int64 `json:"processing"`

	CreatedAt int64 `json:"created_at"`

	Status string `json:"status"`
}
