package worker

type BatchSummary struct {
	BatchID string `json:"batch_id"`

	Total int64 `json:"total"`
	Done  int64 `json:"done"`

	Success int64 `json:"success"`
	Failed  int64 `json:"failed"`

	FinishedAt int64 `json:"finished_at"`

	Status string `json:"status"`

	Cancelled bool `json:"cancelled"`
}

func (t *RedisBatchTracker) BuildSummary(batchID string) (*BatchSummary, error) {
	state, err := t.Get(
		batchID,
	)

	if err != nil {
		return nil, err
	}

	finishedAt, _ := t.redis.HGet(t.stateKey(batchID), "finished_at").Int64()

	status := "running"

	cancelled, _ := t.IsCancelled(
		batchID,
	)

	switch {

	case state.Done >= state.Total:
		status = "completed"

	case cancelled:
		status = "cancelled"
	}

	return &BatchSummary{
		BatchID: state.BatchID,

		Total: state.Total,
		Done:  state.Done,

		Success: state.Success,
		Failed:  state.Failed,

		FinishedAt: finishedAt,

		Status: status,

		Cancelled: cancelled,
	}, nil
}
