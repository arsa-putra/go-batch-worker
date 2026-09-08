package worker

type BulkTrackable interface {
	GetBatchID() string
}

func GetBatchID[T any](job T) (string, bool) {

	trackable, ok := any(job).(BulkTrackable)
	if !ok {
		return "", false
	}

	return trackable.GetBatchID(), true
}
