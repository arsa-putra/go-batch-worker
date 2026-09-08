package worker

type BatchIdentifiable interface {
	BatchID() string
}
