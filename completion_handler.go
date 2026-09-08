package worker

type CompletionHandler interface {
	OnCompleted(batchID string) error
}
