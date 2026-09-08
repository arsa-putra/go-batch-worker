package worker

type BatchOptions struct {
	StoreSuccessItems  bool
	StoreFailedItems   bool
	MaxStoredErrors    int
	MaxStoredSuccesses int
}

func DefaultBatchOptions() BatchOptions {
	return BatchOptions{
		StoreSuccessItems: true,
		StoreFailedItems:  true,

		// Soft limits.
		// Counters remain accurate even when
		// additional items are not stored.
		MaxStoredSuccesses: 1000,
		MaxStoredErrors:    1000,
	}
}
