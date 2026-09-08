package worker

import "sort"

const defaultTopErrors = 10

type ErrorCount struct {
	Message string `json:"message"`
	Count   int64  `json:"count"`
}

func (w *BulkWorker[T]) TopErrors(
	limit int,
) []ErrorCount {

	w.errorCountMu.RLock()

	result := make(
		[]ErrorCount,
		0,
		len(w.errorCounts),
	)

	for msg, count := range w.errorCounts {

		result = append(
			result,
			ErrorCount{
				Message: msg,
				Count:   count,
			},
		)
	}

	w.errorCountMu.RUnlock()

	sort.Slice(
		result,
		func(i, j int) bool {

			if result[i].Count ==
				result[j].Count {

				return result[i].Message <
					result[j].Message
			}

			return result[i].Count >
				result[j].Count
		},
	)

	if limit > 0 &&
		len(result) > limit {

		result = result[:limit]
	}

	return result
}
