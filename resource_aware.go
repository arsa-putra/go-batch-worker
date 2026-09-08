package worker

type ResourceAware interface {
	SetResourceLimiter(
		limiter *ResourceLimiter,
	)
}
