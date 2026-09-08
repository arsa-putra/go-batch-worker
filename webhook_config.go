package worker

import "time"

type WebhookConfig struct {
	MaxAttempts  int
	RetryDelay   time.Duration
	PollInterval time.Duration
}

func DefaultWebhookConfig() WebhookConfig {
	return WebhookConfig{
		MaxAttempts:  5,
		RetryDelay:   30 * time.Second,
		PollInterval: 5 * time.Second,
	}
}
