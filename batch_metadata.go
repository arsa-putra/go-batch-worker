package worker

type BatchMetadata struct {
	CallbackURL    string            `json:"callback_url,omitempty"`
	CallbackSecret string            `json:"callback_secret,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	Keys           []string          `json:"keys,omitempty"`
}
