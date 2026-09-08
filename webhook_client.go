package worker

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type WebhookClient struct {
	httpClient *http.Client
}

func NewWebhookClient() *WebhookClient {
	return &WebhookClient{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		},
	}
}

func (c *WebhookClient) Send(
	metadata *BatchMetadata,
	payload any,
) error {

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	log.Printf("Sending webhook to %s with payload: %s",
		metadata.CallbackURL,
		string(body),
	)
	req, err := http.NewRequest(
		http.MethodPost,
		metadata.CallbackURL,
		bytes.NewBuffer(body),
	)

	if err != nil {
		return err
	}

	req.Header.Set(
		"Content-Type",
		"application/json",
	)
	for k, v := range metadata.Headers {
		req.Header.Set(k, v)
	}
	if metadata.CallbackSecret != "" {
		req.Header.Set(
			"X-Signature",
			buildSignature(
				metadata.CallbackSecret,
				body,
			),
		)
	}

	resp, err := c.httpClient.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 ||
		resp.StatusCode >= 300 {

		return fmt.Errorf(
			"webhook returned status %d",
			resp.StatusCode,
		)
	}
	return nil
}

func buildSignature(
	secret string,
	payload []byte,
) string {

	h := hmac.New(
		sha256.New,
		[]byte(secret),
	)

	h.Write(payload)

	return hex.EncodeToString(
		h.Sum(nil),
	)
}
