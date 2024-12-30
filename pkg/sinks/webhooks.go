package sinks

import (
	"bytes"
	"fmt"
	"net/http"
	"os"

	"github.com/goccy/go-json"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05.000",
	})
}

// WebhookSink represents a sink that sends data to a webhook endpoint
type WebhookSink struct {
	webhookURL string
	client     *http.Client
}

// NewWebhookSink creates a new WebhookSink instance
func NewWebhookSink(webhookURL string) (*WebhookSink, error) {
	sink := &WebhookSink{
		webhookURL: webhookURL,
		client:     &http.Client{},
	}

	return sink, nil
}

// WriteBatch sends a batch of data to the webhook endpoint
func (s *WebhookSink) WriteBatch(messages []*utils.CDCMessage) error {
	for _, message := range messages {
		jsonData, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to marshal data to JSON: %v", err)
		}

		if err = s.sendWithRetry(jsonData); err != nil {
			return err
		}
	}
	return nil
}

// sendWithRetry sends data to the webhook endpoint with retry logic
func (s *WebhookSink) sendWithRetry(jsonData []byte) error {
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", s.webhookURL, bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := s.client.Do(req)
		if err != nil {
			if attempt == maxRetries {
				return fmt.Errorf("failed to send webhook after %d attempts: %v", maxRetries, err)
			}
			log.Warn().Err(err).Int("attempt", attempt).Msg("Webhook request failed, retrying...")
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}

		if attempt == maxRetries {
			return fmt.Errorf("webhook request failed with status code: %d after %d attempts", resp.StatusCode, maxRetries)
		}
		log.Warn().Int("statusCode", resp.StatusCode).Int("attempt", attempt).Msg("Received non-2xx status code, retrying...")
	}
	return nil
}

// Close performs any necessary cleanup (no-op for WebhookSink)
func (s *WebhookSink) Close() error {
	return nil
}
