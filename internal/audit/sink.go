package audit

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Sink is the abstraction shared by every audit-event consumer. The
// recorder writes one Event per successful control-plane mutation to
// the configured Sink. Failure of the sink does not roll back the
// mutation (the kv commit already happened); instead the recorder
// records a SinkLag warning so operators can investigate without
// risking double-mutations on retry.
type Sink interface {
	// Emit publishes one event. Implementations must be safe for
	// concurrent use.
	Emit(ctx context.Context, event Event) error
	// Name returns the sink driver name (e.g., "stdout", "codeq",
	// "webhook"). Used for log/metric labels.
	Name() string
	// Close releases any resources held by the sink. Idempotent.
	Close() error
}

// StdoutJSONSink writes one JSON line per event to an io.Writer. It
// is the simplest sink, used in dev and as a CI default; production
// deployments typically use codeq or webhook instead.
type StdoutJSONSink struct {
	mu sync.Mutex
	w  io.Writer
}

// NewStdoutJSONSink wires a writer for the JSON-line audit stream.
// Passing nil falls back to os.Stdout via the caller's choice — we
// keep this strict to avoid hidden writes from tests.
func NewStdoutJSONSink(w io.Writer) *StdoutJSONSink {
	return &StdoutJSONSink{w: w}
}

func (s *StdoutJSONSink) Emit(_ context.Context, event Event) error {
	raw, err := event.Marshal()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(raw); err != nil {
		return err
	}
	_, err = s.w.Write([]byte("\n"))
	return err
}

func (s *StdoutJSONSink) Name() string { return "stdout" }
func (s *StdoutJSONSink) Close() error { return nil }

// CodeQPublisher is the narrow contract CodeQSink consumes. It mirrors
// the messaging.Provider.Publish signature so the existing codeQ
// driver plugs in directly without an adapter.
type CodeQPublisher interface {
	Publish(ctx context.Context, topic, tenant, typ string, body any) error
}

// CodeQSink fan-outs audit events through a codeQ topic. The topic is
// computed per-tenant (cs.audit.{tenant}) so consumers can subscribe
// without reading other tenants' lines.
type CodeQSink struct {
	pub         CodeQPublisher
	topicPrefix string
}

// NewCodeQSink builds a sink backed by a CodeQPublisher. The topic
// prefix defaults to "cs.audit" when empty; the final topic is
// "{prefix}.{tenant}".
func NewCodeQSink(pub CodeQPublisher, topicPrefix string) *CodeQSink {
	if topicPrefix == "" {
		topicPrefix = "cs.audit"
	}
	return &CodeQSink{pub: pub, topicPrefix: topicPrefix}
}

func (c *CodeQSink) Emit(ctx context.Context, event Event) error {
	if c.pub == nil {
		return errors.New("audit codeq sink: publisher is nil")
	}
	topic := c.topicPrefix + "." + event.Tenant
	return c.pub.Publish(ctx, topic, event.Tenant, "AuditEvent", event)
}

func (c *CodeQSink) Name() string { return "codeq" }
func (c *CodeQSink) Close() error { return nil }

// WebhookSink POSTs each event to a tenant URL with an HMAC-SHA256
// signature computed over the JSON body. The signature lives in the
// X-CS-Audit-Signature header (hex-encoded) so SIEM endpoints can
// verify origin without TLS client certs.
type WebhookSink struct {
	url    string
	secret []byte
	client *http.Client
}

// NewWebhookSink builds a webhook sink. A nil client uses a default
// 5-second http.Client; pass a custom one for tests or production
// tuning.
func NewWebhookSink(url, secret string, client *http.Client) *WebhookSink {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &WebhookSink{
		url:    url,
		secret: []byte(secret),
		client: client,
	}
}

func (s *WebhookSink) Emit(ctx context.Context, event Event) error {
	if s.url == "" {
		return errors.New("audit webhook sink: url is empty")
	}
	body, err := event.Marshal()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if len(s.secret) > 0 {
		req.Header.Set("X-CS-Audit-Signature", computeSignature(s.secret, body))
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("audit webhook sink: non-2xx status %d", resp.StatusCode)
	}
	return nil
}

func (s *WebhookSink) Name() string { return "webhook" }
func (s *WebhookSink) Close() error { return nil }

// computeSignature returns the hex-encoded HMAC-SHA256 of body keyed
// by secret. Exported callers (verifier libraries, tests) can rely on
// the same helper to compare signatures.
func computeSignature(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature returns true when sig matches the HMAC-SHA256 of
// body keyed by secret. Used by test suites and the webhook
// verification helper documented in docs/14-observability.md.
func VerifySignature(secret, body []byte, sig string) bool {
	expected := computeSignature(secret, body)
	return hmac.Equal([]byte(expected), []byte(sig))
}
