package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStdoutJSONSinkEmit(t *testing.T) {
	var buf bytes.Buffer
	sink := NewStdoutJSONSink(&buf)
	ev := NewEvent("t1", "u1", "function.publish", "fn://t1/ns/f@v1", OutcomeSuccess)
	if err := sink.Emit(context.Background(), ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if sink.Name() != "stdout" {
		t.Fatalf("name=%q", sink.Name())
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("missing newline terminator: %q", out)
	}
	var decoded Event
	if err := json.Unmarshal([]byte(strings.TrimRight(out, "\n")), &decoded); err != nil {
		t.Fatalf("emitted line is not valid JSON: %v body=%q", err, out)
	}
	if decoded.Tenant != "t1" || decoded.Action != "function.publish" {
		t.Fatalf("decoded mismatch: %+v", decoded)
	}
}

// fakePublisher is the minimal CodeQPublisher fake for sink tests.
type fakePublisher struct {
	calls []struct {
		Topic  string
		Tenant string
		Type   string
		Body   any
	}
	err error
}

func (f *fakePublisher) Publish(_ context.Context, topic, tenant, typ string, body any) error {
	f.calls = append(f.calls, struct {
		Topic  string
		Tenant string
		Type   string
		Body   any
	}{topic, tenant, typ, body})
	return f.err
}

func TestCodeQSinkEmit(t *testing.T) {
	pub := &fakePublisher{}
	sink := NewCodeQSink(pub, "cs.audit")
	ev := NewEvent("acme", "u1", "function.publish", "fn://acme/ns/f@v1", OutcomeSuccess)
	if err := sink.Emit(context.Background(), ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if sink.Name() != "codeq" {
		t.Fatalf("name=%q", sink.Name())
	}
	if len(pub.calls) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(pub.calls))
	}
	call := pub.calls[0]
	if call.Topic != "cs.audit.acme" {
		t.Fatalf("topic=%q want cs.audit.acme", call.Topic)
	}
	if call.Tenant != "acme" || call.Type != "AuditEvent" {
		t.Fatalf("unexpected publish args: %+v", call)
	}
}

func TestCodeQSinkDefaultPrefix(t *testing.T) {
	pub := &fakePublisher{}
	sink := NewCodeQSink(pub, "")
	ev := NewEvent("acme", "u1", "function.create", "fn://acme/ns/f", OutcomeSuccess)
	if err := sink.Emit(context.Background(), ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if pub.calls[0].Topic != "cs.audit.acme" {
		t.Fatalf("default prefix topic=%q", pub.calls[0].Topic)
	}
}

func TestCodeQSinkNilPublisher(t *testing.T) {
	sink := &CodeQSink{}
	if err := sink.Emit(context.Background(), Event{}); err == nil {
		t.Fatalf("expected error on nil publisher")
	}
}

func TestWebhookSinkEmitWithHMAC(t *testing.T) {
	var receivedBody []byte
	var receivedSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		receivedSig = r.Header.Get("X-CS-Audit-Signature")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := NewWebhookSink(srv.URL, "supersecret", nil)
	if sink.Name() != "webhook" {
		t.Fatalf("name=%q", sink.Name())
	}
	ev := NewEvent("acme", "u1", "function.delete", "fn://acme/ns/f", OutcomeSuccess)
	if err := sink.Emit(context.Background(), ev); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if len(receivedBody) == 0 {
		t.Fatalf("expected webhook to receive body")
	}
	if receivedSig == "" {
		t.Fatalf("expected signature header")
	}
	if !VerifySignature([]byte("supersecret"), receivedBody, receivedSig) {
		t.Fatalf("HMAC signature did not verify")
	}
	if VerifySignature([]byte("wrong-key"), receivedBody, receivedSig) {
		t.Fatalf("HMAC signature should fail with wrong key")
	}
}

func TestWebhookSinkNon2XX(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	sink := NewWebhookSink(srv.URL, "k", nil)
	if err := sink.Emit(context.Background(), Event{Tenant: "t1"}); err == nil {
		t.Fatalf("expected error on 5xx response")
	}
}

func TestWebhookSinkEmptyURL(t *testing.T) {
	sink := NewWebhookSink("", "k", nil)
	if err := sink.Emit(context.Background(), Event{Tenant: "t1"}); err == nil {
		t.Fatalf("expected error on empty url")
	}
}

func TestNewSinkFromConfig(t *testing.T) {
	var buf bytes.Buffer
	sink, err := NewSinkFromConfig(Config{Sink: ""}, nil, &buf)
	if err != nil {
		t.Fatalf("default stdout: %v", err)
	}
	if sink.Name() != "stdout" {
		t.Fatalf("default sink name=%q", sink.Name())
	}

	pub := &fakePublisher{}
	sink, err = NewSinkFromConfig(Config{Sink: "codeq"}, pub, &buf)
	if err != nil {
		t.Fatalf("codeq sink: %v", err)
	}
	if sink.Name() != "codeq" {
		t.Fatalf("codeq sink name=%q", sink.Name())
	}

	sink, err = NewSinkFromConfig(Config{Sink: "webhook", WebhookURL: "http://example.com/hook", HMACSecret: "k"}, nil, &buf)
	if err != nil {
		t.Fatalf("webhook sink: %v", err)
	}
	if sink.Name() != "webhook" {
		t.Fatalf("webhook sink name=%q", sink.Name())
	}

	if _, err := NewSinkFromConfig(Config{Sink: "webhook"}, nil, &buf); err == nil {
		t.Fatalf("expected error when webhook url is empty")
	}
	if _, err := NewSinkFromConfig(Config{Sink: "codeq"}, nil, &buf); err == nil {
		t.Fatalf("expected error when codeq publisher is nil")
	}
	if _, err := NewSinkFromConfig(Config{Sink: "stdout"}, nil, nil); err == nil {
		t.Fatalf("expected error when stdout writer is nil")
	}
	if _, err := NewSinkFromConfig(Config{Sink: "unknown"}, nil, &buf); err == nil {
		t.Fatalf("expected error on unknown driver")
	}
}
