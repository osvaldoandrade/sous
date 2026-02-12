package codeq

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/osvaldoandrade/sous/internal/api"
	internalcodeq "github.com/osvaldoandrade/sous/internal/codeq"
	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
)

func TestNewFromConfigValidation(t *testing.T) {
	cfg := config.Config{}
	if _, err := NewFromConfig(cfg); err == nil {
		t.Fatal("expected missing brokers error")
	}

	cfg.Plugins.Messaging.CodeQ.Brokers = []string{"localhost:9092"}
	if _, err := NewFromConfig(cfg); err == nil {
		t.Fatal("expected missing topics error")
	}
}

func TestNewFromConfigAndToEnvelope(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.Messaging.CodeQ.Brokers = []string{"localhost:9092"}
	cfg.Plugins.Messaging.CodeQ.Topics.Invoke = "cs.invoke"
	cfg.Plugins.Messaging.CodeQ.Topics.Results = "cs.results"
	provider, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	if provider == nil {
		t.Fatal("expected provider")
	}
	_ = provider.Close()

	rawBody, _ := json.Marshal(map[string]any{"ok": true})
	env := internalcodeq.Envelope{Schema: "cs.envelope.v1", ID: "msg", TSMS: 1, Tenant: "t", Type: "X", Body: rawBody}
	mapped := toEnvelope(env)
	if mapped.ID != "msg" || mapped.Type != "X" {
		t.Fatalf("unexpected envelope: %+v", mapped)
	}
}

func TestConsumeMappingsUseWrapper(t *testing.T) {
	k := internalcodeq.NewKafka([]string{"localhost:9092"}, internalcodeq.Topics{Invoke: "a", Results: "b"})
	p := &Provider{Kafka: k}
	_ = p.Close()

	// compile-time interface checks + signatures sanity
	var _ messaging.Provider = p

	_ = context.Background()
	_ = api.InvocationRequest{}
}

func TestConsumeWrappersWithCanceledContext(t *testing.T) {
	k := internalcodeq.NewKafka([]string{"127.0.0.1:1"}, internalcodeq.Topics{Invoke: "a", Results: "b"})
	p := &Provider{Kafka: k}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := p.ConsumeInvocations(ctx, "g", func(env messaging.Envelope, req api.InvocationRequest) error {
		_ = env
		_ = req
		return nil
	}); err != nil {
		t.Fatalf("ConsumeInvocations returned error: %v", err)
	}
	if err := p.ConsumeResults(ctx, "g", func(env messaging.Envelope, res api.InvocationResult) error {
		_ = env
		_ = res
		return nil
	}); err != nil {
		t.Fatalf("ConsumeResults returned error: %v", err)
	}
	if err := p.ConsumeTopic(ctx, "x", "g", func(env messaging.Envelope) error {
		_ = env
		return nil
	}); err != nil {
		t.Fatalf("ConsumeTopic returned error: %v", err)
	}
}
