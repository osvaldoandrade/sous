package codeq

import (
	"context"
	"fmt"
	"strings"

	"github.com/osvaldoandrade/sous/internal/api"
	internalcodeq "github.com/osvaldoandrade/sous/internal/codeq"
	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
)

type Provider struct {
	*internalcodeq.Kafka
}

func init() {
	registry.RegisterMessaging("codeq", NewFromConfig)
}

func NewFromConfig(cfg config.Config) (messaging.Provider, error) {
	if strings.TrimSpace(cfg.Plugins.Messaging.CodeQ.BaseURL) != "" {
		return NewHTTPProviderFromConfig(cfg)
	}

	brokers := cfg.Plugins.Messaging.CodeQ.Brokers
	if len(brokers) == 0 {
		return nil, fmt.Errorf("plugins.messaging.codeq.brokers is required")
	}
	topics := cfg.Plugins.Messaging.CodeQ.Topics
	if topics.Invoke == "" || topics.Results == "" {
		return nil, fmt.Errorf("plugins.messaging.codeq.topics.invoke and results are required")
	}
	k := internalcodeq.NewKafka(brokers, internalcodeq.Topics{
		Invoke:    topics.Invoke,
		Results:   topics.Results,
		DLQInvoke: topics.DLQInvoke,
		DLQResult: topics.DLQResult,
	})
	return &Provider{Kafka: k}, nil
}

func (p *Provider) ConsumeInvocations(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationRequest) error) error {
	return p.Kafka.ConsumeInvocations(ctx, groupID, func(env internalcodeq.Envelope, req api.InvocationRequest) error {
		return handler(toEnvelope(env), req)
	})
}

func (p *Provider) ConsumeResults(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationResult) error) error {
	return p.Kafka.ConsumeResults(ctx, groupID, func(env internalcodeq.Envelope, res api.InvocationResult) error {
		return handler(toEnvelope(env), res)
	})
}

func (p *Provider) ConsumeTopic(ctx context.Context, topic, groupID string, handler func(messaging.Envelope) error) error {
	return p.Kafka.ConsumeTopic(ctx, topic, groupID, func(env internalcodeq.Envelope) error {
		return handler(toEnvelope(env))
	})
}

func toEnvelope(e internalcodeq.Envelope) messaging.Envelope {
	return messaging.Envelope{
		Schema: e.Schema,
		ID:     e.ID,
		TSMS:   e.TSMS,
		Tenant: e.Tenant,
		Type:   e.Type,
		Body:   e.Body,
	}
}
