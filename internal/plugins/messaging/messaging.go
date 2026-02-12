package messaging

import (
	"context"
	"encoding/json"

	"github.com/osvaldoandrade/sous/internal/api"
)

type Envelope struct {
	Schema string          `json:"schema"`
	ID     string          `json:"id"`
	TSMS   int64           `json:"ts_ms"`
	Tenant string          `json:"tenant"`
	Type   string          `json:"type"`
	Body   json.RawMessage `json:"body"`
}

// Provider defines transport extension points for invoke and result flows.
type Provider interface {
	Close() error

	Publish(ctx context.Context, topic, tenant, typ string, body any) error

	PublishInvocation(ctx context.Context, req api.InvocationRequest) error
	PublishResult(ctx context.Context, tenant string, result api.InvocationResult) error
	PublishDLQInvoke(ctx context.Context, tenant string, req api.InvocationRequest) error
	PublishDLQResult(ctx context.Context, tenant string, result api.InvocationResult) error

	ConsumeInvocations(ctx context.Context, groupID string, handler func(Envelope, api.InvocationRequest) error) error
	ConsumeResults(ctx context.Context, groupID string, handler func(Envelope, api.InvocationResult) error) error
	ConsumeTopic(ctx context.Context, topic, groupID string, handler func(Envelope) error) error

	WaitForResult(ctx context.Context, requestID string) (api.InvocationResult, error)
}
