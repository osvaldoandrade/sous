package codeq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

type Topics struct {
	Invoke    string
	Results   string
	DLQInvoke string
	DLQResult string
}

type Envelope struct {
	Schema string          `json:"schema"`
	ID     string          `json:"id"`
	TSMS   int64           `json:"ts_ms"`
	Tenant string          `json:"tenant"`
	Type   string          `json:"type"`
	Body   json.RawMessage `json:"body"`
}

type Kafka struct {
	brokers []string
	topics  Topics

	mu      sync.Mutex
	writers map[string]*kafka.Writer

	newReaderFn func(topic, groupID string) kafkaReader
}

func NewKafka(brokers []string, topics Topics) *Kafka {
	return &Kafka{brokers: brokers, topics: topics, writers: make(map[string]*kafka.Writer)}
}

type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

func (k *Kafka) topicWriter(topic string) *kafka.Writer {
	k.mu.Lock()
	defer k.mu.Unlock()
	if w, ok := k.writers[topic]; ok {
		return w
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(k.brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireOne,
		Balancer:     &kafka.LeastBytes{},
	}
	k.writers[topic] = w
	return w
}

func (k *Kafka) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	var firstErr error
	for _, w := range k.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (k *Kafka) Publish(ctx context.Context, topic, tenant, typ string, body any) error {
	rawBody, err := json.Marshal(body)
	if err != nil {
		return err
	}
	env := Envelope{
		Schema: "cs.envelope.v1",
		ID:     "msg_" + uuid.NewString(),
		TSMS:   time.Now().UnixMilli(),
		Tenant: tenant,
		Type:   typ,
		Body:   rawBody,
	}
	rawEnv, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if err := k.topicWriter(topic).WriteMessages(ctx, kafka.Message{Key: []byte(tenant), Value: rawEnv, Time: time.Now()}); err != nil {
		return cserrors.Wrap(cserrors.CSCodeQPublishFailed, fmt.Sprintf("failed to publish to topic %s", topic), err)
	}
	return nil
}

func (k *Kafka) PublishInvocation(ctx context.Context, req api.InvocationRequest) error {
	return k.Publish(ctx, k.topics.Invoke, req.Tenant, "InvocationRequest", req)
}

func (k *Kafka) PublishResult(ctx context.Context, tenant string, result api.InvocationResult) error {
	return k.Publish(ctx, k.topics.Results, tenant, "InvocationResult", result)
}

func (k *Kafka) PublishDLQInvoke(ctx context.Context, tenant string, req api.InvocationRequest) error {
	if k.topics.DLQInvoke == "" {
		return nil
	}
	return k.Publish(ctx, k.topics.DLQInvoke, tenant, "InvocationRequest", req)
}

func (k *Kafka) PublishDLQResult(ctx context.Context, tenant string, result api.InvocationResult) error {
	if k.topics.DLQResult == "" {
		return nil
	}
	return k.Publish(ctx, k.topics.DLQResult, tenant, "InvocationResult", result)
}

func (k *Kafka) ConsumeInvocations(ctx context.Context, groupID string, handler func(Envelope, api.InvocationRequest) error) error {
	return k.consume(ctx, k.topics.Invoke, groupID, func(env Envelope) error {
		var req api.InvocationRequest
		if err := json.Unmarshal(env.Body, &req); err != nil {
			return err
		}
		return handler(env, req)
	})
}

func (k *Kafka) ConsumeResults(ctx context.Context, groupID string, handler func(Envelope, api.InvocationResult) error) error {
	return k.consume(ctx, k.topics.Results, groupID, func(env Envelope) error {
		var res api.InvocationResult
		if err := json.Unmarshal(env.Body, &res); err != nil {
			return err
		}
		return handler(env, res)
	})
}

func (k *Kafka) ConsumeTopic(ctx context.Context, topic, groupID string, handler func(Envelope) error) error {
	return k.consume(ctx, topic, groupID, handler)
}

func (k *Kafka) consume(ctx context.Context, topic, groupID string, handler func(Envelope) error) error {
	reader := k.newReader(topic, groupID)
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return cserrors.Wrap(cserrors.CSCodeQSubFailed, "failed to fetch kafka message", err)
		}
		var env Envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		if err := handler(env); err != nil {
			return err
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return cserrors.Wrap(cserrors.CSCodeQSubFailed, "failed to commit kafka message", err)
		}
	}
}

func (k *Kafka) WaitForResult(ctx context.Context, requestID string) (api.InvocationResult, error) {
	var out api.InvocationResult
	groupID := "cs-http-gateway-wait-" + uuid.NewString()
	reader := k.newReader(k.topics.Results, groupID)
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return out, cserrors.New(cserrors.CSCodeQTimeout, "result wait timed out")
			}
			return out, err
		}
		var env Envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			continue
		}
		if env.Type != "InvocationResult" {
			continue
		}
		if err := json.Unmarshal(env.Body, &out); err != nil {
			continue
		}
		if out.RequestID == requestID {
			_ = reader.CommitMessages(ctx, msg)
			return out, nil
		}
		_ = reader.CommitMessages(ctx, msg)
	}
}

func (k *Kafka) newReader(topic, groupID string) kafkaReader {
	if k.newReaderFn != nil {
		return k.newReaderFn(topic, groupID)
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  k.brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
}
