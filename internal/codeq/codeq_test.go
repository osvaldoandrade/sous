package codeq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

func TestNewKafkaTopicWriterReuseAndClose(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, Topics{
		Invoke:  "invoke",
		Results: "results",
	})

	w1 := k.topicWriter("topic-a")
	w2 := k.topicWriter("topic-a")
	if w1 != w2 {
		t.Fatal("topicWriter should reuse writer per topic")
	}
	if err := k.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func TestPublishAndDLQBranches(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, Topics{
		Invoke:    "invoke",
		Results:   "results",
		DLQInvoke: "",
		DLQResult: "",
	})

	// Marshal error branch.
	if err := k.Publish(context.Background(), "x", "t1", "Type", map[string]any{"bad": make(chan int)}); err == nil {
		t.Fatal("expected marshal error")
	}

	// Empty DLQ topic should no-op.
	if err := k.PublishDLQInvoke(context.Background(), "t1", api.InvocationRequest{}); err != nil {
		t.Fatalf("PublishDLQInvoke no-op failed: %v", err)
	}
	if err := k.PublishDLQResult(context.Background(), "t1", api.InvocationResult{}); err != nil {
		t.Fatalf("PublishDLQResult no-op failed: %v", err)
	}
}

func TestPublishDLQWithTopics(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, Topics{
		Invoke:    "invoke",
		Results:   "results",
		DLQInvoke: "dlq.invoke",
		DLQResult: "dlq.result",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := k.PublishDLQInvoke(ctx, "t1", api.InvocationRequest{
		ActivationID: "act_1",
		RequestID:    "req_1",
		Tenant:       "t1",
		Namespace:    "ns1",
		Ref:          api.FunctionRef{Function: "fn1", Version: 1},
	}); err == nil {
		t.Fatal("expected DLQ invoke publish error with unreachable broker")
	}
	if err := k.PublishDLQResult(ctx, "t1", api.InvocationResult{
		ActivationID: "act_1",
		RequestID:    "req_1",
		Status:       "error",
	}); err == nil {
		t.Fatal("expected DLQ result publish error with unreachable broker")
	}
}

func TestPublishInvocationAndResultErrorWrapping(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, Topics{
		Invoke:  "invoke",
		Results: "results",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err := k.PublishInvocation(ctx, api.InvocationRequest{
		ActivationID: "act_1",
		RequestID:    "req_1",
		Tenant:       "t_abc123",
		Namespace:    "ns1",
		Ref:          api.FunctionRef{Function: "fn1", Version: 1},
	})
	if err == nil {
		t.Fatal("expected publish invocation error")
	}
	var csErr *cserrors.CSError
	if !errors.As(err, &csErr) || csErr.Code != cserrors.CSCodeQPublishFailed {
		t.Fatalf("expected wrapped CSCodeQPublishFailed, got %v", err)
	}

	err = k.PublishResult(ctx, "t_abc123", api.InvocationResult{
		ActivationID: "act_1",
		RequestID:    "req_1",
		Status:       "success",
	})
	if err == nil {
		t.Fatal("expected publish result error")
	}
	if !errors.As(err, &csErr) || csErr.Code != cserrors.CSCodeQPublishFailed {
		t.Fatalf("expected wrapped CSCodeQPublishFailed, got %v", err)
	}
}

func TestConsumeWithCanceledContext(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, Topics{
		Invoke:  "invoke",
		Results: "results",
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := k.ConsumeInvocations(ctx, "g1", func(Envelope, api.InvocationRequest) error { return nil }); err != nil {
		t.Fatalf("ConsumeInvocations canceled context should return nil, got %v", err)
	}
	if err := k.ConsumeResults(ctx, "g1", func(Envelope, api.InvocationResult) error { return nil }); err != nil {
		t.Fatalf("ConsumeResults canceled context should return nil, got %v", err)
	}
	if err := k.ConsumeTopic(ctx, "topic", "g1", func(Envelope) error { return nil }); err != nil {
		t.Fatalf("ConsumeTopic canceled context should return nil, got %v", err)
	}
}

func TestWaitForResultTimeoutOnCanceledContext(t *testing.T) {
	k := NewKafka([]string{"127.0.0.1:1"}, Topics{
		Results: "results",
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := k.WaitForResult(ctx, "req_1")
	if err == nil {
		t.Fatal("expected timeout error")
	}
	var csErr *cserrors.CSError
	if !errors.As(err, &csErr) || csErr.Code != cserrors.CSCodeQTimeout {
		t.Fatalf("expected CSCodeQTimeout, got %v", err)
	}
}

type fakeReader struct {
	fetch      []fetchStep
	fetchIdx   int
	commitErr  error
	commits    []kafka.Message
	closed     bool
	fetchAfter func()
}

type fetchStep struct {
	msg kafka.Message
	err error
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.fetchIdx >= len(f.fetch) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	step := f.fetch[f.fetchIdx]
	f.fetchIdx++
	if f.fetchAfter != nil {
		f.fetchAfter()
	}
	return step.msg, step.err
}

func (f *fakeReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	_ = ctx
	f.commits = append(f.commits, msgs...)
	return f.commitErr
}

func (f *fakeReader) Close() error {
	f.closed = true
	return nil
}

func mustEnvelope(t *testing.T, typ string, body any) []byte {
	t.Helper()
	rawBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	rawEnv, err := json.Marshal(Envelope{
		Schema: "cs.envelope.v1",
		ID:     "msg_1",
		TSMS:   1,
		Tenant: "t_abc123",
		Type:   typ,
		Body:   rawBody,
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return rawEnv
}

func TestConsumeAndWaitForResultSearchBasedBranches(t *testing.T) {
	t.Run("consume wraps fetch errors and closes reader", func(t *testing.T) {
		fr := &fakeReader{
			fetch: []fetchStep{{err: errors.New("fetch failed")}},
		}
		k := NewKafka(nil, Topics{Invoke: "invoke"})
		k.newReaderFn = func(topic, groupID string) kafkaReader {
			_ = topic
			_ = groupID
			return fr
		}
		err := k.ConsumeInvocations(context.Background(), "g1", func(Envelope, api.InvocationRequest) error { return nil })
		if err == nil {
			t.Fatal("expected consume error")
		}
		var csErr *cserrors.CSError
		if !errors.As(err, &csErr) || csErr.Code != cserrors.CSCodeQSubFailed {
			t.Fatalf("expected CSCodeQSubFailed, got %v", err)
		}
		if !fr.closed {
			t.Fatal("reader should be closed on consume exit")
		}
	})

	t.Run("consume skips malformed envelope and commits valid message", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fr := &fakeReader{
			fetch: []fetchStep{
				{msg: kafka.Message{Value: []byte("not-json")}},
				{msg: kafka.Message{Value: mustEnvelope(t, "InvocationRequest", api.InvocationRequest{
					ActivationID: "act_1",
					RequestID:    "req_1",
					Tenant:       "t_abc123",
					Namespace:    "ns1",
					Ref:          api.FunctionRef{Function: "fn1", Version: 1},
				})}},
			},
		}
		k := NewKafka(nil, Topics{Invoke: "invoke"})
		k.newReaderFn = func(topic, groupID string) kafkaReader {
			_ = topic
			_ = groupID
			return fr
		}
		calls := 0
		err := k.ConsumeInvocations(ctx, "g1", func(env Envelope, req api.InvocationRequest) error {
			_ = env
			calls++
			if req.RequestID != "req_1" {
				t.Fatalf("unexpected request id: %+v", req)
			}
			cancel()
			return nil
		})
		if err != nil {
			t.Fatalf("consume should end cleanly on canceled context: %v", err)
		}
		if calls != 1 {
			t.Fatalf("expected 1 handler call, got %d", calls)
		}
		if len(fr.commits) != 2 {
			t.Fatalf("expected commits for malformed+valid messages, got %d", len(fr.commits))
		}
	})

	t.Run("consume returns handler decoding error for invalid invocation body", func(t *testing.T) {
		fr := &fakeReader{
			fetch: []fetchStep{
				{msg: kafka.Message{Value: mustEnvelope(t, "InvocationRequest", "invalid-req-body")}},
			},
		}
		k := NewKafka(nil, Topics{Invoke: "invoke"})
		k.newReaderFn = func(topic, groupID string) kafkaReader {
			_ = topic
			_ = groupID
			return fr
		}
		err := k.ConsumeInvocations(context.Background(), "g1", func(Envelope, api.InvocationRequest) error { return nil })
		if err == nil {
			t.Fatal("expected invocation body decode error")
		}
		if len(fr.commits) != 0 {
			t.Fatalf("invalid invocation body should not commit message, commits=%d", len(fr.commits))
		}
	})

	t.Run("consume wraps commit errors", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fr := &fakeReader{
			fetch: []fetchStep{
				{msg: kafka.Message{Value: mustEnvelope(t, "x", map[string]any{"ok": true})}},
			},
			commitErr: errors.New("commit failed"),
		}
		k := NewKafka(nil, Topics{})
		k.newReaderFn = func(topic, groupID string) kafkaReader {
			_ = topic
			_ = groupID
			return fr
		}
		err := k.ConsumeTopic(ctx, "topic", "g1", func(Envelope) error { return nil })
		if err == nil {
			t.Fatal("expected commit error")
		}
		var csErr *cserrors.CSError
		if !errors.As(err, &csErr) || csErr.Code != cserrors.CSCodeQSubFailed {
			t.Fatalf("expected CSCodeQSubFailed, got %v", err)
		}
	})

	t.Run("wait for result uses correlation semantics", func(t *testing.T) {
		match := api.InvocationResult{
			ActivationID: "act_2",
			RequestID:    "req_match",
			Status:       "success",
			DurationMS:   12,
			Result:       &api.FunctionResponse{StatusCode: 200, Body: "ok"},
		}
		fr := &fakeReader{
			fetch: []fetchStep{
				{msg: kafka.Message{Value: []byte("{bad")}},
				{msg: kafka.Message{Value: mustEnvelope(t, "OtherType", map[string]any{"x": 1})}},
				{msg: kafka.Message{Value: mustEnvelope(t, "InvocationResult", "invalid-body")}},
				{msg: kafka.Message{Value: mustEnvelope(t, "InvocationResult", api.InvocationResult{
					ActivationID: "act_1",
					RequestID:    "req_other",
					Status:       "success",
				})}},
				{msg: kafka.Message{Value: mustEnvelope(t, "InvocationResult", match)}},
			},
		}
		k := NewKafka(nil, Topics{Results: "results"})
		k.newReaderFn = func(topic, groupID string) kafkaReader {
			if topic != "results" {
				t.Fatalf("unexpected topic: %s", topic)
			}
			if groupID == "" {
				t.Fatal("groupID should not be empty")
			}
			return fr
		}
		got, err := k.WaitForResult(context.Background(), "req_match")
		if err != nil {
			t.Fatalf("wait for result returned error: %v", err)
		}
		if got.RequestID != "req_match" || got.ActivationID != "act_2" || got.Status != "success" {
			t.Fatalf("unexpected correlated result: %+v", got)
		}
		if len(fr.commits) != 2 {
			t.Fatalf("expected commits for unmatched+matched result messages, got %d", len(fr.commits))
		}
	})

	t.Run("wait for result propagates non-context fetch error", func(t *testing.T) {
		fr := &fakeReader{
			fetch: []fetchStep{{err: errors.New("fetch failed")}},
		}
		k := NewKafka(nil, Topics{Results: "results"})
		k.newReaderFn = func(topic, groupID string) kafkaReader {
			_ = topic
			_ = groupID
			return fr
		}
		_, err := k.WaitForResult(context.Background(), "req_1")
		if err == nil || err.Error() != "fetch failed" {
			t.Fatalf("expected raw fetch error, got %v", err)
		}
	})
}
