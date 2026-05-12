package codeq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/osvaldoandrade/sous/internal/api"
)

func TestDeterministicMessageIDStability(t *testing.T) {
	a := DeterministicMessageID("t1", "act_1", 0)
	b := DeterministicMessageID("t1", "act_1", 0)
	if a != b {
		t.Fatalf("DeterministicMessageID must be deterministic for the same input: %s vs %s", a, b)
	}
	if a == DeterministicMessageID("t2", "act_1", 0) {
		t.Fatal("message id must differ across tenants")
	}
	if a == DeterministicMessageID("t1", "act_2", 0) {
		t.Fatal("message id must differ across activations")
	}
	if a == DeterministicMessageID("t1", "act_1", 1) {
		t.Fatal("message id must differ across sequence numbers")
	}
}

func TestMemorySeenSetSecondCallIsDuplicate(t *testing.T) {
	cur := time.Unix(1000, 0)
	s := NewMemorySeenSet(func() time.Time { return cur })
	if !s.MarkSeen("a", time.Minute) {
		t.Fatal("first mark should report fresh")
	}
	if s.MarkSeen("a", time.Minute) {
		t.Fatal("second mark should report duplicate")
	}
	cur = cur.Add(2 * time.Minute)
	if !s.MarkSeen("a", time.Minute) {
		t.Fatal("post-TTL mark should report fresh again")
	}
}

func TestConsumeSkipsDuplicateEnvelopes(t *testing.T) {
	// Build two messages whose envelopes share an ID — simulating a codeQ
	// redelivery — and verify the consumer only invokes the handler once.
	req := api.InvocationRequest{
		ActivationID: "act_dedup",
		RequestID:    "req_dedup",
		Tenant:       "t_abc123",
		Namespace:    "ns",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
	}
	id := DeterministicMessageID(req.Tenant, req.ActivationID, 0)
	raw := mustEnvelopeWithID(t, "InvocationRequest", id, req)
	fr := &fakeReader{
		fetch: []fetchStep{
			{msg: kafka.Message{Value: raw}},
			{msg: kafka.Message{Value: raw}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	k := NewKafka(nil, Topics{Invoke: "invoke"})
	k.newReaderFn = func(string, string) kafkaReader { return fr }

	calls := 0
	err := k.ConsumeInvocations(ctx, "g1", func(env Envelope, got api.InvocationRequest) error {
		_ = env
		calls++
		if got.ActivationID != "act_dedup" {
			t.Fatalf("activation id=%s", got.ActivationID)
		}
		// after the first successful handle, cancel so the consumer loop
		// exits once it has processed (and silently dropped) the duplicate.
		if calls == 1 {
			go func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("consume returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler to run exactly once; got %d", calls)
	}
	if len(fr.commits) != 2 {
		t.Fatalf("both deliveries (original + duplicate) should commit offsets; commits=%d", len(fr.commits))
	}
}

// mustEnvelopeWithID marshals an envelope with a caller-supplied id so
// dedup tests can simulate a redelivery (two messages, identical envelope
// id) without going through the production publish path.
func mustEnvelopeWithID(t *testing.T, typ, id string, body any) []byte {
	t.Helper()
	rawBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	rawEnv, err := json.Marshal(Envelope{
		Schema: "cs.envelope.v1",
		ID:     id,
		TSMS:   time.Now().UnixMilli(),
		Tenant: "t_abc123",
		Type:   typ,
		Body:   rawBody,
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return rawEnv
}
