package codeq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// stubConsumer plays a fixed list of envelopes into the handler. It records
// which envelopes the handler returned nil for so tests can assert that
// non-matching messages were consumed (skipped) and matching messages were
// dispatched.
type stubConsumer struct {
	envs []Envelope
}

func (s *stubConsumer) ConsumeTopic(ctx context.Context, topic, groupID string, handler func(Envelope) error) error {
	_ = topic
	_ = groupID
	for _, env := range s.envs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := handler(env); err != nil {
			return err
		}
	}
	return nil
}

func envelopeWithBody(t *testing.T, id, tenant, typ string, body any) Envelope {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return Envelope{
		Schema: "cs.envelope.v1",
		ID:     id,
		Tenant: tenant,
		Type:   typ,
		Body:   raw,
		TSMS:   1,
	}
}

func TestParseFilterAcceptsAndRejects(t *testing.T) {
	cases := []struct {
		raw     string
		wantErr bool
	}{
		{"", false},
		{`event.tenant == "t_x"`, false},
		{`body.kind != "ignored"`, false},
		{`type == "InvocationRequest"`, false},
		{`body.user.id == "u1"`, false},
		{"invalid expression", true},
		{`body.kind = "x"`, true},
		{`body.kind == x`, true},
	}
	for _, tc := range cases {
		_, err := ParseFilter(tc.raw)
		if tc.wantErr && err == nil {
			t.Fatalf("ParseFilter(%q) expected error", tc.raw)
		}
		if !tc.wantErr && err != nil {
			t.Fatalf("ParseFilter(%q) unexpected error: %v", tc.raw, err)
		}
	}
}

func TestFilterMatches(t *testing.T) {
	env := envelopeWithBody(t, "msg_1", "t_abc123", "OrderCreated", map[string]any{
		"kind":   "premium",
		"amount": 42.0,
		"nested": map[string]any{"flag": true},
	})

	cases := []struct {
		raw  string
		want bool
	}{
		{"", true},
		{`event.tenant == "t_abc123"`, true},
		{`event.tenant != "t_other"`, true},
		{`type == "OrderCreated"`, true},
		{`type != "OrderCreated"`, false},
		{`body.kind == "premium"`, true},
		{`body.kind == "basic"`, false},
		{`body.kind != "basic"`, true},
		{`body.nested.flag == "true"`, true},
		{`body.amount == "42"`, true},
		{`body.missing == ""`, true},
		{`kind == "premium"`, true}, // unprefixed -> body
	}
	for _, tc := range cases {
		f, err := ParseFilter(tc.raw)
		if err != nil {
			t.Fatalf("ParseFilter(%q): %v", tc.raw, err)
		}
		if got := f.Matches(env); got != tc.want {
			t.Fatalf("Matches(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

func TestRunSubscriptionOrderedPreservesOrder(t *testing.T) {
	const n = 100
	envs := make([]Envelope, n)
	for i := 0; i < n; i++ {
		envs[i] = envelopeWithBody(t, fmt.Sprintf("msg_%d", i), "t_abc123", "Type", map[string]any{"seq": i, "kind": "match"})
	}
	consumer := &stubConsumer{envs: envs}
	filter, _ := ParseFilter(`body.kind == "match"`)

	var got []int
	var mu sync.Mutex
	handler := func(ctx context.Context, env Envelope) error {
		var body struct{ Seq int }
		if err := json.Unmarshal(env.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		got = append(got, body.Seq)
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := RunSubscription(ctx, consumer, "topic", "grp", "ordered", 0, filter, handler); err != nil {
		t.Fatalf("RunSubscription returned: %v", err)
	}
	if len(got) != n {
		t.Fatalf("expected %d activations, got %d", n, len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("expected ordered seq[%d]=%d, got %d", i, i, v)
		}
	}
}

func TestRunSubscriptionUnorderedUsesConcurrency(t *testing.T) {
	const n = 50
	const concurrency = 4
	envs := make([]Envelope, n)
	for i := 0; i < n; i++ {
		envs[i] = envelopeWithBody(t, fmt.Sprintf("msg_%d", i), "t_abc123", "Type", map[string]any{"seq": i, "kind": "match"})
	}
	consumer := &stubConsumer{envs: envs}
	filter, _ := ParseFilter(`body.kind == "match"`)

	var inflight, peak int64
	var processed int64
	gate := make(chan struct{})
	var gateOnce sync.Once

	handler := func(ctx context.Context, env Envelope) error {
		cur := atomic.AddInt64(&inflight, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
				break
			}
		}
		// Hold each worker until at least `concurrency` are in flight, then
		// allow them to drain. This proves the pool runs in parallel.
		if cur >= int64(concurrency) {
			gateOnce.Do(func() { close(gate) })
		} else {
			select {
			case <-gate:
			case <-time.After(2 * time.Second):
			}
		}
		atomic.AddInt64(&processed, 1)
		atomic.AddInt64(&inflight, -1)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := RunSubscription(ctx, consumer, "topic", "grp", "unordered", concurrency, filter, handler); err != nil {
		t.Fatalf("RunSubscription returned: %v", err)
	}
	if got := atomic.LoadInt64(&processed); got != n {
		t.Fatalf("expected %d processed, got %d", n, got)
	}
	if got := atomic.LoadInt64(&peak); got < int64(concurrency) {
		t.Fatalf("expected peak inflight >= %d, got %d", concurrency, got)
	}
}

func TestRunSubscriptionFilterRejectsNonMatching(t *testing.T) {
	envs := []Envelope{
		envelopeWithBody(t, "msg_1", "t_abc123", "T", map[string]any{"kind": "yes"}),
		envelopeWithBody(t, "msg_2", "t_abc123", "T", map[string]any{"kind": "no"}),
		envelopeWithBody(t, "msg_3", "t_abc123", "T", map[string]any{"kind": "yes"}),
	}
	consumer := &stubConsumer{envs: envs}
	filter, _ := ParseFilter(`body.kind == "yes"`)

	var got []string
	handler := func(ctx context.Context, env Envelope) error {
		got = append(got, env.ID)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := RunSubscription(ctx, consumer, "topic", "grp", "ordered", 0, filter, handler); err != nil {
		t.Fatalf("RunSubscription returned: %v", err)
	}
	want := []string{"msg_1", "msg_3"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("filter rejection mismatch: got %v want %v", got, want)
	}
}

// TestRunSubscriptionDedupShortCircuitsReplays drives the actual Kafka
// consumer with a fake reader to confirm that the SeenSet-based redelivery
// suppression also covers subscription consumers. The same envelope id is
// fetched twice; the handler must only see it once.
func TestRunSubscriptionDedupShortCircuitsReplays(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"kind": "match"})
	env := Envelope{
		Schema: "cs.envelope.v1",
		ID:     "msg_dedup",
		Tenant: "t_abc123",
		Type:   "T",
		Body:   body,
		TSMS:   1,
	}
	rawEnv, _ := json.Marshal(env)

	fetched := 0
	fr := &fakeReader{
		fetch: []fetchStep{
			{msg: kafka.Message{Value: rawEnv}},
			{msg: kafka.Message{Value: rawEnv}}, // same envelope id — must be suppressed.
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	k := NewKafka(nil, Topics{})
	k.newReaderFn = func(topic, groupID string) kafkaReader {
		_ = topic
		_ = groupID
		fr.fetchAfter = func() {
			fetched++
			if fetched >= 2 {
				// Cancel after both messages have been served so the
				// consumer loop returns cleanly.
				go func() {
					time.Sleep(10 * time.Millisecond)
					cancel()
				}()
			}
		}
		return fr
	}
	filter, _ := ParseFilter("")

	var calls int32
	handler := func(ctx context.Context, env Envelope) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}
	if err := RunSubscription(ctx, k, "topic", "grp-dedup", "ordered", 0, filter, handler); err != nil {
		t.Fatalf("RunSubscription returned: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected dedup to suppress replay, got %d handler calls", got)
	}
	if len(fr.commits) != 2 {
		t.Fatalf("expected both fetches committed (initial + replay), got %d", len(fr.commits))
	}
}
