package main

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// fakeClock drives the backoff loop deterministically. It never blocks; each
// Sleep call records the requested duration and returns immediately so the
// test can assert the schedule of delays without spending real wall time.
type fakeClock struct {
	mu       sync.Mutex
	now      time.Time
	sleeps   []time.Duration
	sleepErr error
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.UnixMilli(1_000)} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	f.mu.Lock()
	if f.sleepErr != nil {
		err := f.sleepErr
		f.mu.Unlock()
		return err
	}
	f.sleeps = append(f.sleeps, d)
	f.now = f.now.Add(d)
	f.mu.Unlock()
	return nil
}

func (f *fakeClock) snapshot() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]time.Duration, len(f.sleeps))
	copy(out, f.sleeps)
	return out
}

type fixedRand struct{ v float64 }

func (f fixedRand) Float64() float64 { return f.v }

func TestComputeBackoffExponentialNoJitter(t *testing.T) {
	p := api.RetryPolicy{MaxAttempts: 5, BaseMs: 100, MaxMs: 2000, JitterPct: 0}
	got := []time.Duration{
		computeBackoff(p, 1, fixedRand{0.5}),
		computeBackoff(p, 2, fixedRand{0.5}),
		computeBackoff(p, 3, fixedRand{0.5}),
		computeBackoff(p, 4, fixedRand{0.5}),
		computeBackoff(p, 5, fixedRand{0.5}),
	}
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("attempt %d: got=%s want=%s", i+1, got[i], w)
		}
	}
	// Capping at MaxMs after enough attempts.
	if cap := computeBackoff(p, 10, fixedRand{0.5}); cap != 2000*time.Millisecond {
		t.Fatalf("expected MaxMs cap, got %s", cap)
	}
}

func TestComputeBackoffJitterClampedToBand(t *testing.T) {
	// JitterPct=50 with rand=1.0 -> +50% (we use frac=(2*1-1)*0.5=0.5), so
	// expected delay = 100*1.5 = 150ms. rand=0 -> -50% -> 50ms.
	p := api.RetryPolicy{MaxAttempts: 3, BaseMs: 100, MaxMs: 1000, JitterPct: 50}
	hi := computeBackoff(p, 1, fixedRand{1.0})
	lo := computeBackoff(p, 1, fixedRand{0.0})
	if hi != 150*time.Millisecond {
		t.Fatalf("hi-bound jitter=%s", hi)
	}
	if lo != 50*time.Millisecond {
		t.Fatalf("lo-bound jitter=%s", lo)
	}
}

func TestTriggerRetryPolicyFromMap(t *testing.T) {
	req := api.InvocationRequest{
		Trigger: api.Trigger{Type: "schedule", Source: map[string]any{
			"retry_policy": map[string]any{
				"max_attempts": 4,
				"base_ms":      250,
				"max_ms":       2000,
				"jitter_pct":   10,
			},
		}},
	}
	p := triggerRetryPolicy(req, api.DefaultRetryPolicy())
	if p.MaxAttempts != 4 || p.BaseMs != 250 || p.MaxMs != 2000 || p.JitterPct != 10 {
		t.Fatalf("decoded policy: %+v", p)
	}
}

func TestTriggerRetryPolicyDefaultWhenMissing(t *testing.T) {
	req := api.InvocationRequest{Trigger: api.Trigger{Type: "schedule"}}
	p := triggerRetryPolicy(req, api.DefaultRetryPolicy())
	if p.MaxAttempts != api.DefaultRetryMaxAttempts {
		t.Fatalf("expected default max attempts, got %d", p.MaxAttempts)
	}
}

func TestTriggerAttempt(t *testing.T) {
	cases := []struct {
		name string
		src  map[string]any
		want int
	}{
		{"missing", nil, 1},
		{"int", map[string]any{"attempt": 3}, 3},
		{"int64", map[string]any{"attempt": int64(7)}, 7},
		{"float64", map[string]any{"attempt": float64(5)}, 5},
		{"clamped", map[string]any{"attempt": 0}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := triggerAttempt(api.InvocationRequest{Trigger: api.Trigger{Source: tc.src}})
			if got != tc.want {
				t.Fatalf("attempt=%d want=%d", got, tc.want)
			}
		})
	}
}

// scriptRunner builds a 1-version-fake-store and broker pair where the
// runtime script is dictated by a per-attempt response slice. Each call to
// executeOnce consumes the next script entry; tests assert how many entries
// were consumed and what the final DLQ envelope looked like.
type scriptedAttempt struct {
	script string
}

func newScriptedInvoker(t *testing.T, attempts []scriptedAttempt, retryPolicy *api.RetryPolicy, trigger api.Trigger) (*invoker, *fakeClock, *capturedDLQ, func() int) {
	t.Helper()
	clk := newFakeClock()
	captured := &capturedDLQ{}

	var (
		mu       sync.Mutex
		consumed int
	)
	nextBundle := func() ([]byte, string) {
		mu.Lock()
		defer mu.Unlock()
		idx := consumed
		if idx >= len(attempts) {
			idx = len(attempts) - 1
		}
		consumed++
		return buildInvokerBundleWithScript(t, attempts[idx].script)
	}

	store := &testutil.FakePersistence{
		IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
			return false, api.ActivationRecord{}, nil
		},
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 1, nil
		},
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			b, sha := nextBundle()
			return api.VersionRecord{
				SHA256: sha,
				Config: api.VersionConfig{TimeoutMS: 1000, MaxConcurrency: 1},
			}, b, nil
		},
		PutActivationRunningFn: func(context.Context, api.ActivationRecord, time.Duration) error {
			return nil
		},
		CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) {
			return true, nil
		},
		SaveResultByRequestIDFn: func(context.Context, string, string, api.InvocationResult, time.Duration) error {
			return nil
		},
		AppendLogChunkFn: func(context.Context, string, string, int64, []byte, time.Duration) error {
			return nil
		},
		ClearScheduleInflightFn: func(context.Context, string, string, string) error { return nil },
	}
	broker := &testutil.FakeMessaging{
		PublishResultFn: func(context.Context, string, api.InvocationResult) error { return nil },
		PublishDLQInvokeFn: func(ctx context.Context, tenant string, req api.InvocationRequest) error {
			captured.recordBare(tenant, req)
			return nil
		},
	}
	inv := newTestInvoker(store, broker)
	inv.logger = observability.NewLoggerWithWriter("test", io.Discard)
	inv.retryMetrics = &retryMetrics{}
	inv.dlqPublisher = func(ctx context.Context, tenant string, env api.DLQEnvelope) error {
		captured.record(tenant, env)
		return nil
	}

	if retryPolicy != nil {
		if trigger.Source == nil {
			trigger.Source = map[string]any{}
		}
		trigger.Source["retry_policy"] = *retryPolicy
	}

	return inv, clk, captured, func() int {
		mu.Lock()
		defer mu.Unlock()
		return consumed
	}
}

type capturedDLQ struct {
	mu        sync.Mutex
	envelopes []api.DLQEnvelope
	bare      []api.InvocationRequest
}

func (c *capturedDLQ) record(tenant string, env api.DLQEnvelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = tenant
	c.envelopes = append(c.envelopes, env)
}
func (c *capturedDLQ) recordBare(tenant string, req api.InvocationRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = tenant
	c.bare = append(c.bare, req)
}
func (c *capturedDLQ) envelopeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.envelopes)
}
func (c *capturedDLQ) bareCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.bare)
}

func sampleRequest(trigger api.Trigger) api.InvocationRequest {
	return api.InvocationRequest{
		ActivationID: "act-1",
		RequestID:    "req-1",
		Tenant:       "t_abc123",
		Namespace:    "ns",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
		Trigger:      trigger,
	}
}

const scriptSuccess = `export default function() { return { statusCode: 200, body: "ok" } }`

// scriptTimeout yields a runtime error whose message contains "timeout";
// internal/runtime.Runner.Execute then maps it to InvocationError.Type="Timeout",
// which the default RetryPolicy treats as a retryable transient failure.
const scriptTimeout = `export default function() {
  throw new Error("simulated timeout downstream")
}`

func TestDispatchRetriesUntilSuccess(t *testing.T) {
	policy := api.RetryPolicy{MaxAttempts: 4, BaseMs: 10, MaxMs: 200, JitterPct: 0}
	attempts := []scriptedAttempt{
		{script: scriptTimeout},
		{script: scriptTimeout},
		{script: scriptSuccess},
	}
	inv, clk, captured, consumed := newScriptedInvoker(t, attempts, &policy, api.Trigger{Type: "schedule"})

	err := inv.dispatchWith(context.Background(), messaging.Envelope{}, sampleRequest(api.Trigger{Type: "schedule", Source: map[string]any{
		"retry_policy": policy,
	}}), clk, fixedRand{0.5})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if c := consumed(); c != 3 {
		t.Fatalf("expected 3 attempts, got %d", c)
	}
	stats := inv.retryMetrics.snapshot()
	if stats.Retry != 2 {
		t.Fatalf("expected 2 retry events, got %+v", stats)
	}
	if stats.RetrySuccess != 1 {
		t.Fatalf("expected 1 retry-success, got %+v", stats)
	}
	if stats.DLQ != 0 {
		t.Fatalf("expected 0 DLQ publishes, got %+v", stats)
	}
	if captured.envelopeCount() != 0 || captured.bareCount() != 0 {
		t.Fatalf("expected no DLQ on success path")
	}
	sleeps := clk.snapshot()
	wantSleeps := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("sleep schedule len=%d want=%d", len(sleeps), len(wantSleeps))
	}
	for i, w := range wantSleeps {
		if sleeps[i] != w {
			t.Fatalf("sleep[%d]=%s want=%s", i, sleeps[i], w)
		}
	}
}

func TestDispatchPublishesDLQOnExhaustion(t *testing.T) {
	policy := api.RetryPolicy{MaxAttempts: 3, BaseMs: 5, MaxMs: 50, JitterPct: 0}
	attempts := []scriptedAttempt{
		{script: scriptTimeout},
		{script: scriptTimeout},
		{script: scriptTimeout},
	}
	inv, clk, captured, consumed := newScriptedInvoker(t, attempts, &policy, api.Trigger{Type: "schedule"})

	err := inv.dispatchWith(context.Background(), messaging.Envelope{}, sampleRequest(api.Trigger{Type: "schedule", Source: map[string]any{
		"retry_policy": policy,
	}}), clk, fixedRand{0.5})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if c := consumed(); c != 3 {
		t.Fatalf("expected 3 attempts, got %d", c)
	}
	stats := inv.retryMetrics.snapshot()
	if stats.DLQ != 1 {
		t.Fatalf("expected 1 DLQ publish, got %+v", stats)
	}
	if stats.RetrySuccess != 0 {
		t.Fatalf("expected 0 retry-success, got %+v", stats)
	}
	if captured.envelopeCount() != 1 {
		t.Fatalf("expected DLQ envelope, got %d (bare=%d)", captured.envelopeCount(), captured.bareCount())
	}
	captured.mu.Lock()
	env := captured.envelopes[0]
	captured.mu.Unlock()
	if env.Schema != api.DLQSchemaV1 {
		t.Fatalf("DLQ schema=%s", env.Schema)
	}
	if env.AttemptCount != 3 {
		t.Fatalf("AttemptCount=%d", env.AttemptCount)
	}
	if env.LastErrorCode != "Timeout" {
		t.Fatalf("LastErrorCode=%s", env.LastErrorCode)
	}
	if len(env.Attempts) != 3 {
		t.Fatalf("attempts history=%d", len(env.Attempts))
	}
	if env.OriginalPayload.ActivationID != "act-1" {
		t.Fatalf("envelope dropped activation id: %+v", env.OriginalPayload)
	}
}

func TestDispatchNonRetryableGoesStraightToDLQ(t *testing.T) {
	// A generic exception (not "Timeout"/"CS_RUNTIME_TIMEOUT") is not on
	// the default retryable allow-list, so the first failure must DLQ
	// without consuming the remaining attempt budget.
	policy := api.RetryPolicy{MaxAttempts: 5, BaseMs: 5, MaxMs: 50, JitterPct: 0}
	const scriptGenericError = `export default function() {
		throw new Error("deterministic bug — should not retry")
	}`
	attempts := []scriptedAttempt{
		{script: scriptGenericError},
	}
	inv, clk, captured, consumed := newScriptedInvoker(t, attempts, &policy, api.Trigger{Type: "schedule"})

	err := inv.dispatchWith(context.Background(), messaging.Envelope{}, sampleRequest(api.Trigger{Type: "schedule", Source: map[string]any{
		"retry_policy": policy,
	}}), clk, fixedRand{0.5})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if c := consumed(); c != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", c)
	}
	if captured.envelopeCount() != 1 {
		t.Fatalf("expected DLQ envelope for non-retryable")
	}
}

func TestDispatchSkipsRetryForHTTPTrigger(t *testing.T) {
	policy := api.RetryPolicy{MaxAttempts: 5, BaseMs: 5, MaxMs: 50, JitterPct: 0}
	attempts := []scriptedAttempt{{script: scriptTimeout}}
	inv, clk, captured, consumed := newScriptedInvoker(t, attempts, &policy, api.Trigger{Type: "http"})

	err := inv.dispatchWith(context.Background(), messaging.Envelope{}, sampleRequest(api.Trigger{Type: "http", Source: map[string]any{
		"retry_policy": policy,
	}}), clk, fixedRand{0.5})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if c := consumed(); c != 1 {
		t.Fatalf("HTTP trigger must execute exactly once, got %d", c)
	}
	if captured.envelopeCount() != 0 || captured.bareCount() != 0 {
		t.Fatalf("HTTP trigger must not publish DLQ — gateway owns retry; envelopes=%d bare=%d", captured.envelopeCount(), captured.bareCount())
	}
}

func TestDispatchHonoursAttemptCountFromTrigger(t *testing.T) {
	// Producer-side already burned attempt 1 and is publishing attempt 2.
	policy := api.RetryPolicy{MaxAttempts: 2, BaseMs: 5, MaxMs: 50, JitterPct: 0}
	attempts := []scriptedAttempt{{script: scriptTimeout}}
	inv, clk, captured, consumed := newScriptedInvoker(t, attempts, nil, api.Trigger{Type: "schedule"})

	req := sampleRequest(api.Trigger{Type: "schedule", Source: map[string]any{
		"retry_policy": policy,
		"attempt":      2,
	}})
	err := inv.dispatchWith(context.Background(), messaging.Envelope{}, req, clk, fixedRand{0.5})
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if c := consumed(); c != 1 {
		t.Fatalf("expected exactly 1 attempt (attempt=2 was the last), got %d", c)
	}
	stats := inv.retryMetrics.snapshot()
	if stats.DLQ != 1 {
		t.Fatalf("expected DLQ publish at exhaustion, got %+v", stats)
	}
	if captured.envelopeCount() != 1 {
		t.Fatalf("expected DLQ envelope, got %d", captured.envelopeCount())
	}
	captured.mu.Lock()
	env := captured.envelopes[0]
	captured.mu.Unlock()
	// History records only the attempts we actually ran (the one we
	// inherited from the producer is documented as attempt=2).
	if len(env.Attempts) != 1 || env.Attempts[0].Attempt != 2 {
		t.Fatalf("attempt history: %+v", env.Attempts)
	}
}

func TestDispatchTransportErrorBubblesUp(t *testing.T) {
	// When executeOnce returns a non-application error (e.g. a store-side
	// failure), dispatch must propagate it so the consumer does NOT commit
	// the offset.
	store := &testutil.FakePersistence{
		IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
			return false, api.ActivationRecord{}, errors.New("terminal lookup failed")
		},
	}
	broker := &testutil.FakeMessaging{}
	inv := newTestInvoker(store, broker)
	inv.logger = observability.NewLoggerWithWriter("test", io.Discard)
	inv.retryMetrics = &retryMetrics{}

	clk := newFakeClock()
	err := inv.dispatchWith(context.Background(), messaging.Envelope{}, sampleRequest(api.Trigger{Type: "schedule"}), clk, fixedRand{0.5})
	if err == nil {
		t.Fatal("expected transport error to bubble up")
	}
}

func TestDispatchContextCancelDuringBackoff(t *testing.T) {
	policy := api.RetryPolicy{MaxAttempts: 5, BaseMs: 5, MaxMs: 50, JitterPct: 0}
	attempts := []scriptedAttempt{{script: scriptTimeout}}
	inv, clk, _, _ := newScriptedInvoker(t, attempts, &policy, api.Trigger{Type: "schedule"})
	clk.sleepErr = context.Canceled

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := inv.dispatchWith(ctx, messaging.Envelope{}, sampleRequest(api.Trigger{Type: "schedule", Source: map[string]any{
		"retry_policy": policy,
	}}), clk, fixedRand{0.5})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
