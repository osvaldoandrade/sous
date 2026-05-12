package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// buildSamplingInvoker wires the invoker with the fakes needed to drive
// handleInvocation end-to-end under different sampling policies. The
// returned counters expose the side effects each sampling mode is meant
// to control: how many running rows were written, how many terminal
// rows were CAS-completed, how many log chunks were appended, and which
// sampling_decision values were stamped on the activation records.
type samplingCounters struct {
	put        atomic.Int32
	cas        atomic.Int32
	logs       atomic.Int32
	decisions  []string
	terminalCh chan api.ActivationRecord
}

func buildSamplingInvoker(t *testing.T, policy *api.SamplingPolicy, clock func() time.Time) (*invoker, *samplingCounters) {
	t.Helper()
	bundleBytes, sha := buildInvokerBundle(t)
	counters := &samplingCounters{terminalCh: make(chan api.ActivationRecord, 64)}
	store := &testutil.FakePersistence{
		IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
			return false, api.ActivationRecord{}, nil
		},
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 1, nil
		},
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				SHA256: sha,
				Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
			}, bundleBytes, nil
		},
		PutActivationRunningFn: func(ctx context.Context, rec api.ActivationRecord, ttl time.Duration) error {
			_ = ctx
			_ = ttl
			counters.put.Add(1)
			counters.decisions = append(counters.decisions, "start:"+rec.SamplingDecision)
			return nil
		},
		CompleteActivationCASFn: func(ctx context.Context, rec api.ActivationRecord, ttl time.Duration) (bool, error) {
			_ = ctx
			_ = ttl
			counters.cas.Add(1)
			counters.decisions = append(counters.decisions, "cas:"+rec.SamplingDecision)
			select {
			case counters.terminalCh <- rec:
			default:
			}
			return true, nil
		},
		SaveResultByRequestIDFn: func(context.Context, string, string, api.InvocationResult, time.Duration) error {
			return nil
		},
		AppendLogChunkFn: func(context.Context, string, string, int64, []byte, time.Duration) error {
			counters.logs.Add(1)
			return nil
		},
	}
	broker := &testutil.FakeMessaging{
		PublishResultFn: func(context.Context, string, api.InvocationResult) error { return nil },
	}
	i := newTestInvoker(store, broker)
	i.samplers = newSamplerCache()
	i.clock = clock
	_ = policy // policies travel on the Trigger; this helper just plumbs.
	return i, counters
}

func runSamplingInvocation(t *testing.T, i *invoker, activationID string, trigger api.Trigger, script string) {
	t.Helper()
	bundleBytes, sha := buildInvokerBundleWithScript(t, script)
	// Replace the GetVersion fake so the test-specific bundle wins. The
	// other fakes from buildSamplingInvoker stay in place.
	if fp, ok := i.store.(*testutil.FakePersistence); ok {
		fp.GetVersionFn = func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				SHA256: sha,
				Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
			}, bundleBytes, nil
		}
	}
	if err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: activationID,
		RequestID:    "req-" + activationID,
		Tenant:       "t_alpha",
		Namespace:    "ns",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
		Trigger:      trigger,
	}); err != nil {
		t.Fatalf("handleInvocation(%s) failed: %v", activationID, err)
	}
}

func TestHandleInvocationAlwaysSampler(t *testing.T) {
	i, c := buildSamplingInvoker(t, nil, time.Now)
	trigger := api.Trigger{Type: "api"} // no policy => always
	runSamplingInvocation(t, i, "act1", trigger, `export default function(){ cs.log.info("hi"); return { statusCode: 200, body: "ok" } }`)
	if c.put.Load() != 1 || c.cas.Load() != 1 {
		t.Fatalf("always sampler must write start and complete: put=%d cas=%d", c.put.Load(), c.cas.Load())
	}
	if c.logs.Load() != 1 {
		t.Fatalf("always sampler must persist logs: logs=%d", c.logs.Load())
	}
	rec := <-c.terminalCh
	if rec.SamplingDecision != api.SamplingDecisionAlways {
		t.Fatalf("expected always decision, got %q", rec.SamplingDecision)
	}
	if rec.Result == nil {
		t.Fatalf("always sampler must persist result blob")
	}
}

func TestHandleInvocationHeadSampler(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	i, c := buildSamplingInvoker(t, nil, clock)
	policy := &api.SamplingPolicy{Mode: api.SamplingModeHead, HeadPerMinute: 2}
	trigger := api.Trigger{Type: "api", Sampling: policy}
	for n := 0; n < 5; n++ {
		runSamplingInvocation(t, i, fmt.Sprintf("act%d", n), trigger,
			`export default function(){ return { statusCode: 200, body: "ok" } }`)
	}
	if c.put.Load() != 2 || c.cas.Load() != 2 {
		t.Fatalf("head sampler kept wrong count: put=%d cas=%d (want 2/2)", c.put.Load(), c.cas.Load())
	}
}

func TestHandleInvocationTailSamplerSuccessKeepsSkeleton(t *testing.T) {
	i, c := buildSamplingInvoker(t, nil, time.Now)
	policy := &api.SamplingPolicy{Mode: api.SamplingModeTail, TailOnError: true, TailOnSlowMs: 10_000}
	trigger := api.Trigger{Type: "api", Sampling: policy}
	runSamplingInvocation(t, i, "act_ok", trigger,
		`export default function(){ cs.log.info("ignored"); return { statusCode: 200, body: "ok" } }`)
	if c.put.Load() != 1 || c.cas.Load() != 1 {
		t.Fatalf("tail sampler must always write a row: put=%d cas=%d", c.put.Load(), c.cas.Load())
	}
	if c.logs.Load() != 0 {
		t.Fatalf("tail success must skip logs: logs=%d", c.logs.Load())
	}
	rec := <-c.terminalCh
	if rec.SamplingDecision != api.SamplingDecisionTail {
		t.Fatalf("tail decision = %q", rec.SamplingDecision)
	}
	if rec.Result != nil {
		t.Fatalf("tail success must NOT persist result blob")
	}
}

func TestHandleInvocationTailSamplerErrorPromotes(t *testing.T) {
	i, c := buildSamplingInvoker(t, nil, time.Now)
	policy := &api.SamplingPolicy{Mode: api.SamplingModeTail, TailOnError: true}
	trigger := api.Trigger{Type: "api", Sampling: policy}
	runSamplingInvocation(t, i, "act_err", trigger,
		`export default function(){ throw new Error("boom") }`)
	if c.put.Load() != 1 || c.cas.Load() != 1 {
		t.Fatalf("tail error must write start+complete: put=%d cas=%d", c.put.Load(), c.cas.Load())
	}
	rec := <-c.terminalCh
	if rec.Status == "success" {
		t.Fatalf("expected non-success status, got %q", rec.Status)
	}
	if rec.Result == nil && rec.Error == nil {
		t.Fatalf("tail error must persist either result or error: %+v", rec)
	}
}

func TestHandleInvocationProbabilisticDropSkipsWrites(t *testing.T) {
	i, c := buildSamplingInvoker(t, nil, time.Now)
	policy := &api.SamplingPolicy{Mode: api.SamplingModeProbabilistic, Probability: 0}
	trigger := api.Trigger{Type: "api", Sampling: policy}
	runSamplingInvocation(t, i, "act_drop", trigger,
		`export default function(){ return { statusCode: 200, body: "ok" } }`)
	if c.put.Load() != 0 || c.cas.Load() != 0 {
		t.Fatalf("p=0 must skip all activation writes: put=%d cas=%d", c.put.Load(), c.cas.Load())
	}
	if c.logs.Load() != 0 {
		t.Fatalf("p=0 must skip logs: logs=%d", c.logs.Load())
	}
}

func TestHandleInvocationProbabilisticKeepWritesEverything(t *testing.T) {
	i, c := buildSamplingInvoker(t, nil, time.Now)
	policy := &api.SamplingPolicy{Mode: api.SamplingModeProbabilistic, Probability: 1}
	trigger := api.Trigger{Type: "api", Sampling: policy}
	runSamplingInvocation(t, i, "act_keep", trigger,
		`export default function(){ cs.log.info("kept"); return { statusCode: 200, body: "ok" } }`)
	if c.put.Load() != 1 || c.cas.Load() != 1 || c.logs.Load() != 1 {
		t.Fatalf("p=1 must persist everything: put=%d cas=%d logs=%d", c.put.Load(), c.cas.Load(), c.logs.Load())
	}
}

func TestSamplerCacheReusesDeciderForSamePolicy(t *testing.T) {
	c := newSamplerCache()
	policy := &api.SamplingPolicy{Mode: api.SamplingModeHead, HeadPerMinute: 4}
	first := c.deciderFor(api.Trigger{Sampling: policy}, time.Now)
	second := c.deciderFor(api.Trigger{Sampling: &api.SamplingPolicy{Mode: api.SamplingModeHead, HeadPerMinute: 4}}, time.Now)
	if first != second {
		t.Fatalf("samplerCache must return the same Decider for equivalent policies")
	}
	other := c.deciderFor(api.Trigger{Sampling: &api.SamplingPolicy{Mode: api.SamplingModeHead, HeadPerMinute: 8}}, time.Now)
	if first == other {
		t.Fatalf("samplerCache must return distinct Deciders for different policies")
	}
}

func TestSamplerInvalidPolicyFallsBackToAlways(t *testing.T) {
	i, c := buildSamplingInvoker(t, nil, time.Now)
	// Probability outside [0,1] is rejected by Normalize; the invoker
	// must default to always-on rather than silently dropping traffic.
	policy := &api.SamplingPolicy{Mode: api.SamplingModeProbabilistic, Probability: -1}
	trigger := api.Trigger{Type: "api", Sampling: policy}
	runSamplingInvocation(t, i, "act_bad", trigger,
		`export default function(){ return { statusCode: 200, body: "ok" } }`)
	if c.put.Load() != 1 || c.cas.Load() != 1 {
		t.Fatalf("invalid policy must fall back to always: put=%d cas=%d", c.put.Load(), c.cas.Load())
	}
}
