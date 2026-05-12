package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
)

// retryClock isolates wall-clock dependencies so unit tests can drive the
// backoff loop deterministically. Production code uses the trivial impl
// realClock{} which forwards to time.Now and time.Sleep with cancellation.
type retryClock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// retryRand exposes a deterministic random source for tests. Production uses
// a goroutine-safe rand.Rand seeded from the system clock.
type retryRand interface {
	Float64() float64
}

type defaultRand struct{}

func (defaultRand) Float64() float64 { return rand.Float64() }

// retryMetrics tracks the counters described in the issue: cs_invoke_retry_total
// (attempts that were retried), cs_invoke_retry_success_total (attempts that
// succeeded on retry, i.e. attempt > 1), and cs_invoke_dlq_total (envelopes
// forwarded to the DLQ on exhaustion). The implementation here exposes simple
// atomic counters that a future PR can wire to a Prometheus collector via
// internal/observability. See docs/14-observability.md.
type retryMetrics struct {
	retry        atomic.Uint64
	retrySuccess atomic.Uint64
	dlq          atomic.Uint64
}

func (m *retryMetrics) incRetry()        { m.retry.Add(1) }
func (m *retryMetrics) incRetrySuccess() { m.retrySuccess.Add(1) }
func (m *retryMetrics) incDLQ()          { m.dlq.Add(1) }

// RetryStats is a snapshot of the running counters; tests use it as their
// assertion surface.
type RetryStats struct {
	Retry        uint64
	RetrySuccess uint64
	DLQ          uint64
}

func (m *retryMetrics) snapshot() RetryStats {
	return RetryStats{Retry: m.retry.Load(), RetrySuccess: m.retrySuccess.Load(), DLQ: m.dlq.Load()}
}

// triggerRetryPolicy returns the RetryPolicy carried on the trigger source
// map for this invocation, falling back to fallback. Producers (scheduler,
// cadence-poller, subscription dispatcher) embed the policy on the trigger
// so cs-invoker-pool can honour it without per-tenant lookup.
//
// The policy is encoded under trigger.source["retry_policy"] either as a
// RetryPolicy value or its JSON shape; both are tolerated to keep the
// producer side flexible. An unparseable policy is treated as missing, so
// a buggy producer never poisons the worker pool.
func triggerRetryPolicy(req api.InvocationRequest, fallback api.RetryPolicy) api.RetryPolicy {
	if req.Trigger.Source == nil {
		return fallback.Resolved()
	}
	raw, ok := req.Trigger.Source["retry_policy"]
	if !ok || raw == nil {
		return fallback.Resolved()
	}
	switch v := raw.(type) {
	case api.RetryPolicy:
		return v.Resolved()
	case map[string]any:
		// JSON round-trip is the cheapest way to coerce a dynamic map into
		// the typed shape and surface fields like base_ms / jitter_pct
		// uniformly. An invalid value falls back to the operator default.
		buf, err := json.Marshal(v)
		if err != nil {
			return fallback.Resolved()
		}
		var p api.RetryPolicy
		if err := json.Unmarshal(buf, &p); err != nil {
			return fallback.Resolved()
		}
		return p.Resolved()
	}
	return fallback.Resolved()
}

// defaultRetryPolicyFromConfig translates the cs_invoker_pool.retry YAML
// block into an api.RetryPolicy. Missing fields fall back to the platform
// default via Resolved().
func defaultRetryPolicyFromConfig(cfg config.Config) api.RetryPolicy {
	r := cfg.CSInvokerPool.Retry
	return api.RetryPolicy{
		MaxAttempts:     r.MaxAttempts,
		BaseMs:          r.BaseMs,
		MaxMs:           r.MaxMs,
		JitterPct:       r.JitterPct,
		RetryableErrors: r.RetryableErrors,
		DLQTopic:        r.DLQTopic,
	}.Resolved()
}

// triggerAttempt extracts the running attempt counter from the trigger
// source, defaulting to 1 (= first attempt). The scheduler/poller increments
// this on each re-publish.
func triggerAttempt(req api.InvocationRequest) int {
	if req.Trigger.Source == nil {
		return 1
	}
	switch v := req.Trigger.Source["attempt"].(type) {
	case int:
		if v < 1 {
			return 1
		}
		return v
	case int64:
		if v < 1 {
			return 1
		}
		return int(v)
	case float64:
		// JSON-decoded ints arrive as float64.
		if v < 1 {
			return 1
		}
		return int(v)
	}
	return 1
}

// computeBackoff returns the (jittered) sleep duration for the Nth retry
// attempt. attempt=1 means "we just finished attempt 1 and the next retry
// will be attempt 2"; the backoff therefore uses 2^(attempt-1).
//
// The formula matches the spec in docs/02-requirements.md:
//
//	delay = min(MaxMs, BaseMs * 2^(attempt-1))
//
// with a symmetric jitter band of ±JitterPct%. We hand-roll the exponent so
// the implementation has no third-party rate-limit dependency.
func computeBackoff(p api.RetryPolicy, attempt int, r retryRand) time.Duration {
	pp := p.Resolved()
	if attempt < 1 {
		attempt = 1
	}
	// Compute 2^(attempt-1) with an overflow guard: a 63-bit shift would
	// wrap to zero or negative. We clamp at MaxMs long before that.
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	scale := int64(1) << uint(shift)
	delay := pp.BaseMs * scale
	if delay <= 0 || delay > pp.MaxMs {
		delay = pp.MaxMs
	}
	if pp.JitterPct > 0 && r != nil {
		// Map Float64()∈[0,1) into [-JitterPct, +JitterPct] / 100.
		frac := (r.Float64()*2 - 1) * float64(pp.JitterPct) / 100.0
		jittered := float64(delay) * (1 + frac)
		if jittered < 0 {
			jittered = 0
		}
		if int64(jittered) > pp.MaxMs {
			jittered = float64(pp.MaxMs)
		}
		delay = int64(jittered)
	}
	return time.Duration(delay) * time.Millisecond
}

// dispatch is the retry-aware entry point. It runs the underlying
// executeOnce repeatedly while the policy says so, recording each attempt
// for the DLQ envelope. When the budget is exhausted (or a non-retryable
// terminal error is returned) it forwards a DLQEnvelope to the DLQ topic
// and ACKs the original message by returning nil — never block the
// consumer loop on a poisoned message.
func (i *invoker) dispatch(ctx context.Context, env messaging.Envelope, req api.InvocationRequest) error {
	return i.dispatchWith(ctx, env, req, realClock{}, defaultRand{})
}

// dispatchWith is the test-injectable form of dispatch.
func (i *invoker) dispatchWith(ctx context.Context, env messaging.Envelope, req api.InvocationRequest, clk retryClock, r retryRand) error {
	// HTTP is synchronous: retry is the caller's responsibility. Route
	// straight through executeOnce so we don't touch the existing fast path.
	if !api.IsRetryableTrigger(req.Trigger.Type) {
		_, err := i.executeOnce(ctx, env, req)
		return err
	}

	policy := triggerRetryPolicy(req, i.defaultRetryPolicy)
	startedAttempt := triggerAttempt(req)
	firstSeenMS := clk.Now().UnixMilli()
	history := make([]api.DLQAttemptRecord, 0, policy.MaxAttempts)

	current := req
	for attempt := startedAttempt; attempt <= policy.MaxAttempts; attempt++ {
		// Make sure the current attempt number is reflected on the trigger
		// metadata so the runtime and downstream observers can see it.
		current = withAttempt(current, attempt)

		startMS := clk.Now().UnixMilli()
		result, err := i.executeOnce(ctx, env, current)
		if err != nil {
			// Transport / store errors are not application-level failures;
			// they should signal the consumer to retry the *codeQ delivery*
			// (i.e. NOT commit the offset). Bubble up unchanged.
			return err
		}

		// Treat a successful terminal as immediate exit. When attempt > 1
		// we also count it as a retry-success metric.
		if isSuccessResult(result) {
			if attempt > startedAttempt && i.retryMetrics != nil {
				i.retryMetrics.incRetrySuccess()
			}
			return nil
		}

		errCode, errMsg := extractTerminalError(result)
		history = append(history, api.DLQAttemptRecord{
			Attempt:      attempt,
			StartedAtMS:  startMS,
			DurationMS:   result.DurationMS,
			ErrorCode:    errCode,
			ErrorMessage: errMsg,
		})

		// Decide: retry, or DLQ + done.
		if !policy.ShouldRetry(current.Trigger.Type, errCode, attempt) {
			i.publishDLQOnExhaustion(ctx, req, errCode, errMsg, attempt, firstSeenMS, clk.Now().UnixMilli(), history)
			return nil
		}

		// We will retry: log+metric and sleep with backoff.
		if i.retryMetrics != nil {
			i.retryMetrics.incRetry()
		}
		if i.logger != nil {
			i.logger.Warn(ctx, "retry scheduled tenant="+req.Tenant+
				" activation_id="+req.ActivationID+
				" attempt="+strconv.Itoa(attempt)+"/"+strconv.Itoa(policy.MaxAttempts)+
				" code="+errCode)
		}
		if err := clk.Sleep(ctx, computeBackoff(policy, attempt, r)); err != nil {
			// Context cancelled during backoff — preserve the message so
			// the next consumer can pick it up.
			return err
		}
	}

	// Budget exhausted: the last attempt's error sits at the tail of the
	// history slice. Publish to DLQ.
	var lastCode, lastMsg string
	if n := len(history); n > 0 {
		lastCode = history[n-1].ErrorCode
		lastMsg = history[n-1].ErrorMessage
	}
	i.publishDLQOnExhaustion(ctx, req, lastCode, lastMsg, len(history), firstSeenMS, clk.Now().UnixMilli(), history)
	return nil
}

// publishDLQOnExhaustion wraps the original request in a DLQEnvelope, stamps
// it with the attempt history, and forwards it to the broker DLQ. Failures
// are logged but never bubble up: the original request has already been
// processed, and re-delivering it would just re-trigger the same loop.
func (i *invoker) publishDLQOnExhaustion(ctx context.Context, req api.InvocationRequest, errCode, errMsg string, attempts int, firstSeenMS, lastSeenMS int64, history []api.DLQAttemptRecord) {
	if i.retryMetrics != nil {
		i.retryMetrics.incDLQ()
	}
	envelope := api.DLQEnvelope{
		Schema:           api.DLQSchemaV1,
		OriginalPayload:  req,
		LastErrorCode:    errCode,
		LastErrorMessage: errMsg,
		AttemptCount:     attempts,
		FirstSeenAtMS:    firstSeenMS,
		LastSeenAtMS:     lastSeenMS,
		Attempts:         history,
	}
	if i.dlqPublisher != nil {
		if err := i.dlqPublisher(ctx, req.Tenant, envelope); err != nil && i.logger != nil {
			i.logger.Error(ctx, "DLQ publish failed tenant="+req.Tenant+" err="+err.Error())
		}
		return
	}
	// Fallback path: best-effort PublishDLQInvoke with the bare original
	// payload. This keeps the platform DLQ contract honest even when the
	// envelope-aware publisher is not configured.
	if err := i.broker.PublishDLQInvoke(ctx, req.Tenant, req); err != nil && i.logger != nil {
		i.logger.Error(ctx, "DLQ publish failed tenant="+req.Tenant+" err="+err.Error())
	}
}

// isSuccessResult reports whether the InvocationResult counts as a terminal
// success. Anything else (error, timeout, empty) is a candidate for retry or
// DLQ depending on the policy.
func isSuccessResult(r api.InvocationResult) bool {
	if r.Status != "success" {
		return false
	}
	if r.Error != nil && r.Error.Type != "" {
		return false
	}
	return true
}

// extractTerminalError pulls a usable (code, message) tuple out of an
// InvocationResult. When no explicit error is present but the status is not
// success we synthesise a generic CS_RUNTIME_EXCEPTION so the policy has
// something to match against.
func extractTerminalError(r api.InvocationResult) (string, string) {
	if r.Error != nil && r.Error.Type != "" {
		return r.Error.Type, r.Error.Message
	}
	switch r.Status {
	case "timeout":
		return "CS_RUNTIME_TIMEOUT", "function timed out"
	case "error":
		return "CS_RUNTIME_EXCEPTION", "function returned a non-success status"
	}
	return "", ""
}

// withAttempt returns a copy of req with trigger.source["attempt"] stamped
// to n. The original request is left untouched so the DLQ envelope still
// carries the producer's view of the payload.
func withAttempt(req api.InvocationRequest, n int) api.InvocationRequest {
	if req.Trigger.Source == nil {
		req.Trigger.Source = map[string]any{}
	} else {
		// Shallow-copy the map so concurrent retries don't race on the
		// shared trigger source.
		clone := make(map[string]any, len(req.Trigger.Source)+1)
		for k, v := range req.Trigger.Source {
			clone[k] = v
		}
		req.Trigger.Source = clone
	}
	req.Trigger.Source["attempt"] = n
	return req
}
