package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	if p.MaxAttempts != DefaultRetryMaxAttempts {
		t.Fatalf("default MaxAttempts=%d want %d", p.MaxAttempts, DefaultRetryMaxAttempts)
	}
	if p.BaseMs != DefaultBaseMs || p.MaxMs != DefaultMaxMs {
		t.Fatalf("default backoff base=%d max=%d", p.BaseMs, p.MaxMs)
	}
	if p.JitterPct != DefaultJitterPct {
		t.Fatalf("default JitterPct=%d", p.JitterPct)
	}
	if len(p.RetryableErrors) != len(DefaultRetryableErrors) {
		t.Fatalf("default RetryableErrors size=%d want %d", len(p.RetryableErrors), len(DefaultRetryableErrors))
	}
	// Caller mutation must not affect the canonical default list — verify
	// DefaultRetryPolicy returns a fresh slice.
	p.RetryableErrors[0] = "CS_TEST_TAMPER"
	again := DefaultRetryPolicy()
	if again.RetryableErrors[0] == "CS_TEST_TAMPER" {
		t.Fatal("DefaultRetryPolicy() aliases the canonical slice")
	}
}

func TestResolvedFillsDefaults(t *testing.T) {
	got := RetryPolicy{}.Resolved()
	if got.MaxAttempts != DefaultRetryMaxAttempts || got.BaseMs != DefaultBaseMs || got.MaxMs != DefaultMaxMs {
		t.Fatalf("zero-value Resolved did not default: %+v", got)
	}
	if got.JitterPct != 0 {
		// JitterPct=0 is a legal value and must not be replaced by the
		// default, because tests rely on it for deterministic behaviour.
		t.Fatalf("Resolved overrode legitimate JitterPct=0: %+v", got)
	}
	// Out-of-range jitter is clamped.
	clamped := RetryPolicy{JitterPct: 999}.Resolved()
	if clamped.JitterPct != 100 {
		t.Fatalf("JitterPct=999 not clamped: %d", clamped.JitterPct)
	}
	// MaxMs < BaseMs is bumped to BaseMs so the backoff is monotonically
	// non-decreasing.
	flipped := RetryPolicy{BaseMs: 5000, MaxMs: 100}.Resolved()
	if flipped.MaxMs < flipped.BaseMs {
		t.Fatalf("MaxMs<BaseMs not normalised: %+v", flipped)
	}
}

func TestResolvedIsIdempotent(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 3, BaseMs: 200, MaxMs: 4000, JitterPct: 10}.Resolved()
	q := p.Resolved()
	if p.MaxAttempts != q.MaxAttempts || p.BaseMs != q.BaseMs || p.MaxMs != q.MaxMs || p.JitterPct != q.JitterPct {
		t.Fatalf("Resolved not idempotent: %+v vs %+v", p, q)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		p       RetryPolicy
		wantErr string
	}{
		{"zero is valid", RetryPolicy{}, ""},
		{"negative max", RetryPolicy{MaxAttempts: -1}, "max_attempts"},
		{"too many", RetryPolicy{MaxAttempts: 1000}, "max_attempts"},
		{"negative base", RetryPolicy{BaseMs: -1}, "base_ms"},
		{"negative cap", RetryPolicy{MaxMs: -1}, "max_ms"},
		{"base > cap", RetryPolicy{BaseMs: 9000, MaxMs: 100}, "base_ms"},
		{"jitter range", RetryPolicy{JitterPct: 101}, "jitter_pct"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected nil, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestIsRetryableTrigger(t *testing.T) {
	for _, tt := range []string{"schedule", "subscription", "cadence"} {
		if !IsRetryableTrigger(tt) {
			t.Fatalf("%q should be retryable", tt)
		}
	}
	for _, tt := range []string{"http", "api", "", "unknown"} {
		if IsRetryableTrigger(tt) {
			t.Fatalf("%q must not be retryable", tt)
		}
	}
}

func TestShouldRetry(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 3}.Resolved()

	cases := []struct {
		name    string
		trigger string
		code    string
		attempt int
		want    bool
	}{
		{"retryable timeout, first attempt", "schedule", "CS_RUNTIME_TIMEOUT", 1, true},
		{"retryable timeout, last attempt", "schedule", "CS_RUNTIME_TIMEOUT", 3, false},
		{"non-retryable validation", "schedule", "CS_VALIDATION_FAILED", 1, false},
		{"non-retryable idempotency", "schedule", "CS_IDEMPOTENCY_CONFLICT", 1, false},
		{"http never retries", "http", "CS_RUNTIME_TIMEOUT", 1, false},
		{"empty code never retries", "schedule", "", 1, false},
		{"unknown code never retries", "schedule", "CS_SOMETHING_ELSE", 1, false},
		{"subscription retries", "subscription", "CS_RATE_LIMITED", 1, true},
		{"cadence retries", "cadence", "CS_RUNTIME_DEPENDENCY_ERROR", 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.ShouldRetry(tc.trigger, tc.code, tc.attempt); got != tc.want {
				t.Fatalf("ShouldRetry(%q,%q,%d)=%v want %v", tc.trigger, tc.code, tc.attempt, got, tc.want)
			}
		})
	}
}

func TestShouldRetryHonoursCustomList(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5, RetryableErrors: []string{"CS_CUSTOM_TRANSIENT"}}
	if !p.ShouldRetry("schedule", "CS_CUSTOM_TRANSIENT", 1) {
		t.Fatal("custom retryable code not honoured")
	}
	if p.ShouldRetry("schedule", "CS_RUNTIME_TIMEOUT", 1) {
		// CS_RUNTIME_TIMEOUT is in the default list but the operator opted
		// in to a narrower allow-list, so it must NOT be retried.
		t.Fatal("custom retryable list did not override default list")
	}
}

func TestShouldRetryNeverOverridesNonRetryableVeto(t *testing.T) {
	// Even an operator who explicitly lists a validation code in
	// RetryableErrors must not be able to defeat the DefaultNonRetryableErrors
	// hard-deny list — those failures will reproduce on retry.
	p := RetryPolicy{
		MaxAttempts:     5,
		RetryableErrors: []string{"CS_VALIDATION_FAILED"},
	}
	if p.ShouldRetry("schedule", "CS_VALIDATION_FAILED", 1) {
		t.Fatal("non-retryable hard-deny was overridden by operator allow-list")
	}
}

func TestRetryPolicyJSONRoundTrip(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 4, BaseMs: 250, MaxMs: 8000, JitterPct: 25, RetryableErrors: []string{"CS_X"}, DLQTopic: "cs.dlq.tenant.t1"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RetryPolicy
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.MaxAttempts != 4 || got.BaseMs != 250 || got.MaxMs != 8000 || got.JitterPct != 25 || got.DLQTopic != "cs.dlq.tenant.t1" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if len(got.RetryableErrors) != 1 || got.RetryableErrors[0] != "CS_X" {
		t.Fatalf("retryable errors round-trip mismatch: %+v", got.RetryableErrors)
	}
}

func TestRetryPolicyMarshalEmitsResolvedDefaults(t *testing.T) {
	// Producers may leave fields unset; MarshalJSON guarantees consumers
	// see the resolved (defaulted) shape.
	raw, err := json.Marshal(RetryPolicy{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(got["max_attempts"].(float64)) != DefaultRetryMaxAttempts {
		t.Fatalf("marshalled max_attempts=%v", got["max_attempts"])
	}
	if int(got["base_ms"].(float64)) != int(DefaultBaseMs) {
		t.Fatalf("marshalled base_ms=%v", got["base_ms"])
	}
	if int(got["max_ms"].(float64)) != int(DefaultMaxMs) {
		t.Fatalf("marshalled max_ms=%v", got["max_ms"])
	}
}

func TestDLQEnvelopeSerialisation(t *testing.T) {
	env := DLQEnvelope{
		Schema:           DLQSchemaV1,
		OriginalPayload:  InvocationRequest{Tenant: "t_abc123", ActivationID: "act1", RequestID: "req1"},
		LastErrorCode:    "CS_RUNTIME_TIMEOUT",
		LastErrorMessage: "exceeded 300ms",
		AttemptCount:     3,
		FirstSeenAtMS:    1000,
		LastSeenAtMS:     2000,
		Attempts: []DLQAttemptRecord{
			{Attempt: 1, StartedAtMS: 1000, DurationMS: 300, ErrorCode: "CS_RUNTIME_TIMEOUT"},
			{Attempt: 2, StartedAtMS: 1500, DurationMS: 300, ErrorCode: "CS_RUNTIME_TIMEOUT"},
			{Attempt: 3, StartedAtMS: 1900, DurationMS: 300, ErrorCode: "CS_RUNTIME_TIMEOUT"},
		},
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), `"schema":"cs.dlq.invoke.v1"`) {
		t.Fatalf("missing schema in DLQ envelope: %s", raw)
	}
	if !strings.Contains(string(raw), `"attempt_count":3`) {
		t.Fatalf("missing attempt_count: %s", raw)
	}
	var rt DLQEnvelope
	if err := json.Unmarshal(raw, &rt); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rt.AttemptCount != 3 || rt.LastErrorCode != "CS_RUNTIME_TIMEOUT" {
		t.Fatalf("round-trip mismatch: %+v", rt)
	}
	if len(rt.Attempts) != 3 {
		t.Fatalf("attempts round-trip length=%d", len(rt.Attempts))
	}
}
