package api

import (
	"encoding/json"
	"fmt"
)

// RetryPolicy describes how cs-invoker-pool reacts to a non-success terminal
// result on an asynchronous trigger (schedule, subscription, cadence).
// HTTP triggers are synchronous and let the client drive its own retry, so
// this policy is intentionally not consulted on the HTTP path. The contract
// is documented in docs/02-requirements.md "Retry & DLQ" and the on-the-wire
// envelope shape in docs/07-codeq-protocol.md.
//
// Defaults are deliberately conservative: MaxAttempts=1 means no retry,
// matching the pre-E4.03 behaviour. Callers opt in by raising MaxAttempts
// and (optionally) tuning the backoff knobs.
type RetryPolicy struct {
	// MaxAttempts is the inclusive total number of invocation attempts.
	// MaxAttempts=1 disables retry (the default). A negative or zero value
	// is normalised to 1 by ResolvedPolicy.
	MaxAttempts int `json:"max_attempts,omitempty" yaml:"max_attempts"`

	// BaseMs is the first backoff sleep in milliseconds. The Nth retry
	// waits min(MaxMs, BaseMs * 2^(N-1)) ± JitterPct percent. A value
	// <= 0 falls back to DefaultBaseMs.
	BaseMs int64 `json:"base_ms,omitempty" yaml:"base_ms"`

	// MaxMs is the upper bound (in milliseconds) on a single backoff sleep.
	// A value <= 0 falls back to DefaultMaxMs.
	MaxMs int64 `json:"max_ms,omitempty" yaml:"max_ms"`

	// JitterPct is the symmetric jitter percentage applied to the backoff
	// delay. Valid range is [0, 100]; values outside are clamped. A value
	// of 0 disables jitter (useful for deterministic tests).
	JitterPct int `json:"jitter_pct,omitempty" yaml:"jitter_pct"`

	// RetryableErrors is the explicit allow-list of error codes that trigger
	// a retry. Anything not listed is sent straight to the DLQ on the first
	// failure. A nil/empty slice falls back to DefaultRetryableErrors so an
	// operator who enables retry without listing codes still gets sensible
	// behaviour.
	RetryableErrors []string `json:"retryable_errors,omitempty" yaml:"retryable_errors"`

	// DLQTopic optionally overrides the default tenant DLQ topic
	// (cs.dlq.invoke). When set, exhausted retries land on that topic
	// instead. The string must follow the codeQ topic-naming rules
	// described in docs/07-codeq-protocol.md.
	DLQTopic string `json:"dlq_topic,omitempty" yaml:"dlq_topic"`
}

// Retry tuning defaults. The values match the recommendations in
// docs/02-requirements.md "Retry & DLQ" and were chosen so a misconfigured
// tenant policy never starves the worker pool with unbounded backoff.
const (
	DefaultRetryMaxAttempts = 1
	DefaultBaseMs           = int64(500)
	DefaultMaxMs            = int64(30_000)
	DefaultJitterPct        = 20
)

// DefaultRetryableErrors enumerates the error codes that the platform treats
// as transient. Validation/auth/idempotency-class failures intentionally stay
// out: re-invoking them with the same payload would just reproduce the same
// terminal error. Keep in sync with docs/21-errors.md.
//
// The list mixes CS_-prefixed codes (used by the platform internals and the
// HTTP error envelope) and short runtime-friendly labels (used by
// internal/runtime to populate InvocationError.Type). The retry decision
// matches against whichever shape the producer sends, so a function author
// who lists "Timeout" gets the same behaviour as one who lists
// "CS_RUNTIME_TIMEOUT".
var DefaultRetryableErrors = []string{
	"CS_RUNTIME_TIMEOUT",
	"CS_RUNTIME_DEPENDENCY_ERROR",
	"CS_RATE_LIMITED",
	"Timeout",
}

// DefaultNonRetryableErrors documents (for reviewers and operators) the
// classes that must never be retried even when an over-eager policy lists
// them. ShouldRetry enforces this list as a hard veto.
var DefaultNonRetryableErrors = []string{
	"CS_VALIDATION_FAILED",
	"CS_VALIDATION_MANIFEST_INVALID",
	"CS_VALIDATION_BUNDLE_TOO_LARGE",
	"CS_VALIDATION_NAME_INVALID",
	"CS_NOT_FOUND",
	"CS_IDEMPOTENCY_CONFLICT",
}

// DefaultRetryPolicy returns the platform-wide default policy. It is the
// zero-retry baseline; callers must opt in to retry by setting MaxAttempts > 1.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:     DefaultRetryMaxAttempts,
		BaseMs:          DefaultBaseMs,
		MaxMs:           DefaultMaxMs,
		JitterPct:       DefaultJitterPct,
		RetryableErrors: append([]string(nil), DefaultRetryableErrors...),
	}
}

// Resolved fills in any unset fields on p with the platform defaults. The
// receiver is returned so the call can be chained. Resolved is idempotent.
func (p RetryPolicy) Resolved() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultRetryMaxAttempts
	}
	if p.BaseMs <= 0 {
		p.BaseMs = DefaultBaseMs
	}
	if p.MaxMs <= 0 {
		p.MaxMs = DefaultMaxMs
	}
	if p.MaxMs < p.BaseMs {
		p.MaxMs = p.BaseMs
	}
	if p.JitterPct < 0 {
		p.JitterPct = 0
	}
	if p.JitterPct > 100 {
		p.JitterPct = 100
	}
	if len(p.RetryableErrors) == 0 {
		p.RetryableErrors = append([]string(nil), DefaultRetryableErrors...)
	}
	return p
}

// Validate returns an error when the policy mixes legal-but-incoherent values
// the caller almost certainly did not mean. Operators get a clear failure at
// configuration parse time instead of a silent runtime mis-behaviour.
func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 0 {
		return fmt.Errorf("retry policy: max_attempts must be >= 0 (got %d)", p.MaxAttempts)
	}
	if p.MaxAttempts > 100 {
		return fmt.Errorf("retry policy: max_attempts must be <= 100 (got %d)", p.MaxAttempts)
	}
	if p.BaseMs < 0 {
		return fmt.Errorf("retry policy: base_ms must be >= 0 (got %d)", p.BaseMs)
	}
	if p.MaxMs < 0 {
		return fmt.Errorf("retry policy: max_ms must be >= 0 (got %d)", p.MaxMs)
	}
	if p.MaxMs > 0 && p.BaseMs > p.MaxMs {
		return fmt.Errorf("retry policy: base_ms (%d) must be <= max_ms (%d)", p.BaseMs, p.MaxMs)
	}
	if p.JitterPct < 0 || p.JitterPct > 100 {
		return fmt.Errorf("retry policy: jitter_pct must be in [0, 100] (got %d)", p.JitterPct)
	}
	return nil
}

// IsRetryableTrigger reports whether triggerType participates in the policy.
// HTTP is excluded by design: the caller is waiting for a response, so the
// retry decision belongs to the client. Schedule, subscription and cadence
// are async/queued so the platform owns the retry contract for them.
func IsRetryableTrigger(triggerType string) bool {
	switch triggerType {
	case "schedule", "subscription", "cadence":
		return true
	default:
		return false
	}
}

// ShouldRetry returns true when (a) the trigger is one we own the retry for,
// (b) we still have attempts left, and (c) the terminal error code is in the
// allow-list and not in DefaultNonRetryableErrors. Callers should treat a
// false return as "publish to DLQ".
//
// errorCode is the InvocationError.Type string from the runtime; an empty
// string is treated as a non-retryable terminal failure so a missing/empty
// error never causes an infinite retry loop.
func (p RetryPolicy) ShouldRetry(triggerType, errorCode string, attempt int) bool {
	if !IsRetryableTrigger(triggerType) {
		return false
	}
	if errorCode == "" {
		return false
	}
	resolved := p.Resolved()
	if attempt >= resolved.MaxAttempts {
		return false
	}
	for _, banned := range DefaultNonRetryableErrors {
		if errorCode == banned {
			return false
		}
	}
	for _, allowed := range resolved.RetryableErrors {
		if errorCode == allowed {
			return true
		}
	}
	return false
}

// MarshalJSON normalises the on-the-wire form by always emitting the resolved
// (defaulted) policy. This guarantees that consumers see the same shape
// regardless of whether the producer left fields unset.
func (p RetryPolicy) MarshalJSON() ([]byte, error) {
	r := p.Resolved()
	type alias RetryPolicy
	return json.Marshal(alias(r))
}

// DLQEnvelope is the body of messages published to the tenant DLQ topic when
// a retry budget is exhausted. It carries enough context for a future replay
// tool (`cs dlq replay`) to re-enqueue the original payload. The shape is
// documented in docs/07-codeq-protocol.md "Retry & DLQ".
type DLQEnvelope struct {
	Schema           string             `json:"schema"`
	OriginalPayload  InvocationRequest  `json:"original_payload"`
	LastErrorCode    string             `json:"last_error_code,omitempty"`
	LastErrorMessage string             `json:"last_error_message,omitempty"`
	AttemptCount     int                `json:"attempt_count"`
	FirstSeenAtMS    int64              `json:"first_seen_at_ms"`
	LastSeenAtMS     int64              `json:"last_seen_at_ms"`
	Attempts         []DLQAttemptRecord `json:"attempts,omitempty"`
}

// DLQAttemptRecord records one attempt in the history embedded on a DLQ
// envelope. Producers may omit Message to keep DLQ payloads small.
type DLQAttemptRecord struct {
	Attempt      int    `json:"attempt"`
	StartedAtMS  int64  `json:"started_at_ms"`
	DurationMS   int64  `json:"duration_ms"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// DLQSchemaV1 is the schema discriminator used on DLQEnvelope so future
// versions can be added without breaking consumers.
const DLQSchemaV1 = "cs.dlq.invoke.v1"
