package api

import (
	"fmt"
	"strings"
)

// Sampling mode identifiers. Defaults to SamplingModeAlways so triggers
// without an explicit policy keep the platform's pre-E7.02 behaviour of
// recording every activation byte-for-byte.
const (
	SamplingModeAlways        = "always"
	SamplingModeHead          = "head"
	SamplingModeTail          = "tail"
	SamplingModeProbabilistic = "probabilistic"
)

// Sampling decision values recorded on the activation when an operator
// wants to know why a particular activation was retained or skipped.
const (
	SamplingDecisionOff           = "off"
	SamplingDecisionHead          = "head"
	SamplingDecisionTail          = "tail"
	SamplingDecisionProbabilistic = "probabilistic"
	SamplingDecisionAlways        = "always"
	SamplingDecisionSkipped       = "skipped"
)

// SamplingPolicy describes how the invoker should decide whether to
// persist an activation's metadata, logs and result blob. The policy is
// attached per-trigger so the same function can be sampled differently
// when invoked via HTTP vs schedule vs Cadence.
//
// The zero value (Mode == "") is equivalent to SamplingModeAlways: this
// keeps existing triggers byte-for-byte compatible with pre-E7.02
// deployments where every activation was retained.
type SamplingPolicy struct {
	// Mode selects the sampler. One of: always, head, tail, probabilistic.
	Mode string `json:"mode,omitempty"`

	// HeadPerMinute caps the number of activations recorded per
	// (tenant, function-ref) per rolling minute window when Mode == head.
	// Values <= 0 mean "record nothing" so callers must validate before
	// install; the Decider treats <= 0 as a misconfiguration and drops.
	HeadPerMinute int `json:"head_per_minute,omitempty"`

	// TailOnError opts in to keeping every activation whose status is not
	// "success" when Mode == tail. Defaults to true through Normalize.
	TailOnError bool `json:"tail_on_error,omitempty"`

	// TailOnSlowMs is the duration threshold (milliseconds) above which a
	// tail-sampled activation is promoted to a full record. Zero disables
	// the slow-call rule; the error rule still applies when TailOnError.
	TailOnSlowMs int `json:"tail_on_slow_ms,omitempty"`

	// Probability is the keep ratio in [0,1] when Mode == probabilistic.
	// 1.0 keeps every activation, 0.0 drops every activation. The decider
	// rejects values outside the range during Normalize.
	Probability float64 `json:"probability,omitempty"`
}

// Normalize fills in zero-value defaults and returns an error when the
// policy is internally inconsistent (e.g. probabilistic mode with a
// probability outside [0,1]). It is safe to call on a nil receiver: in
// that case Normalize returns the default always-on policy.
func (p *SamplingPolicy) Normalize() (SamplingPolicy, error) {
	if p == nil {
		return SamplingPolicy{Mode: SamplingModeAlways}, nil
	}
	out := *p
	out.Mode = strings.TrimSpace(strings.ToLower(out.Mode))
	if out.Mode == "" {
		out.Mode = SamplingModeAlways
	}
	switch out.Mode {
	case SamplingModeAlways:
		return out, nil
	case SamplingModeHead:
		if out.HeadPerMinute <= 0 {
			return out, fmt.Errorf("sampling head_per_minute must be > 0")
		}
		return out, nil
	case SamplingModeTail:
		if !out.TailOnError && out.TailOnSlowMs <= 0 {
			return out, fmt.Errorf("sampling tail requires tail_on_error or tail_on_slow_ms > 0")
		}
		return out, nil
	case SamplingModeProbabilistic:
		if out.Probability < 0 || out.Probability > 1 {
			return out, fmt.Errorf("sampling probability must be within [0,1]")
		}
		return out, nil
	default:
		return out, fmt.Errorf("sampling mode %q not supported", out.Mode)
	}
}

// DefaultSamplingPolicy returns the policy used when no per-trigger
// override is configured. It records every activation in full so the
// platform stays backward-compatible with existing deployments.
func DefaultSamplingPolicy() SamplingPolicy {
	return SamplingPolicy{Mode: SamplingModeAlways}
}
