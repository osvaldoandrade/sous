package api

import (
	"encoding/json"
	"testing"
)

func TestSamplingPolicyNormalizeDefaults(t *testing.T) {
	var nilPolicy *SamplingPolicy
	out, err := nilPolicy.Normalize()
	if err != nil {
		t.Fatalf("nil policy should normalize without error: %v", err)
	}
	if out.Mode != SamplingModeAlways {
		t.Fatalf("nil policy should default to always, got %q", out.Mode)
	}

	zero := SamplingPolicy{}
	got, err := zero.Normalize()
	if err != nil {
		t.Fatalf("zero policy should normalize: %v", err)
	}
	if got.Mode != SamplingModeAlways {
		t.Fatalf("zero policy mode = %q, want always", got.Mode)
	}
}

func TestSamplingPolicyNormalizeValidation(t *testing.T) {
	cases := []struct {
		name    string
		policy  SamplingPolicy
		wantErr bool
	}{
		{name: "always ok", policy: SamplingPolicy{Mode: "always"}, wantErr: false},
		{name: "head requires per-minute", policy: SamplingPolicy{Mode: "head"}, wantErr: true},
		{name: "head with cap ok", policy: SamplingPolicy{Mode: "head", HeadPerMinute: 5}, wantErr: false},
		{name: "tail requires criterion", policy: SamplingPolicy{Mode: "tail"}, wantErr: true},
		{name: "tail on error ok", policy: SamplingPolicy{Mode: "tail", TailOnError: true}, wantErr: false},
		{name: "tail on slow ok", policy: SamplingPolicy{Mode: "tail", TailOnSlowMs: 100}, wantErr: false},
		{name: "probabilistic in range ok", policy: SamplingPolicy{Mode: "probabilistic", Probability: 0.25}, wantErr: false},
		{name: "probabilistic negative rejected", policy: SamplingPolicy{Mode: "probabilistic", Probability: -0.1}, wantErr: true},
		{name: "probabilistic above 1 rejected", policy: SamplingPolicy{Mode: "probabilistic", Probability: 1.1}, wantErr: true},
		{name: "unknown mode rejected", policy: SamplingPolicy{Mode: "adaptive"}, wantErr: true},
		{name: "mixed-case mode normalized", policy: SamplingPolicy{Mode: "ALWAYS"}, wantErr: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.policy.Normalize()
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %+v", tc.policy)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for %+v: %v", tc.policy, err)
			}
		})
	}
}

func TestSamplingPolicyJSONRoundTrip(t *testing.T) {
	p := SamplingPolicy{
		Mode:          SamplingModeTail,
		TailOnError:   true,
		TailOnSlowMs:  250,
		HeadPerMinute: 0,
		Probability:   0,
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(raw); got != `{"mode":"tail","tail_on_error":true,"tail_on_slow_ms":250}` {
		t.Fatalf("unexpected json: %s", got)
	}

	var back SamplingPolicy
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != p {
		t.Fatalf("round trip mismatch: %+v vs %+v", back, p)
	}
}

func TestDefaultSamplingPolicyIsAlways(t *testing.T) {
	p := DefaultSamplingPolicy()
	if p.Mode != SamplingModeAlways {
		t.Fatalf("default policy mode = %q", p.Mode)
	}
	norm, err := p.Normalize()
	if err != nil || norm.Mode != SamplingModeAlways {
		t.Fatalf("default policy must normalize cleanly: %+v err=%v", norm, err)
	}
}
