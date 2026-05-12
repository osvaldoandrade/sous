package observability

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
)

func TestAlwaysSamplerKeepsEverything(t *testing.T) {
	s := AlwaysSampler{}
	dec := s.OnStart(SamplingMeta{ActivationID: "x"})
	if !dec.PersistFull || !dec.PersistLogs || dec.Reason != api.SamplingDecisionAlways {
		t.Fatalf("unexpected decision: %+v", dec)
	}
	final := s.OnComplete(SamplingMeta{}, SamplingOutcome{Status: "error"}, dec)
	if final != dec {
		t.Fatalf("OnComplete must be a pass-through: %+v vs %+v", final, dec)
	}
}

func TestHeadSamplerKeepsFirstNPerMinute(t *testing.T) {
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	policy := api.SamplingPolicy{Mode: api.SamplingModeHead, HeadPerMinute: 3}
	s := NewDecider(policy, clock)

	meta := SamplingMeta{Tenant: "t_alpha", Namespace: "ns", Function: "fn"}
	var kept, dropped int
	for i := 0; i < 10; i++ {
		m := meta
		m.ActivationID = fmt.Sprintf("a%d", i)
		d := s.OnStart(m)
		if d.PersistFull {
			kept++
		} else {
			dropped++
		}
	}
	if kept != 3 || dropped != 7 {
		t.Fatalf("head sampler kept=%d dropped=%d, want 3/7", kept, dropped)
	}

	// Advance clock by one full minute; the window resets and the cap
	// should refill so the next 3 activations are kept.
	now = now.Add(time.Minute)
	kept = 0
	for i := 0; i < 5; i++ {
		m := meta
		m.ActivationID = fmt.Sprintf("b%d", i)
		if s.OnStart(m).PersistFull {
			kept++
		}
	}
	if kept != 3 {
		t.Fatalf("head sampler post-rollover kept=%d, want 3", kept)
	}
}

func TestHeadSamplerSeparatesByFunctionRef(t *testing.T) {
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	policy := api.SamplingPolicy{Mode: api.SamplingModeHead, HeadPerMinute: 2}
	s := NewDecider(policy, func() time.Time { return now })

	for i := 0; i < 5; i++ {
		s.OnStart(SamplingMeta{Tenant: "t_a", Namespace: "ns", Function: "f1", ActivationID: fmt.Sprintf("a%d", i)})
	}
	// fn2 should have its own independent window.
	var f2Kept int
	for i := 0; i < 5; i++ {
		if s.OnStart(SamplingMeta{Tenant: "t_a", Namespace: "ns", Function: "f2", ActivationID: fmt.Sprintf("b%d", i)}).PersistFull {
			f2Kept++
		}
	}
	if f2Kept != 2 {
		t.Fatalf("f2 head kept=%d, want 2", f2Kept)
	}
}

func TestHeadSamplerInvalidCapDropsAll(t *testing.T) {
	s := &HeadSampler{capPerMinute: 0, clock: time.Now, windows: map[string]headWindow{}}
	d := s.OnStart(SamplingMeta{Tenant: "t", Function: "fn"})
	if d.PersistFull {
		t.Fatalf("zero cap must drop")
	}
}

func TestTailSamplerWritesSkeletonAndPromotesOnError(t *testing.T) {
	s := NewDecider(api.SamplingPolicy{Mode: api.SamplingModeTail, TailOnError: true}, nil)
	init := s.OnStart(SamplingMeta{ActivationID: "x"})
	if init.PersistFull || !init.Skeleton {
		t.Fatalf("tail start must be skeleton-only: %+v", init)
	}
	keep := s.OnComplete(SamplingMeta{}, SamplingOutcome{Status: "error"}, init)
	if !keep.PersistFull || !keep.PersistLogs {
		t.Fatalf("tail must promote on error: %+v", keep)
	}
	drop := s.OnComplete(SamplingMeta{}, SamplingOutcome{Status: "success"}, init)
	if drop.PersistFull {
		t.Fatalf("tail must not promote on success: %+v", drop)
	}
}

func TestTailSamplerPromotesOnSlow(t *testing.T) {
	s := NewDecider(api.SamplingPolicy{Mode: api.SamplingModeTail, TailOnSlowMs: 200}, nil)
	init := s.OnStart(SamplingMeta{})

	if got := s.OnComplete(SamplingMeta{}, SamplingOutcome{Status: "success", DurationMS: 199}, init); got.PersistFull {
		t.Fatalf("199ms must not promote: %+v", got)
	}
	if got := s.OnComplete(SamplingMeta{}, SamplingOutcome{Status: "success", DurationMS: 200}, init); !got.PersistFull {
		t.Fatalf("200ms must promote: %+v", got)
	}
}

func TestProbabilisticSamplerExtremes(t *testing.T) {
	zero := NewDecider(api.SamplingPolicy{Mode: api.SamplingModeProbabilistic, Probability: 0}, nil)
	for i := 0; i < 10; i++ {
		d := zero.OnStart(SamplingMeta{ActivationID: fmt.Sprintf("a%d", i)})
		if d.PersistFull {
			t.Fatalf("p=0 must drop everything; got %+v", d)
		}
	}
	one := NewDecider(api.SamplingPolicy{Mode: api.SamplingModeProbabilistic, Probability: 1}, nil)
	for i := 0; i < 10; i++ {
		d := one.OnStart(SamplingMeta{ActivationID: fmt.Sprintf("a%d", i)})
		if !d.PersistFull {
			t.Fatalf("p=1 must keep everything; got %+v", d)
		}
	}
}

func TestProbabilisticSamplerIsDeterministic(t *testing.T) {
	s := NewDecider(api.SamplingPolicy{Mode: api.SamplingModeProbabilistic, Probability: 0.5}, nil)
	first := s.OnStart(SamplingMeta{ActivationID: "stable-id"})
	for i := 0; i < 5; i++ {
		again := s.OnStart(SamplingMeta{ActivationID: "stable-id"})
		if again.PersistFull != first.PersistFull {
			t.Fatalf("decision must be deterministic per activation id; got %+v vs %+v", again, first)
		}
	}
}

func TestProbabilisticSamplerApproximateRate(t *testing.T) {
	// Use a moderate sample size so the test runs fast; tolerance is wide
	// enough to keep the assertion non-flaky.
	const total = 4000
	policy := api.SamplingPolicy{Mode: api.SamplingModeProbabilistic, Probability: 0.25}
	s := NewDecider(policy, nil)
	var kept int
	for i := 0; i < total; i++ {
		if s.OnStart(SamplingMeta{ActivationID: fmt.Sprintf("act-%06d", i)}).PersistFull {
			kept++
		}
	}
	ratio := float64(kept) / float64(total)
	if math.Abs(ratio-0.25) > 0.05 {
		t.Fatalf("probabilistic ratio %.3f deviated too far from 0.25", ratio)
	}
}

func TestNewDeciderFallsBackToAlways(t *testing.T) {
	// Empty mode + unknown mode both fall back to AlwaysSampler so that a
	// misconfiguration never silently drops production traffic.
	for _, mode := range []string{"", "ignored"} {
		d := NewDecider(api.SamplingPolicy{Mode: mode}, nil)
		out := d.OnStart(SamplingMeta{ActivationID: "x"})
		if !out.PersistFull {
			t.Fatalf("mode=%q should fall back to always; got %+v", mode, out)
		}
	}
}
