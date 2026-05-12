package observability

import (
	"hash/fnv"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
)

// SamplingMeta is the minimum information a sampler needs to make a
// decision before the activation starts. The invoker fills this struct
// at the top of handleInvocation; the sampler does not depend on the
// runtime, on persistence, or on the broker. Time is supplied through
// the Decider's injected clock so a single process-wide fake clock can
// drive deterministic windowed samplers.
type SamplingMeta struct {
	Tenant       string
	Namespace    string
	Function     string
	Version      int64
	ActivationID string
}

// SamplingOutcome is the information the sampler needs to finalise a
// tail decision after the function has completed. The invoker calls
// Decider.OnComplete with this struct once the runtime returns.
type SamplingOutcome struct {
	Status     string
	DurationMS int64
}

// SamplingDecision is the result returned by Decider.OnStart. The
// invoker uses it to drive three branches: do we write the full
// activation record now, or only a skeleton; do we write logs at the
// end; and which reason should we stamp on the record.
type SamplingDecision struct {
	// PersistFull is true when the activation should be written in full
	// (metadata + result blob + logs) at the usual lifecycle points.
	PersistFull bool
	// Skeleton is true when only a skeleton row should be written at the
	// start. The Decider may later decide via OnComplete to promote to a
	// full record (tail sampling).
	Skeleton bool
	// PersistLogs is the default policy for log persistence. Tail samplers
	// override it via OnComplete once they see the final status/duration.
	PersistLogs bool
	// Reason captures the SamplingDecision* constant stamped on the
	// activation record so operators can later understand the choice.
	Reason string
}

// Decider is the contract used by cs-invoker-pool. Implementations must
// be safe for concurrent use; the same Decider is shared across all
// worker goroutines.
type Decider interface {
	// OnStart returns the initial decision and the reason to stamp on the
	// activation record. It is consulted BEFORE the activation record is
	// written so head samplers can drop without spending any IO.
	OnStart(meta SamplingMeta) SamplingDecision
	// OnComplete is consulted AFTER the runtime returns so tail samplers
	// can decide whether to upgrade a skeleton to a full record. The
	// returned decision may set PersistFull=true to overwrite the
	// skeleton with the full record (and write logs), or leave it false
	// to keep the skeleton-only state.
	OnComplete(meta SamplingMeta, outcome SamplingOutcome, initial SamplingDecision) SamplingDecision
}

// NewDecider constructs the concrete sampler that matches the supplied
// policy. The clock argument allows tests to inject a deterministic time
// source; production callers pass time.Now.
func NewDecider(policy api.SamplingPolicy, clock func() time.Time) Decider {
	if clock == nil {
		clock = time.Now
	}
	switch policy.Mode {
	case api.SamplingModeHead:
		return &HeadSampler{capPerMinute: policy.HeadPerMinute, clock: clock, windows: map[string]headWindow{}}
	case api.SamplingModeTail:
		return &TailSampler{onError: policy.TailOnError, slowMs: policy.TailOnSlowMs}
	case api.SamplingModeProbabilistic:
		return &ProbabilisticSampler{probability: policy.Probability}
	default:
		return AlwaysSampler{}
	}
}

// AlwaysSampler keeps every activation in full. It is the default when
// no policy is configured.
type AlwaysSampler struct{}

// OnStart returns a "record everything" decision.
func (AlwaysSampler) OnStart(SamplingMeta) SamplingDecision {
	return SamplingDecision{PersistFull: true, PersistLogs: true, Reason: api.SamplingDecisionAlways}
}

// OnComplete is a pass-through for AlwaysSampler.
func (AlwaysSampler) OnComplete(_ SamplingMeta, _ SamplingOutcome, initial SamplingDecision) SamplingDecision {
	return initial
}

// HeadSampler keeps the first N activations per (tenant, function-ref)
// per rolling minute window. The window is computed from the supplied
// clock; tests can inject a fake clock to make decisions deterministic.
type HeadSampler struct {
	capPerMinute int
	clock        func() time.Time

	mu      sync.Mutex
	windows map[string]headWindow
}

type headWindow struct {
	minute int64
	count  int
}

// OnStart consults the per-minute window for the activation's
// (tenant, function-ref) key and returns PersistFull until the cap is
// reached. Once the cap is hit the activation is dropped entirely (no
// skeleton row).
func (h *HeadSampler) OnStart(meta SamplingMeta) SamplingDecision {
	if h.capPerMinute <= 0 {
		return SamplingDecision{Reason: api.SamplingDecisionSkipped}
	}
	key := headKey(meta)
	minute := h.clock().Unix() / 60
	h.mu.Lock()
	w := h.windows[key]
	if w.minute != minute {
		w = headWindow{minute: minute}
	}
	w.count++
	h.windows[key] = w
	count := w.count
	h.mu.Unlock()
	if count > h.capPerMinute {
		return SamplingDecision{Reason: api.SamplingDecisionSkipped}
	}
	return SamplingDecision{PersistFull: true, PersistLogs: true, Reason: api.SamplingDecisionHead}
}

// OnComplete is a pass-through for HeadSampler — the decision is fixed
// at start time.
func (h *HeadSampler) OnComplete(_ SamplingMeta, _ SamplingOutcome, initial SamplingDecision) SamplingDecision {
	return initial
}

func headKey(meta SamplingMeta) string {
	var b strings.Builder
	b.Grow(len(meta.Tenant) + len(meta.Namespace) + len(meta.Function) + 8)
	b.WriteString(meta.Tenant)
	b.WriteByte('|')
	b.WriteString(meta.Namespace)
	b.WriteByte('|')
	b.WriteString(meta.Function)
	return b.String()
}

// TailSampler writes a skeleton row at start and promotes to a full
// record only when the completion outcome matches (error or slow call).
// Activations that complete cleanly within the threshold leave the
// skeleton in place — logs and the full result blob are skipped.
type TailSampler struct {
	onError bool
	slowMs  int
}

// OnStart returns a skeleton-only decision so the invoker writes a
// minimal row at start. OnComplete promotes when warranted.
func (t *TailSampler) OnStart(SamplingMeta) SamplingDecision {
	return SamplingDecision{Skeleton: true, Reason: api.SamplingDecisionTail}
}

// OnComplete promotes the skeleton to a full record when the outcome
// matches one of the tail rules. Otherwise it returns the initial
// skeleton-only decision unchanged.
func (t *TailSampler) OnComplete(_ SamplingMeta, outcome SamplingOutcome, initial SamplingDecision) SamplingDecision {
	if t.onError && outcome.Status != "" && outcome.Status != "success" {
		return SamplingDecision{PersistFull: true, PersistLogs: true, Reason: api.SamplingDecisionTail}
	}
	if t.slowMs > 0 && outcome.DurationMS >= int64(t.slowMs) {
		return SamplingDecision{PersistFull: true, PersistLogs: true, Reason: api.SamplingDecisionTail}
	}
	return initial
}

// ProbabilisticSampler keeps a deterministic fraction of activations
// based on a stable hash of the activation id. The decision is
// reproducible across processes — useful for end-to-end tracing because
// every replica picks the same activations to retain.
type ProbabilisticSampler struct {
	probability float64
}

// OnStart hashes the activation id and compares against the configured
// probability scaled to the full uint64 range.
func (p *ProbabilisticSampler) OnStart(meta SamplingMeta) SamplingDecision {
	if p.probability <= 0 {
		return SamplingDecision{Reason: api.SamplingDecisionSkipped}
	}
	if p.probability >= 1 {
		return SamplingDecision{PersistFull: true, PersistLogs: true, Reason: api.SamplingDecisionProbabilistic}
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(meta.ActivationID))
	hashed := h.Sum64()
	threshold := uint64(p.probability * math.MaxUint64)
	if hashed < threshold {
		return SamplingDecision{PersistFull: true, PersistLogs: true, Reason: api.SamplingDecisionProbabilistic}
	}
	return SamplingDecision{Reason: api.SamplingDecisionSkipped}
}

// OnComplete is a pass-through for ProbabilisticSampler.
func (p *ProbabilisticSampler) OnComplete(_ SamplingMeta, _ SamplingOutcome, initial SamplingDecision) SamplingDecision {
	return initial
}
