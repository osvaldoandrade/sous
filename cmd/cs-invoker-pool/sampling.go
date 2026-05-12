package main

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/observability"
)

// samplerCache memoises a Decider per normalized SamplingPolicy so that
// stateful samplers (HeadSampler, which holds rolling-minute counters)
// keep their state across activations that share a policy. The cache key
// is the trigger's policy fields rendered into a stable string.
type samplerCache struct {
	mu sync.Mutex
	by map[string]observability.Decider
}

func newSamplerCache() *samplerCache {
	return &samplerCache{by: map[string]observability.Decider{}}
}

// deciderFor returns the Decider for the supplied trigger. An unknown or
// invalid policy falls back to the always-on decider so the invoker
// never silently drops traffic when the operator's config is broken.
func (c *samplerCache) deciderFor(trigger api.Trigger, clock func() time.Time) observability.Decider {
	policy, err := trigger.Sampling.Normalize()
	if err != nil {
		policy = api.DefaultSamplingPolicy()
	}
	key := samplerCacheKey(policy)
	c.mu.Lock()
	defer c.mu.Unlock()
	if d, ok := c.by[key]; ok {
		return d
	}
	d := observability.NewDecider(policy, clock)
	c.by[key] = d
	return d
}

// samplerCacheKey renders a SamplingPolicy into a stable cache key.
// Probability is bucketed to 6 decimal places so float jitter never
// fragments stateful samplers.
func samplerCacheKey(p api.SamplingPolicy) string {
	tail := "0"
	if p.TailOnError {
		tail = "1"
	}
	return strings.Join([]string{
		p.Mode,
		strconv.Itoa(p.HeadPerMinute),
		tail,
		strconv.Itoa(p.TailOnSlowMs),
		strconv.Itoa(int(p.Probability * 1_000_000)),
	}, "|")
}

// samplingMetaFromRequest builds the observability.SamplingMeta the
// Decider expects from the in-flight invocation request.
func samplingMetaFromRequest(req api.InvocationRequest) observability.SamplingMeta {
	return observability.SamplingMeta{
		Tenant:       req.Tenant,
		Namespace:    req.Namespace,
		Function:     req.Ref.Function,
		Version:      req.Ref.Version,
		ActivationID: req.ActivationID,
	}
}
