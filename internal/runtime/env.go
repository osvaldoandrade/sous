package runtime

import "context"

// envContextKey is the unexported key used to propagate per-activation
// secret material into the runtime through the standard context chain.
// cs-invoker-pool stamps the resolved env map onto the context just
// before calling Runner.Execute, and the runJS host-binding setup pulls
// it out to populate cs.env.get(name). Keeping this in a side channel —
// rather than on api.InvocationRequest — means no codeQ envelope ever
// carries secret bytes over the wire, satisfying the E6.01 contract that
// secrets never serialise into bundles, results, or DLQ payloads.
type envContextKey struct{}

// WithEnv returns a derived context that carries env for the duration of
// a single activation. Passing a nil or empty map leaves ctx untouched so
// the runtime fast-paths to "no secrets configured" without an extra
// allocation. Callers should never reuse the returned context across
// activations: the embedded map is read-only by convention but mutating
// the caller's copy after the call would still race the runtime read.
func WithEnv(ctx context.Context, env map[string]string) context.Context {
	if ctx == nil || len(env) == 0 {
		return ctx
	}
	// Shallow-copy so a misbehaving caller cannot mutate the env after
	// the runtime started reading from it.
	frozen := make(map[string]string, len(env))
	for k, v := range env {
		frozen[k] = v
	}
	return context.WithValue(ctx, envContextKey{}, frozen)
}

// EnvFromContext returns the per-activation env map stamped by WithEnv,
// or nil when no secrets were injected. The returned map is read-only —
// the runtime treats it as such and so should any other consumer.
func EnvFromContext(ctx context.Context) map[string]string {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(envContextKey{}).(map[string]string)
	return v
}
