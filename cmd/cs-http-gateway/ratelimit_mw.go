package main

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/limits"
	"github.com/osvaldoandrade/sous/internal/observability"
)

// rateLimitConfig captures the per-tenant token-bucket parameters. RPS is the
// steady-state replenishment rate and Burst is the maximum bucket capacity.
// Zero or negative values fall back to the documented defaults in
// internal/limits.
type rateLimitConfig struct {
	TenantRPS   float64
	TenantBurst int
}

// rateLimitConfigFromLimits projects a limits.Limits value into the
// middleware configuration. Burst defaults to TenantRPS when no explicit
// burst is documented (matches the contract in docs/26-capacity-and-limits.md).
func rateLimitConfigFromLimits(l limits.Limits) rateLimitConfig {
	rps := float64(l.TenantRPS)
	if rps <= 0 {
		rps = float64(limits.DefaultTenantRPS)
	}
	burst := l.TenantRPS
	if burst <= 0 {
		burst = limits.DefaultTenantRPS
	}
	return rateLimitConfig{TenantRPS: rps, TenantBurst: burst}
}

// tokenBucket is a minimal time-based token-bucket implementation. It is
// intentionally allocation-free on the steady-state path: a single mutex
// guards two scalar fields. We avoid pulling in golang.org/x/time/rate to
// keep the dependency surface unchanged.
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64 // current balance, in tokens; capped at burst
	lastRefill time.Time
	rps        float64 // refill rate (tokens per second)
	burst      float64 // bucket capacity
}

func newTokenBucket(rps float64, burst int, now time.Time) *tokenBucket {
	b := float64(burst)
	if b < 1 {
		b = 1
	}
	return &tokenBucket{
		tokens:     b,
		lastRefill: now,
		rps:        rps,
		burst:      b,
	}
}

// allow attempts to consume one token. It returns (true, 0) when permitted
// and (false, retryAfter) when the bucket is empty, where retryAfter is the
// time until the next token will be available.
func (b *tokenBucket) allow(now time.Time) (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	b.tokens += elapsed * b.rps
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.lastRefill = now
	if b.tokens >= 1 {
		b.tokens -= 1
		return true, 0
	}
	// Tokens missing: 1 - tokens, at rate b.rps tokens/sec.
	missing := 1 - b.tokens
	if b.rps <= 0 {
		// Pathological config; surface a one-second retry rather than
		// dividing by zero.
		return false, time.Second
	}
	retry := time.Duration(math.Ceil(missing/b.rps*1000)) * time.Millisecond
	if retry < time.Millisecond {
		retry = time.Millisecond
	}
	return false, retry
}

// bucketEntry tracks a bucket together with its last-used timestamp for LRU
// eviction. Buckets that have not been touched for evictTTL are removed by
// the next janitor pass.
type bucketEntry struct {
	bucket   *tokenBucket
	lastSeen time.Time
}

// rateLimiter is the per-tenant rate limiter. It owns a small map of
// tenant -> bucket, protected by a single mutex. A janitor evicts entries
// older than evictTTL to keep memory bounded in long-running gateways.
type rateLimiter struct {
	cfg      rateLimitConfig
	now      func() time.Time
	evictTTL time.Duration

	mu      sync.Mutex
	tenants map[string]*bucketEntry

	// rejects counts 429 responses produced by this limiter. It is a
	// best-effort, in-process counter used by tests and surfaced via the
	// /metrics handler in future work (see docs/14-observability.md).
	rejects map[string]uint64
}

// newRateLimiter constructs a rate limiter from limits.Limits. The clock and
// eviction TTL are exposed so tests can drive deterministic scenarios.
func newRateLimiter(cfg rateLimitConfig) *rateLimiter {
	return &rateLimiter{
		cfg:      cfg,
		now:      time.Now,
		evictTTL: 10 * time.Minute,
		tenants:  make(map[string]*bucketEntry),
		rejects:  make(map[string]uint64),
	}
}

// allow returns whether the request for tenant t should be admitted. The
// retryAfter return is only meaningful when allowed is false.
func (l *rateLimiter) allow(tenant string) (allowed bool, retryAfter time.Duration) {
	now := l.now()
	l.mu.Lock()
	entry, ok := l.tenants[tenant]
	if !ok {
		entry = &bucketEntry{bucket: newTokenBucket(l.cfg.TenantRPS, l.cfg.TenantBurst, now)}
		l.tenants[tenant] = entry
	}
	entry.lastSeen = now
	bucket := entry.bucket
	// Opportunistic janitor: every call may evict a single stale entry to
	// keep map size bounded without holding the mutex across allow().
	l.evictOneLocked(now)
	l.mu.Unlock()

	ok, retry := bucket.allow(now)
	if !ok {
		l.mu.Lock()
		l.rejects[tenant]++
		l.mu.Unlock()
	}
	return ok, retry
}

func (l *rateLimiter) evictOneLocked(now time.Time) {
	for key, entry := range l.tenants {
		if now.Sub(entry.lastSeen) > l.evictTTL {
			delete(l.tenants, key)
			return
		}
	}
}

// rejectCount returns the in-process 429 count for tenant t. Used by tests.
func (l *rateLimiter) rejectCount(tenant string) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rejects[tenant]
}

// rateLimitMiddleware enforces a per-tenant token bucket on all requests
// whose path matches the gateway invoke prefix. Requests that do not embed a
// tenant (health/metrics/etc.) are passed through unchanged.
func rateLimitMiddleware(limiter *rateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenant := tenantFromInvokePath(r.URL.Path)
			if tenant == "" {
				next.ServeHTTP(w, r)
				return
			}
			ok, retry := limiter.allow(tenant)
			if !ok {
				seconds := int(math.Ceil(retry.Seconds()))
				if seconds < 1 {
					seconds = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(seconds))
				cserrors.WriteHTTP(w,
					cserrors.New(cserrors.CSRateLimited, "tenant rate limit exceeded"),
					observability.RequestIDFromContext(r.Context()))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// tenantFromInvokePath extracts {tenant} from a /v1/web/{tenant}/... URL.
// Returns "" when the path is not an invoke route so health endpoints and
// other middleware-irrelevant paths bypass quota accounting.
func tenantFromInvokePath(path string) string {
	const prefix = "/v1/web/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return strings.TrimSpace(rest)
	}
	return strings.TrimSpace(rest[:slash])
}
