package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/limits"
)

// fakeClock is a manually advanced clock used to keep the token-bucket tests
// deterministic without sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{now: start} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestRateLimitConfigFromLimits(t *testing.T) {
	cfg := rateLimitConfigFromLimits(limits.Defaults())
	if cfg.TenantRPS != float64(limits.DefaultTenantRPS) {
		t.Fatalf("TenantRPS=%v want=%v", cfg.TenantRPS, limits.DefaultTenantRPS)
	}
	if cfg.TenantBurst != limits.DefaultTenantRPS {
		t.Fatalf("TenantBurst=%v want=%v", cfg.TenantBurst, limits.DefaultTenantRPS)
	}

	// Zero TenantRPS falls back to defaults.
	cfg = rateLimitConfigFromLimits(limits.Limits{TenantRPS: 0})
	if cfg.TenantRPS != float64(limits.DefaultTenantRPS) {
		t.Fatalf("zero fallback RPS=%v", cfg.TenantRPS)
	}
}

func TestTenantFromInvokePath(t *testing.T) {
	cases := []struct{ path, want string }{
		{"/v1/web/t1/ns/fn/prod", "t1"},
		{"/v1/web/t1/ns/fn/prod/sub", "t1"},
		{"/v1/web/t1", "t1"},
		{"/healthz", ""},
		{"/metrics", ""},
		{"/", ""},
	}
	for _, c := range cases {
		got := tenantFromInvokePath(c.path)
		if got != c.want {
			t.Fatalf("tenantFromInvokePath(%q)=%q want=%q", c.path, got, c.want)
		}
	}
}

func TestTokenBucketAllow(t *testing.T) {
	clk := newFakeClock(time.Unix(0, 0))
	bucket := newTokenBucket(2, 2, clk.Now())

	// Burst of 2 should pass.
	for i := 0; i < 2; i++ {
		ok, _ := bucket.allow(clk.Now())
		if !ok {
			t.Fatalf("expected burst request %d to pass", i)
		}
	}
	// Third request in the same instant should be denied with a positive
	// Retry-After.
	ok, retry := bucket.allow(clk.Now())
	if ok {
		t.Fatal("expected third request to be denied")
	}
	if retry <= 0 {
		t.Fatalf("expected positive Retry-After, got %s", retry)
	}

	// After 1 second the bucket should refill enough for 2 more requests.
	clk.advance(time.Second)
	for i := 0; i < 2; i++ {
		ok, _ := bucket.allow(clk.Now())
		if !ok {
			t.Fatalf("expected refilled request %d to pass", i)
		}
	}
}

func TestTokenBucketZeroRPS(t *testing.T) {
	clk := newFakeClock(time.Unix(0, 0))
	bucket := newTokenBucket(0, 1, clk.Now())
	// First token is in the bucket initially.
	if ok, _ := bucket.allow(clk.Now()); !ok {
		t.Fatal("expected initial token to be available")
	}
	ok, retry := bucket.allow(clk.Now())
	if ok {
		t.Fatal("expected zero-RPS bucket to deny once empty")
	}
	if retry != time.Second {
		t.Fatalf("expected 1s fallback retry, got %s", retry)
	}
}

func TestRateLimiterAllowAndReject(t *testing.T) {
	clk := newFakeClock(time.Unix(0, 0))
	limiter := newRateLimiter(rateLimitConfig{TenantRPS: 1, TenantBurst: 1})
	limiter.now = clk.Now

	if ok, _ := limiter.allow("t1"); !ok {
		t.Fatal("first request must pass")
	}
	if ok, _ := limiter.allow("t1"); ok {
		t.Fatal("second request in same instant must be denied")
	}
	if got := limiter.rejectCount("t1"); got != 1 {
		t.Fatalf("rejectCount=%d want=1", got)
	}

	// A separate tenant has its own bucket.
	if ok, _ := limiter.allow("t2"); !ok {
		t.Fatal("first request for new tenant must pass")
	}
}

func TestRateLimiterEvictsStaleTenants(t *testing.T) {
	clk := newFakeClock(time.Unix(0, 0))
	limiter := newRateLimiter(rateLimitConfig{TenantRPS: 1, TenantBurst: 1})
	limiter.now = clk.Now
	limiter.evictTTL = 100 * time.Millisecond

	for _, tenant := range []string{"t1", "t2", "t3"} {
		limiter.allow(tenant)
	}
	if len(limiter.tenants) != 3 {
		t.Fatalf("expected 3 tenants tracked, got %d", len(limiter.tenants))
	}

	// Advance well past TTL and touch a fresh tenant; the janitor should
	// evict one stale entry.
	clk.advance(time.Second)
	limiter.allow("t4")
	if len(limiter.tenants) > 3 {
		t.Fatalf("expected evictor to keep map bounded, got %d", len(limiter.tenants))
	}
}

func TestRateLimitMiddleware429Envelope(t *testing.T) {
	clk := newFakeClock(time.Unix(0, 0))
	limiter := newRateLimiter(rateLimitConfig{TenantRPS: 1, TenantBurst: 1})
	limiter.now = clk.Now

	calls := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	})
	handler := rateLimitMiddleware(limiter)(next)

	// First request: 204 from downstream.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/web/t1/ns/fn/prod", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent || calls != 1 {
		t.Fatalf("first call status=%d calls=%d", w.Code, calls)
	}

	// Second request: 429 + Retry-After.
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/web/t1/ns/fn/prod", nil)
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d body=%s", w.Code, w.Body.String())
	}
	retryHeader := w.Header().Get("Retry-After")
	if retryHeader == "" {
		t.Fatal("missing Retry-After header")
	}
	if n, err := strconv.Atoi(retryHeader); err != nil || n < 1 {
		t.Fatalf("Retry-After=%q must parse to >=1", retryHeader)
	}
	if calls != 1 {
		t.Fatalf("downstream was called on rejection: calls=%d", calls)
	}
	// Decoded body must carry CS_RATE_LIMITED.
	if got := w.Body.String(); !strings.Contains(got, string(cserrors.CSRateLimited)) {
		t.Fatalf("expected envelope to contain %s, got %s", cserrors.CSRateLimited, got)
	}
}

func TestRateLimitMiddlewareBypassesNonInvokePaths(t *testing.T) {
	limiter := newRateLimiter(rateLimitConfig{TenantRPS: 0, TenantBurst: 0})
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	handler := rateLimitMiddleware(limiter)(next)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handler.ServeHTTP(w, req)
	if !called || w.Code != http.StatusOK {
		t.Fatalf("non-invoke paths must bypass limiter, got code=%d called=%v", w.Code, called)
	}
}
