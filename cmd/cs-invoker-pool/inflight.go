package main

import (
	"context"
	"sync"

	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/limits"
)

// tenantInflight enforces a per-tenant cap on concurrent activations inside
// cs-invoker-pool. It complements the global `inflight` channel (which caps
// total worker concurrency for the whole pool) and prevents a single tenant
// from exhausting the worker fleet.
//
// Capacity is taken from internal/limits.TenantMaxInflight (default 64). The
// limiter offers two acquire modes:
//
//   - acquireSync: returns CS_TENANT_INFLIGHT_LIMIT immediately when the
//     semaphore is full. This is the mode used for synchronous HTTP-style
//     invocations where the caller is waiting on a response.
//   - acquireAsync: blocks until a slot is available or the context is
//     cancelled. This is the mode used for queued (codeQ-delivered) invokes
//     where the producer has already enqueued the work and we expect the
//     consumer to drain in FIFO order.
type tenantInflight struct {
	capacity int

	mu      sync.Mutex
	rejects uint64
	tenants map[string]chan struct{}
}

// newTenantInflight returns an inflight limiter with the configured per-tenant
// capacity. A zero or negative capacity falls back to
// limits.DefaultTenantMaxInflight.
func newTenantInflight(capacity int) *tenantInflight {
	if capacity <= 0 {
		capacity = limits.DefaultTenantMaxInflight
	}
	return &tenantInflight{
		capacity: capacity,
		tenants:  make(map[string]chan struct{}),
	}
}

func (t *tenantInflight) semaphoreFor(tenant string) chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	sem, ok := t.tenants[tenant]
	if !ok {
		sem = make(chan struct{}, t.capacity)
		t.tenants[tenant] = sem
	}
	return sem
}

// acquireSync attempts to take a slot without blocking. When the semaphore is
// full it returns a CS_TENANT_INFLIGHT_LIMIT error so the caller can map it
// to HTTP 429.
func (t *tenantInflight) acquireSync(tenant string) (release func(), err error) {
	sem := t.semaphoreFor(tenant)
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	default:
		t.mu.Lock()
		t.rejects++
		t.mu.Unlock()
		return nil, cserrors.New(cserrors.CSTenantInflightLim,
			"tenant inflight activation limit exceeded")
	}
}

// acquireAsync blocks until a slot is available or the context is cancelled.
// Async invokes are expected to be queued upstream (codeQ), so the natural
// behaviour on saturation is to wait rather than return an error.
func (t *tenantInflight) acquireAsync(ctx context.Context, tenant string) (release func(), err error) {
	sem := t.semaphoreFor(tenant)
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// rejectCount returns the number of synchronous acquires that were rejected.
// Used by tests and exposed for future metrics integration
// (cs_invoker_inflight_rejected_total per docs/14-observability.md).
func (t *tenantInflight) rejectCount() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rejects
}
