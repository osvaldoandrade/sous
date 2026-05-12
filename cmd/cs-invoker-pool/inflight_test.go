package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/limits"
)

func TestTenantInflightSyncAllowsThenRejects(t *testing.T) {
	ti := newTenantInflight(1)

	release1, err := ti.acquireSync("t1")
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// Second sync acquire while the first holds the slot must reject.
	_, err = ti.acquireSync("t1")
	if err == nil {
		t.Fatal("expected second sync acquire to be rejected")
	}
	var csErr *cserrors.CSError
	if !errors.As(err, &csErr) || csErr.Code != cserrors.CSTenantInflightLim {
		t.Fatalf("expected CS_TENANT_INFLIGHT_LIMIT, got %v", err)
	}
	if got := ti.rejectCount(); got != 1 {
		t.Fatalf("rejectCount=%d want=1", got)
	}

	// A different tenant has its own bucket.
	release2, err := ti.acquireSync("t2")
	if err != nil {
		t.Fatalf("second tenant acquire failed: %v", err)
	}
	release2()

	// Releasing the first should re-open the slot.
	release1()
	release3, err := ti.acquireSync("t1")
	if err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}
	release3()
}

func TestTenantInflightAsyncWaitsForSlot(t *testing.T) {
	ti := newTenantInflight(1)

	release1, err := ti.acquireSync("t1")
	if err != nil {
		t.Fatalf("seed acquire failed: %v", err)
	}

	var (
		wg       sync.WaitGroup
		acquired = make(chan struct{})
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		r, err := ti.acquireAsync(context.Background(), "t1")
		if err != nil {
			t.Errorf("async acquire returned error: %v", err)
			return
		}
		close(acquired)
		r()
	}()

	// The async goroutine must NOT have acquired yet.
	select {
	case <-acquired:
		t.Fatal("async acquire returned before slot was released")
	case <-time.After(25 * time.Millisecond):
	}

	// Release the first slot; the async waiter should proceed.
	release1()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("async acquire did not unblock after release")
	}
	wg.Wait()
}

func TestTenantInflightAsyncRespectsContext(t *testing.T) {
	ti := newTenantInflight(1)
	release, err := ti.acquireSync("t1")
	if err != nil {
		t.Fatalf("seed acquire failed: %v", err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err = ti.acquireAsync(ctx, "t1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestTenantInflightDefaultCapacity(t *testing.T) {
	ti := newTenantInflight(0)
	if ti.capacity != limits.DefaultTenantMaxInflight {
		t.Fatalf("capacity=%d want default %d", ti.capacity, limits.DefaultTenantMaxInflight)
	}
}

func TestIsSyncTrigger(t *testing.T) {
	cases := map[string]bool{
		"http":     true,
		"api":      false,
		"schedule": false,
		"cadence":  false,
		"":         false,
	}
	for trig, want := range cases {
		if got := isSyncTrigger(trig); got != want {
			t.Fatalf("isSyncTrigger(%q)=%v want=%v", trig, got, want)
		}
	}
}
