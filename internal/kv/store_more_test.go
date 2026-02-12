package kv

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
)

func TestStoreNotFoundAndDefaultBranches(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	if _, err := store.GetFunction(ctx, "t_abc123", "ns1", "missing"); err == nil {
		t.Fatal("expected GetFunction not found error")
	}
	if _, err := store.GetDraft(ctx, "t_abc123", "ns1", "fn1", "missing"); err == nil {
		t.Fatal("expected GetDraft not found error")
	}
	if _, _, err := store.GetVersion(ctx, "t_abc123", "ns1", "fn1", 1); err == nil {
		t.Fatal("expected GetVersion not found error")
	}
	if _, err := store.GetLatestVersion(ctx, "t_abc123", "ns1", "fn1"); err == nil {
		t.Fatal("expected GetLatestVersion not found error")
	}
	if _, err := store.GetAlias(ctx, "t_abc123", "ns1", "fn1", "prod"); err == nil {
		t.Fatal("expected GetAlias not found error")
	}
	if _, err := store.GetActivation(ctx, "t_abc123", "act_missing"); err == nil {
		t.Fatal("expected GetActivation not found error")
	}
	if _, err := store.GetResultByRequestID(ctx, "t_abc123", "req_missing"); err == nil {
		t.Fatal("expected GetResultByRequestID timeout/not found error")
	}
	if _, err := store.GetSchedule(ctx, "t_abc123", "ns1", "missing"); err == nil {
		t.Fatal("expected GetSchedule not found error")
	}
	state, err := store.GetScheduleState(ctx, "t_abc123", "ns1", "missing")
	if err != nil {
		t.Fatalf("GetScheduleState should return empty state without error, got %v", err)
	}
	if state.NextTickMS != 0 || state.TickSeq != 0 {
		t.Fatalf("unexpected default state: %+v", state)
	}
	inflight, err := store.GetScheduleInflight(ctx, "t_abc123", "ns1", "missing")
	if err != nil || inflight != "" {
		t.Fatalf("GetScheduleInflight default = %q, %v", inflight, err)
	}
	if _, err := store.GetWorkerBinding(ctx, "t_abc123", "ns1", "missing"); err == nil {
		t.Fatal("expected GetWorkerBinding not found error")
	}
	mapping, err := store.GetCadenceTaskMapping(ctx, "t_abc123", "ns1", "missing")
	if err != nil || mapping != "" {
		t.Fatalf("GetCadenceTaskMapping default = %q, %v", mapping, err)
	}
	terminal, rec, err := store.IsActivationTerminal(ctx, "t_abc123", "act_missing")
	if err != nil {
		t.Fatalf("IsActivationTerminal missing returned error: %v", err)
	}
	if terminal || rec.ActivationID != "" {
		t.Fatalf("IsActivationTerminal missing expected false+empty, got terminal=%v rec=%+v", terminal, rec)
	}
}

func TestStoreCASFailureRawClientAndCursorHelpers(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	if store.RawClient() == nil {
		t.Fatal("RawClient should not be nil")
	}

	rec := api.ActivationRecord{
		ActivationID: "act1",
		Tenant:       "t_abc123",
		Namespace:    "ns1",
		Function:     "fn1",
		Status:       "running",
		StartMS:      1,
	}
	if err := store.PutActivationRunning(ctx, rec, time.Hour); err != nil {
		t.Fatalf("PutActivationRunning failed: %v", err)
	}
	rec.Status = "success"
	ok, err := store.CompleteActivationCAS(ctx, rec, time.Hour)
	if err != nil || !ok {
		t.Fatalf("first CompleteActivationCAS = %v, %v", ok, err)
	}
	// Status key is no longer "running", so CAS should fail without error.
	ok, err = store.CompleteActivationCAS(ctx, rec, time.Hour)
	if err != nil {
		t.Fatalf("second CompleteActivationCAS returned error: %v", err)
	}
	if ok {
		t.Fatal("second CompleteActivationCAS should fail due stale state")
	}

	if got := ParseCursor(""); got != 0 {
		t.Fatalf("ParseCursor empty=%d", got)
	}
	if got := ParseCursor("-10"); got != 0 {
		t.Fatalf("ParseCursor negative=%d", got)
	}
	if got := ParseCursor("abc"); got != 0 {
		t.Fatalf("ParseCursor invalid=%d", got)
	}
	if got := ParseCursor("12"); got != 12 {
		t.Fatalf("ParseCursor valid=%d", got)
	}
	if got := EncodeCursor(-1); got != "0" {
		t.Fatalf("EncodeCursor negative=%q", got)
	}
	if got := EncodeCursor(25); got != "25" {
		t.Fatalf("EncodeCursor positive=%q", got)
	}
}

func TestPingFailure(t *testing.T) {
	store := NewStore("127.0.0.1:1", "")
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := store.Ping(ctx); err == nil {
		t.Fatal("expected Ping to fail with unreachable address")
	} else if !strings.Contains(err.Error(), "kvrocks ping failed") {
		t.Fatalf("unexpected ping error: %v", err)
	}
}
