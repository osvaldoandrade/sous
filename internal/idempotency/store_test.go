package idempotency

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReserveDedupesSameFingerprint(t *testing.T) {
	s := NewMemoryStore(nil)
	ctx := context.Background()

	body := []byte(`{"x":1}`)
	fp := Fingerprint(body)

	first, err := s.Reserve(ctx, "k1", "act_1", fp, time.Hour)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if !first.Created {
		t.Fatal("first reserve should have Created=true")
	}

	second, err := s.Reserve(ctx, "k1", "act_2", fp, time.Hour)
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if second.Created {
		t.Fatal("second reserve should observe existing record")
	}
	if second.Record.ActivationID != "act_1" {
		t.Fatalf("activation_id should be sticky; got %s", second.Record.ActivationID)
	}
}

func TestReserveConflictsOnDifferentFingerprint(t *testing.T) {
	s := NewMemoryStore(nil)
	ctx := context.Background()
	if _, err := s.Reserve(ctx, "k1", "act_1", Fingerprint([]byte("A")), time.Hour); err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	_, err := s.Reserve(ctx, "k1", "act_2", Fingerprint([]byte("B")), time.Hour)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestCommitAndReplay(t *testing.T) {
	s := NewMemoryStore(nil)
	ctx := context.Background()
	fp := Fingerprint([]byte("body"))
	if _, err := s.Reserve(ctx, "k1", "act_1", fp, time.Hour); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := s.Commit(ctx, "k1", fp, []byte("OK"), ""); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got, ok := s.Get(ctx, "k1")
	if !ok || !got.Terminal {
		t.Fatalf("expected terminal record, got %+v ok=%v", got, ok)
	}
	if string(got.Result) != "OK" {
		t.Fatalf("result=%q", got.Result)
	}
}

func TestTTLExpiry(t *testing.T) {
	cur := time.Unix(1000, 0)
	s := NewMemoryStore(func() time.Time { return cur })
	ctx := context.Background()
	fp := Fingerprint([]byte("body"))
	if _, err := s.Reserve(ctx, "k1", "act_1", fp, 10*time.Second); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	cur = cur.Add(20 * time.Second)
	if _, ok := s.Get(ctx, "k1"); ok {
		t.Fatal("expected expired record to be absent")
	}
	// A fresh reserve after TTL must succeed with Created=true.
	res, err := s.Reserve(ctx, "k1", "act_2", Fingerprint([]byte("other")), 10*time.Second)
	if err != nil {
		t.Fatalf("reserve after expiry: %v", err)
	}
	if !res.Created || res.Record.ActivationID != "act_2" {
		t.Fatalf("expected fresh reservation, got %+v", res)
	}
}
