package audit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeIndex implements IndexStore with an in-memory map.
type fakeIndex struct {
	mu sync.Mutex
	m  map[string]string
}

func newFakeIndex() *fakeIndex { return &fakeIndex{m: map[string]string{}} }

func (f *fakeIndex) KVGet(_ context.Context, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.m[key], nil
}

func (f *fakeIndex) KVSet(_ context.Context, key, value string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

type fakeLogger struct {
	warns []string
}

func (l *fakeLogger) Warn(_ context.Context, msg string) { l.warns = append(l.warns, msg) }

// fakeSink keeps a copy of emitted events for assertions.
type fakeSink struct {
	mu      sync.Mutex
	emitted []Event
	err     error
}

func (s *fakeSink) Emit(_ context.Context, ev Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emitted = append(s.emitted, ev)
	return s.err
}

func (s *fakeSink) Name() string { return "fake" }
func (s *fakeSink) Close() error { return nil }

func TestRecorderAfterCommitFills(t *testing.T) {
	sink := &fakeSink{}
	idx := newFakeIndex()
	rec := NewRecorder(sink, idx, nil, time.Minute, 100)
	ev := Event{Tenant: "t1", Actor: "u1", Action: "function.publish", Resource: "fn://t1/ns/f@v1"}
	if err := rec.AfterCommit(context.Background(), ev); err != nil {
		t.Fatalf("AfterCommit: %v", err)
	}
	if len(sink.emitted) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(sink.emitted))
	}
	got := sink.emitted[0]
	if got.EventID == "" {
		t.Fatalf("EventID not populated")
	}
	if got.SchemaVersion != SchemaVersion {
		t.Fatalf("schema_version=%q", got.SchemaVersion)
	}
	if got.Timestamp.IsZero() {
		t.Fatalf("timestamp not populated")
	}
	if got.Outcome != OutcomeSuccess {
		t.Fatalf("outcome=%q", got.Outcome)
	}

	history, err := rec.ReadHistory(context.Background(), "t1", 0, "", "", 100)
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("history length=%d", len(history))
	}
	if history[0].Action != "function.publish" {
		t.Fatalf("history action=%q", history[0].Action)
	}
}

func TestRecorderEmitFailureMarksError(t *testing.T) {
	sink := &fakeSink{}
	rec := NewRecorder(sink, nil, nil, 0, 0)
	ev := Event{Tenant: "t1", Action: "function.delete", Resource: "fn://t1/ns/f"}
	if err := rec.EmitFailure(context.Background(), ev); err != nil {
		t.Fatalf("EmitFailure: %v", err)
	}
	if sink.emitted[0].Outcome != OutcomeError {
		t.Fatalf("expected error outcome, got %q", sink.emitted[0].Outcome)
	}
}

func TestRecorderHistoryFiltering(t *testing.T) {
	rec := NewRecorder(nil, newFakeIndex(), nil, time.Minute, 100)
	ctx := context.Background()
	mustEmit := func(actor, action string, atMS int64) {
		ev := Event{Tenant: "t1", Actor: actor, Action: action, Resource: "fn://t1/ns/f", Timestamp: time.UnixMilli(atMS), Outcome: OutcomeSuccess}
		if err := rec.AfterCommit(ctx, ev); err != nil {
			t.Fatalf("AfterCommit: %v", err)
		}
	}
	mustEmit("u1", "function.publish", 1000)
	mustEmit("u1", "function.delete", 2000)
	mustEmit("u2", "function.publish", 3000)

	all, err := rec.ReadHistory(ctx, "t1", 0, "", "", 100)
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all len=%d", len(all))
	}

	since, err := rec.ReadHistory(ctx, "t1", 1500, "", "", 100)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	if len(since) != 2 {
		t.Fatalf("since len=%d", len(since))
	}

	byActor, err := rec.ReadHistory(ctx, "t1", 0, "u2", "", 100)
	if err != nil {
		t.Fatalf("byActor: %v", err)
	}
	if len(byActor) != 1 || byActor[0].Actor != "u2" {
		t.Fatalf("byActor: %+v", byActor)
	}

	byAction, err := rec.ReadHistory(ctx, "t1", 0, "", "function.publish", 100)
	if err != nil {
		t.Fatalf("byAction: %v", err)
	}
	if len(byAction) != 2 {
		t.Fatalf("byAction len=%d", len(byAction))
	}

	capped, err := rec.ReadHistory(ctx, "t1", 0, "", "", 1)
	if err != nil {
		t.Fatalf("capped: %v", err)
	}
	if len(capped) != 1 {
		t.Fatalf("capped len=%d", len(capped))
	}
	// Cap returns the most recent entries.
	if capped[0].Actor != "u2" {
		t.Fatalf("capped actor=%q", capped[0].Actor)
	}
}

func TestRecorderHistoryTenantScoping(t *testing.T) {
	rec := NewRecorder(nil, newFakeIndex(), nil, time.Minute, 100)
	ctx := context.Background()
	emit := func(tenant string) {
		ev := Event{Tenant: tenant, Actor: "u", Action: "function.publish", Resource: "fn://" + tenant + "/ns/f"}
		if err := rec.AfterCommit(ctx, ev); err != nil {
			t.Fatalf("AfterCommit: %v", err)
		}
	}
	emit("a")
	emit("a")
	emit("b")

	a, _ := rec.ReadHistory(ctx, "a", 0, "", "", 100)
	if len(a) != 2 {
		t.Fatalf("tenant a expected 2, got %d", len(a))
	}
	b, _ := rec.ReadHistory(ctx, "b", 0, "", "", 100)
	if len(b) != 1 {
		t.Fatalf("tenant b expected 1, got %d", len(b))
	}
	for _, ev := range a {
		if ev.Tenant != "a" {
			t.Fatalf("cross-tenant leak: %+v", ev)
		}
	}
}

func TestRecorderRingBufferEviction(t *testing.T) {
	rec := NewRecorder(nil, newFakeIndex(), nil, time.Minute, 3)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		ev := Event{Tenant: "t1", Actor: "u", Action: "function.publish", Resource: "fn://t1/ns/f"}
		if err := rec.AfterCommit(ctx, ev); err != nil {
			t.Fatalf("AfterCommit %d: %v", i, err)
		}
	}
	history, err := rec.ReadHistory(ctx, "t1", 0, "", "", 100)
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("ring buffer expected 3, got %d", len(history))
	}
}

func TestRecorderSinkFailureWarns(t *testing.T) {
	sink := &fakeSink{err: errors.New("boom")}
	logger := &fakeLogger{}
	rec := NewRecorder(sink, nil, logger, 0, 0)
	ev := Event{Tenant: "t1", Action: "function.publish"}
	// AfterCommit returns the sink error so callers can metric on it,
	// but the kv mutation is already durable so the caller ignores it.
	if err := rec.AfterCommit(context.Background(), ev); err == nil {
		t.Fatalf("expected error from failing sink")
	}
	if len(logger.warns) == 0 {
		t.Fatalf("expected SinkLag warning")
	}
}

func TestRecorderHandlesNilDependencies(t *testing.T) {
	rec := NewRecorder(nil, nil, nil, 0, 0)
	if err := rec.AfterCommit(context.Background(), Event{Tenant: "t1", Action: "x"}); err != nil {
		t.Fatalf("AfterCommit with nil sink/index: %v", err)
	}
	out, err := rec.ReadHistory(context.Background(), "t1", 0, "", "", 100)
	if err != nil {
		t.Fatalf("ReadHistory nil index: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil history without index, got %+v", out)
	}
}
