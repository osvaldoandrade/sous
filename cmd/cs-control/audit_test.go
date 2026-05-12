package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/audit"
	"github.com/osvaldoandrade/sous/internal/bundle"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// inMemoryKVStore is a thin map-backed IndexStore used by tests that
// need to assert on the per-tenant audit ring buffer.
type inMemoryKVStore struct {
	mu sync.Mutex
	m  map[string]string
}

func newInMemoryKVStore() *inMemoryKVStore { return &inMemoryKVStore{m: map[string]string{}} }

func (s *inMemoryKVStore) KVGet(_ context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

func (s *inMemoryKVStore) KVSet(_ context.Context, key, value string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
	return nil
}

// captureSink records every emitted audit event for assertions.
type captureSink struct {
	mu       sync.Mutex
	events   []audit.Event
	emitFail bool
}

func (c *captureSink) Emit(_ context.Context, ev audit.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
	if c.emitFail {
		return io.EOF
	}
	return nil
}

func (c *captureSink) Name() string { return "capture" }
func (c *captureSink) Close() error { return nil }

func (c *captureSink) snapshot() []audit.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]audit.Event, len(c.events))
	copy(out, c.events)
	return out
}

// newAuditTestServer wires a server with a real recorder + capture
// sink + in-memory KV index so tests can assert on both the live sink
// stream and the replay endpoint.
func newAuditTestServer(store *testutil.FakePersistence, broker *testutil.FakeMessaging) (*server, *captureSink, *inMemoryKVStore) {
	s := newControlServer(store, broker)
	sink := &captureSink{}
	idx := newInMemoryKVStore()
	s.recorder = audit.NewRecorder(sink, idx, observability.NewLoggerWithWriter("test", io.Discard), time.Hour, 100)
	return s, sink, idx
}

func TestPublishVersionEmitsAuditOnSuccess(t *testing.T) {
	store := &testutil.FakePersistence{}
	s, sink, _ := newAuditTestServer(store, &testutil.FakeMessaging{})

	files := map[string]string{
		"function.js":   base64.StdEncoding.EncodeToString([]byte(`export default function(){ return {statusCode:200, body:"ok"} }`)),
		"manifest.json": base64.StdEncoding.EncodeToString([]byte(`{"schema":"cs.function.script.v1","runtime":"cs-js","entry":"function.js","handler":"default","limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1},"capabilities":{"kv":{"prefixes":["ctr:"],"ops":["get","set","del"]},"codeq":{"publishTopics":["jobs.*"]},"http":{"allowHosts":["example.com"],"timeoutMs":1000}}}`)),
	}
	decoded := map[string][]byte{}
	for n, b64 := range files {
		decoded[n], _ = base64.StdEncoding.DecodeString(b64)
	}
	_, sha, _, _ := bundle.BuildCanonical(decoded)

	draft := api.DraftRecord{
		DraftID:     "drf_42",
		SHA256:      sha,
		Files:       files,
		CreatedAtMS: time.Now().Add(-time.Minute).UnixMilli(),
		ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	store.GetDraftFn = func(context.Context, string, string, string, string) (api.DraftRecord, error) {
		return draft, nil
	}
	store.PublishVersionFn = func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
		return 9, nil
	}
	store.MarkDraftConsumedFn = func(context.Context, string, string, string, string, time.Duration) error {
		return nil
	}

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_42","alias":"prod","config":{"timeout_ms":1000,"memory_mb":64,"max_concurrency":1}}`, map[string]string{
		"tenant": "t_audit", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_audit", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", w.Code, w.Body.String())
	}

	events := sink.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 audit event on publish success, got %d", len(events))
	}
	ev := events[0]
	if ev.Tenant != "t_audit" || ev.Action != "function.publish" {
		t.Fatalf("audit event tenant/action mismatch: %+v", ev)
	}
	if ev.Outcome != audit.OutcomeSuccess {
		t.Fatalf("audit outcome=%q want success", ev.Outcome)
	}
	if ev.Actor != "u1" {
		t.Fatalf("audit actor=%q want u1", ev.Actor)
	}
	if ev.Resource != "fn://t_audit/ns1/fn1@v9" {
		t.Fatalf("audit resource=%q", ev.Resource)
	}
}

func TestPublishVersionNoAuditOnFailure(t *testing.T) {
	store := &testutil.FakePersistence{}
	s, sink, _ := newAuditTestServer(store, &testutil.FakeMessaging{})

	files := map[string]string{
		"function.js":   base64.StdEncoding.EncodeToString([]byte(`x`)),
		"manifest.json": base64.StdEncoding.EncodeToString([]byte(`{"schema":"cs.function.script.v1","runtime":"cs-js","entry":"function.js","handler":"default","limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1},"capabilities":{"kv":{"prefixes":[],"ops":[]},"codeq":{"publishTopics":[]},"http":{"allowHosts":[],"timeoutMs":1000}}}`)),
	}
	decoded := map[string][]byte{}
	for n, b64 := range files {
		decoded[n], _ = base64.StdEncoding.DecodeString(b64)
	}
	_, sha, _, _ := bundle.BuildCanonical(decoded)

	draft := api.DraftRecord{
		DraftID:     "drf_1",
		SHA256:      sha,
		Files:       files,
		CreatedAtMS: time.Now().Add(-time.Minute).UnixMilli(),
		ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	store.GetDraftFn = func(context.Context, string, string, string, string) (api.DraftRecord, error) {
		return draft, nil
	}
	store.PublishVersionFn = func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
		return 0, cserrors.New(cserrors.CSKVWriteFailed, "kv down")
	}

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1","alias":"prod","config":{"timeout_ms":1000,"memory_mb":64,"max_concurrency":1}}`, map[string]string{
		"tenant": "t_failure", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_failure", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code == http.StatusCreated {
		t.Fatalf("expected publish failure, got 201 body=%s", w.Body.String())
	}

	if len(sink.snapshot()) != 0 {
		t.Fatalf("expected no audit event on publish failure, got %d", len(sink.snapshot()))
	}
}

func TestDeleteFunctionEmitsAudit(t *testing.T) {
	store := &testutil.FakePersistence{
		SoftDeleteFunctionFn: func(context.Context, string, string, string, int64) error { return nil },
	}
	s, sink, _ := newAuditTestServer(store, &testutil.FakeMessaging{})

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodDelete, "/", "", map[string]string{
		"tenant": "t_del", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_del", "cs:function:delete"))
	s.deleteFunction(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", w.Code, w.Body.String())
	}
	events := sink.snapshot()
	if len(events) != 1 || events[0].Action != "function.delete" {
		t.Fatalf("expected one function.delete audit event, got %+v", events)
	}
}

func TestSetAliasEmitsAudit(t *testing.T) {
	store := &testutil.FakePersistence{
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{}, nil, nil
		},
		SetAliasFn: func(context.Context, string, string, string, string, int64) error { return nil },
	}
	s, sink, _ := newAuditTestServer(store, &testutil.FakeMessaging{})

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPut, "/alias", `{"version":4}`, map[string]string{
		"tenant": "t_alias", "namespace": "ns1", "name": "fn1", "alias": "prod",
	}, principalWith("t_alias", "cs:function:alias:set"))
	s.setAlias(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("setAlias status=%d body=%s", w.Code, w.Body.String())
	}
	events := sink.snapshot()
	if len(events) != 1 || events[0].Action != "function.alias.set" {
		t.Fatalf("expected one function.alias.set audit event, got %+v", events)
	}
	// Detail enters the recorder as a Go map (int64 stays int64).
	// Re-marshal to JSON so the assertion is type-agnostic.
	raw, _ := json.Marshal(events[0].Detail)
	if !bytes.Contains(raw, []byte(`"version":4`)) {
		t.Fatalf("audit detail missing version=4: %s", string(raw))
	}
}

func TestGetAuditHistoryTenantScoping(t *testing.T) {
	s, sink, _ := newAuditTestServer(&testutil.FakePersistence{}, &testutil.FakeMessaging{})

	// Seed events via the live recorder so the in-memory KV index is
	// populated through the same code path the runtime uses.
	for i, tenant := range []string{"t_a", "t_a", "t_b"} {
		ev := audit.NewEvent(tenant, "u1", "function.publish", "fn://"+tenant+"/ns/f", audit.OutcomeSuccess)
		ev.Detail = map[string]any{"i": i}
		if err := s.recorder.AfterCommit(context.Background(), ev); err != nil {
			t.Fatalf("AfterCommit: %v", err)
		}
	}
	_ = sink // sink also captured them; we focus on the replay endpoint here.

	// Tenant t_a sees only its own events.
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/v1/tenants/t_a/audit", "", map[string]string{
		"tenant": "t_a",
	}, principalWith("t_a", "cs:audit:read"))
	s.getAuditHistory(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("audit status=%d body=%s", w.Code, w.Body.String())
	}
	body := parseJSONBody(t, w.Body.Bytes())
	evs, ok := body["events"].([]any)
	if !ok {
		t.Fatalf("events not an array: %+v", body)
	}
	if len(evs) != 2 {
		t.Fatalf("tenant t_a expected 2 events, got %d", len(evs))
	}
	for _, ev := range evs {
		m := ev.(map[string]any)
		if m["tenant"] != "t_a" {
			t.Fatalf("cross-tenant leak in t_a response: %+v", m)
		}
	}

	// A principal whose Tikti tenant differs from the URL tenant is
	// rejected at authorize().
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/v1/tenants/t_a/audit", "", map[string]string{
		"tenant": "t_a",
	}, principalWith("t_b", "cs:audit:read"))
	s.getAuditHistory(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant request status=%d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSAuthzResourceMis {
		t.Fatalf("cross-tenant error code=%s", code)
	}

	// Missing role denies.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/v1/tenants/t_a/audit", "", map[string]string{
		"tenant": "t_a",
	}, principalWith("t_a", "role:other"))
	s.getAuditHistory(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("missing role status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestGetAuditHistoryFilters(t *testing.T) {
	s, _, _ := newAuditTestServer(&testutil.FakePersistence{}, &testutil.FakeMessaging{})

	base := time.Now().UTC()
	seed := []audit.Event{
		{Tenant: "t1", Actor: "u1", Action: "function.publish", Resource: "fn://t1/ns/f@v1", Timestamp: base.Add(-3 * time.Minute), Outcome: audit.OutcomeSuccess},
		{Tenant: "t1", Actor: "u2", Action: "function.alias.set", Resource: "fn://t1/ns/f/alias/prod", Timestamp: base.Add(-2 * time.Minute), Outcome: audit.OutcomeSuccess},
		{Tenant: "t1", Actor: "u1", Action: "function.delete", Resource: "fn://t1/ns/f", Timestamp: base.Add(-1 * time.Minute), Outcome: audit.OutcomeSuccess},
	}
	for _, ev := range seed {
		if err := s.recorder.AfterCommit(context.Background(), ev); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/v1/tenants/t1/audit?actor=u2", "", map[string]string{
		"tenant": "t1",
	}, principalWith("t1", "cs:audit:read"))
	s.getAuditHistory(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("filter status=%d body=%s", w.Code, w.Body.String())
	}
	body := parseJSONBody(t, w.Body.Bytes())
	evs, _ := body["events"].([]any)
	if len(evs) != 1 || evs[0].(map[string]any)["actor"] != "u2" {
		t.Fatalf("actor filter result: %+v", evs)
	}

	since := base.Add(-90 * time.Second).UnixMilli()
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/v1/tenants/t1/audit?since="+strconv.FormatInt(since, 10), "", map[string]string{
		"tenant": "t1",
	}, principalWith("t1", "cs:audit:read"))
	s.getAuditHistory(w, req)
	body = parseJSONBody(t, w.Body.Bytes())
	evs, _ = body["events"].([]any)
	if len(evs) != 1 {
		t.Fatalf("since filter expected 1 event, got %d", len(evs))
	}

	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/v1/tenants/t1/audit?action=function.delete", "", map[string]string{
		"tenant": "t1",
	}, principalWith("t1", "cs:audit:read"))
	s.getAuditHistory(w, req)
	body = parseJSONBody(t, w.Body.Bytes())
	evs, _ = body["events"].([]any)
	if len(evs) != 1 || evs[0].(map[string]any)["action"] != "function.delete" {
		t.Fatalf("action filter result: %+v", evs)
	}
}
