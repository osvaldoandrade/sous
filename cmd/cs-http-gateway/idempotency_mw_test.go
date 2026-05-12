package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"

	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/idempotency"
)

// newMiddlewareRouter wires the idempotency middleware behind a chi route
// pattern that exposes the same {tenant}/{namespace}/{function}/{ref} URL
// params the production gateway uses. The supplied handler is what the
// middleware is supposed to defer to on first execution.
func newMiddlewareRouter(t *testing.T, store idempotency.Store, handler http.HandlerFunc) http.Handler {
	t.Helper()
	r := chi.NewRouter()
	r.Group(func(pr chi.Router) {
		pr.Use(idempotencyMiddleware(store, 0))
		pr.HandleFunc("/v1/web/{tenant}/{namespace}/{function}/{ref}", handler)
	})
	return r
}

func TestIdempotencyMiddlewarePassThroughWithoutHeader(t *testing.T) {
	store := idempotency.NewMemoryStore(nil)
	var calls int32
	handler := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("hello"))
	}
	ts := httptest.NewServer(newMiddlewareRouter(t, store, handler))
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/web/t_abc/ns/fn/prod", "text/plain", strings.NewReader("body"))
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get(idempotencyReplayHeader); got != "" {
		t.Fatalf("replay header should not be set: %q", got)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("handler should run exactly once: %d", calls)
	}
}

func TestIdempotencyMiddlewareReplaysCachedResponse(t *testing.T) {
	store := idempotency.NewMemoryStore(nil)
	var calls int32
	handler := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Test", "ok")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("echo:" + string(body)))
	}
	ts := httptest.NewServer(newMiddlewareRouter(t, store, handler))
	defer ts.Close()

	makeReq := func(key, body string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/web/t_abc/ns/fn/prod", strings.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set(idempotencyHeader, key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do request: %v", err)
		}
		return resp
	}

	first := makeReq("abcdef12", `{"v":1}`)
	defer first.Body.Close()
	firstBody, _ := io.ReadAll(first.Body)
	if first.StatusCode != http.StatusCreated || first.Header.Get(idempotencyReplayHeader) != "" {
		t.Fatalf("first response unexpected: status=%d replay=%q", first.StatusCode, first.Header.Get(idempotencyReplayHeader))
	}
	if string(firstBody) != `echo:{"v":1}` {
		t.Fatalf("first body=%q", string(firstBody))
	}

	second := makeReq("abcdef12", `{"v":1}`)
	defer second.Body.Close()
	secondBody, _ := io.ReadAll(second.Body)
	if second.StatusCode != http.StatusCreated {
		t.Fatalf("replay status=%d body=%s", second.StatusCode, string(secondBody))
	}
	if second.Header.Get(idempotencyReplayHeader) != "1" {
		t.Fatalf("missing replay header: %#v", second.Header)
	}
	if string(secondBody) != `echo:{"v":1}` {
		t.Fatalf("replay body should match original: %q", string(secondBody))
	}
	if second.Header.Get("X-Test") != "ok" {
		t.Fatalf("replay should preserve original headers: %#v", second.Header)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("handler must execute exactly once across two idempotent calls; got %d", calls)
	}
}

func TestIdempotencyMiddlewareConflictsOnDifferentBody(t *testing.T) {
	store := idempotency.NewMemoryStore(nil)
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
	ts := httptest.NewServer(newMiddlewareRouter(t, store, handler))
	defer ts.Close()

	req1, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/web/t_abc/ns/fn/prod", strings.NewReader("body-A"))
	req1.Header.Set(idempotencyHeader, "abcdef12")
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first status=%d", resp1.StatusCode)
	}

	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/web/t_abc/ns/fn/prod", strings.NewReader("body-B"))
	req2.Header.Set(idempotencyHeader, "abcdef12")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("conflict call: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp2.StatusCode)
	}
	body, _ := io.ReadAll(resp2.Body)
	if !strings.Contains(string(body), string(cserrors.CSIdempotencyConflict)) {
		t.Fatalf("expected CS_IDEMPOTENCY_CONFLICT in body, got %s", string(body))
	}
}

func TestIdempotencyMiddlewareInvalidKeyFormat(t *testing.T) {
	store := idempotency.NewMemoryStore(nil)
	handler := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }
	ts := httptest.NewServer(newMiddlewareRouter(t, store, handler))
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/web/t_abc/ns/fn/prod", strings.NewReader("{}"))
	req.Header.Set(idempotencyHeader, "short")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed key, got %d", resp.StatusCode)
	}
}

func TestIdempotencyMiddlewareDoesNotCacheServerErrors(t *testing.T) {
	store := idempotency.NewMemoryStore(nil)
	var calls int32
	handler := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}
	ts := httptest.NewServer(newMiddlewareRouter(t, store, handler))
	defer ts.Close()

	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/web/t_abc/ns/fn/prod", strings.NewReader("body"))
		req.Header.Set(idempotencyHeader, "abcdef12")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("call %d status=%d", i, resp.StatusCode)
		}
		if resp.Header.Get(idempotencyReplayHeader) == "1" {
			t.Fatalf("5xx responses must not be replayed (call %d)", i)
		}
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected both calls to execute the handler; got %d", calls)
	}
}

func TestIdempotencyMiddlewareNilStorePassesThrough(t *testing.T) {
	called := false
	handler := func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}
	mw := idempotencyMiddleware(nil, 0)
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("body"))
	req.Header.Set(idempotencyHeader, "abcdef12")
	w := httptest.NewRecorder()
	mw(http.HandlerFunc(handler)).ServeHTTP(w, req)
	if !called || w.Code != http.StatusOK {
		t.Fatalf("nil store should pass through; called=%v code=%d", called, w.Code)
	}
}

func TestBuildStoreKeyIncludesTenantAndRef(t *testing.T) {
	if buildStoreKey("t1", "ns", "fn", "prod", "k") == buildStoreKey("t2", "ns", "fn", "prod", "k") {
		t.Fatal("store key must isolate by tenant")
	}
	if buildStoreKey("t1", "ns", "fn", "prod", "k") == buildStoreKey("t1", "ns", "fn", "stage", "k") {
		t.Fatal("store key must isolate by ref")
	}
	if buildStoreKey("t1", "ns", "fn", "prod", "k") == buildStoreKey("t1", "ns", "fn2", "prod", "k") {
		t.Fatal("store key must isolate by function")
	}
}

func TestCapturingWriterCapturesStatusAndBody(t *testing.T) {
	rec := httptest.NewRecorder()
	cw := &capturingWriter{ResponseWriter: rec, status: http.StatusOK}
	if _, err := cw.Write([]byte("body")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if cw.status != http.StatusOK || cw.body.String() != "body" {
		t.Fatalf("default status/body: status=%d body=%q", cw.status, cw.body.String())
	}
	rec2 := httptest.NewRecorder()
	cw2 := &capturingWriter{ResponseWriter: rec2, status: http.StatusOK}
	cw2.WriteHeader(http.StatusTeapot)
	cw2.WriteHeader(http.StatusOK) // second call should be a no-op
	if cw2.status != http.StatusTeapot {
		t.Fatalf("WriteHeader should be sticky; got %d", cw2.status)
	}
}
