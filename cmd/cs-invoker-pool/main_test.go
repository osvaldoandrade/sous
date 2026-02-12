package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/runtime"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

func buildInvokerBundle(t *testing.T) ([]byte, string) {
	t.Helper()
	return buildInvokerBundleWithScript(t, `export default function(){ return { statusCode: 200, body: "ok" } }`)
}

func defaultInvokerManifest() api.FunctionManifest {
	return api.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: "cs-js",
		Entry:   "function.js",
		Handler: "default",
		Limits: api.ManifestLimits{
			TimeoutMS:      300,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Capabilities: api.ManifestCapabilities{
			KV:    api.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get", "set", "del"}},
			CodeQ: api.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
			HTTP:  api.ManifestHTTPCaps{AllowHosts: []string{"example.com"}, TimeoutMS: 100},
		},
	}
}

func buildInvokerBundleWithManifest(t *testing.T, manifest api.FunctionManifest, script string) ([]byte, string) {
	t.Helper()
	rawManifest, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	b, sha, _, err := bundle.BuildCanonical(map[string][]byte{
		"function.js":   []byte(script),
		"manifest.json": rawManifest,
	})
	if err != nil {
		t.Fatalf("build bundle: %v", err)
	}
	return b, sha
}

func buildInvokerBundleWithScript(t *testing.T, script string) ([]byte, string) {
	t.Helper()
	return buildInvokerBundleWithManifest(t, defaultInvokerManifest(), script)
}

func newTestInvoker(store *testutil.FakePersistence, broker *testutil.FakeMessaging) *invoker {
	cfg := config.Config{}
	cfg.CSControl.Limits.ActTTLSeconds = 60
	i := &invoker{
		cfg:         cfg,
		store:       store,
		broker:      broker,
		inflight:    make(chan struct{}, 8),
		versionSems: map[string]chan struct{}{},
	}
	i.runner = runtime.NewRunner(runtimeKV{store: store}, runtimePublisher{broker: broker}, 256*1024, 64*1024, 1024*1024)
	return i
}

func TestHelpersAndAdapters(t *testing.T) {
	if got := max(7, 3); got != 7 {
		t.Fatalf("max(7,3)=%d", got)
	}
	if got := max(2, 5); got != 5 {
		t.Fatalf("max(2,5)=%d", got)
	}
	if got := atoi("123", 9); got != 123 {
		t.Fatalf("atoi parse=%d", got)
	}
	if got := atoi("x", 9); got != 9 {
		t.Fatalf("atoi fallback=%d", got)
	}

	store := &testutil.FakePersistence{
		KVSetFn: func(ctx context.Context, key, value string, ttl time.Duration) error {
			_ = ctx
			if key != "k1" || value != "v1" || ttl != 0 {
				t.Fatalf("unexpected KVSet args: key=%s value=%s ttl=%s", key, value, ttl)
			}
			return nil
		},
		KVGetFn: func(ctx context.Context, key string) (string, error) {
			_ = ctx
			if key != "k1" {
				t.Fatalf("unexpected KVGet key: %s", key)
			}
			return "v1", nil
		},
		KVDelFn: func(ctx context.Context, key string) error {
			_ = ctx
			if key != "k1" {
				t.Fatalf("unexpected KVDel key: %s", key)
			}
			return nil
		},
	}
	kv := runtimeKV{store: store}
	if err := kv.Set(context.Background(), "k1", "v1", 0); err != nil {
		t.Fatalf("runtimeKV.Set failed: %v", err)
	}
	got, err := kv.Get(context.Background(), "k1")
	if err != nil || got != "v1" {
		t.Fatalf("runtimeKV.Get = %q, %v", got, err)
	}
	if err := kv.Del(context.Background(), "k1"); err != nil {
		t.Fatalf("runtimeKV.Del failed: %v", err)
	}

	var publishedTenant string
	pub := runtimePublisher{broker: &testutil.FakeMessaging{
		PublishFn: func(ctx context.Context, topic, tenant, typ string, body any) error {
			_ = ctx
			_ = topic
			_ = typ
			_ = body
			publishedTenant = tenant
			return nil
		},
	}}
	if err := pub.Publish(context.Background(), "jobs", map[string]any{"tenant": "t1"}); err != nil {
		t.Fatalf("runtimePublisher.Publish failed: %v", err)
	}
	if publishedTenant != "t1" {
		t.Fatalf("runtimePublisher tenant=%q", publishedTenant)
	}
	if err := pub.Publish(context.Background(), "jobs", map[string]any{}); err != nil {
		t.Fatalf("runtimePublisher.Publish default tenant failed: %v", err)
	}

	i := &invoker{versionSems: map[string]chan struct{}{}}
	req := api.InvocationRequest{Tenant: "t", Namespace: "n", Ref: api.FunctionRef{Function: "f", Version: 1}}
	s1 := i.versionSemaphore(req, 2)
	s2 := i.versionSemaphore(req, 5)
	if s1 != s2 {
		t.Fatal("versionSemaphore should reuse semaphore for same key")
	}
}

func TestHandleInvocationInvalidAndTerminal(t *testing.T) {
	var dlqCalled bool
	broker := &testutil.FakeMessaging{
		PublishDLQInvokeFn: func(ctx context.Context, tenant string, req api.InvocationRequest) error {
			_ = ctx
			_ = tenant
			_ = req
			dlqCalled = true
			return nil
		},
	}
	i := newTestInvoker(&testutil.FakePersistence{}, broker)

	if err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{}); err != nil {
		t.Fatalf("invalid invocation should not error: %v", err)
	}
	if !dlqCalled {
		t.Fatal("expected DLQ publish for invalid invocation")
	}

	var publishedResult bool
	store := &testutil.FakePersistence{
		IsActivationTerminalFn: func(ctx context.Context, tenant, activationID string) (bool, api.ActivationRecord, error) {
			_ = ctx
			_ = tenant
			_ = activationID
			return true, api.ActivationRecord{
				ActivationID: "act1",
				Status:       "success",
				DurationMS:   10,
				Result:       &api.FunctionResponse{StatusCode: 200, Body: "ok"},
			}, nil
		},
	}
	broker = &testutil.FakeMessaging{
		PublishResultFn: func(ctx context.Context, tenant string, result api.InvocationResult) error {
			_ = ctx
			_ = tenant
			if result.ActivationID != "act1" {
				t.Fatalf("unexpected cached result: %+v", result)
			}
			publishedResult = true
			return nil
		},
	}
	i = newTestInvoker(store, broker)
	err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act1",
		RequestID:    "req1",
		Tenant:       "t1",
		Namespace:    "n1",
		Ref:          api.FunctionRef{Function: "f1", Version: 1},
	})
	if err != nil {
		t.Fatalf("terminal invocation failed: %v", err)
	}
	if !publishedResult {
		t.Fatal("expected PublishResult for terminal activation")
	}
}

func TestHandleInvocationBundleMismatchAndPublishFailure(t *testing.T) {
	bundleBytes, _ := buildInvokerBundle(t)

	store := &testutil.FakePersistence{
		IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
			return false, api.ActivationRecord{}, nil
		},
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 1, nil
		},
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				SHA256: "wrong",
				Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
			}, bundleBytes, nil
		},
	}
	i := newTestInvoker(store, &testutil.FakeMessaging{})
	err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act1",
		RequestID:    "req1",
		Tenant:       "t1",
		Namespace:    "n1",
		Ref:          api.FunctionRef{Function: "f1", Version: 1},
	})
	if err == nil || !strings.Contains(err.Error(), "bundle sha mismatch") {
		t.Fatalf("expected bundle mismatch error, got %v", err)
	}

	bundleBytes, sha := buildInvokerBundle(t)
	var dlqResult bool
	store = &testutil.FakePersistence{
		IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
			return false, api.ActivationRecord{}, nil
		},
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 1, nil
		},
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				SHA256: sha,
				Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
			}, bundleBytes, nil
		},
		PutActivationRunningFn: func(context.Context, api.ActivationRecord, time.Duration) error { return nil },
		CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) {
			return true, nil
		},
		SaveResultByRequestIDFn: func(context.Context, string, string, api.InvocationResult, time.Duration) error { return nil },
	}
	broker := &testutil.FakeMessaging{
		PublishResultFn: func(context.Context, string, api.InvocationResult) error { return errors.New("publish failed") },
		PublishDLQResultFn: func(context.Context, string, api.InvocationResult) error {
			dlqResult = true
			return nil
		},
	}
	i = newTestInvoker(store, broker)
	err = i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act2",
		RequestID:    "req2",
		Tenant:       "t1",
		Namespace:    "n1",
		Ref:          api.FunctionRef{Function: "f1", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
	})
	if err == nil {
		t.Fatal("expected publish failure error")
	}
	if !dlqResult {
		t.Fatal("expected DLQ publish when PublishResult fails")
	}
}

func TestHandleInvocationUserCodeBehavior(t *testing.T) {
	run := func(t *testing.T, manifest api.FunctionManifest, script string, event map[string]any) api.InvocationResult {
		t.Helper()
		bundleBytes, sha := buildInvokerBundleWithManifest(t, manifest, script)
		var published api.InvocationResult
		store := &testutil.FakePersistence{
			IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
				return false, api.ActivationRecord{}, nil
			},
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 1, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					SHA256: sha,
					Config: api.VersionConfig{TimeoutMS: manifest.Limits.TimeoutMS, MaxConcurrency: 1},
				}, bundleBytes, nil
			},
			PutActivationRunningFn: func(context.Context, api.ActivationRecord, time.Duration) error { return nil },
			CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) {
				return true, nil
			},
			SaveResultByRequestIDFn: func(context.Context, string, string, api.InvocationResult, time.Duration) error { return nil },
		}
		broker := &testutil.FakeMessaging{
			PublishResultFn: func(ctx context.Context, tenant string, result api.InvocationResult) error {
				_ = ctx
				_ = tenant
				published = result
				return nil
			},
		}
		i := newTestInvoker(store, broker)
		err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
			ActivationID: "act_user",
			RequestID:    "req_user",
			Tenant:       "t1",
			Namespace:    "n1",
			Ref:          api.FunctionRef{Function: "f1", Version: 1},
			Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
			Event:        event,
		})
		if err != nil {
			t.Fatalf("handleInvocation returned error: %v", err)
		}
		return published
	}

	t.Run("executes arbitrary javascript with deterministic output", func(t *testing.T) {
		manifest := defaultInvokerManifest()
		res := run(t, manifest, `
export default function(event) {
  const sum = Number(event.a || 0) + Number(event.b || 0)
  return { statusCode: 201, headers: {"content-type":"application/json"}, body: JSON.stringify({sum}) }
}
`, map[string]any{"a": 2, "b": 5})
		if res.Status != "success" || res.Result == nil {
			t.Fatalf("unexpected invocation result: %+v", res)
		}
		if res.Result.StatusCode != 201 || !strings.Contains(res.Result.Body, `"sum":7`) {
			t.Fatalf("unexpected function output: %+v", res.Result)
		}
	})

	t.Run("enforces function scopes (capability denied)", func(t *testing.T) {
		manifest := defaultInvokerManifest()
		res := run(t, manifest, `
export default function() {
  cs.kv.get("forbidden:key")
  return { statusCode: 200, body: "ok" }
}
`, map[string]any{})
		if res.Status != "error" || res.Error == nil {
			t.Fatalf("expected runtime capability error, got %+v", res)
		}
		if !strings.Contains(strings.ToLower(res.Error.Message), "kv prefix") {
			t.Fatalf("expected kv prefix denied error, got %+v", res.Error)
		}
	})

	t.Run("reports syntax errors from invalid user code", func(t *testing.T) {
		manifest := defaultInvokerManifest()
		res := run(t, manifest, `export default function() {`, map[string]any{})
		if res.Status != "error" || res.Error == nil {
			t.Fatalf("expected syntax error result, got %+v", res)
		}
		if !strings.Contains(strings.ToLower(res.Error.Message), "syntax") {
			t.Fatalf("expected syntax error details, got %+v", res.Error)
		}
	})

	t.Run("times out on infinite loop user code", func(t *testing.T) {
		manifest := defaultInvokerManifest()
		manifest.Limits.TimeoutMS = 30
		res := run(t, manifest, `export default function() { while (true) {} }`, map[string]any{})
		if res.Status != "timeout" || res.Error == nil {
			t.Fatalf("expected timeout result, got %+v", res)
		}
	})
}

func TestRunRetriesWhenConsumerExitsWithoutError(t *testing.T) {
	var calls atomic.Int32
	var once sync.Once
	done := make(chan struct{})

	store := &testutil.FakePersistence{}
	broker := &testutil.FakeMessaging{
		ConsumeInvocationsFn: func(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationRequest) error) error {
			_ = ctx
			_ = groupID
			_ = handler
			if calls.Add(1) >= 3 {
				once.Do(func() { close(done) })
			}
			return nil
		},
	}
	i := newTestInvoker(store, broker)
	i.cfg.CSInvokerPool.Workers.Threads = 1
	i.cfg.CSInvokerPool.HTTP.Addr = ":-1"
	i.logger = observability.NewLoggerWithWriter("test", io.Discard)

	go func() {
		_ = i.run()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected consumer retries after clean exit; calls=%d", calls.Load())
	}
}

func TestRunRetriesWhenConsumerReturnsError(t *testing.T) {
	var calls atomic.Int32
	var once sync.Once
	done := make(chan struct{})

	store := &testutil.FakePersistence{}
	broker := &testutil.FakeMessaging{
		ConsumeInvocationsFn: func(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationRequest) error) error {
			_ = ctx
			_ = groupID
			_ = handler
			if calls.Add(1) >= 3 {
				once.Do(func() { close(done) })
			}
			return errors.New("consumer failed")
		},
	}
	i := newTestInvoker(store, broker)
	i.cfg.CSInvokerPool.Workers.Threads = 1
	i.cfg.CSInvokerPool.HTTP.Addr = ":-1"
	i.logger = observability.NewLoggerWithWriter("test", io.Discard)

	go func() {
		_ = i.run()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected consumer retries after error; calls=%d", calls.Load())
	}
}

func TestServeHTTPEndpoints(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve test address: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	var failPing atomic.Bool
	store := &testutil.FakePersistence{
		PingFn: func(context.Context) error {
			if failPing.Load() {
				return errors.New("down")
			}
			return nil
		},
	}
	i := newTestInvoker(store, &testutil.FakeMessaging{})
	i.cfg.CSInvokerPool.HTTP.Addr = addr
	go i.serveHTTP()

	baseURL := "http://" + addr
	var resp *http.Response
	for attempt := 0; attempt < 20; attempt++ {
		resp, err = http.Get(baseURL + "/healthz")
		if err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("healthz request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status=%d", resp.StatusCode)
	}

	failPing.Store(true)
	resp, err = http.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz failure status=%d", resp.StatusCode)
	}

	failPing.Store(false)
	resp, err = http.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz success status=%d", resp.StatusCode)
	}
}

func TestHandleInvocationExceptionMatrix(t *testing.T) {
	t.Run("context canceled while waiting for global inflight slot", func(t *testing.T) {
		store := &testutil.FakePersistence{}
		i := newTestInvoker(store, &testutil.FakeMessaging{})
		i.inflight = make(chan struct{}, 1)
		i.inflight <- struct{}{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := i.handleInvocation(ctx, messaging.Envelope{}, api.InvocationRequest{
			ActivationID: "act1",
			RequestID:    "req1",
			Tenant:       "t1",
			Namespace:    "n1",
			Ref:          api.FunctionRef{Function: "fn1", Version: 1},
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
	})

	t.Run("activation terminal lookup error is propagated", func(t *testing.T) {
		store := &testutil.FakePersistence{
			IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
				return false, api.ActivationRecord{}, errors.New("terminal lookup failed")
			},
		}
		i := newTestInvoker(store, &testutil.FakeMessaging{})
		err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
			ActivationID: "act1",
			RequestID:    "req1",
			Tenant:       "t1",
			Namespace:    "n1",
			Ref:          api.FunctionRef{Function: "fn1", Version: 1},
		})
		if err == nil || !strings.Contains(err.Error(), "terminal lookup failed") {
			t.Fatalf("expected terminal lookup error, got %v", err)
		}
	})

	t.Run("resolve version error is propagated", func(t *testing.T) {
		store := &testutil.FakePersistence{
			IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
				return false, api.ActivationRecord{}, nil
			},
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 0, errors.New("resolve failed")
			},
		}
		i := newTestInvoker(store, &testutil.FakeMessaging{})
		err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
			ActivationID: "act1",
			RequestID:    "req1",
			Tenant:       "t1",
			Namespace:    "n1",
			Ref:          api.FunctionRef{Function: "fn1", Alias: "prod"},
		})
		if err == nil || !strings.Contains(err.Error(), "resolve failed") {
			t.Fatalf("expected resolve failure, got %v", err)
		}
	})

	t.Run("get version error is propagated", func(t *testing.T) {
		store := &testutil.FakePersistence{
			IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
				return false, api.ActivationRecord{}, nil
			},
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 3, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{}, nil, errors.New("version read failed")
			},
		}
		i := newTestInvoker(store, &testutil.FakeMessaging{})
		err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
			ActivationID: "act1",
			RequestID:    "req1",
			Tenant:       "t1",
			Namespace:    "n1",
			Ref:          api.FunctionRef{Function: "fn1", Alias: "prod"},
		})
		if err == nil || !strings.Contains(err.Error(), "version read failed") {
			t.Fatalf("expected version read failure, got %v", err)
		}
	})

	t.Run("put activation running error is propagated", func(t *testing.T) {
		bundleBytes, sha := buildInvokerBundle(t)
		store := &testutil.FakePersistence{
			IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
				return false, api.ActivationRecord{}, nil
			},
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 4, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					SHA256: sha,
					Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
				}, bundleBytes, nil
			},
			PutActivationRunningFn: func(context.Context, api.ActivationRecord, time.Duration) error {
				return errors.New("put activation failed")
			},
		}
		i := newTestInvoker(store, &testutil.FakeMessaging{})
		err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
			ActivationID: "act1",
			RequestID:    "req1",
			Tenant:       "t1",
			Namespace:    "n1",
			Ref:          api.FunctionRef{Function: "fn1", Alias: "prod"},
		})
		if err == nil || !strings.Contains(err.Error(), "put activation failed") {
			t.Fatalf("expected put activation failure, got %v", err)
		}
	})

	t.Run("cas conflict returns CSKVCASFailed", func(t *testing.T) {
		bundleBytes, sha := buildInvokerBundle(t)
		store := &testutil.FakePersistence{
			IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
				return false, api.ActivationRecord{}, nil
			},
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 5, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					SHA256: sha,
					Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
				}, bundleBytes, nil
			},
			PutActivationRunningFn: func(context.Context, api.ActivationRecord, time.Duration) error { return nil },
			CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) {
				return false, nil
			},
		}
		i := newTestInvoker(store, &testutil.FakeMessaging{})
		err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
			ActivationID: "act1",
			RequestID:    "req1",
			Tenant:       "t1",
			Namespace:    "n1",
			Ref:          api.FunctionRef{Function: "fn1", Alias: "prod"},
		})
		var csErr *cserrors.CSError
		if !errors.As(err, &csErr) || csErr.Code != cserrors.CSKVCASFailed {
			t.Fatalf("expected CSKVCASFailed, got %v", err)
		}
	})

	t.Run("cas write error is propagated", func(t *testing.T) {
		bundleBytes, sha := buildInvokerBundle(t)
		store := &testutil.FakePersistence{
			IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
				return false, api.ActivationRecord{}, nil
			},
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 6, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					SHA256: sha,
					Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
				}, bundleBytes, nil
			},
			PutActivationRunningFn: func(context.Context, api.ActivationRecord, time.Duration) error { return nil },
			CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) {
				return false, errors.New("cas write failed")
			},
		}
		i := newTestInvoker(store, &testutil.FakeMessaging{})
		err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
			ActivationID: "act1",
			RequestID:    "req1",
			Tenant:       "t1",
			Namespace:    "n1",
			Ref:          api.FunctionRef{Function: "fn1", Alias: "prod"},
		})
		if err == nil || !strings.Contains(err.Error(), "cas write failed") {
			t.Fatalf("expected cas write failure, got %v", err)
		}
	})

	t.Run("schedule trigger clears inflight even with log chunk persistence failures", func(t *testing.T) {
		bundleBytes, sha := buildInvokerBundleWithScript(t, `export default function(){ console.log("tick"); return { statusCode: 200, body: "ok" } }`)
		var clearScheduleCalls int
		store := &testutil.FakePersistence{
			IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
				return false, api.ActivationRecord{}, nil
			},
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 7, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					SHA256: sha,
					Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
				}, bundleBytes, nil
			},
			PutActivationRunningFn: func(context.Context, api.ActivationRecord, time.Duration) error { return nil },
			CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) {
				return true, nil
			},
			AppendLogChunkFn: func(context.Context, string, string, int64, []byte, time.Duration) error {
				return errors.New("log append failed")
			},
			SaveResultByRequestIDFn: func(context.Context, string, string, api.InvocationResult, time.Duration) error {
				return errors.New("cache write failed")
			},
			ClearScheduleInflightFn: func(context.Context, string, string, string) error {
				clearScheduleCalls++
				return nil
			},
		}
		broker := &testutil.FakeMessaging{
			PublishResultFn: func(context.Context, string, api.InvocationResult) error { return nil },
		}
		i := newTestInvoker(store, broker)
		err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
			ActivationID: "act1",
			RequestID:    "req1",
			Tenant:       "t1",
			Namespace:    "n1",
			Ref:          api.FunctionRef{Function: "fn1", Alias: "prod"},
			Trigger:      api.Trigger{Type: "schedule", Source: map[string]any{"schedule_name": "sch1"}},
		})
		if err != nil {
			t.Fatalf("expected success with non-fatal log/cache failures, got %v", err)
		}
		if clearScheduleCalls != 1 {
			t.Fatalf("expected one schedule inflight clear call, got %d", clearScheduleCalls)
		}
	})
}
