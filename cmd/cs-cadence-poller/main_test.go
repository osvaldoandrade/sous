package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/cadence"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

type fakeCadenceClient struct {
	mu sync.Mutex

	pollFn      func(context.Context, string, string, string) (*cadence.ActivityTask, error)
	completedFn func(context.Context, string, []byte) error
	failedFn    func(context.Context, string, string, []byte) error
	heartbeatFn func(context.Context, string, []byte) error
}

func (f *fakeCadenceClient) PollActivityTask(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
	if f.pollFn != nil {
		return f.pollFn(ctx, domain, tasklist, workerID)
	}
	return nil, ctx.Err()
}

func (f *fakeCadenceClient) RespondActivityCompleted(ctx context.Context, taskToken string, payload []byte) error {
	if f.completedFn != nil {
		return f.completedFn(ctx, taskToken, payload)
	}
	return nil
}

func (f *fakeCadenceClient) RespondActivityFailed(ctx context.Context, taskToken, reason string, details []byte) error {
	if f.failedFn != nil {
		return f.failedFn(ctx, taskToken, reason, details)
	}
	return nil
}

func (f *fakeCadenceClient) RecordActivityHeartbeat(ctx context.Context, taskToken string, details []byte) error {
	if f.heartbeatFn != nil {
		return f.heartbeatFn(ctx, taskToken, details)
	}
	return nil
}

func newTestPoller(store *testutil.FakePersistence, broker *testutil.FakeMessaging, client cadence.Client) *poller {
	cfg := config.Config{}
	cfg.CSCadencePoller.RefreshSeconds = 1
	cfg.CSCadencePoller.Heartbeat.MaxPerSecond = 2
	cfg.CSCadencePoller.Limits.MaxInflightTasksDefault = 4
	return &poller{
		cfg:           cfg,
		store:         store,
		broker:        broker,
		client:        client,
		logger:        observability.NewLoggerWithWriter("test", io.Discard),
		running:       make(map[string]context.CancelFunc),
		bindingSems:   make(map[string]chan struct{}),
		activation:    make(map[string]taskInfo),
		heartbeatLast: make(map[string]time.Time),
	}
}

func TestHelpers(t *testing.T) {
	b := api.WorkerBinding{Tenant: "t1", Namespace: "n1", Name: "w1"}
	if got := bindingKey(b); got != "t1:n1:w1" {
		t.Fatalf("bindingKey=%q", got)
	}
	if hashToken("abc") == hashToken("abd") {
		t.Fatal("hashToken should differ for different tokens")
	}
	if max(5, 2) != 5 || max(1, 9) != 9 {
		t.Fatalf("max helper failed")
	}
	if min(3, 4) != 3 || min(9, 4) != 4 {
		t.Fatalf("min helper failed")
	}

	var a atomicInt64
	if got := a.Add(3); got != 3 {
		t.Fatalf("atomic add=%d", got)
	}
	if got := a.Load(); got != 3 {
		t.Fatalf("atomic load=%d", got)
	}
}

func TestRefreshBindingsStartsAndStopsWorkers(t *testing.T) {
	var stopped bool
	store := &testutil.FakePersistence{
		ListAllWorkerBindingsFn: func(context.Context) ([]api.WorkerBinding, error) {
			binding := api.WorkerBinding{
				Tenant:    "t1",
				Namespace: "n1",
				Name:      "w-new",
				Enabled:   true,
			}
			binding.Pollers.Activity = 1
			binding.Limits.MaxInflightTasks = 1
			return []api.WorkerBinding{
				binding,
			}, nil
		},
	}
	client := &fakeCadenceClient{
		pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	p := newTestPoller(store, &testutil.FakeMessaging{}, client)
	p.running["t1:n1:w-old"] = func() { stopped = true }
	p.bindingSems["t1:n1:w-old"] = make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.refreshBindings(ctx); err != nil {
		t.Fatalf("refreshBindings failed: %v", err)
	}
	if !stopped {
		t.Fatal("expected stale worker binding to be stopped")
	}
	if _, ok := p.running["t1:n1:w-new"]; !ok {
		t.Fatal("expected new binding to be started")
	}
}

func TestPollLoopMappingNotFoundAndSuccess(t *testing.T) {
	t.Run("mapping-not-found", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var failedReason string
		client := &fakeCadenceClient{
			pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
				cancel()
				return &cadence.ActivityTask{TaskToken: "tok1", ActivityType: "A"}, nil
			},
			failedFn: func(ctx context.Context, taskToken, reason string, details []byte) error {
				_ = ctx
				_ = taskToken
				_ = details
				failedReason = reason
				return nil
			},
		}
		p := newTestPoller(&testutil.FakePersistence{}, &testutil.FakeMessaging{}, client)
		b := api.WorkerBinding{
			Tenant:      "t1",
			Namespace:   "n1",
			Name:        "w1",
			ActivityMap: map[string]api.WorkerBindingRef{},
		}
		sem := make(chan struct{}, 1)
		p.pollLoop(ctx, b, sem)
		if failedReason != "mapping_not_found" {
			t.Fatalf("unexpected failure reason: %q", failedReason)
		}
	})

	t.Run("success-path", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		store := &testutil.FakePersistence{
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 5, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					Config: api.VersionConfig{
						TimeoutMS: 200,
						Authz: api.VersionAuthz{
							InvokeCadenceRoles: []string{"role:cadence"},
						},
					},
				}, nil, nil
			},
			PutCadenceTaskMappingFn: func(context.Context, string, string, string, string, time.Duration) error {
				return nil
			},
		}
		broker := &testutil.FakeMessaging{
			PublishInvocationFn: func(ctx context.Context, req api.InvocationRequest) error {
				_ = ctx
				if req.Ref.Version != 5 {
					t.Fatalf("expected resolved version 5, got %+v", req.Ref)
				}
				cancel()
				return nil
			},
		}
		client := &fakeCadenceClient{
			pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
				return &cadence.ActivityTask{
					TaskToken:    "tok2",
					Domain:       "d",
					Tasklist:     "tl",
					WorkflowID:   "wf",
					RunID:        "run",
					ActivityID:   "act",
					ActivityType: "TypeA",
					Attempt:      1,
				}, nil
			},
		}
		p := newTestPoller(store, broker, client)
		b := api.WorkerBinding{
			Tenant:    "t1",
			Namespace: "n1",
			Name:      "w1",
			ActivityMap: map[string]api.WorkerBindingRef{
				"TypeA": {Function: "fn1", Alias: "prod"},
			},
		}
		sem := make(chan struct{}, 1)
		p.pollLoop(ctx, b, sem)

		p.mu.Lock()
		defer p.mu.Unlock()
		if len(p.activation) != 1 {
			t.Fatalf("expected one activation map entry, got %d", len(p.activation))
		}
	})
}

func TestConsumeResultsAndHeartbeats(t *testing.T) {
	t.Run("consume-results", func(t *testing.T) {
		var completedToken, failedReason string
		var deletedMapping bool

		client := &fakeCadenceClient{
			completedFn: func(ctx context.Context, taskToken string, payload []byte) error {
				_ = ctx
				completedToken = taskToken
				if _, err := base64.StdEncoding.DecodeString(string(payload)); err != nil {
					t.Fatalf("completed payload should be base64: %v", err)
				}
				return nil
			},
			failedFn: func(ctx context.Context, taskToken, reason string, details []byte) error {
				_ = ctx
				_ = taskToken
				_ = details
				failedReason = reason
				return nil
			},
		}
		store := &testutil.FakePersistence{
			DeleteCadenceTaskMappingFn: func(context.Context, string, string, string) error {
				deletedMapping = true
				return nil
			},
		}
		broker := &testutil.FakeMessaging{
			ConsumeResultsFn: func(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationResult) error) error {
				_ = ctx
				_ = groupID
				if err := handler(messaging.Envelope{}, api.InvocationResult{
					ActivationID: "act-success",
					Status:       "success",
					Result:       &api.FunctionResponse{StatusCode: 200, Body: "ok"},
				}); err != nil {
					return err
				}
				return handler(messaging.Envelope{}, api.InvocationResult{
					ActivationID: "act-failed",
					Status:       "timeout",
					Error:        &api.InvocationError{Message: "timed out"},
				})
			},
		}
		p := newTestPoller(store, broker, client)
		sem := make(chan struct{}, 1)
		sem <- struct{}{}
		p.activation["act-success"] = taskInfo{Tenant: "t1", Namespace: "n1", TaskToken: "tok_success", TaskHash: "h1", Semaphore: sem}
		p.activation["act-failed"] = taskInfo{Tenant: "t1", Namespace: "n1", TaskToken: "tok_failed", TaskHash: "h2", Semaphore: sem}

		p.consumeResults(context.Background())
		if completedToken != "tok_success" {
			t.Fatalf("expected completed token tok_success, got %q", completedToken)
		}
		if failedReason != "timeout" {
			t.Fatalf("expected timeout reason, got %q", failedReason)
		}
		if !deletedMapping {
			t.Fatal("expected cadence task mapping deletion")
		}
	})

	t.Run("consume-heartbeats", func(t *testing.T) {
		var hbCalls int
		client := &fakeCadenceClient{
			heartbeatFn: func(ctx context.Context, taskToken string, details []byte) error {
				_ = ctx
				_ = taskToken
				_ = details
				hbCalls++
				return nil
			},
		}
		payload, _ := json.Marshal(map[string]string{
			"activation_id":  "act-hb",
			"details_base64": base64.StdEncoding.EncodeToString([]byte(`{"ok":true}`)),
		})
		broker := &testutil.FakeMessaging{
			ConsumeTopicFn: func(ctx context.Context, topic, groupID string, handler func(messaging.Envelope) error) error {
				_ = ctx
				_ = topic
				_ = groupID
				if err := handler(messaging.Envelope{Body: []byte(`not-json`)}); err != nil {
					return err
				}
				if err := handler(messaging.Envelope{Body: payload}); err != nil {
					return err
				}
				// Second heartbeat immediately after the first one should be rate-limited.
				return handler(messaging.Envelope{Body: payload})
			},
		}
		p := newTestPoller(&testutil.FakePersistence{}, broker, client)
		p.cfg.CSCadencePoller.Heartbeat.MaxPerSecond = 1
		p.activation["act-hb"] = taskInfo{TaskToken: "token-hb"}

		p.consumeHeartbeats(context.Background())
		if hbCalls != 1 {
			t.Fatalf("expected exactly one heartbeat call due rate limiting, got %d", hbCalls)
		}
	})
}

func TestRefreshBindingsError(t *testing.T) {
	p := newTestPoller(&testutil.FakePersistence{
		ListAllWorkerBindingsFn: func(context.Context) ([]api.WorkerBinding, error) {
			return nil, errors.New("store down")
		},
	}, &testutil.FakeMessaging{}, &fakeCadenceClient{})

	if err := p.refreshBindings(context.Background()); err == nil {
		t.Fatal("expected refreshBindings error")
	}
}

func TestServeHTTPReturnsWhenPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", ":8084")
	if err != nil {
		t.Skipf("unable to reserve :8084: %v", err)
	}
	defer ln.Close()

	p := newTestPoller(&testutil.FakePersistence{
		PingFn: func(context.Context) error { return nil },
	}, &testutil.FakeMessaging{}, &fakeCadenceClient{})
	p.serveHTTP()
}

func TestRefreshLoopStopsOnContextDone(t *testing.T) {
	var calls int
	p := newTestPoller(&testutil.FakePersistence{
		ListAllWorkerBindingsFn: func(context.Context) ([]api.WorkerBinding, error) {
			calls++
			return nil, errors.New("boom")
		},
	}, &testutil.FakeMessaging{}, &fakeCadenceClient{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.refreshLoop(ctx)
	if calls == 0 {
		t.Fatal("expected at least one refresh attempt")
	}
}

func TestPollLoopErrorBranches(t *testing.T) {
	t.Run("cadence poll errors release semaphore and exit on canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var polls int
		client := &fakeCadenceClient{
			pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
				_ = domain
				_ = tasklist
				_ = workerID
				polls++
				cancel()
				return nil, errors.New("cadence unavailable")
			},
		}
		p := newTestPoller(&testutil.FakePersistence{}, &testutil.FakeMessaging{}, client)
		b := api.WorkerBinding{
			Tenant:      "t1",
			Namespace:   "n1",
			Name:        "w1",
			Domain:      "d1",
			Tasklist:    "tl1",
			WorkerID:    "wid1",
			ActivityMap: map[string]api.WorkerBindingRef{"TypeA": {Function: "fn1", Alias: "prod"}},
		}
		sem := make(chan struct{}, 1)
		p.pollLoop(ctx, b, sem)
		if polls == 0 {
			t.Fatal("expected cadence poll attempt")
		}
		if len(sem) != 0 {
			t.Fatalf("semaphore should be released, len=%d", len(sem))
		}
	})

	t.Run("empty task token is skipped", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := &fakeCadenceClient{
			pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
				_ = ctx
				_ = domain
				_ = tasklist
				_ = workerID
				cancel()
				return &cadence.ActivityTask{}, nil
			},
		}
		p := newTestPoller(&testutil.FakePersistence{}, &testutil.FakeMessaging{}, client)
		b := api.WorkerBinding{
			Tenant:      "t1",
			Namespace:   "n1",
			Name:        "w1",
			Domain:      "d1",
			Tasklist:    "tl1",
			WorkerID:    "wid1",
			ActivityMap: map[string]api.WorkerBindingRef{"TypeA": {Function: "fn1", Alias: "prod"}},
		}
		sem := make(chan struct{}, 1)
		p.pollLoop(ctx, b, sem)
		if len(sem) != 0 {
			t.Fatalf("semaphore should be released after empty-task branch, len=%d", len(sem))
		}
	})

	t.Run("resolve version failure responds with resolve_version_failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var reason string
		client := &fakeCadenceClient{
			pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
				_ = ctx
				_ = domain
				_ = tasklist
				_ = workerID
				return &cadence.ActivityTask{TaskToken: "tok1", ActivityType: "TypeA"}, nil
			},
			failedFn: func(ctx context.Context, taskToken, gotReason string, details []byte) error {
				_ = ctx
				_ = taskToken
				_ = details
				reason = gotReason
				cancel()
				return nil
			},
		}
		store := &testutil.FakePersistence{
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 0, errors.New("resolve failed")
			},
		}
		p := newTestPoller(store, &testutil.FakeMessaging{}, client)
		b := api.WorkerBinding{
			Tenant:      "t1",
			Namespace:   "n1",
			Name:        "w1",
			Domain:      "d1",
			Tasklist:    "tl1",
			WorkerID:    "wid1",
			ActivityMap: map[string]api.WorkerBindingRef{"TypeA": {Function: "fn1", Alias: "prod"}},
		}
		sem := make(chan struct{}, 1)
		p.pollLoop(ctx, b, sem)
		if reason != "resolve_version_failed" {
			t.Fatalf("expected reason resolve_version_failed, got %q", reason)
		}
	})

	t.Run("missing version responds with version_not_found", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var reason string
		client := &fakeCadenceClient{
			pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
				_ = ctx
				_ = domain
				_ = tasklist
				_ = workerID
				return &cadence.ActivityTask{TaskToken: "tok1", ActivityType: "TypeA"}, nil
			},
			failedFn: func(ctx context.Context, taskToken, gotReason string, details []byte) error {
				_ = ctx
				_ = taskToken
				_ = details
				reason = gotReason
				cancel()
				return nil
			},
		}
		store := &testutil.FakePersistence{
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 7, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{}, nil, errors.New("version missing")
			},
		}
		p := newTestPoller(store, &testutil.FakeMessaging{}, client)
		b := api.WorkerBinding{
			Tenant:      "t1",
			Namespace:   "n1",
			Name:        "w1",
			Domain:      "d1",
			Tasklist:    "tl1",
			WorkerID:    "wid1",
			ActivityMap: map[string]api.WorkerBindingRef{"TypeA": {Function: "fn1", Alias: "prod"}},
		}
		sem := make(chan struct{}, 1)
		p.pollLoop(ctx, b, sem)
		if reason != "version_not_found" {
			t.Fatalf("expected reason version_not_found, got %q", reason)
		}
	})

	t.Run("role mismatch responds with role_missing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var reason string
		client := &fakeCadenceClient{
			pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
				_ = ctx
				_ = domain
				_ = tasklist
				_ = workerID
				return &cadence.ActivityTask{TaskToken: "tok1", ActivityType: "TypeA"}, nil
			},
			failedFn: func(ctx context.Context, taskToken, gotReason string, details []byte) error {
				_ = ctx
				_ = taskToken
				_ = details
				reason = gotReason
				cancel()
				return nil
			},
		}
		store := &testutil.FakePersistence{
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 7, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					Config: api.VersionConfig{
						Authz: api.VersionAuthz{
							InvokeCadenceRoles: []string{"role:not-cadence"},
						},
					},
				}, nil, nil
			},
		}
		p := newTestPoller(store, &testutil.FakeMessaging{}, client)
		b := api.WorkerBinding{
			Tenant:      "t1",
			Namespace:   "n1",
			Name:        "w1",
			Domain:      "d1",
			Tasklist:    "tl1",
			WorkerID:    "wid1",
			ActivityMap: map[string]api.WorkerBindingRef{"TypeA": {Function: "fn1", Alias: "prod"}},
		}
		sem := make(chan struct{}, 1)
		p.pollLoop(ctx, b, sem)
		if reason != "role_missing" {
			t.Fatalf("expected reason role_missing, got %q", reason)
		}
	})

	t.Run("publish failure responds with publish_failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var reason string
		client := &fakeCadenceClient{
			pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
				_ = ctx
				_ = domain
				_ = tasklist
				_ = workerID
				return &cadence.ActivityTask{TaskToken: "tok1", ActivityType: "TypeA"}, nil
			},
			failedFn: func(ctx context.Context, taskToken, gotReason string, details []byte) error {
				_ = ctx
				_ = taskToken
				_ = details
				reason = gotReason
				cancel()
				return nil
			},
		}
		store := &testutil.FakePersistence{
			ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
				return 9, nil
			},
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{
					Config: api.VersionConfig{
						TimeoutMS: 200,
						Authz: api.VersionAuthz{
							InvokeCadenceRoles: []string{"role:cadence"},
						},
					},
				}, nil, nil
			},
		}
		broker := &testutil.FakeMessaging{
			PublishInvocationFn: func(context.Context, api.InvocationRequest) error {
				return errors.New("publish unavailable")
			},
		}
		p := newTestPoller(store, broker, client)
		b := api.WorkerBinding{
			Tenant:      "t1",
			Namespace:   "n1",
			Name:        "w1",
			Domain:      "d1",
			Tasklist:    "tl1",
			WorkerID:    "wid1",
			ActivityMap: map[string]api.WorkerBindingRef{"TypeA": {Function: "fn1", Alias: "prod"}},
		}
		sem := make(chan struct{}, 1)
		p.pollLoop(ctx, b, sem)
		if reason != "publish_failed" {
			t.Fatalf("expected reason publish_failed, got %q", reason)
		}
	})
}

func TestConsumeHandlersWrapCadenceErrors(t *testing.T) {
	t.Run("consumeResults wraps completed callback errors", func(t *testing.T) {
		var handlerErr error
		client := &fakeCadenceClient{
			completedFn: func(context.Context, string, []byte) error {
				return errors.New("cadence complete failed")
			},
		}
		broker := &testutil.FakeMessaging{
			ConsumeResultsFn: func(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationResult) error) error {
				_ = ctx
				_ = groupID
				handlerErr = handler(messaging.Envelope{}, api.InvocationResult{
					ActivationID: "act1",
					Status:       "success",
					Result:       &api.FunctionResponse{StatusCode: 200, Body: "ok"},
				})
				return nil
			},
		}
		p := newTestPoller(&testutil.FakePersistence{}, broker, client)
		sem := make(chan struct{}, 1)
		sem <- struct{}{}
		p.activation["act1"] = taskInfo{TaskToken: "token1", Semaphore: sem}

		p.consumeResults(context.Background())
		if handlerErr == nil {
			t.Fatal("expected wrapped completion error")
		}
		var csErr *cserrors.CSError
		if !errors.As(handlerErr, &csErr) || csErr.Code != cserrors.CSCadenceRespFailed {
			t.Fatalf("expected CSCadenceRespFailed, got %v", handlerErr)
		}
	})

	t.Run("consumeHeartbeats wraps cadence heartbeat errors", func(t *testing.T) {
		var handlerErr error
		payload, _ := json.Marshal(map[string]string{
			"activation_id":  "act-hb",
			"details_base64": base64.StdEncoding.EncodeToString([]byte(`{"step":"x"}`)),
		})
		client := &fakeCadenceClient{
			heartbeatFn: func(context.Context, string, []byte) error {
				return errors.New("cadence hb failed")
			},
		}
		broker := &testutil.FakeMessaging{
			ConsumeTopicFn: func(ctx context.Context, topic, groupID string, handler func(messaging.Envelope) error) error {
				_ = ctx
				_ = topic
				_ = groupID
				handlerErr = handler(messaging.Envelope{Body: payload})
				return nil
			},
		}
		p := newTestPoller(&testutil.FakePersistence{}, broker, client)
		p.activation["act-hb"] = taskInfo{TaskToken: "tok-hb"}
		p.cfg.CSCadencePoller.Heartbeat.MaxPerSecond = 1000

		p.consumeHeartbeats(context.Background())
		if handlerErr == nil {
			t.Fatal("expected wrapped heartbeat error")
		}
		var csErr *cserrors.CSError
		if !errors.As(handlerErr, &csErr) || csErr.Code != cserrors.CSCadenceHBFailed {
			t.Fatalf("expected CSCadenceHBFailed, got %v", handlerErr)
		}
	})
}

func TestServeHTTPEndpoints(t *testing.T) {
	ln, err := net.Listen("tcp", ":8084")
	if err != nil {
		t.Skipf("unable to reserve :8084: %v", err)
	}
	_ = ln.Close()

	var failPing atomic.Bool
	p := newTestPoller(&testutil.FakePersistence{
		PingFn: func(context.Context) error {
			if failPing.Load() {
				return errors.New("down")
			}
			return nil
		},
	}, &testutil.FakeMessaging{}, &fakeCadenceClient{})
	go p.serveHTTP()

	var resp *http.Response
	for attempt := 0; attempt < 30; attempt++ {
		resp, err = http.Get("http://127.0.0.1:8084/healthz")
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
	resp, err = http.Get("http://127.0.0.1:8084/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz failure status=%d", resp.StatusCode)
	}

	failPing.Store(false)
	resp, err = http.Get("http://127.0.0.1:8084/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz success status=%d", resp.StatusCode)
	}
}

func TestCadenceWorkerPollAndReturnFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var completedToken string
	var mappingPut, mappingDeleted bool
	invocationCh := make(chan api.InvocationRequest, 1)
	published := make(chan struct{}, 1)
	var taskReturned atomic.Bool

	store := &testutil.FakePersistence{
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 3, nil
		},
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 200,
					Authz: api.VersionAuthz{
						InvokeCadenceRoles: []string{"role:cadence"},
					},
				},
			}, nil, nil
		},
		PutCadenceTaskMappingFn: func(context.Context, string, string, string, string, time.Duration) error {
			mappingPut = true
			return nil
		},
		DeleteCadenceTaskMappingFn: func(context.Context, string, string, string) error {
			mappingDeleted = true
			return nil
		},
	}
	broker := &testutil.FakeMessaging{
		PublishInvocationFn: func(ctx context.Context, req api.InvocationRequest) error {
			_ = ctx
			invocationCh <- req
			select {
			case published <- struct{}{}:
			default:
			}
			return nil
		},
		ConsumeResultsFn: func(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationResult) error) error {
			_ = ctx
			_ = groupID
			req := <-invocationCh
			return handler(messaging.Envelope{}, api.InvocationResult{
				ActivationID: req.ActivationID,
				RequestID:    req.RequestID,
				Status:       "success",
				Result:       &api.FunctionResponse{StatusCode: 200, Body: "ok"},
			})
		},
	}
	client := &fakeCadenceClient{
		pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
			_ = domain
			_ = tasklist
			_ = workerID
			if taskReturned.Load() {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			taskReturned.Store(true)
			return &cadence.ActivityTask{
				TaskToken:    "token-flow",
				Domain:       "payments",
				Tasklist:     "tl1",
				WorkflowID:   "wf1",
				RunID:        "run1",
				ActivityID:   "activity1",
				ActivityType: "SousInvokeActivity",
				Attempt:      1,
			}, nil
		},
		completedFn: func(ctx context.Context, taskToken string, payload []byte) error {
			_ = ctx
			_ = payload
			completedToken = taskToken
			cancel()
			return nil
		},
	}

	p := newTestPoller(store, broker, client)
	binding := api.WorkerBinding{
		Tenant:    "t1",
		Namespace: "n1",
		Name:      "worker1",
		Domain:    "payments",
		Tasklist:  "tl1",
		WorkerID:  "wid1",
		ActivityMap: map[string]api.WorkerBindingRef{
			"SousInvokeActivity": {Function: "fn1", Alias: "prod"},
		},
	}
	sem := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.pollLoop(ctx, binding, sem)
	}()

	select {
	case <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("publish invocation was not called")
	}

	p.consumeResults(context.Background())
	<-done

	if completedToken != "token-flow" {
		t.Fatalf("expected completion token token-flow, got %q", completedToken)
	}
	if !mappingPut || !mappingDeleted {
		t.Fatalf("expected cadence mapping put+delete, got put=%v delete=%v", mappingPut, mappingDeleted)
	}
}

func TestPollLoopRetriesAfterPollErrorWithBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var polls int32
	client := &fakeCadenceClient{
		pollFn: func(context.Context, string, string, string) (*cadence.ActivityTask, error) {
			call := atomic.AddInt32(&polls, 1)
			if call == 1 {
				return nil, errors.New("temporary cadence failure")
			}
			cancel()
			return &cadence.ActivityTask{}, nil
		},
	}
	p := newTestPoller(&testutil.FakePersistence{}, &testutil.FakeMessaging{}, client)
	binding := api.WorkerBinding{
		Tenant:      "t1",
		Namespace:   "n1",
		Name:        "worker1",
		Domain:      "d1",
		Tasklist:    "tl1",
		WorkerID:    "wid1",
		ActivityMap: map[string]api.WorkerBindingRef{"A": {Function: "fn1", Alias: "prod"}},
	}
	sem := make(chan struct{}, 1)

	start := time.Now()
	p.pollLoop(ctx, binding, sem)
	elapsed := time.Since(start)

	if atomic.LoadInt32(&polls) < 2 {
		t.Fatalf("expected poll retry after first error, polls=%d", polls)
	}
	if elapsed < 180*time.Millisecond {
		t.Fatalf("expected backoff delay near 200ms, elapsed=%s", elapsed)
	}
}
