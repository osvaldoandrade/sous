package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

func TestMax(t *testing.T) {
	if got := max(5, 2); got != 5 {
		t.Fatalf("max(5,2)=%d", got)
	}
	if got := max(1, 4); got != 4 {
		t.Fatalf("max(1,4)=%d", got)
	}
}

func TestPublishScheduleTickBranches(t *testing.T) {
	sch := api.ScheduleRecord{
		Tenant:        "t1",
		Namespace:     "n1",
		Name:          "sch1",
		OverlapPolicy: "skip",
		Ref:           api.ScheduleRef{Function: "fn1", Alias: "prod"},
	}

	// Inflight schedule with skip policy should not publish.
	persistence := &testutil.FakePersistence{
		GetScheduleInflightFn: func(ctx context.Context, tenant, namespace, name string) (string, error) {
			_ = ctx
			_ = tenant
			_ = namespace
			_ = name
			return "act_inflight", nil
		},
	}
	broker := &testutil.FakeMessaging{
		PublishInvocationFn: func(context.Context, api.InvocationRequest) error {
			t.Fatal("PublishInvocation should not be called when inflight exists")
			return nil
		},
	}
	s := &scheduler{store: persistence, broker: broker}
	if err := s.publishScheduleTick(context.Background(), sch, 1); err != nil {
		t.Fatalf("publishScheduleTick inflight branch returned error: %v", err)
	}

	// ResolveVersion failure should bubble up.
	persistence = &testutil.FakePersistence{
		GetScheduleInflightFn: func(context.Context, string, string, string) (string, error) { return "", nil },
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 0, errors.New("resolve failed")
		},
	}
	s = &scheduler{store: persistence, broker: broker}
	if err := s.publishScheduleTick(context.Background(), sch, 1); err == nil {
		t.Fatal("expected resolve error")
	}
}

func TestTickPublishesAndPersistsState(t *testing.T) {
	var published int
	var savedState api.ScheduleState
	var inflightSet bool

	persistence := &testutil.FakePersistence{
		ListAllSchedulesFn: func(ctx context.Context) ([]api.ScheduleRecord, error) {
			_ = ctx
			return []api.ScheduleRecord{
				{
					Tenant:        "t1",
					Namespace:     "n1",
					Name:          "enabled",
					EverySeconds:  1,
					OverlapPolicy: "skip",
					Enabled:       true,
					Ref:           api.ScheduleRef{Function: "fn1", Alias: "prod"},
				},
				{
					Tenant:       "t1",
					Namespace:    "n1",
					Name:         "disabled",
					EverySeconds: 1,
					Enabled:      false,
				},
			}, nil
		},
		GetScheduleStateFn: func(ctx context.Context, tenant, namespace, name string) (api.ScheduleState, error) {
			_ = ctx
			_ = tenant
			_ = namespace
			_ = name
			return api.ScheduleState{}, nil
		},
		PutScheduleStateFn: func(ctx context.Context, tenant, namespace, name string, state api.ScheduleState) error {
			_ = ctx
			_ = tenant
			_ = namespace
			if name != "enabled" {
				t.Fatalf("unexpected schedule state write for %s", name)
			}
			savedState = state
			return nil
		},
		GetScheduleInflightFn: func(context.Context, string, string, string) (string, error) { return "", nil },
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) { return 2, nil },
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 100,
					Authz: api.VersionAuthz{
						InvokeScheduleRoles: []string{"role:worker"},
					},
				},
			}, nil, nil
		},
		SetScheduleInflightFn: func(context.Context, string, string, string, string, time.Duration) error {
			inflightSet = true
			return nil
		},
	}
	broker := &testutil.FakeMessaging{
		PublishInvocationFn: func(ctx context.Context, req api.InvocationRequest) error {
			_ = ctx
			published++
			if req.Trigger.Type != "schedule" || req.Ref.Version != 2 {
				t.Fatalf("unexpected invocation request: %+v", req)
			}
			return nil
		},
	}
	cfg := config.Config{}
	cfg.CSScheduler.MaxCatchupTick = 1
	s := &scheduler{cfg: cfg, store: persistence, broker: broker}

	if err := s.tick(context.Background()); err != nil {
		t.Fatalf("tick returned error: %v", err)
	}
	if published != 1 {
		t.Fatalf("expected 1 published invocation, got %d", published)
	}
	if !inflightSet {
		t.Fatal("expected inflight marker to be set for skip overlap policy")
	}
	if savedState.TickSeq != 1 || savedState.NextTickMS == 0 {
		t.Fatalf("unexpected persisted schedule state: %+v", savedState)
	}
}

func TestTickListSchedulesError(t *testing.T) {
	persistence := &testutil.FakePersistence{
		ListAllSchedulesFn: func(context.Context) ([]api.ScheduleRecord, error) {
			return nil, errors.New("store down")
		},
	}
	s := &scheduler{store: persistence}
	if err := s.tick(context.Background()); err == nil {
		t.Fatal("expected ListAllSchedules error")
	}
}

func TestServeHTTPReturnsWhenPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", ":8083")
	if err != nil {
		t.Skipf("unable to reserve :8083: %v", err)
	}
	defer ln.Close()

	s := &scheduler{
		store: &testutil.FakePersistence{
			PingFn: func(context.Context) error { return nil },
		},
	}
	// With port already in use, serveHTTP should return quickly.
	s.serveHTTP()
}

func TestLeaderLoopAcquireAndExtend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var leaseValue string
	var attempts int
	var extended bool

	store := &testutil.FakePersistence{
		TryAcquireLeaseFn: func(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
			_ = ctx
			_ = key
			_ = ttl
			leaseValue = value
			attempts++
			if attempts == 1 {
				return true, nil
			}
			if attempts == 2 {
				return false, nil
			}
			return false, context.Canceled
		},
		GetLeaseValueFn: func(context.Context, string) (string, error) {
			return leaseValue, nil
		},
		ExtendLeaseFn: func(context.Context, string, time.Duration) error {
			extended = true
			cancel()
			return nil
		},
	}

	s := &scheduler{store: store}
	s.leaderLoop(ctx)

	if attempts < 2 {
		t.Fatalf("expected at least two lease attempts, got %d", attempts)
	}
	if !extended {
		t.Fatal("expected lease extension path")
	}
	if !s.isLeader.Load() {
		t.Fatal("expected scheduler to remain leader")
	}
}

func TestServeHTTPEndpoints(t *testing.T) {
	// Ensure port can be used before starting the fixed-port server.
	ln, err := net.Listen("tcp", ":8083")
	if err != nil {
		t.Skipf("unable to reserve :8083: %v", err)
	}
	_ = ln.Close()

	var failPing atomic.Bool
	s := &scheduler{
		store: &testutil.FakePersistence{
			PingFn: func(context.Context) error {
				if failPing.Load() {
					return errors.New("down")
				}
				return nil
			},
		},
	}
	go s.serveHTTP()

	var resp *http.Response
	for attempt := 0; attempt < 30; attempt++ {
		resp, err = http.Get("http://127.0.0.1:8083/healthz")
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
	resp, err = http.Get("http://127.0.0.1:8083/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz failure status=%d", resp.StatusCode)
	}

	failPing.Store(false)
	resp, err = http.Get("http://127.0.0.1:8083/readyz")
	if err != nil {
		t.Fatalf("readyz request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz success status=%d", resp.StatusCode)
	}
}
