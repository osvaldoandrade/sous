package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// TestTickCronAdvancesToNextQuarterHour drives the scheduler tick loop
// with a fake clock + the in-memory FakePersistence and asserts the cron
// schedule's state.next_tick_ms is the wall-clock-aligned next quarter
// hour in America/Sao_Paulo. This is the integration recipe called out
// in the issue acceptance criteria.
func TestTickCronAdvancesToNextQuarterHour(t *testing.T) {
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		t.Skipf("America/Sao_Paulo unavailable: %v", err)
	}
	// Fake clock: 2026-05-12 09:07 -03 (within working hours, mid-week).
	fakeNow := time.Date(2026, 5, 12, 9, 7, 30, 0, loc)
	sch := api.ScheduleRecord{
		Tenant:        "t_test01",
		Namespace:     "default",
		Name:          "quarter_hour",
		OverlapPolicy: "skip",
		Enabled:       true,
		Kind:          "cron",
		Cron:          "*/15 * * * *",
		TZ:            "America/Sao_Paulo",
		Ref:           api.ScheduleRef{Function: "fn1", Alias: "prod"},
	}

	var savedState api.ScheduleState
	store := &testutil.FakePersistence{
		ListAllSchedulesFn: func(context.Context) ([]api.ScheduleRecord, error) {
			return []api.ScheduleRecord{sch}, nil
		},
		GetScheduleStateFn: func(context.Context, string, string, string) (api.ScheduleState, error) {
			// Force the first-tick branch: state.NextTickMS=0 so the
			// loop seeds it to `now` then walks forward via cron.
			return api.ScheduleState{}, nil
		},
		PutScheduleStateFn: func(_ context.Context, _, _, _ string, st api.ScheduleState) error {
			savedState = st
			return nil
		},
		GetScheduleInflightFn: func(context.Context, string, string, string) (string, error) { return "", nil },
		ResolveVersionFn:      func(context.Context, string, string, string, string, int64) (int64, error) { return 1, nil },
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 1000,
					Authz:     api.VersionAuthz{InvokeScheduleRoles: []string{"role:worker"}},
				},
			}, nil, nil
		},
	}
	var published int
	broker := &testutil.FakeMessaging{
		PublishInvocationFn: func(context.Context, api.InvocationRequest) error {
			published++
			return nil
		},
	}
	cfg := config.Config{}
	cfg.CSScheduler.MaxCatchupTick = 1
	s := &scheduler{cfg: cfg, store: store, broker: broker, nowFn: func() time.Time { return fakeNow }}

	if err := s.tick(context.Background()); err != nil {
		t.Fatalf("tick returned error: %v", err)
	}
	if published != 1 {
		t.Fatalf("expected 1 published invocation, got %d", published)
	}

	// Next quarter-hour in São Paulo wall time after 09:07 is 09:15.
	wantNext := time.Date(2026, 5, 12, 9, 15, 0, 0, loc).UnixMilli()
	if savedState.NextTickMS != wantNext {
		got := time.UnixMilli(savedState.NextTickMS).In(loc)
		t.Fatalf("next tick = %s (%d), want %s (%d)",
			got, savedState.NextTickMS,
			time.UnixMilli(wantNext).In(loc), wantNext)
	}
	if savedState.TickSeq != 1 {
		t.Fatalf("tick seq = %d, want 1", savedState.TickSeq)
	}
}

// TestTickIntervalUnchanged guards backward-compat: an interval schedule
// with no Kind set keeps the old "+every_seconds*1000" semantics.
func TestTickIntervalUnchanged(t *testing.T) {
	sch := api.ScheduleRecord{
		Tenant:        "t_test01",
		Namespace:     "default",
		Name:          "every_10s",
		EverySeconds:  10,
		OverlapPolicy: "skip",
		Enabled:       true,
		Ref:           api.ScheduleRef{Function: "fn1", Alias: "prod"},
	}
	var savedState api.ScheduleState
	store := &testutil.FakePersistence{
		ListAllSchedulesFn: func(context.Context) ([]api.ScheduleRecord, error) { return []api.ScheduleRecord{sch}, nil },
		GetScheduleStateFn: func(context.Context, string, string, string) (api.ScheduleState, error) {
			return api.ScheduleState{}, nil
		},
		PutScheduleStateFn:    func(_ context.Context, _, _, _ string, st api.ScheduleState) error { savedState = st; return nil },
		GetScheduleInflightFn: func(context.Context, string, string, string) (string, error) { return "", nil },
		ResolveVersionFn:      func(context.Context, string, string, string, string, int64) (int64, error) { return 1, nil },
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{TimeoutMS: 1000, Authz: api.VersionAuthz{InvokeScheduleRoles: []string{"role:worker"}}},
			}, nil, nil
		},
	}
	broker := &testutil.FakeMessaging{}
	cfg := config.Config{}
	cfg.CSScheduler.MaxCatchupTick = 1
	fakeNow := time.Unix(1_700_000_000, 0)
	s := &scheduler{cfg: cfg, store: store, broker: broker, nowFn: func() time.Time { return fakeNow }}

	if err := s.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := fakeNow.UnixMilli() + 10_000
	if savedState.NextTickMS != want {
		t.Fatalf("interval next tick = %d, want %d", savedState.NextTickMS, want)
	}
}

// TestApplyJitterDeterministic checks that two schedules with the same
// triple (tenant, namespace, name) get the same offset and that two
// different schedules get different offsets within the window.
func TestApplyJitterDeterministic(t *testing.T) {
	a := api.ScheduleRecord{Tenant: "t_a", Namespace: "ns", Name: "s1"}
	b := api.ScheduleRecord{Tenant: "t_a", Namespace: "ns", Name: "s2"}
	const base = int64(1_700_000_000_000)
	const window = int64(5000)
	ja := applyJitter(base, window, a)
	if ja < base || ja >= base+window {
		t.Fatalf("jitter out of window: %d", ja-base)
	}
	if got := applyJitter(base, window, a); got != ja {
		t.Fatalf("jitter not deterministic: %d vs %d", got, ja)
	}
	if applyJitter(base, window, b) == ja {
		// fnv collisions are theoretically possible; sample two more
		// schedules to be confident this is a real bug.
		c := api.ScheduleRecord{Tenant: "t_a", Namespace: "ns", Name: "s3"}
		d := api.ScheduleRecord{Tenant: "t_a", Namespace: "ns", Name: "s4"}
		jc := applyJitter(base, window, c)
		jd := applyJitter(base, window, d)
		if jc == ja && jd == ja {
			t.Fatalf("jitter offset is constant across schedules")
		}
	}
}

// TestIsCronBranching covers the kind-resolution semantics surfaced by
// advanceNextTickMS so a stray Kind value never falls through to the
// interval path with a non-empty Cron string.
func TestIsCronBranching(t *testing.T) {
	cases := []struct {
		name string
		sch  api.ScheduleRecord
		want bool
	}{
		{"empty defaults to interval", api.ScheduleRecord{EverySeconds: 30}, false},
		{"explicit cron kind", api.ScheduleRecord{Kind: "cron", Cron: "*/5 * * * *"}, true},
		{"empty kind with cron string", api.ScheduleRecord{Cron: "*/5 * * * *"}, true},
		{"interval kind ignores cron", api.ScheduleRecord{Kind: "interval", EverySeconds: 5, Cron: "*/5 * * * *"}, false},
	}
	for _, c := range cases {
		if got := isCron(c.sch); got != c.want {
			t.Errorf("%s: isCron=%v want %v", c.name, got, c.want)
		}
	}
}

// TestCronNextMSFallsBackOnBadExpression ensures the tick loop cannot be
// wedged by a corrupt cron expression — it logs and retries 60s later.
func TestCronNextMSFallsBackOnBadExpression(t *testing.T) {
	var buf bytes.Buffer
	s := &scheduler{logger: observability.NewLoggerWithWriter("cs-scheduler-test", &buf)}
	sch := api.ScheduleRecord{Kind: "cron", Cron: "nonsense", TZ: "UTC"}
	prev := time.Now().UnixMilli()
	got := s.cronNextMS(sch, prev)
	if got != prev+60_000 {
		t.Fatalf("expected fallback +60s, got delta %d", got-prev)
	}
	if buf.Len() == 0 {
		t.Fatal("expected a warn log for bad cron expression")
	}
}
