package main

import (
	"context"
	"flag"
	"hash/fnv"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/observability"
	_ "github.com/osvaldoandrade/sous/internal/plugins/drivers"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
	cronpkg "github.com/osvaldoandrade/sous/internal/scheduler"
)

type scheduler struct {
	cfg      config.Config
	store    persistence.Provider
	broker   messaging.Provider
	logger   *observability.Logger
	isLeader atomic.Bool
	// nowFn is the wall-clock provider; tests inject a fake clock here.
	// When nil the scheduler uses time.Now.
	nowFn func() time.Time
}

func (s *scheduler) now() time.Time {
	if s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now()
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "Path to config YAML")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		panic(err)
	}
	store, err := registry.NewPersistence(cfg)
	if err != nil {
		panic(err)
	}
	defer store.Close()
	broker, err := registry.NewMessaging(cfg)
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	s := &scheduler{cfg: cfg, store: store, broker: broker, logger: observability.NewLogger("cs-scheduler")}
	s.isLeader.Store(!cfg.CSScheduler.LeaderElection.Enabled)
	if err := s.run(); err != nil {
		panic(err)
	}
}

func (s *scheduler) run() error {
	go s.serveHTTP()
	if s.cfg.CSScheduler.LeaderElection.Enabled {
		go s.leaderLoop(context.Background())
	}
	ticker := time.NewTicker(time.Duration(max(100, s.cfg.CSScheduler.TickMS)) * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if !s.isLeader.Load() {
			continue
		}
		if err := s.tick(context.Background()); err != nil {
			s.logger.Error(context.Background(), "tick failed: "+err.Error())
		}
	}
	return nil
}

func (s *scheduler) serveHTTP() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", observability.MetricsHandler(prometheus.NewRegistry()))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.store.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write([]byte("ready"))
	})
	_ = http.ListenAndServe(":8083", mux)
}

func (s *scheduler) leaderLoop(ctx context.Context) {
	leaseKey := "cs:scheduler:leader:lease"
	leaseVal := uuid.NewString()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := s.store.TryAcquireLease(ctx, leaseKey, leaseVal, 5*time.Second)
			if err != nil {
				s.isLeader.Store(false)
				continue
			}
			if ok {
				s.isLeader.Store(true)
				continue
			}
			curr, err := s.store.GetLeaseValue(ctx, leaseKey)
			if err != nil {
				s.isLeader.Store(false)
				continue
			}
			if curr == leaseVal {
				_ = s.store.ExtendLease(ctx, leaseKey, 5*time.Second)
				s.isLeader.Store(true)
				continue
			}
			s.isLeader.Store(false)
		}
	}
}

func (s *scheduler) tick(ctx context.Context) error {
	schedules, err := s.store.ListAllSchedules(ctx)
	if err != nil {
		return err
	}
	now := s.now().UnixMilli()
	maxCatchup := max(1, s.cfg.CSScheduler.MaxCatchupTick)
	for _, sch := range schedules {
		if !sch.Enabled {
			continue
		}
		state, err := s.store.GetScheduleState(ctx, sch.Tenant, sch.Namespace, sch.Name)
		if err != nil {
			continue
		}
		if state.NextTickMS == 0 {
			state.NextTickMS = now
		}

		published := 0
		for state.NextTickMS <= now && published < maxCatchup {
			if err := s.publishScheduleTick(ctx, sch, state.TickSeq); err != nil {
				s.logger.Warn(ctx, "schedule publish failed: "+err.Error())
			}
			state.TickSeq++
			state.NextTickMS = s.advanceNextTickMS(sch, state.NextTickMS)
			published++
		}
		if state.NextTickMS <= now && published >= maxCatchup {
			// Catchup cap hit; jump forward from `now` so we resume
			// fresh next tick instead of looping again immediately.
			state.NextTickMS = s.advanceNextTickMS(sch, now)
		}
		if err := s.store.PutScheduleState(ctx, sch.Tenant, sch.Namespace, sch.Name, state); err != nil {
			s.logger.Warn(ctx, "schedule state write failed: "+err.Error())
		}
	}
	return nil
}

// advanceNextTickMS computes the schedule's next tick in absolute Unix-ms.
//
// For interval schedules (default; Kind="" or "interval") it adds
// every_seconds*1000 to the prior tick. For cron schedules it parses
// sch.Cron (skipping invalid expressions by falling back to a safe 60s
// retry) and evaluates Schedule.NextAfter in the schedule's timezone.
// Jitter, when configured, adds a deterministic per-tick offset so that
// many schedules sharing the same cron expression do not stampede the
// downstream codeQ.
func (s *scheduler) advanceNextTickMS(sch api.ScheduleRecord, prevMS int64) int64 {
	if isCron(sch) {
		base := s.cronNextMS(sch, prevMS)
		return applyJitter(base, sch.JitterMs, sch)
	}
	if sch.EverySeconds <= 0 {
		// Defensive: avoid a busy loop if a malformed record sneaks in.
		// 60s is the smallest unit the cron path supports and a sane
		// default for unspecified interval schedules.
		return prevMS + 60_000
	}
	return applyJitter(prevMS+int64(sch.EverySeconds)*1000, sch.JitterMs, sch)
}

// isCron reports whether a schedule should be evaluated as cron. We
// accept either Kind="cron" or a non-empty Cron string for ergonomics
// (the cs-control validator already enforces a canonical Kind value).
func isCron(sch api.ScheduleRecord) bool {
	return sch.Kind == "cron" || (sch.Kind == "" && sch.Cron != "")
}

// cronNextMS evaluates the cron schedule's next fire time strictly after
// `prevMS`. On any parse or zoneinfo error the function logs at warn and
// returns a 60s retry so a broken schedule never wedges the loop.
func (s *scheduler) cronNextMS(sch api.ScheduleRecord, prevMS int64) int64 {
	expr, err := cronpkg.Parse(sch.Cron)
	if err != nil {
		s.logger.Warn(context.Background(), "cron parse failed for "+sch.Name+": "+err.Error())
		return prevMS + 60_000
	}
	loc, err := cronpkg.LoadLocation(sch.TZ)
	if err != nil {
		s.logger.Warn(context.Background(), "cron timezone load failed for "+sch.Name+": "+err.Error())
		return prevMS + 60_000
	}
	next := expr.NextAfter(time.UnixMilli(prevMS), loc)
	if next.IsZero() {
		return prevMS + 60_000
	}
	return next.UnixMilli()
}

// applyJitter spreads ticks deterministically. The offset is keyed by
// (tenant, namespace, name) so the same schedule always gets the same
// offset for the same target second; replicas elected as leader at
// different times produce identical schedules.
func applyJitter(baseMS, jitterMS int64, sch api.ScheduleRecord) int64 {
	if jitterMS <= 0 {
		return baseMS
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(sch.Tenant))
	_, _ = h.Write([]byte{'|'})
	_, _ = h.Write([]byte(sch.Namespace))
	_, _ = h.Write([]byte{'|'})
	_, _ = h.Write([]byte(sch.Name))
	offset := int64(h.Sum64() % uint64(jitterMS))
	return baseMS + offset
}

func (s *scheduler) publishScheduleTick(ctx context.Context, sch api.ScheduleRecord, tickSeq int64) error {
	if sch.OverlapPolicy == "skip" {
		inflight, err := s.store.GetScheduleInflight(ctx, sch.Tenant, sch.Namespace, sch.Name)
		if err != nil {
			return err
		}
		if inflight != "" {
			return nil
		}
	}
	version, err := s.store.ResolveVersion(ctx, sch.Tenant, sch.Namespace, sch.Ref.Function, sch.Ref.Alias, sch.Ref.Version)
	if err != nil {
		return err
	}
	meta, _, err := s.store.GetVersion(ctx, sch.Tenant, sch.Namespace, sch.Ref.Function, version)
	if err != nil {
		return err
	}
	principal := api.Principal{Sub: "sp:cs-scheduler", Roles: []string{"role:worker", "sp:cs-scheduler"}}
	if !api.IntersectsRoles(meta.Config.Authz.InvokeScheduleRoles, principal) {
		return nil
	}
	activationID := uuid.NewString()
	if sch.OverlapPolicy == "skip" {
		_ = s.store.SetScheduleInflight(ctx, sch.Tenant, sch.Namespace, sch.Name, activationID, time.Duration(meta.Config.TimeoutMS+5000)*time.Millisecond)
	}

	inv := api.InvocationRequest{
		ActivationID: activationID,
		RequestID:    "req_" + uuid.NewString(),
		Tenant:       sch.Tenant,
		Namespace:    sch.Namespace,
		Ref:          api.FunctionRef{Function: sch.Ref.Function, Alias: sch.Ref.Alias, Version: version},
		Trigger: api.Trigger{Type: "schedule", Source: map[string]any{
			"schedule_name": sch.Name,
			"tick_seq":      tickSeq,
		}},
		Principal:  principal,
		DeadlineMS: s.now().Add(time.Duration(meta.Config.TimeoutMS) * time.Millisecond).UnixMilli(),
		Event:      sch.Payload,
	}
	return s.broker.PublishInvocation(ctx, inv)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
