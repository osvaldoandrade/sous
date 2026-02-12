package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/observability"
	_ "github.com/osvaldoandrade/sous/internal/plugins/drivers"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
	"github.com/osvaldoandrade/sous/internal/runtime"
)

type invoker struct {
	cfg      config.Config
	store    persistence.Provider
	broker   messaging.Provider
	runner   *runtime.Runner
	logger   *observability.Logger
	inflight chan struct{}

	mu          sync.Mutex
	versionSems map[string]chan struct{}
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

	inv := &invoker{
		cfg:         cfg,
		store:       store,
		broker:      broker,
		logger:      observability.NewLogger("cs-invoker-pool"),
		inflight:    make(chan struct{}, max(1, cfg.CSInvokerPool.Workers.MaxInflight)),
		versionSems: make(map[string]chan struct{}),
	}
	inv.runner = runtime.NewRunner(runtimeKV{store: store}, runtimePublisher{broker: broker}, cfg.CSInvokerPool.Limits.MaxResultBytes, cfg.CSInvokerPool.Limits.MaxErrorBytes, cfg.CSInvokerPool.Limits.MaxLogBytes)

	if err := inv.run(); err != nil {
		panic(err)
	}
}

func (i *invoker) run() error {
	go i.serveHTTP()

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = uuid.NewString()
	}
	groupID := "cs-invoker-pool-" + hostname

	threads := max(1, i.cfg.CSInvokerPool.Workers.Threads)
	var wg sync.WaitGroup
	ctx := context.Background()
	for n := 0; n < threads; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := i.broker.ConsumeInvocations(ctx, groupID, func(env messaging.Envelope, req api.InvocationRequest) error {
					return i.handleInvocation(ctx, env, req)
				})
				if err != nil {
					i.logger.Error(context.Background(), "consumer failed: "+err.Error())
					time.Sleep(500 * time.Millisecond)
					continue
				}
				// Keep workers alive on unexpected consumer returns.
				i.logger.Error(context.Background(), "consumer stopped without error; restarting")
				time.Sleep(500 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	return nil
}

func (i *invoker) serveHTTP() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", observability.MetricsHandler(prometheus.NewRegistry()))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := i.store.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write([]byte("ready"))
	})
	_ = http.ListenAndServe(i.cfg.CSInvokerPool.HTTP.Addr, mux)
}

func (i *invoker) handleInvocation(ctx context.Context, env messaging.Envelope, req api.InvocationRequest) error {
	_ = env
	if req.ActivationID == "" || req.RequestID == "" || req.Tenant == "" || req.Namespace == "" || req.Ref.Function == "" {
		_ = i.broker.PublishDLQInvoke(ctx, req.Tenant, req)
		return nil
	}
	select {
	case i.inflight <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-i.inflight }()

	isTerminal, existing, err := i.store.IsActivationTerminal(ctx, req.Tenant, req.ActivationID)
	if err != nil {
		return err
	}
	if isTerminal {
		stored := api.InvocationResult{
			ActivationID: existing.ActivationID,
			RequestID:    req.RequestID,
			Status:       existing.Status,
			DurationMS:   existing.DurationMS,
			Result:       existing.Result,
			Error:        existing.Error,
		}
		if err := i.store.SaveResultByRequestID(ctx, req.Tenant, req.RequestID, stored, 10*time.Minute); err != nil {
			if i.logger != nil {
				i.logger.Warn(ctx, "failed to save request correlation result: "+err.Error())
			}
		}
		_ = i.broker.PublishResult(ctx, req.Tenant, stored)
		return nil
	}

	version, err := i.store.ResolveVersion(ctx, req.Tenant, req.Namespace, req.Ref.Function, req.Ref.Alias, req.Ref.Version)
	if err != nil {
		return err
	}
	req.Ref.Version = version

	versionMeta, bundleBytes, err := i.store.GetVersion(ctx, req.Tenant, req.Namespace, req.Ref.Function, version)
	if err != nil {
		return err
	}
	if !bundle.VerifySHA256(bundleBytes, versionMeta.SHA256) {
		return cserrors.New(cserrors.CSValidationFailed, "bundle sha mismatch")
	}

	sem := i.versionSemaphore(req, max(1, versionMeta.Config.MaxConcurrency))
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-sem }()

	start := time.Now().UnixMilli()
	activation := api.ActivationRecord{
		ActivationID:    req.ActivationID,
		Tenant:          req.Tenant,
		Namespace:       req.Namespace,
		Function:        req.Ref.Function,
		Ref:             req.Ref,
		Trigger:         req.Trigger,
		Status:          "running",
		StartMS:         start,
		RequestID:       req.RequestID,
		ResolvedVersion: version,
	}
	actTTL := time.Duration(i.cfg.CSControl.Limits.ActTTLSeconds) * time.Second
	if err := i.store.PutActivationRunning(ctx, activation, actTTL); err != nil {
		return err
	}

	out := i.runner.Execute(ctx, bundleBytes, req)
	terminal := activation
	terminal.Status = out.Status
	terminal.EndMS = time.Now().UnixMilli()
	terminal.DurationMS = out.DurationMS
	terminal.Error = out.Error
	terminal.Result = out.Result
	terminal.ResultTruncated = out.Truncated

	updated, err := i.store.CompleteActivationCAS(ctx, terminal, actTTL)
	if err != nil {
		return err
	}
	if !updated {
		return cserrors.New(cserrors.CSKVCASFailed, "activation state changed concurrently")
	}

	for idx, line := range out.Logs {
		if err := i.store.AppendLogChunk(ctx, req.Tenant, req.ActivationID, int64(idx), []byte(line), actTTL); err != nil {
			i.logger.Warn(ctx, "failed to append log chunk: "+err.Error())
		}
	}

	result := api.InvocationResult{
		ActivationID: req.ActivationID,
		RequestID:    req.RequestID,
		Status:       out.Status,
		DurationMS:   out.DurationMS,
		Result:       out.Result,
		Error:        out.Error,
	}
	if err := i.store.SaveResultByRequestID(ctx, req.Tenant, req.RequestID, result, 10*time.Minute); err != nil {
		if i.logger != nil {
			i.logger.Warn(ctx, "failed to save request correlation result: "+err.Error())
		}
	}
	if err := i.broker.PublishResult(ctx, req.Tenant, result); err != nil {
		_ = i.broker.PublishDLQResult(ctx, req.Tenant, result)
		return err
	}

	if req.Trigger.Type == "schedule" {
		if sched, ok := req.Trigger.Source["schedule_name"].(string); ok && sched != "" {
			_ = i.store.ClearScheduleInflight(ctx, req.Tenant, req.Namespace, sched)
		}
	}
	return nil
}

func (i *invoker) versionSemaphore(req api.InvocationRequest, maxConcurrency int) chan struct{} {
	key := fmt.Sprintf("%s:%s:%s:%d", req.Tenant, req.Namespace, req.Ref.Function, req.Ref.Version)
	i.mu.Lock()
	defer i.mu.Unlock()
	if sem, ok := i.versionSems[key]; ok {
		return sem
	}
	sem := make(chan struct{}, max(1, maxConcurrency))
	i.versionSems[key] = sem
	return sem
}

type runtimeKV struct {
	store persistence.Provider
}

func (k runtimeKV) Get(ctx context.Context, key string) (string, error) {
	return k.store.KVGet(ctx, key)
}

func (k runtimeKV) Set(ctx context.Context, key string, value string, ttlSeconds int) error {
	ttl := time.Duration(ttlSeconds) * time.Second
	if ttlSeconds <= 0 {
		ttl = 0
	}
	return k.store.KVSet(ctx, key, value, ttl)
}

func (k runtimeKV) Del(ctx context.Context, key string) error {
	return k.store.KVDel(ctx, key)
}

type runtimePublisher struct {
	broker messaging.Provider
}

func (p runtimePublisher) Publish(ctx context.Context, topic string, payload any) error {
	tenant := "runtime"
	if m, ok := payload.(map[string]any); ok {
		if v, ok := m["tenant"].(string); ok && v != "" {
			tenant = v
		}
	}
	return p.broker.Publish(ctx, topic, tenant, "RuntimePublish", payload)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func atoi(v string, fallback int) int {
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return parsed
}
