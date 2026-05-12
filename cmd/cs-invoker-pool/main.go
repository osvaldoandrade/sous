package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/limits"
	"github.com/osvaldoandrade/sous/internal/observability"
	_ "github.com/osvaldoandrade/sous/internal/plugins/drivers"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
	"github.com/osvaldoandrade/sous/internal/runtime"
)

type invoker struct {
	cfg            config.Config
	store          persistence.Provider
	broker         messaging.Provider
	runner         *runtime.Runner
	logger         *observability.Logger
	inflight       chan struct{}
	tenantInflight *tenantInflight
	samplers       *samplerCache
	clock          func() time.Time

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
		cfg:            cfg,
		store:          store,
		broker:         broker,
		logger:         observability.NewLogger("cs-invoker-pool"),
		inflight:       make(chan struct{}, max(1, cfg.CSInvokerPool.Workers.MaxInflight)),
		tenantInflight: newTenantInflight(tenantInflightCapacity()),
		samplers:       newSamplerCache(),
		clock:          time.Now,
		versionSems:    make(map[string]chan struct{}),
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

	// Per-tenant inflight quota. HTTP-triggered (sync) invokes must fail
	// fast with CS_TENANT_INFLIGHT_LIMIT so the gateway can surface 429;
	// queued (async) invokes wait until a slot is available. See
	// docs/26-capacity-and-limits.md for the contract.
	if i.tenantInflight != nil {
		release, err := i.acquireTenantInflight(ctx, req)
		if err != nil {
			return err
		}
		defer release()
	}

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

	// Stamp parent/root activation IDs for agent decision-tree tracing (E7.01).
	// When the trigger carries parent_activation_id (set by the gateway after
	// reading X-CS-Parent-Activation), inherit the parent's root if known.
	// Otherwise this is a root activation whose RootActivationID equals its
	// own ID. See docs/14-observability.md.
	resolveTraceParent(ctx, i.store, &activation)

	// E7.02: ask the sampler whether to persist this activation at all,
	// keep only a skeleton row, or keep everything in full. Skipped
	// activations bypass PutActivationRunning entirely; tail-sampled
	// activations write the standard running row but withhold logs +
	// result blob unless OnComplete promotes them. See
	// docs/14-observability.md for the contract.
	decider := i.sampler(req.Trigger)
	samplingMeta := samplingMetaFromRequest(req)
	initialDecision := decider.OnStart(samplingMeta)
	activation.SamplingDecision = initialDecision.Reason
	if initialDecision.PersistFull || initialDecision.Skeleton {
		if err := i.store.PutActivationRunning(ctx, activation, actTTL); err != nil {
			return err
		}
	}
	if activation.ParentActivationID != "" {
		if err := i.store.AppendActivationChild(ctx, activation.Tenant, activation.ParentActivationID, activation.ActivationID, actTTL); err != nil && i.logger != nil {
			// Best-effort: the tree index is observability-only, so log and
			// continue rather than failing the invocation.
			i.logger.Warn(ctx, "failed to append activation child: "+err.Error())
		}
	}

	// Tag the runtime context with the current activation ID so the cs-js
	// egress shim (cs.http.fetch) can inject X-CS-Parent-Activation on
	// outbound HTTP calls without each handler threading the value through.
	// See internal/runtime/runner.go and docs/14-observability.md.
	execCtx := observability.WithParent(ctx, activation.ActivationID)
	out := i.runner.Execute(execCtx, bundleBytes, req)
	// Surface size-limit enforcement at the invoker boundary. The runtime
	// has already truncated to limits.MaxResultBytes / MaxLogBytes; here we
	// stamp the documented sentinel header on the response so synchronous
	// callers (via cs-http-gateway) can detect truncation, and emit a
	// warning log line tagged with CS_RESULT_TOO_LARGE for observability.
	// See docs/26-capacity-and-limits.md.
	if out.Truncated {
		out.Result = annotateTruncatedResult(out.Result)
		if i.logger != nil {
			i.logger.Warn(ctx, "activation truncated tenant="+req.Tenant+
				" activation_id="+req.ActivationID+
				" code="+string(cserrors.CSResultTooLarge))
		}
	}
	// E7.02: ask the sampler what to do now that the runtime has returned.
	// Tail samplers may promote a skeleton to a full record (record the
	// result blob + logs) when the outcome matches an error/slow rule.
	finalDecision := decider.OnComplete(samplingMeta, observability.SamplingOutcome{
		Status:     out.Status,
		DurationMS: out.DurationMS,
	}, initialDecision)

	terminal := activation
	terminal.Status = out.Status
	terminal.EndMS = time.Now().UnixMilli()
	terminal.DurationMS = out.DurationMS
	terminal.SamplingDecision = finalDecision.Reason
	if finalDecision.PersistFull {
		terminal.Error = out.Error
		terminal.Result = out.Result
		terminal.ResultTruncated = out.Truncated
	}

	// Skipped activations did not write a running row, so there is no CAS
	// row to update. Skeleton-only completions still CAS so the row
	// transitions from running to its terminal status (with status +
	// duration only — no result blob).
	if initialDecision.PersistFull || initialDecision.Skeleton {
		updated, err := i.store.CompleteActivationCAS(ctx, terminal, actTTL)
		if err != nil {
			return err
		}
		if !updated {
			return cserrors.New(cserrors.CSKVCASFailed, "activation state changed concurrently")
		}
	}

	if finalDecision.PersistLogs {
		for idx, line := range out.Logs {
			if err := i.store.AppendLogChunk(ctx, req.Tenant, req.ActivationID, int64(idx), []byte(line), actTTL); err != nil {
				i.logger.Warn(ctx, "failed to append log chunk: "+err.Error())
			}
		}
		// Append a CS_LOG_LIMIT_EXCEEDED sentinel chunk so log readers
		// (CLI, dashboards) see a stable marker when the log buffer was
		// capped at limits.MaxLogBytes. The runtime truncates the buffer;
		// this writes the documented sentinel exactly once at the end.
		if out.Truncated && len(out.Logs) > 0 {
			sentinel := []byte("[warn] " + string(cserrors.CSLogLimitExceeded))
			_ = i.store.AppendLogChunk(ctx, req.Tenant, req.ActivationID, int64(len(out.Logs)), sentinel, actTTL)
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

// annotateTruncatedResult returns a copy of the function response with the
// documented X-CS-Truncated sentinel header set so synchronous HTTP callers
// can detect that the runtime capped the payload at the configured limits.
// When result is nil the function is a no-op.
func annotateTruncatedResult(result *api.FunctionResponse) *api.FunctionResponse {
	if result == nil {
		return nil
	}
	out := *result
	if out.Headers == nil {
		out.Headers = make(map[string]string, 1)
	} else {
		copyHeaders := make(map[string]string, len(out.Headers)+1)
		for k, v := range out.Headers {
			copyHeaders[k] = v
		}
		out.Headers = copyHeaders
	}
	out.Headers["X-CS-Truncated"] = "response"
	return &out
}

// tenantInflightCapacity returns the per-tenant inflight cap. It is
// independent of the global worker pool capacity: a single tenant must not
// be able to exhaust the pool for everyone else. The CS_INVOKER_TENANT_INFLIGHT
// env var allows an operator override without a config-schema change; future
// PRs may surface a dedicated YAML key. See docs/26-capacity-and-limits.md.
func tenantInflightCapacity() int {
	if v := os.Getenv("CS_INVOKER_TENANT_INFLIGHT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return limits.DefaultTenantMaxInflight
}

// acquireTenantInflight reserves a per-tenant inflight slot using the right
// mode for the trigger. Sync triggers (http) fail fast with
// CS_TENANT_INFLIGHT_LIMIT; async/queued triggers wait until capacity is
// available. The caller is responsible for invoking the returned release on
// completion.
func (i *invoker) acquireTenantInflight(ctx context.Context, req api.InvocationRequest) (func(), error) {
	if isSyncTrigger(req.Trigger.Type) {
		release, err := i.tenantInflight.acquireSync(req.Tenant)
		if err != nil {
			if i.logger != nil {
				i.logger.Warn(ctx, "tenant inflight rejection tenant="+req.Tenant+" trigger="+req.Trigger.Type)
			}
			return nil, err
		}
		return release, nil
	}
	return i.tenantInflight.acquireAsync(ctx, req.Tenant)
}

// isSyncTrigger returns true when the trigger has a caller waiting on a
// response (HTTP). Schedule and cadence triggers are queued, so saturation is
// signalled by waiting rather than a 429.
func isSyncTrigger(triggerType string) bool {
	return triggerType == "http"
}

// sampler returns the Decider for the supplied trigger. It lazily
// constructs samplers per normalized policy so stateful samplers (head)
// keep their per-minute counters across invocations.
func (i *invoker) sampler(trigger api.Trigger) observability.Decider {
	if i.samplers == nil {
		i.samplers = newSamplerCache()
	}
	clock := i.clock
	if clock == nil {
		clock = time.Now
	}
	return i.samplers.deciderFor(trigger, clock)
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

// resolveTraceParent stamps ParentActivationID / RootActivationID on the
// activation record using the trigger's parent_activation_id hint (set by
// the gateway after reading X-CS-Parent-Activation). When no parent is set
// the activation is the root of its decision tree and inherits its own ID
// as RootActivationID. When the parent activation has already terminated we
// reuse its RootActivationID so siblings share a stable root pointer.
// Lookup failures are non-fatal: tracing is best-effort and must never
// block the invocation hot path.
func resolveTraceParent(ctx context.Context, store persistence.Provider, rec *api.ActivationRecord) {
	if rec == nil {
		return
	}
	parent := triggerParentActivationID(rec.Trigger)
	if parent == "" {
		// Root activation: every node in the call tree references this ID.
		rec.RootActivationID = rec.ActivationID
		return
	}
	rec.ParentActivationID = parent
	// Best-effort: ignore lookup errors. We default to "parent is its own
	// root" so the child still has a defined RootActivationID even if the
	// parent record has already expired.
	if parentRec, err := store.GetActivation(ctx, rec.Tenant, parent); err == nil {
		if parentRec.RootActivationID != "" {
			rec.RootActivationID = parentRec.RootActivationID
			return
		}
		rec.RootActivationID = parentRec.ActivationID
		return
	}
	rec.RootActivationID = parent
}

// triggerParentActivationID extracts the parent_activation_id hint that the
// gateway places in Trigger.Source when an inbound HTTP carries the
// X-CS-Parent-Activation header. Returns "" when no parent was advertised.
func triggerParentActivationID(t api.Trigger) string {
	if t.Source == nil {
		return ""
	}
	v, _ := t.Source["parent_activation_id"].(string)
	return strings.TrimSpace(v)
}
