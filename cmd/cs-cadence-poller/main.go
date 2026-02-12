package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/cadence"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/observability"
	_ "github.com/osvaldoandrade/sous/internal/plugins/drivers"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
)

type poller struct {
	cfg    config.Config
	store  persistence.Provider
	broker messaging.Provider
	client cadence.Client
	logger *observability.Logger

	mu          sync.Mutex
	running     map[string]context.CancelFunc
	bindingSems map[string]chan struct{}
	activation  map[string]taskInfo

	heartbeatLast map[string]time.Time
}

type taskInfo struct {
	Tenant     string
	Namespace  string
	TaskToken  string
	TaskHash   string
	BindingKey string
	Semaphore  chan struct{}
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

	p := &poller{
		cfg:           cfg,
		store:         store,
		broker:        broker,
		client:        cadence.NewHTTPClient(cfg.CSCadencePoller.Cadence.Addr),
		logger:        observability.NewLogger("cs-cadence-poller"),
		running:       make(map[string]context.CancelFunc),
		bindingSems:   make(map[string]chan struct{}),
		activation:    make(map[string]taskInfo),
		heartbeatLast: make(map[string]time.Time),
	}
	if err := p.run(); err != nil {
		panic(err)
	}
}

func (p *poller) run() error {
	go p.serveHTTP()
	ctx := context.Background()
	go p.refreshLoop(ctx)
	go p.consumeResults(ctx)
	go p.consumeHeartbeats(ctx)
	select {}
}

func (p *poller) serveHTTP() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", observability.MetricsHandler(prometheus.NewRegistry()))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := p.store.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write([]byte("ready"))
	})
	_ = http.ListenAndServe(":8084", mux)
}

func (p *poller) refreshLoop(ctx context.Context) {
	interval := time.Duration(max(1, p.cfg.CSCadencePoller.RefreshSeconds)) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := p.refreshBindings(ctx); err != nil {
			p.logger.Warn(ctx, "binding refresh failed: "+err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *poller) refreshBindings(ctx context.Context) error {
	bindings, err := p.store.ListAllWorkerBindings(ctx)
	if err != nil {
		return err
	}
	desired := make(map[string]api.WorkerBinding)
	for _, b := range bindings {
		if !b.Enabled {
			continue
		}
		key := bindingKey(b)
		desired[key] = b
	}

	p.mu.Lock()
	for key, cancel := range p.running {
		if _, ok := desired[key]; !ok {
			cancel()
			delete(p.running, key)
			delete(p.bindingSems, key)
		}
	}
	for key, binding := range desired {
		if _, ok := p.running[key]; ok {
			continue
		}
		pollCtx, cancel := context.WithCancel(ctx)
		p.running[key] = cancel
		sem := make(chan struct{}, max(1, binding.Limits.MaxInflightTasks))
		p.bindingSems[key] = sem
		for n := 0; n < max(1, binding.Pollers.Activity); n++ {
			go p.pollLoop(pollCtx, binding, sem)
		}
	}
	p.mu.Unlock()
	return nil
}

func (p *poller) pollLoop(ctx context.Context, binding api.WorkerBinding, sem chan struct{}) {
	key := bindingKey(binding)
	principal := api.Principal{Sub: "sp:cs-cadence-poller", Roles: []string{"role:cadence", "sp:cs-cadence-poller"}}
	for {
		select {
		case <-ctx.Done():
			return
		case sem <- struct{}{}:
		default:
			time.Sleep(100 * time.Millisecond)
			continue
		}

		task, err := p.client.PollActivityTask(ctx, binding.Domain, binding.Tasklist, binding.WorkerID)
		if err != nil {
			<-sem
			p.logger.Warn(ctx, "cadence poll failed: "+err.Error())
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if task == nil || task.TaskToken == "" {
			<-sem
			continue
		}
		ref, ok := binding.ActivityMap[task.ActivityType]
		if !ok {
			_ = p.client.RespondActivityFailed(ctx, task.TaskToken, "mapping_not_found", []byte("activity not mapped"))
			<-sem
			continue
		}
		version, err := p.store.ResolveVersion(ctx, binding.Tenant, binding.Namespace, ref.Function, ref.Alias, ref.Version)
		if err != nil {
			_ = p.client.RespondActivityFailed(ctx, task.TaskToken, "resolve_version_failed", []byte(err.Error()))
			<-sem
			continue
		}
		meta, _, err := p.store.GetVersion(ctx, binding.Tenant, binding.Namespace, ref.Function, version)
		if err != nil {
			_ = p.client.RespondActivityFailed(ctx, task.TaskToken, "version_not_found", []byte(err.Error()))
			<-sem
			continue
		}
		if !api.IntersectsRoles(meta.Config.Authz.InvokeCadenceRoles, principal) {
			_ = p.client.RespondActivityFailed(ctx, task.TaskToken, "role_missing", []byte("invoke_cadence_roles missing"))
			<-sem
			continue
		}

		activationID := uuid.NewString()
		taskHash := hashToken(task.TaskToken)
		invocation := api.InvocationRequest{
			ActivationID: activationID,
			RequestID:    "req_" + uuid.NewString(),
			Tenant:       binding.Tenant,
			Namespace:    binding.Namespace,
			Ref:          api.FunctionRef{Function: ref.Function, Alias: ref.Alias, Version: version},
			Trigger: api.Trigger{Type: "cadence", Source: map[string]any{
				"domain":       task.Domain,
				"tasklist":     task.Tasklist,
				"workflowId":   task.WorkflowID,
				"runId":        task.RunID,
				"activityId":   task.ActivityID,
				"activityType": task.ActivityType,
				"attempt":      task.Attempt,
			}},
			Principal:  principal,
			DeadlineMS: time.Now().Add(time.Duration(meta.Config.TimeoutMS) * time.Millisecond).UnixMilli(),
			Event: map[string]any{
				"type": "cadence.activity",
				"cadence": map[string]any{
					"domain":       task.Domain,
					"tasklist":     task.Tasklist,
					"workflowId":   task.WorkflowID,
					"runId":        task.RunID,
					"activityId":   task.ActivityID,
					"activityType": task.ActivityType,
					"attempt":      task.Attempt,
				},
				"input": map[string]any{"raw_base64": task.InputBase64},
			},
		}
		if err := p.broker.PublishInvocation(ctx, invocation); err != nil {
			_ = p.client.RespondActivityFailed(ctx, task.TaskToken, "publish_failed", []byte(err.Error()))
			<-sem
			continue
		}

		_ = p.store.PutCadenceTaskMapping(ctx, binding.Tenant, binding.Namespace, taskHash, activationID, 24*time.Hour)
		p.mu.Lock()
		p.activation[activationID] = taskInfo{
			Tenant:     binding.Tenant,
			Namespace:  binding.Namespace,
			TaskToken:  task.TaskToken,
			TaskHash:   taskHash,
			BindingKey: key,
			Semaphore:  sem,
		}
		p.mu.Unlock()
	}
}

func (p *poller) consumeResults(ctx context.Context) {
	groupID := "cs-cadence-poller-results"
	_ = p.broker.ConsumeResults(ctx, groupID, func(env messaging.Envelope, res api.InvocationResult) error {
		_ = env
		p.mu.Lock()
		info, ok := p.activation[res.ActivationID]
		if ok {
			delete(p.activation, res.ActivationID)
		}
		p.mu.Unlock()
		if !ok {
			return nil
		}
		defer func() {
			select {
			case <-info.Semaphore:
			default:
			}
		}()

		if res.Status == "success" {
			payload, _ := json.Marshal(res.Result)
			encoded := []byte(base64.StdEncoding.EncodeToString(payload))
			if err := p.client.RespondActivityCompleted(ctx, info.TaskToken, encoded); err != nil {
				return cserrors.Wrap(cserrors.CSCadenceRespFailed, "failed to respond completed", err)
			}
		} else {
			reason := "error"
			if res.Status == "timeout" {
				reason = "timeout"
			}
			details, _ := json.Marshal(res.Error)
			encoded := []byte(base64.StdEncoding.EncodeToString(details))
			if err := p.client.RespondActivityFailed(ctx, info.TaskToken, reason, encoded); err != nil {
				return cserrors.Wrap(cserrors.CSCadenceRespFailed, "failed to respond failed", err)
			}
		}
		_ = p.store.DeleteCadenceTaskMapping(ctx, info.Tenant, info.Namespace, info.TaskHash)
		return nil
	})
}

func (p *poller) consumeHeartbeats(ctx context.Context) {
	groupID := "cs-cadence-poller-heartbeats"
	_ = p.broker.ConsumeTopic(ctx, "cs.cadence.heartbeat", groupID, func(env messaging.Envelope) error {
		var body struct {
			ActivationID  string `json:"activation_id"`
			DetailsBase64 string `json:"details_base64"`
		}
		if err := json.Unmarshal(env.Body, &body); err != nil {
			return nil
		}
		p.mu.Lock()
		info, ok := p.activation[body.ActivationID]
		if !ok {
			p.mu.Unlock()
			return nil
		}
		last := p.heartbeatLast[body.ActivationID]
		if !last.IsZero() {
			minGap := time.Second / time.Duration(max(1, p.cfg.CSCadencePoller.Heartbeat.MaxPerSecond))
			if time.Since(last) < minGap {
				p.mu.Unlock()
				return nil
			}
		}
		p.heartbeatLast[body.ActivationID] = time.Now()
		p.mu.Unlock()

		details, _ := base64.StdEncoding.DecodeString(body.DetailsBase64)
		if err := p.client.RecordActivityHeartbeat(ctx, info.TaskToken, details); err != nil {
			return cserrors.Wrap(cserrors.CSCadenceHBFailed, "failed to record cadence heartbeat", err)
		}
		return nil
	})
}

func bindingKey(b api.WorkerBinding) string {
	return b.Tenant + ":" + b.Namespace + ":" + b.Name
}

func hashToken(token string) string {
	s := sha256.Sum256([]byte(token))
	return hex.EncodeToString(s[:])
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

type atomicInt64 struct {
	v int64
}

func (a *atomicInt64) Add(delta int64) int64 {
	return atomic.AddInt64(&a.v, delta)
}

func (a *atomicInt64) Load() int64 {
	return atomic.LoadInt64(&a.v)
}
