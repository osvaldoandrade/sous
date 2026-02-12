package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/authz"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/observability"
	_ "github.com/osvaldoandrade/sous/internal/plugins/drivers"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
)

type server struct {
	cfg    config.Config
	store  persistence.Provider
	broker messaging.Provider
	logger *observability.Logger
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

	s := &server{cfg: cfg, store: store, broker: broker, logger: observability.NewLogger("cs-control")}
	if err := s.serve(); err != nil {
		panic(err)
	}
}

func (s *server) serve() error {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(observability.RequestIDMiddleware)

	authnProvider, err := registry.NewAuthN(s.cfg)
	if err != nil {
		return err
	}

	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)

	r.Handle("/metrics", observability.MetricsHandler(prometheus.NewRegistry()))

	r.Group(func(pr chi.Router) {
		pr.Use(authz.AuthnMiddleware(authnProvider))
		pr.Post("/v1/tenants/{tenant}/namespaces/{namespace}/functions", s.createFunction)
		pr.Get("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}", s.readFunction)
		pr.Delete("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}", s.deleteFunction)
		pr.Put("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/draft", s.uploadDraft)
		pr.Post("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions", s.publishVersion)
		pr.Put("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases/{alias}", s.setAlias)
		pr.Get("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases", s.listAliases)
		pr.Post("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}:invoke", s.invokeAPI)
		pr.Get("/v1/tenants/{tenant}/activations/{activation_id}", s.getActivation)
		pr.Get("/v1/tenants/{tenant}/activations/{activation_id}/logs", s.getActivationLogs)
		pr.Post("/v1/tenants/{tenant}/namespaces/{namespace}/schedules", s.createSchedule)
		pr.Delete("/v1/tenants/{tenant}/namespaces/{namespace}/schedules/{name}", s.deleteSchedule)
		pr.Post("/v1/tenants/{tenant}/namespaces/{namespace}/cadence/workers", s.createWorkerBinding)
		pr.Delete("/v1/tenants/{tenant}/namespaces/{namespace}/cadence/workers/{name}", s.deleteWorkerBinding)
	})

	httpServer := &http.Server{
		Addr:         s.cfg.CSControl.HTTP.Addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	s.logger.Info(context.Background(), "cs-control starting on "+s.cfg.CSControl.HTTP.Addr)
	return httpServer.ListenAndServe()
}

func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	api.WriteJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		cserrors.WriteHTTP(w, err, observability.RequestIDFromContext(r.Context()))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func (s *server) authorize(w http.ResponseWriter, r *http.Request, action string) (authz.Principal, string, string, string, bool) {
	principal, ok := authz.PrincipalFromContext(r.Context())
	if !ok {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthnInvalidToken, "principal missing"), requestID(r))
		return authz.Principal{}, "", "", "", false
	}
	tenant := chi.URLParam(r, "tenant")
	namespace := chi.URLParam(r, "namespace")
	name := chi.URLParam(r, "name")
	if principal.Tenant != "" && principal.Tenant != tenant {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthzResourceMis, "tenant mismatch"), requestID(r))
		return authz.Principal{}, "", "", "", false
	}
	if !authz.CheckAction(principal, action) {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthzDenied, "action denied"), requestID(r))
		return authz.Principal{}, "", "", "", false
	}
	return principal, tenant, namespace, name, true
}

func requestID(r *http.Request) string {
	return observability.RequestIDFromContext(r.Context())
}

func (s *server) createFunction(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, _, ok := s.authorize(w, r, "cs:function:create")
	if !ok {
		return
	}
	var req api.CreateFunctionRequest
	if err := api.ReadJSON(r, &req); err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "invalid request body", err), requestID(r))
		return
	}
	if err := api.ValidateTenant(tenant); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationName, err.Error()), requestID(r))
		return
	}
	if err := api.ValidateNamespace(namespace); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationName, err.Error()), requestID(r))
		return
	}
	if err := api.ValidateFunction(req.Name); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationName, err.Error()), requestID(r))
		return
	}
	rec := api.FunctionRecord{
		Tenant:      tenant,
		Namespace:   namespace,
		Name:        req.Name,
		Runtime:     req.Runtime,
		Entry:       req.Entry,
		Handler:     req.Handler,
		CreatedAtMS: time.Now().UnixMilli(),
	}
	if rec.Runtime == "" {
		rec.Runtime = "cs-js"
	}
	if rec.Entry == "" {
		rec.Entry = "function.js"
	}
	if rec.Handler == "" {
		rec.Handler = "default"
	}
	if err := s.store.CreateFunction(r.Context(), rec); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusCreated, rec)
}

func (s *server) readFunction(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, name, ok := s.authorize(w, r, "cs:function:read")
	if !ok {
		return
	}
	rec, err := s.store.GetFunction(r.Context(), tenant, namespace, name)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if rec.DeletedAtMS != nil {
		if r.URL.Query().Get("include_deleted") != "true" {
			cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "function not found"), requestID(r))
			return
		}
	}
	aliases, err := s.store.ListAliases(r.Context(), tenant, namespace, name)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	var latestVersion *api.VersionRecord
	latest, err := s.store.GetLatestVersion(r.Context(), tenant, namespace, name)
	if err != nil {
		var csErr *cserrors.CSError
		if !errors.As(err, &csErr) || csErr.Code != cserrors.CSValidationFailed {
			cserrors.WriteHTTP(w, err, requestID(r))
			return
		}
	} else {
		latestVersion = &latest
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"function": rec, "aliases": aliases, "latest_version": latestVersion})
}

func (s *server) deleteFunction(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, name, ok := s.authorize(w, r, "cs:function:delete")
	if !ok {
		return
	}
	if err := s.store.SoftDeleteFunction(r.Context(), tenant, namespace, name, time.Now().UnixMilli()); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
}

func (s *server) uploadDraft(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, name, ok := s.authorize(w, r, "cs:function:draft:upload")
	if !ok {
		return
	}
	var req api.UploadDraftRequest
	if err := api.ReadJSON(r, &req); err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "invalid request body", err), requestID(r))
		return
	}
	if len(req.Files) == 0 {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "files are required"), requestID(r))
		return
	}

	decoded := make(map[string][]byte, len(req.Files))
	for name, b64 := range req.Files {
		b, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "files must be base64"), requestID(r))
			return
		}
		decoded[name] = b
	}
	bundleBytes, sha, size, err := bundle.BuildCanonical(decoded)
	if err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, err.Error()), requestID(r))
		return
	}
	if size > s.cfg.CSControl.Limits.MaxBundleBytes {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationBundle, "bundle too large"), requestID(r))
		return
	}
	draftID := "drf_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	now := time.Now().UnixMilli()
	expires := time.Now().Add(time.Duration(s.cfg.CSControl.Limits.DraftTTLSeconds) * time.Second).UnixMilli()

	rec := api.DraftRecord{
		DraftID:     draftID,
		SHA256:      sha,
		Files:       req.Files,
		CreatedAtMS: now,
		ExpiresAtMS: expires,
	}
	if err := s.store.PutDraft(r.Context(), tenant, namespace, name, rec, time.Duration(s.cfg.CSControl.Limits.DraftTTLSeconds)*time.Second); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	_ = bundleBytes
	api.WriteJSON(w, http.StatusOK, map[string]any{
		"draft_id":      draftID,
		"sha256":        sha,
		"size_bytes":    size,
		"expires_at_ms": expires,
	})
}

func (s *server) publishVersion(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, name, ok := s.authorize(w, r, "cs:function:publish")
	if !ok {
		return
	}
	var req api.PublishVersionRequest
	if err := api.ReadJSON(r, &req); err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "invalid request body", err), requestID(r))
		return
	}
	if req.DraftID == "" {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "draft_id is required"), requestID(r))
		return
	}
	if err := validatePublishConfig(req.Config); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, err.Error()), requestID(r))
		return
	}
	draft, err := s.store.GetDraft(r.Context(), tenant, namespace, name, req.DraftID)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if draft.Consumed {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "draft already consumed"), requestID(r))
		return
	}
	if time.Now().UnixMilli() > draft.ExpiresAtMS {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "draft expired"), requestID(r))
		return
	}

	decoded := make(map[string][]byte, len(draft.Files))
	for fn, v := range draft.Files {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "invalid draft file encoding"), requestID(r))
			return
		}
		decoded[fn] = b
	}
	bundleBytes, sha, _, err := bundle.BuildCanonical(decoded)
	if err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, err.Error()), requestID(r))
		return
	}
	if sha != draft.SHA256 {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "sha256 mismatch with draft"), requestID(r))
		return
	}
	manifest, err := api.ParseManifest(decoded["manifest.json"])
	if err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationManifest, err.Error()), requestID(r))
		return
	}

	if req.Config.TimeoutMS == 0 {
		req.Config.TimeoutMS = manifest.Limits.TimeoutMS
	}
	if req.Config.MemoryMB == 0 {
		req.Config.MemoryMB = manifest.Limits.MemoryMB
	}
	if req.Config.MaxConcurrency == 0 {
		req.Config.MaxConcurrency = manifest.Limits.MaxConcurrency
	}
	if req.Config.Capabilities == nil {
		req.Config.Capabilities = map[string]any{
			"kv":    map[string]any{"prefixes": manifest.Capabilities.KV.Prefixes, "ops": manifest.Capabilities.KV.Ops},
			"codeq": map[string]any{"publish_topics": manifest.Capabilities.CodeQ.PublishTopics},
			"http":  map[string]any{"allow_hosts": manifest.Capabilities.HTTP.AllowHosts, "timeout_ms": manifest.Capabilities.HTTP.TimeoutMS},
		}
	}

	now := time.Now().UnixMilli()
	version, err := s.store.PublishVersion(r.Context(), tenant, namespace, name, api.VersionRecord{
		SHA256:        sha,
		Config:        req.Config,
		PublishedAtMS: now,
	}, bundleBytes, req.Alias)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if err := s.store.MarkDraftConsumed(r.Context(), tenant, namespace, name, draft.DraftID, time.Duration(s.cfg.CSControl.Limits.DraftTTLSeconds)*time.Second); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusCreated, map[string]any{"version": version, "sha256": sha, "published_at_ms": now})
}

func (s *server) setAlias(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, name, ok := s.authorize(w, r, "cs:function:alias:set")
	if !ok {
		return
	}
	alias := chi.URLParam(r, "alias")
	if err := api.ValidateAlias(alias); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationName, err.Error()), requestID(r))
		return
	}
	var req api.SetAliasRequest
	if err := api.ReadJSON(r, &req); err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "invalid request body", err), requestID(r))
		return
	}
	if req.Version <= 0 {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "version must be positive"), requestID(r))
		return
	}
	if _, _, err := s.store.GetVersion(r.Context(), tenant, namespace, name, req.Version); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if err := s.store.SetAlias(r.Context(), tenant, namespace, name, alias, req.Version); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"alias": alias, "version": req.Version, "updated_at_ms": time.Now().UnixMilli()})
}

func (s *server) listAliases(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, name, ok := s.authorize(w, r, "cs:function:read")
	if !ok {
		return
	}
	aliases, err := s.store.ListAliases(r.Context(), tenant, namespace, name)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"aliases": aliases})
}

func (s *server) invokeAPI(w http.ResponseWriter, r *http.Request) {
	principal, tenant, namespace, name, ok := s.authorize(w, r, "cs:function:invoke:api")
	if !ok {
		return
	}
	var req api.InvokeAPIRequest
	if err := api.ReadJSON(r, &req); err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "invalid request body", err), requestID(r))
		return
	}
	version, err := s.store.ResolveVersion(r.Context(), tenant, namespace, name, req.Ref.Alias, req.Ref.Version)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	meta, _, err := s.store.GetVersion(r.Context(), tenant, namespace, name, version)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if err := authz.RequireRoleIntersection(meta.Config.Authz.InvokeHTTPRoles, principal); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	activationID := uuid.NewString()
	reqID := "req_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	deadline := time.Now().Add(time.Duration(meta.Config.TimeoutMS) * time.Millisecond).UnixMilli()
	inv := api.InvocationRequest{
		ActivationID: activationID,
		RequestID:    reqID,
		Tenant:       tenant,
		Namespace:    namespace,
		Ref:          api.FunctionRef{Function: name, Alias: req.Ref.Alias, Version: version},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{"path": r.URL.Path}},
		Principal:    api.Principal{Sub: principal.Sub, Roles: principal.Roles},
		DeadlineMS:   deadline,
		Event:        req.Event,
	}
	if err := s.broker.PublishInvocation(r.Context(), inv); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if strings.EqualFold(req.Mode, "async") {
		api.WriteJSON(w, http.StatusAccepted, map[string]any{"activation_id": activationID, "status": "queued"})
		return
	}
	waitCtx, cancel := context.WithTimeout(r.Context(), time.Duration(meta.Config.TimeoutMS+250)*time.Millisecond)
	defer cancel()
	res, err := waitForResultByRequestID(waitCtx, s.store, tenant, reqID)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{
		"activation_id": res.ActivationID,
		"status":        res.Status,
		"result":        res.Result,
		"duration_ms":   res.DurationMS,
		"error":         res.Error,
	})
}

func waitForResultByRequestID(ctx context.Context, store persistence.Provider, tenant, requestID string) (api.InvocationResult, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		res, err := store.GetResultByRequestID(ctx, tenant, requestID)
		if err == nil {
			return res, nil
		}
		if ctx.Err() != nil {
			return api.InvocationResult{}, cserrors.New(cserrors.CSCodeQTimeout, "result wait timed out")
		}
		if isResultNotReady(err) {
			select {
			case <-ctx.Done():
				return api.InvocationResult{}, cserrors.New(cserrors.CSCodeQTimeout, "result wait timed out")
			case <-ticker.C:
				continue
			}
		}
		return api.InvocationResult{}, err
	}
}

func isResultNotReady(err error) bool {
	var csErr *cserrors.CSError
	if !errors.As(err, &csErr) {
		return false
	}
	return csErr.Code == cserrors.CSCodeQTimeout && strings.Contains(csErr.Message, "result not found")
}

func (s *server) getActivation(w http.ResponseWriter, r *http.Request) {
	_, tenant, _, _, ok := s.authorize(w, r, "cs:activation:read")
	if !ok {
		return
	}
	activationID := chi.URLParam(r, "activation_id")
	rec, err := s.store.GetActivation(r.Context(), tenant, activationID)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, rec)
}

func (s *server) getActivationLogs(w http.ResponseWriter, r *http.Request) {
	_, tenant, _, _, ok := s.authorize(w, r, "cs:activation:read")
	if !ok {
		return
	}
	activationID := chi.URLParam(r, "activation_id")
	cursor := kv.ParseCursor(r.URL.Query().Get("cursor"))
	limit := int64(100)
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 && parsed <= 500 {
			limit = parsed
		}
	}
	chunks, next, err := s.store.ListLogChunks(r.Context(), tenant, activationID, cursor, limit)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"chunks": chunks, "cursor": kv.EncodeCursor(next)})
}

func (s *server) createSchedule(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, _, ok := s.authorize(w, r, "cs:schedule:create")
	if !ok {
		return
	}
	var req api.CreateScheduleRequest
	if err := api.ReadJSON(r, &req); err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "invalid request body", err), requestID(r))
		return
	}
	if err := validateScheduleRequest(&req); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, err.Error()), requestID(r))
		return
	}
	rec := api.ScheduleRecord{
		Tenant:        tenant,
		Namespace:     namespace,
		Name:          req.Name,
		EverySeconds:  req.EverySeconds,
		OverlapPolicy: req.OverlapPolicy,
		Ref:           req.Ref,
		Payload:       req.Payload,
		Enabled:       true,
		CreatedAtMS:   time.Now().UnixMilli(),
	}
	if err := s.store.PutSchedule(r.Context(), rec); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusCreated, rec)
}

func (s *server) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, _, ok := s.authorize(w, r, "cs:schedule:delete")
	if !ok {
		return
	}
	name := chi.URLParam(r, "name")
	if name == "" {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "name required"), requestID(r))
		return
	}
	if err := s.store.DeleteSchedule(r.Context(), tenant, namespace, name); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
}

func (s *server) createWorkerBinding(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, _, ok := s.authorize(w, r, "cs:cadence:worker:create")
	if !ok {
		return
	}
	var req api.CreateWorkerBindingRequest
	if err := api.ReadJSON(r, &req); err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "invalid request body", err), requestID(r))
		return
	}
	if err := validateWorkerBindingRequest(req); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, err.Error()), requestID(r))
		return
	}
	rec := api.WorkerBinding{
		Tenant:      tenant,
		Namespace:   namespace,
		Name:        req.Name,
		Domain:      req.Domain,
		Tasklist:    req.Tasklist,
		WorkerID:    req.WorkerID,
		ActivityMap: req.ActivityMap,
		Enabled:     true,
	}
	rec.Pollers.Activity = req.Pollers.Activity
	rec.Limits.MaxInflightTasks = req.Limits.MaxInflightTasks
	if rec.Pollers.Activity <= 0 {
		rec.Pollers.Activity = 1
	}
	if rec.Limits.MaxInflightTasks <= 0 {
		rec.Limits.MaxInflightTasks = s.cfg.CSCadencePoller.Limits.MaxInflightTasksDefault
	}
	if err := s.store.PutWorkerBinding(r.Context(), rec); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusCreated, rec)
}

func (s *server) deleteWorkerBinding(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, _, ok := s.authorize(w, r, "cs:cadence:worker:delete")
	if !ok {
		return
	}
	name := chi.URLParam(r, "name")
	if name == "" {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "name required"), requestID(r))
		return
	}
	if err := s.store.DeleteWorkerBinding(r.Context(), tenant, namespace, name); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
}

func validatePublishConfig(cfg api.VersionConfig) error {
	if cfg.TimeoutMS != 0 && (cfg.TimeoutMS < 1 || cfg.TimeoutMS > 900000) {
		return fmt.Errorf("timeout_ms out of range")
	}
	if cfg.MemoryMB != 0 && (cfg.MemoryMB < 16 || cfg.MemoryMB > 4096) {
		return fmt.Errorf("memory_mb out of range")
	}
	if cfg.MaxConcurrency != 0 && (cfg.MaxConcurrency < 1 || cfg.MaxConcurrency > 100) {
		return fmt.Errorf("max_concurrency out of range")
	}
	return nil
}

func validateScheduleRequest(req *api.CreateScheduleRequest) error {
	req.Name = strings.TrimSpace(req.Name)
	if len(req.Name) < 3 || len(req.Name) > 64 {
		return fmt.Errorf("invalid schedule name")
	}
	if req.EverySeconds < 1 || req.EverySeconds > 86400 {
		return fmt.Errorf("every_seconds out of range")
	}
	if req.OverlapPolicy == "" {
		req.OverlapPolicy = "skip"
	}
	switch req.OverlapPolicy {
	case "skip", "queue", "parallel":
	default:
		return fmt.Errorf("invalid overlap_policy")
	}
	req.Ref.Function = strings.TrimSpace(req.Ref.Function)
	if req.Ref.Function == "" || len(req.Ref.Function) > 64 {
		return fmt.Errorf("invalid schedule function reference")
	}
	if err := api.ValidateAlias(req.Ref.Alias); err != nil {
		return err
	}
	if req.Ref.Version < 0 {
		return fmt.Errorf("invalid schedule version reference")
	}
	return nil
}

func validateWorkerBindingRequest(req api.CreateWorkerBindingRequest) error {
	name := strings.TrimSpace(req.Name)
	if len(name) < 3 || len(name) > 64 {
		return fmt.Errorf("invalid worker binding name")
	}
	domain := strings.TrimSpace(req.Domain)
	if domain == "" || len(domain) > 128 {
		return fmt.Errorf("invalid cadence domain")
	}
	tasklist := strings.TrimSpace(req.Tasklist)
	if tasklist == "" || len(tasklist) > 128 {
		return fmt.Errorf("invalid cadence tasklist")
	}
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" || len(workerID) > 128 {
		return fmt.Errorf("invalid cadence worker_id")
	}
	if len(req.ActivityMap) == 0 {
		return fmt.Errorf("activity_map is required")
	}
	for activityType, ref := range req.ActivityMap {
		if strings.TrimSpace(activityType) == "" {
			return fmt.Errorf("invalid activity map entry")
		}
		if strings.TrimSpace(ref.Function) == "" || len(ref.Function) > 64 {
			return fmt.Errorf("invalid function reference in activity_map")
		}
		if err := api.ValidateAlias(ref.Alias); err != nil {
			return err
		}
		if ref.Version < 0 {
			return fmt.Errorf("invalid version reference in activity_map")
		}
	}
	if req.Pollers.Activity < 0 || req.Pollers.Activity > 256 {
		return fmt.Errorf("pollers.activity out of range")
	}
	if req.Limits.MaxInflightTasks < 0 || req.Limits.MaxInflightTasks > 100000 {
		return fmt.Errorf("limits.max_inflight_tasks out of range")
	}
	return nil
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func parseInt(v string, fallback int) int {
	x, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return x
}

func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func fmtErr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func debugf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}
