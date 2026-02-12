package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
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
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
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

	s := &server{cfg: cfg, store: store, broker: broker}
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

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		api.WriteJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.store.Ping(r.Context()); err != nil {
			cserrors.WriteHTTP(w, err, observability.RequestIDFromContext(r.Context()))
			return
		}
		api.WriteJSON(w, http.StatusOK, map[string]any{"status": "ready"})
	})
	r.Handle("/metrics", observability.MetricsHandler(prometheus.NewRegistry()))

	r.Group(func(pr chi.Router) {
		pr.Use(authz.AuthnMiddleware(authnProvider))
		pr.HandleFunc("/v1/web/{tenant}/{namespace}/{function}/{ref}", s.invokeHTTP)
		pr.HandleFunc("/v1/web/{tenant}/{namespace}/{function}/{ref}/*", s.invokeHTTP)
	})

	httpServer := &http.Server{
		Addr:         s.cfg.CSHTTPGateway.HTTP.Addr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return httpServer.ListenAndServe()
}

func (s *server) invokeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, ok := authz.PrincipalFromContext(r.Context())
	if !ok {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthnInvalidToken, "principal missing"), requestID(r))
		return
	}
	if !authz.CheckAction(principal, "cs:function:invoke:http") {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthzDenied, "action denied"), requestID(r))
		return
	}
	tenant := chi.URLParam(r, "tenant")
	namespace := chi.URLParam(r, "namespace")
	function := chi.URLParam(r, "function")
	ref := chi.URLParam(r, "ref")
	if principal.Tenant != "" && principal.Tenant != tenant {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthzResourceMis, "tenant mismatch"), requestID(r))
		return
	}

	var version int64
	alias := ""
	if parsed, err := strconv.ParseInt(ref, 10, 64); err == nil && parsed > 0 {
		version = parsed
	} else {
		alias = ref
	}
	resolvedVersion, err := s.store.ResolveVersion(r.Context(), tenant, namespace, function, alias, version)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	meta, _, err := s.store.GetVersion(r.Context(), tenant, namespace, function, resolvedVersion)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if err := authz.RequireRoleIntersection(meta.Config.Authz.InvokeHTTPRoles, principal); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if len(r.URL.RawQuery) > applyDefaultLimit(s.cfg.CSHTTPGateway.Limits.MaxQueryBytes, 16*1024) {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "query string too large"), requestID(r))
		return
	}
	if estimateHeaderBytes(r.Header) > applyDefaultLimit(s.cfg.CSHTTPGateway.Limits.MaxHeaderBytes, 64*1024) {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "headers too large"), requestID(r))
		return
	}

	body, err := readBoundedBody(r, int64(s.cfg.CSHTTPGateway.Limits.MaxBodyBytes))
	if err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, err.Error()), requestID(r))
		return
	}
	activationID := deriveActivationID(tenant, function, ref, r.Header.Get("Idempotency-Key"))
	reqID := "req_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	deadline := time.Now().Add(time.Duration(meta.Config.TimeoutMS) * time.Millisecond).UnixMilli()

	event := map[string]any{
		"version":         "2.0",
		"routeKey":        "$default",
		"rawPath":         r.URL.Path,
		"rawQueryString":  r.URL.RawQuery,
		"headers":         flattenHeaders(r.Header),
		"requestContext":  map[string]any{"http": map[string]any{"method": r.Method, "path": r.URL.Path}},
		"body":            base64.StdEncoding.EncodeToString(body),
		"isBase64Encoded": true,
	}

	invocation := api.InvocationRequest{
		ActivationID: activationID,
		RequestID:    reqID,
		Tenant:       tenant,
		Namespace:    namespace,
		Ref:          api.FunctionRef{Function: function, Alias: alias, Version: resolvedVersion},
		Trigger:      api.Trigger{Type: "http", Source: map[string]any{"path": r.URL.Path, "traceparent": r.Header.Get("traceparent")}},
		Principal:    api.Principal{Sub: principal.Sub, Roles: principal.Roles},
		DeadlineMS:   deadline,
		Event:        event,
	}

	if err := s.broker.PublishInvocation(r.Context(), invocation); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}

	waitCtx, cancel := context.WithTimeout(r.Context(), time.Duration(meta.Config.TimeoutMS+250)*time.Millisecond)
	defer cancel()
	result, err := s.broker.WaitForResult(waitCtx, reqID)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if result.Result == nil {
		if result.Error != nil {
			cserrors.WriteHTTP(w, cserrors.New(cserrors.CSRuntimeException, result.Error.Message), requestID(r))
			return
		}
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSRuntimeException, "missing function result"), requestID(r))
		return
	}

	for k, v := range result.Result.Headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(result.Result.StatusCode)
	if result.Result.IsBase64Encoded {
		decoded, err := base64.StdEncoding.DecodeString(result.Result.Body)
		if err == nil {
			_, _ = w.Write(decoded)
			return
		}
	}
	_, _ = w.Write([]byte(result.Result.Body))
}

func deriveActivationID(tenant, function, ref, idempotencyKey string) string {
	if strings.TrimSpace(idempotencyKey) == "" {
		return uuid.NewString()
	}
	seed := tenant + ":" + function + ":" + ref + ":" + idempotencyKey
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(seed)).String()
}

func requestID(r *http.Request) string {
	return observability.RequestIDFromContext(r.Context())
}

func flattenHeaders(header http.Header) map[string]string {
	out := make(map[string]string, len(header))
	for k, values := range header {
		if len(values) > 0 {
			out[strings.ToLower(k)] = strings.Join(values, ",")
		}
	}
	return out
}

func readBoundedBody(r *http.Request, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = 6 * 1024 * 1024
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("request body too large")
	}
	return body, nil
}

func applyDefaultLimit(v, defaultValue int) int {
	if v <= 0 {
		return defaultValue
	}
	return v
}

func estimateHeaderBytes(header http.Header) int {
	size := 0
	for key, values := range header {
		size += len(key) + 2
		for idx, value := range values {
			size += len(value)
			if idx < len(values)-1 {
				size++
			}
		}
		size += 2
	}
	return size
}
