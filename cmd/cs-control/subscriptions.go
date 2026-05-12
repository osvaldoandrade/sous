package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/codeq"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

// subscriptionMaxConcurrencyCap bounds the per-binding worker pool size for
// unordered mode. Tenants can request any value up to this ceiling; values
// above it are clamped at create time. The cap is intentionally small for
// v0.1 — see docs/07-codeq-protocol.md ("Subscription triggers") for the
// design rationale.
const subscriptionMaxConcurrencyCap = 64

// subscriptionRoutes wires the CRUD surface for codeQ subscription bindings.
// The route layout mirrors schedules and worker bindings to keep the API
// uniform (see docs/04-api-rest.md). cs-control owns binding persistence; the
// in-process subscription worker reads ListAllSubscriptionBindings on a
// refresh tick.
func (s *server) subscriptionRoutes(pr chi.Router) {
	pr.Post("/v1/tenants/{tenant}/namespaces/{namespace}/subscriptions", s.createSubscription)
	pr.Get("/v1/tenants/{tenant}/namespaces/{namespace}/subscriptions", s.listSubscriptions)
	pr.Get("/v1/tenants/{tenant}/namespaces/{namespace}/subscriptions/{name}", s.readSubscription)
	pr.Delete("/v1/tenants/{tenant}/namespaces/{namespace}/subscriptions/{name}", s.deleteSubscription)
}

func (s *server) createSubscription(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, _, ok := s.authorize(w, r, "cs:subscription:create")
	if !ok {
		return
	}
	var req api.CreateSubscriptionRequest
	if err := api.ReadJSON(r, &req); err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "invalid request body", err), requestID(r))
		return
	}
	rec, err := s.buildSubscription(tenant, namespace, req)
	if err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, err.Error()), requestID(r))
		return
	}
	if err := s.store.PutSubscriptionBinding(r.Context(), rec); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusCreated, rec)
}

func (s *server) readSubscription(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, name, ok := s.authorize(w, r, "cs:subscription:read")
	if !ok {
		return
	}
	rec, err := s.store.GetSubscriptionBinding(r.Context(), tenant, namespace, name)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, rec)
}

func (s *server) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, _, ok := s.authorize(w, r, "cs:subscription:read")
	if !ok {
		return
	}
	out, err := s.store.ListSubscriptionBindings(r.Context(), tenant, namespace)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	if out == nil {
		out = []api.SubscriptionBinding{}
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"subscriptions": out})
}

func (s *server) deleteSubscription(w http.ResponseWriter, r *http.Request) {
	_, tenant, namespace, _, ok := s.authorize(w, r, "cs:subscription:delete")
	if !ok {
		return
	}
	name := chi.URLParam(r, "name")
	if name == "" {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "name required"), requestID(r))
		return
	}
	if err := s.store.DeleteSubscriptionBinding(r.Context(), tenant, namespace, name); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"status": "deleted"})
}

// buildSubscription validates the request and produces the persisted record.
// Errors here surface as 400 CS_VALIDATION_FAILED so misconfigured filters or
// bad refs are rejected at create time, matching the acceptance criteria in
// issue #7.
func (s *server) buildSubscription(tenant, namespace string, req api.CreateSubscriptionRequest) (api.SubscriptionBinding, error) {
	name := strings.TrimSpace(req.Name)
	if len(name) < 3 || len(name) > 64 {
		return api.SubscriptionBinding{}, fmt.Errorf("invalid subscription name")
	}
	topic := strings.TrimSpace(req.Topic)
	if topic == "" || len(topic) > 256 {
		return api.SubscriptionBinding{}, fmt.Errorf("invalid topic")
	}
	if strings.TrimSpace(req.FunctionRef.Function) == "" {
		return api.SubscriptionBinding{}, fmt.Errorf("function_ref.function is required")
	}
	if err := api.ValidateAlias(req.FunctionRef.Alias); err != nil {
		return api.SubscriptionBinding{}, err
	}
	if req.FunctionRef.Version < 0 {
		return api.SubscriptionBinding{}, fmt.Errorf("invalid function_ref.version")
	}
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = api.SubscriptionModeOrdered
	}
	if mode != api.SubscriptionModeOrdered && mode != api.SubscriptionModeUnordered {
		return api.SubscriptionBinding{}, fmt.Errorf("invalid mode (must be ordered or unordered)")
	}
	if _, err := codeq.ParseFilter(req.Filter); err != nil {
		return api.SubscriptionBinding{}, err
	}
	maxConcurrency := req.MaxConcurrency
	if mode == api.SubscriptionModeUnordered {
		if maxConcurrency <= 0 {
			maxConcurrency = s.subscriptionPoolDefault()
		}
		if maxConcurrency > subscriptionMaxConcurrencyCap {
			maxConcurrency = subscriptionMaxConcurrencyCap
		}
	} else {
		maxConcurrency = 1
	}
	groupID := strings.TrimSpace(req.GroupID)
	if groupID == "" {
		groupID = fmt.Sprintf("cs-sub-%s-%s-%s", tenant, namespace, name)
	}
	return api.SubscriptionBinding{
		Tenant:         tenant,
		Namespace:      namespace,
		Name:           name,
		Topic:          topic,
		Filter:         strings.TrimSpace(req.Filter),
		FunctionRef:    req.FunctionRef,
		Mode:           mode,
		GroupID:        groupID,
		MaxConcurrency: maxConcurrency,
		CreatedAtMS:    time.Now().UnixMilli(),
		Enabled:        true,
	}, nil
}

// subscriptionPoolDefault returns the default unordered worker pool size,
// reading the config knob with a documented fallback.
func (s *server) subscriptionPoolDefault() int {
	if v := s.cfg.CSControl.Subscriptions.WorkerPoolDefault; v > 0 {
		return v
	}
	return 4
}
