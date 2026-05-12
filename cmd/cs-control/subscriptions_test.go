package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/authz"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// subStoreFake wraps the shared FakePersistence with in-memory storage so the
// handler tests can do real CRUD without a KVRocks instance.
type subStoreFake struct {
	*testutil.FakePersistence

	mu    sync.Mutex
	items map[string]api.SubscriptionBinding
}

func newSubStore() *subStoreFake {
	s := &subStoreFake{
		FakePersistence: &testutil.FakePersistence{},
		items:           map[string]api.SubscriptionBinding{},
	}
	s.PutSubscriptionBindingFn = func(_ context.Context, rec api.SubscriptionBinding) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.items[rec.Tenant+"/"+rec.Namespace+"/"+rec.Name] = rec
		return nil
	}
	s.GetSubscriptionBindingFn = func(_ context.Context, tenant, namespace, name string) (api.SubscriptionBinding, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if v, ok := s.items[tenant+"/"+namespace+"/"+name]; ok {
			return v, nil
		}
		return api.SubscriptionBinding{}, cserrors.New(cserrors.CSValidationFailed, "subscription binding not found")
	}
	s.ListSubscriptionBindingsFn = func(_ context.Context, tenant, namespace string) ([]api.SubscriptionBinding, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		var out []api.SubscriptionBinding
		prefix := tenant + "/" + namespace + "/"
		for k, v := range s.items {
			if strings.HasPrefix(k, prefix) {
				out = append(out, v)
			}
		}
		return out, nil
	}
	s.DeleteSubscriptionBindingFn = func(_ context.Context, tenant, namespace, name string) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.items, tenant+"/"+namespace+"/"+name)
		return nil
	}
	return s
}

func subServer(store *subStoreFake) *server {
	return &server{
		store:  store,
		broker: &testutil.FakeMessaging{},
		logger: observability.NewLoggerWithWriter("test", io.Discard),
	}
}

func subRequest(method, path, body string, params map[string]string, p *authz.Principal) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rc := chi.NewRouteContext()
	for k, v := range params {
		rc.URLParams.Add(k, v)
	}
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, rc)
	ctx = observability.WithRequestID(ctx, "req_sub")
	if p != nil {
		ctx = authz.WithPrincipal(ctx, *p)
	}
	return req.WithContext(ctx)
}

func subPrincipal(tenant string, roles ...string) *authz.Principal {
	return &authz.Principal{Sub: "u_sub", Tenant: tenant, Roles: roles}
}

func decodeBinding(t *testing.T, body []byte) api.SubscriptionBinding {
	t.Helper()
	var b api.SubscriptionBinding
	if err := json.Unmarshal(body, &b); err != nil {
		t.Fatalf("decode binding: %v body=%s", err, string(body))
	}
	return b
}

func TestCreateSubscriptionAppliesDefaults(t *testing.T) {
	store := newSubStore()
	s := subServer(store)

	body := `{
		"name":"sub1",
		"topic":"orders.created",
		"filter":"body.kind == \"premium\"",
		"function_ref":{"function":"fn1","alias":"prod"},
		"mode":"ordered"
	}`
	w := httptest.NewRecorder()
	req := subRequest(http.MethodPost, "/subscriptions", body, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, subPrincipal("t_abc123", "cs:subscription:create"))
	s.createSubscription(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("createSubscription status=%d body=%s", w.Code, w.Body.String())
	}
	got := decodeBinding(t, w.Body.Bytes())
	if got.Mode != "ordered" {
		t.Fatalf("Mode=%q", got.Mode)
	}
	if got.GroupID == "" {
		t.Fatal("expected GroupID default to be set")
	}
	if got.MaxConcurrency != 1 {
		t.Fatalf("ordered mode should pin MaxConcurrency=1, got %d", got.MaxConcurrency)
	}
	if !got.Enabled {
		t.Fatal("expected Enabled=true by default")
	}
}

func TestCreateSubscriptionUnorderedClampsConcurrency(t *testing.T) {
	store := newSubStore()
	s := subServer(store)

	body := `{
		"name":"sub2",
		"topic":"orders.created",
		"function_ref":{"function":"fn1"},
		"mode":"unordered",
		"max_concurrency": 999
	}`
	w := httptest.NewRecorder()
	req := subRequest(http.MethodPost, "/subscriptions", body, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, subPrincipal("t_abc123", "cs:subscription:create"))
	s.createSubscription(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("createSubscription status=%d body=%s", w.Code, w.Body.String())
	}
	got := decodeBinding(t, w.Body.Bytes())
	if got.MaxConcurrency != subscriptionMaxConcurrencyCap {
		t.Fatalf("expected clamped concurrency=%d, got %d", subscriptionMaxConcurrencyCap, got.MaxConcurrency)
	}
}

func TestCreateSubscriptionRejectsInvalidFilter(t *testing.T) {
	store := newSubStore()
	s := subServer(store)

	body := `{
		"name":"sub_bad",
		"topic":"orders.created",
		"filter":"this is not a valid filter",
		"function_ref":{"function":"fn1"},
		"mode":"ordered"
	}`
	w := httptest.NewRecorder()
	req := subRequest(http.MethodPost, "/subscriptions", body, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, subPrincipal("t_abc123", "cs:subscription:create"))
	s.createSubscription(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	var env cserrors.HTTPErrorEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if env.Error.Code != cserrors.CSValidationFailed {
		t.Fatalf("expected CSValidationFailed, got %s", env.Error.Code)
	}
}

func TestCreateSubscriptionRejectsBadInputs(t *testing.T) {
	store := newSubStore()
	s := subServer(store)

	cases := []struct {
		name string
		body string
	}{
		{"missing name", `{"topic":"x","function_ref":{"function":"fn1"}}`},
		{"short name", `{"name":"a","topic":"x","function_ref":{"function":"fn1"}}`},
		{"missing topic", `{"name":"sub1","function_ref":{"function":"fn1"}}`},
		{"missing function", `{"name":"sub1","topic":"x"}`},
		{"bad mode", `{"name":"sub1","topic":"x","function_ref":{"function":"fn1"},"mode":"weird"}`},
		{"negative version", `{"name":"sub1","topic":"x","function_ref":{"function":"fn1","version":-1}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req := subRequest(http.MethodPost, "/subscriptions", tc.body, map[string]string{
				"tenant": "t_abc123", "namespace": "ns1",
			}, subPrincipal("t_abc123", "cs:subscription:create"))
			s.createSubscription(w, req)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 got %d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestCreateReadListDeleteSubscription_EndToEnd(t *testing.T) {
	store := newSubStore()
	srv := subServer(store)

	r := chi.NewRouter()
	r.Group(func(pr chi.Router) {
		pr.Use(func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				p := authz.Principal{Sub: "u", Tenant: "t_abc123", Roles: []string{
					"cs:subscription:create",
					"cs:subscription:read",
					"cs:subscription:delete",
				}}
				h.ServeHTTP(w, req.WithContext(authz.WithPrincipal(req.Context(), p)))
			})
		})
		srv.subscriptionRoutes(pr)
	})

	ts := httptest.NewServer(r)
	defer ts.Close()

	create := `{"name":"sub_1","topic":"t","function_ref":{"function":"fn1"},"mode":"ordered"}`
	resp, err := http.Post(ts.URL+"/v1/tenants/t_abc123/namespaces/ns1/subscriptions", "application/json", strings.NewReader(create))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("create status=%d body=%s", resp.StatusCode, string(raw))
	}
	resp.Body.Close()

	// READ
	resp, err = http.Get(ts.URL + "/v1/tenants/t_abc123/namespaces/ns1/subscriptions/sub_1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("read status=%d body=%s", resp.StatusCode, string(raw))
	}
	got := decodeBinding(t, raw)
	if got.Name != "sub_1" {
		t.Fatalf("read name=%q", got.Name)
	}

	// LIST
	resp, err = http.Get(ts.URL + "/v1/tenants/t_abc123/namespaces/ns1/subscriptions")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	raw, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status=%d body=%s", resp.StatusCode, string(raw))
	}
	var listOut struct {
		Subscriptions []api.SubscriptionBinding `json:"subscriptions"`
	}
	if err := json.Unmarshal(raw, &listOut); err != nil {
		t.Fatalf("decode list: %v body=%s", err, string(raw))
	}
	if len(listOut.Subscriptions) != 1 || listOut.Subscriptions[0].Name != "sub_1" {
		t.Fatalf("list mismatch: %+v", listOut)
	}

	// DELETE
	delReq, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/tenants/t_abc123/namespaces/ns1/subscriptions/sub_1", nil)
	resp, err = http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status=%d", resp.StatusCode)
	}

	// READ after delete should 400 (not found).
	resp, _ = http.Get(ts.URL + "/v1/tenants/t_abc123/namespaces/ns1/subscriptions/sub_1")
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("expected non-OK after delete, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestDeleteSubscriptionMissingNameReturns400(t *testing.T) {
	store := newSubStore()
	s := subServer(store)

	w := httptest.NewRecorder()
	req := subRequest(http.MethodDelete, "/subscriptions", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "",
	}, subPrincipal("t_abc123", "cs:subscription:delete"))
	s.deleteSubscription(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCreateSubscriptionStoreError(t *testing.T) {
	store := newSubStore()
	wantErr := errors.New("disk full")
	store.PutSubscriptionBindingFn = func(context.Context, api.SubscriptionBinding) error {
		return cserrors.Wrap(cserrors.CSKVWriteFailed, "put failed", wantErr)
	}
	s := subServer(store)

	body := `{"name":"sub_err","topic":"x","function_ref":{"function":"fn1"},"mode":"ordered"}`
	w := httptest.NewRecorder()
	req := subRequest(http.MethodPost, "/subscriptions", body, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, subPrincipal("t_abc123", "cs:subscription:create"))
	s.createSubscription(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on store error, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestCreateSubscriptionInvalidJSON(t *testing.T) {
	store := newSubStore()
	s := subServer(store)

	w := httptest.NewRecorder()
	req := subRequest(http.MethodPost, "/subscriptions", "{not json", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, subPrincipal("t_abc123", "cs:subscription:create"))
	s.createSubscription(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 invalid json, got %d body=%s", w.Code, w.Body.String())
	}
}
