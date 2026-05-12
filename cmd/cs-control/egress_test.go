package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/authz"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// egressTestServer wires the cs-control egress routes against a real
// miniredis-backed kv.Store. It mirrors lifecycleHarness so the handler
// behaviour rides on the actual persistence path rather than a stub.
func egressTestServer(t *testing.T) (*httptest.Server, *kv.Store) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	store := kv.NewStore(mr.Addr(), "")
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}

	s := &server{
		store:  store,
		broker: &testutil.FakeMessaging{},
		logger: observability.NewLoggerWithWriter("egress-test", io.Discard),
	}

	r := chi.NewRouter()
	r.Use(observability.RequestIDMiddleware)
	r.Group(func(pr chi.Router) {
		pr.Use(func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				p := authz.Principal{Sub: "u_test", Roles: []string{"role:admin"}}
				h.ServeHTTP(w, req.WithContext(authz.WithPrincipal(req.Context(), p)))
			})
		})
		s.egressRoutes(pr)
	})

	ts := httptest.NewServer(r)
	t.Cleanup(ts.Close)
	return ts, store
}

func egressDo(t *testing.T, method, url string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func TestEgressPolicyGetReturnsDefaultDenyWhenAbsent(t *testing.T) {
	ts, _ := egressTestServer(t)

	status, body := egressDo(t, http.MethodGet, ts.URL+"/v1/tenants/t_abc123/egress-policy", nil)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", status, body)
	}
	var got api.EgressPolicy
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v body=%s", err, body)
	}
	if !got.DefaultDeny {
		t.Fatalf("missing policy should default to default-deny stub, got %+v", got)
	}
	if len(got.AllowedHosts) != 0 || len(got.AllowedCIDRs) != 0 {
		t.Fatalf("default stub should be empty, got %+v", got)
	}
}

func TestEgressPolicyPutThenGetRoundTrips(t *testing.T) {
	ts, _ := egressTestServer(t)

	policy := api.EgressPolicy{
		AllowedHosts: []string{"*.example.com", "api.partner.net"},
		AllowedCIDRs: []string{"203.0.113.0/24"},
		DeniedHosts:  []string{"abuse.example.com"},
		DefaultDeny:  true,
	}
	status, body := egressDo(t, http.MethodPut, ts.URL+"/v1/tenants/t_abc123/egress-policy", policy)
	if status != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", status, body)
	}
	var stored api.EgressPolicy
	if err := json.Unmarshal(body, &stored); err != nil {
		t.Fatalf("decode put: %v body=%s", err, body)
	}
	if stored.UpdatedAtMS == 0 {
		t.Fatal("expected UpdatedAtMS to be server-stamped")
	}
	if len(stored.AllowedHosts) != 2 || stored.AllowedHosts[0] != "*.example.com" {
		t.Fatalf("allowed_hosts not persisted: %+v", stored)
	}

	status, body = egressDo(t, http.MethodGet, ts.URL+"/v1/tenants/t_abc123/egress-policy", nil)
	if status != http.StatusOK {
		t.Fatalf("GET status=%d body=%s", status, body)
	}
	var got api.EgressPolicy
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode get: %v body=%s", err, body)
	}
	if !got.DefaultDeny || len(got.AllowedHosts) != 2 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if got.UpdatedAtMS != stored.UpdatedAtMS {
		t.Fatalf("UpdatedAtMS mismatch: put=%d get=%d", stored.UpdatedAtMS, got.UpdatedAtMS)
	}
}

func TestEgressPolicyPutRejectsInvalidPolicy(t *testing.T) {
	ts, _ := egressTestServer(t)

	cases := []struct {
		name   string
		policy api.EgressPolicy
	}{
		{"bad CIDR", api.EgressPolicy{AllowedCIDRs: []string{"not-a-cidr"}}},
		{"empty host", api.EgressPolicy{AllowedHosts: []string{""}}},
		{"overlap", api.EgressPolicy{AllowedHosts: []string{"x.test"}, DeniedHosts: []string{"x.test"}}},
		{"wildcard mid-label", api.EgressPolicy{AllowedHosts: []string{"foo.*.test"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := egressDo(t, http.MethodPut, ts.URL+"/v1/tenants/t_abc123/egress-policy", tc.policy)
			if status != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d body=%s", status, body)
			}
			var env cserrors.HTTPErrorEnvelope
			if err := json.Unmarshal(body, &env); err != nil {
				t.Fatalf("decode err: %v", err)
			}
			if env.Error.Code != cserrors.CSValidationFailed {
				t.Fatalf("expected CSValidationFailed, got %s", env.Error.Code)
			}
		})
	}
}

func TestEgressPolicyPutRejectsInvalidJSON(t *testing.T) {
	ts, _ := egressTestServer(t)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/tenants/t_abc123/egress-policy", strings.NewReader("{not json"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestEgressPolicyRejectsInvalidTenant(t *testing.T) {
	ts, _ := egressTestServer(t)

	status, body := egressDo(t, http.MethodGet, ts.URL+"/v1/tenants/INVALID/egress-policy", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 on bad tenant, got %d body=%s", status, body)
	}
	var env cserrors.HTTPErrorEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode env: %v", err)
	}
	if env.Error.Code != cserrors.CSValidationName {
		t.Fatalf("expected CSValidationName got %s", env.Error.Code)
	}
}

func TestEgressPolicyPutClearsClientUpdatedAt(t *testing.T) {
	ts, _ := egressTestServer(t)

	// Caller tries to lie about UpdatedAtMS; server must overwrite.
	policy := api.EgressPolicy{
		AllowedHosts: []string{"good.example.com"},
		DefaultDeny:  true,
		UpdatedAtMS:  1,
	}
	status, body := egressDo(t, http.MethodPut, ts.URL+"/v1/tenants/t_abc123/egress-policy", policy)
	if status != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", status, body)
	}
	var got api.EgressPolicy
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.UpdatedAtMS == 1 {
		t.Fatal("server must overwrite client-supplied UpdatedAtMS")
	}
}
