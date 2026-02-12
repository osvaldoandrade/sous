package tikti

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/config"
)

func TestNewFromConfigValidation(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.AuthN.Tikti.IntrospectionURL = ""
	if _, err := NewFromConfig(cfg); err == nil {
		t.Fatal("expected required url error")
	}
}

func TestProviderIntrospectAndCache(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("key") != "" {
			t.Fatalf("did not expect query api key in this test")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"active": true,
			"sub":    "user:1",
			"tenant": "t_abc123",
			"roles":  []string{"role:app"},
			"exp":    time.Now().Add(time.Hour).Unix(),
		})
	}))
	defer ts.Close()

	cfg := config.Config{}
	cfg.Plugins.AuthN.Tikti.IntrospectionURL = ts.URL
	cfg.Plugins.AuthN.Tikti.CacheTTLSeconds = 60
	providerRaw, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	provider := providerRaw.(*Provider)

	principal, err := provider.Introspect(context.Background(), "token")
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	if principal.Sub != "user:1" {
		t.Fatalf("unexpected principal: %+v", principal)
	}
	if _, err := provider.Introspect(context.Background(), "token"); err != nil {
		t.Fatalf("cached introspect: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected cache hit, calls=%d", calls)
	}
	if provider.Name() != "tikti" {
		t.Fatalf("unexpected name: %s", provider.Name())
	}
}

func TestProviderIntrospectFailures(t *testing.T) {
	provider := &Provider{url: "http://127.0.0.1:0", httpClient: &http.Client{Timeout: 50 * time.Millisecond}}
	if _, err := provider.Introspect(context.Background(), ""); err == nil {
		t.Fatal("expected missing token error")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer ts.Close()
	provider.url = ts.URL
	provider.httpClient = &http.Client{Timeout: 100 * time.Millisecond}
	provider.cache = make(map[string]cacheEntry)
	if _, err := provider.Introspect(context.Background(), "token"); err == nil {
		t.Fatal("expected invalid token error")
	}
}

func TestProviderLookupContractWithAPIKey(t *testing.T) {
	var gotPath string
	var gotBody map[string]string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.String()
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"users": []map[string]any{{
				"localId":  "usr_1",
				"email":    "admin@codecompany.com.br",
				"role":     "ADMIN",
				"tenantId": "t_abc123",
				"status":   "ACTIVE",
			}},
		})
	}))
	defer ts.Close()

	cfg := config.Config{}
	cfg.Plugins.AuthN.Tikti.IntrospectionURL = ts.URL + "/v1/accounts/lookup"
	cfg.Plugins.AuthN.Tikti.APIKey = "api_key_1"
	providerRaw, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	provider := providerRaw.(*Provider)

	token := fakeJWT(t, map[string]any{"tid": "t_from_token", "exp": time.Now().Add(time.Hour).Unix()})
	principal, err := provider.Introspect(context.Background(), token)
	if err != nil {
		t.Fatalf("introspect lookup contract: %v", err)
	}
	if !strings.Contains(gotPath, "?key=api_key_1") {
		t.Fatalf("expected key query param, path=%q", gotPath)
	}
	if gotBody["token"] != token || gotBody["idToken"] != token {
		t.Fatalf("unexpected introspection payload: %+v", gotBody)
	}
	if principal.Sub != "usr_1" || principal.Tenant != "t_abc123" {
		t.Fatalf("unexpected principal identity: %+v", principal)
	}
	if len(principal.Roles) != 1 || principal.Roles[0] != "admin" {
		t.Fatalf("unexpected principal roles: %+v", principal.Roles)
	}
}

func TestProviderLookupContractFallsBackToTokenClaims(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"users": []map[string]any{{
				"localId": "",
				"email":   "member@codecompany.com.br",
				"role":    "COMPANY_EMPLOYEE",
				"status":  "ACTIVE",
			}},
		})
	}))
	defer ts.Close()

	cfg := config.Config{}
	cfg.Plugins.AuthN.Tikti.IntrospectionURL = ts.URL + "/v1/accounts/lookup"
	providerRaw, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	provider := providerRaw.(*Provider)

	exp := time.Now().Add(time.Hour).Unix()
	token := fakeJWT(t, map[string]any{"sub": "sub_from_token", "tid": "t_from_token", "exp": exp})
	principal, err := provider.Introspect(context.Background(), token)
	if err != nil {
		t.Fatalf("introspect lookup fallback: %v", err)
	}
	if principal.Sub != "member@codecompany.com.br" {
		t.Fatalf("expected email fallback for sub, got %+v", principal)
	}
	if principal.Tenant != "t_from_token" {
		t.Fatalf("expected tenant from token, got %+v", principal)
	}
	if principal.Exp != exp {
		t.Fatalf("expected exp from token, got %+v", principal)
	}
	if len(principal.Roles) != 2 {
		t.Fatalf("expected role + normalized role, got %+v", principal.Roles)
	}
}

func fakeJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "none", "typ": "JWT"})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload) + ".x"
}
