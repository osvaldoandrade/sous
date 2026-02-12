package tikti

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
