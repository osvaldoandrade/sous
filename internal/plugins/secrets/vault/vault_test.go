package vault_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/secrets"
	"github.com/osvaldoandrade/sous/internal/plugins/secrets/vault"
)

// fakeVault builds an httptest.Server that emulates the subset of the
// Vault KV v2 API the driver depends on. The handler is callable with a
// table of paths to canned responses so each test case stays compact.
func fakeVault(t *testing.T, token string, store map[string]any, errors map[string]int) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		// Strip "/v1/<mount>/data/" prefix to recover the path the
		// driver actually requested.
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/v1/"), "/", 3)
		if len(parts) < 3 || parts[1] != "data" {
			http.NotFound(w, r)
			return
		}
		mount := parts[0]
		_ = mount
		path := parts[2]
		if got := r.Header.Get("X-Vault-Token"); got != token {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"errors":["permission denied"]}`))
			return
		}
		if status, ok := errors[path]; ok {
			w.WriteHeader(status)
			return
		}
		data, ok := store[path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		envelope := map[string]any{
			"data": map[string]any{
				"data":     data,
				"metadata": map[string]any{"version": 1},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(envelope)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestVaultSuccessSingleField(t *testing.T) {
	srv := fakeVault(t, "tok", map[string]any{
		"payments/stripe": map[string]any{"value": "sk_live_abc"},
	}, nil)
	p := vault.New(srv.URL, "tok", "secret", "", srv.Client())
	t.Cleanup(func() { _ = p.Close() })

	got, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name: "STRIPE_KEY",
		Path: "payments/stripe",
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "sk_live_abc" {
		t.Fatalf("want sk_live_abc, got %q", got)
	}
}

func TestVaultJSONField(t *testing.T) {
	srv := fakeVault(t, "tok", map[string]any{
		"payments/multi": map[string]any{
			"api_key": "sk_live_xyz",
			"tier":    float64(7),
			"sandbox": true,
			"webhook": "https://example.com/hook",
		},
	}, nil)
	p := vault.New(srv.URL, "tok", "secret", "", srv.Client())

	got, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name:   "API_KEY",
		Path:   "payments/multi",
		Format: "json-field:api_key",
	})
	if err != nil {
		t.Fatalf("Get(api_key): %v", err)
	}
	if got != "sk_live_xyz" {
		t.Fatalf("api_key: got %q", got)
	}

	tier, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name:   "TIER",
		Path:   "payments/multi",
		Format: "json-field:tier",
	})
	if err != nil {
		t.Fatalf("Get(tier): %v", err)
	}
	if tier != "7" {
		t.Fatalf("tier: got %q", tier)
	}

	sandbox, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name:   "SANDBOX",
		Path:   "payments/multi",
		Format: "json-field:sandbox",
	})
	if err != nil {
		t.Fatalf("Get(sandbox): %v", err)
	}
	if sandbox != "true" {
		t.Fatalf("sandbox: got %q", sandbox)
	}
}

func TestVaultNotFound(t *testing.T) {
	srv := fakeVault(t, "tok", nil, nil)
	p := vault.New(srv.URL, "tok", "secret", "", srv.Client())

	_, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name: "MISSING",
		Path: "nope/nada",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var cserr *cserrors.CSError
	if !errors.As(err, &cserr) {
		t.Fatalf("expected *CSError, got %T", err)
	}
	if cserr.Code != cserrors.CSSecretNotFound {
		t.Fatalf("expected CS_SECRET_NOT_FOUND, got %s", cserr.Code)
	}
}

func TestVaultAuthFailure(t *testing.T) {
	srv := fakeVault(t, "right-token", nil, nil)
	p := vault.New(srv.URL, "wrong-token", "secret", "", srv.Client())

	_, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name: "X",
		Path: "any/path",
	})
	if err == nil {
		t.Fatal("expected auth failure")
	}
	var cserr *cserrors.CSError
	if !errors.As(err, &cserr) || cserr.Code != cserrors.CSSecretUnavailable {
		t.Fatalf("expected CS_SECRET_UNAVAILABLE, got %v", err)
	}
}

func TestVaultEmptyToken(t *testing.T) {
	p := vault.New("http://127.0.0.1:0", "", "secret", "", nil)
	_, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{Name: "X", Path: "p"})
	if err == nil {
		t.Fatal("expected error for empty token")
	}
	var cserr *cserrors.CSError
	if !errors.As(err, &cserr) || cserr.Code != cserrors.CSSecretUnavailable {
		t.Fatalf("expected CS_SECRET_UNAVAILABLE, got %v", err)
	}
}

func TestVaultNamespaceHeaderForwarded(t *testing.T) {
	var seenNS string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/secret/data/p", func(w http.ResponseWriter, r *http.Request) {
		seenNS = r.Header.Get("X-Vault-Namespace")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"data":{"value":"v"}}}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	p := vault.New(srv.URL, "tok", "secret", "ent/team-a", srv.Client())
	_, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{Name: "X", Path: "p"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if seenNS != "ent/team-a" {
		t.Fatalf("namespace header: want ent/team-a, got %q", seenNS)
	}
}

func TestVaultUnknownFieldMissing(t *testing.T) {
	srv := fakeVault(t, "tok", map[string]any{
		"p": map[string]any{"value": "x"},
	}, nil)
	p := vault.New(srv.URL, "tok", "secret", "", srv.Client())

	_, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name:   "Y",
		Path:   "p",
		Format: "json-field:missing",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var cserr *cserrors.CSError
	if !errors.As(err, &cserr) || cserr.Code != cserrors.CSSecretNotFound {
		t.Fatalf("expected CS_SECRET_NOT_FOUND, got %v", err)
	}
}

func TestVaultFactoryReadsConfig(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.Secrets.Vault.Addr = "https://vault.example.com:8200"
	cfg.Plugins.Secrets.Vault.Token = "tok"
	cfg.Plugins.Secrets.Vault.KVMount = "secret"
	cfg.Plugins.Secrets.Vault.TimeoutMS = 100
	p, err := vault.NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
}
