// Package vault implements the HashiCorp Vault KV v2 secret-provider driver
// for E6.01. It speaks the public Vault HTTP API directly through stdlib
// net/http so the binary footprint stays the same as the v0.1 baseline —
// no SDK dependency, no transitive code to audit.
//
// Auth model: a static token loaded once at construction time, either from
// plugins.secrets.vault.token in YAML or from the env var named by
// plugins.secrets.vault.token_env (default VAULT_TOKEN). The token is
// applied as the X-Vault-Token header on every request. Lease renewal,
// AppRole, and Kubernetes auth are out of scope for the v0.1 driver — see
// issue #23 acceptance criteria; this PR lands the KV-read surface needed
// by cs-invoker-pool and leaves richer auth modes to follow-up work.
package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
	"github.com/osvaldoandrade/sous/internal/plugins/secrets"
)

// Provider issues KV v2 reads against a Vault cluster identified by Addr.
// The driver is stateless beyond the cached http.Client so a single
// instance can be shared across all invoker goroutines.
type Provider struct {
	addr      string
	token     string
	mount     string
	namespace string
	client    *http.Client
}

func init() {
	registry.RegisterSecrets("vault", NewFromConfig)
}

// NewFromConfig hydrates a Provider from cfg.Plugins.Secrets.Vault. Token
// resolution prefers the static YAML value when present so a deployment
// can ship a sealed config; otherwise it reads the env var named by
// TokenEnv (default "VAULT_TOKEN"). The resulting Provider returns a
// transport-level error for every Get when neither source yields a token.
func NewFromConfig(cfg config.Config) (secrets.Provider, error) {
	addr := strings.TrimRight(strings.TrimSpace(cfg.Plugins.Secrets.Vault.Addr), "/")
	if addr == "" {
		return nil, fmt.Errorf("plugins.secrets.vault.addr is required")
	}
	token := strings.TrimSpace(cfg.Plugins.Secrets.Vault.Token)
	if token == "" {
		envName := cfg.Plugins.Secrets.Vault.TokenEnv
		if envName == "" {
			envName = "VAULT_TOKEN"
		}
		token = strings.TrimSpace(os.Getenv(envName))
	}
	mount := strings.TrimSpace(cfg.Plugins.Secrets.Vault.KVMount)
	if mount == "" {
		mount = "secret"
	}
	timeout := time.Duration(cfg.Plugins.Secrets.Vault.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Provider{
		addr:      addr,
		token:     token,
		mount:     mount,
		namespace: strings.TrimSpace(cfg.Plugins.Secrets.Vault.Namespace),
		client:    &http.Client{Timeout: timeout},
	}, nil
}

// New builds a Provider with explicit dependencies, intended for tests that
// drive against httptest.NewServer. The httpClient is required so a test
// can swap in a transport with custom DialContext / RoundTripper hooks.
func New(addr, token, mount, namespace string, httpClient *http.Client) *Provider {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 2 * time.Second}
	}
	if mount == "" {
		mount = "secret"
	}
	return &Provider{
		addr:      strings.TrimRight(addr, "/"),
		token:     token,
		mount:     mount,
		namespace: namespace,
		client:    httpClient,
	}
}

// Close is a no-op: the embedded http.Client owns no goroutines and the
// host kernel reclaims idle connections automatically.
func (p *Provider) Close() error { return nil }

// Get resolves SecretRef.Path against Vault's KV v2 API. The path is
// always rewritten to "<mount>/data/<path>" because v2 stores versioned
// secret bytes under the data/ sub-path; the wrapping JSON envelope is
// unwrapped here so callers see the inner data map directly.
//
// Behaviour summary:
//   - Format == "" / "raw": when the entry contains exactly one field the
//     driver returns that field as a string (Vault's idiomatic single-
//     string secret shape, {"value":"..."}). Otherwise the whole data
//     map is re-serialised as compact JSON.
//   - Format == "json-field:<key>": the driver returns data[<key>] as a
//     string. Missing keys surface CS_SECRET_NOT_FOUND.
func (p *Provider) Get(ctx context.Context, tenant string, ref secrets.SecretRef) (string, error) {
	_ = tenant
	if strings.TrimSpace(ref.Path) == "" {
		return "", cserrors.New(cserrors.CSSecretNotFound, "empty vault path")
	}
	if p.token == "" {
		return "", cserrors.New(cserrors.CSSecretUnavailable, "vault token is empty")
	}

	target, err := p.buildURL(ref.Path)
	if err != nil {
		return "", cserrors.Wrap(cserrors.CSSecretUnavailable, "build vault url", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", cserrors.Wrap(cserrors.CSSecretUnavailable, "build vault request", err)
	}
	req.Header.Set("X-Vault-Token", p.token)
	req.Header.Set("Accept", "application/json")
	if p.namespace != "" {
		req.Header.Set("X-Vault-Namespace", p.namespace)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return "", cserrors.Wrap(cserrors.CSSecretUnavailable, "vault request failed", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", cserrors.Wrap(cserrors.CSSecretUnavailable, "read vault response", err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return parseKVResponse(body, ref)
	case http.StatusNotFound:
		return "", cserrors.New(cserrors.CSSecretNotFound, "secret not found: "+ref.Path)
	case http.StatusUnauthorized, http.StatusForbidden:
		return "", cserrors.New(cserrors.CSSecretUnavailable, fmt.Sprintf("vault auth failed (status=%d)", resp.StatusCode))
	default:
		return "", cserrors.New(cserrors.CSSecretUnavailable, fmt.Sprintf("unexpected vault status %d", resp.StatusCode))
	}
}

// buildURL constructs "<addr>/v1/<mount>/data/<path>" while honouring URL
// path encoding so paths with slashes round-trip cleanly. We deliberately
// do not invoke url.PathEscape on the whole path argument because Vault
// expects forward slashes between path components.
func (p *Provider) buildURL(secretPath string) (string, error) {
	base, err := url.Parse(p.addr)
	if err != nil {
		return "", err
	}
	cleaned := strings.TrimPrefix(strings.TrimSpace(secretPath), "/")
	base.Path = path.Join(base.Path, "v1", p.mount, "data", cleaned)
	return base.String(), nil
}

// kvResponse is the KV v2 wire shape we depend on. We only model the data
// envelope; the wider response carries lease + metadata that the v0.1
// driver ignores.
type kvResponse struct {
	Data struct {
		Data map[string]any `json:"data"`
	} `json:"data"`
}

func parseKVResponse(body []byte, ref secrets.SecretRef) (string, error) {
	var parsed kvResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", cserrors.Wrap(cserrors.CSSecretUnavailable, "decode vault response", err)
	}
	data := parsed.Data.Data
	if len(data) == 0 {
		return "", cserrors.New(cserrors.CSSecretNotFound, "secret payload empty: "+ref.Path)
	}

	// Single-field shortcut: when the caller declared no Format hint and
	// the entry has exactly one key, return that key's value as a string.
	// This matches Vault's idiomatic single-string secret encoding
	// ({"value": "..."}) without forcing every function manifest to
	// declare a format.
	if ref.Format == "" || ref.Format == "raw" {
		if len(data) == 1 {
			for _, v := range data {
				return stringify(v), nil
			}
		}
		// Fall back to a deterministic JSON re-encoding so the function
		// receives the whole data map. Sorting via json.Marshal isn't
		// stable across versions, but the runtime never compares
		// secret bytes — only the function consumes them.
		raw, err := json.Marshal(data)
		if err != nil {
			return "", cserrors.Wrap(cserrors.CSSecretUnavailable, "re-encode vault payload", err)
		}
		return string(raw), nil
	}

	// json-field shortcut: Vault already returns the structured map so
	// we can answer directly without re-marshalling.
	if strings.HasPrefix(ref.Format, "json-field:") {
		key := strings.TrimPrefix(ref.Format, "json-field:")
		if key == "" {
			return "", cserrors.New(cserrors.CSSecretNotFound, "json-field format requires a key")
		}
		v, ok := data[key]
		if !ok {
			return "", cserrors.New(cserrors.CSSecretNotFound, fmt.Sprintf("vault payload missing field %q", key))
		}
		return stringify(v), nil
	}

	return "", cserrors.New(cserrors.CSSecretNotFound, "unsupported secret format: "+ref.Format)
}

func stringify(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case bool:
		if val {
			return "true"
		}
		return "false"
	case float64:
		return fmt.Sprintf("%v", val)
	case nil:
		return ""
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return ""
		}
		return string(b)
	}
}
