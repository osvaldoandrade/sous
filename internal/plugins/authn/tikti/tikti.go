package tikti

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/osvaldoandrade/sous/internal/authz"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
)

type Provider struct {
	url        string
	httpClient *http.Client
	cacheTTL   time.Duration

	mu    sync.RWMutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	principal authz.Principal
	expiresAt time.Time
}

func init() {
	registry.RegisterAuthN("tikti", NewFromConfig)
}

func NewFromConfig(cfg config.Config) (authz.Provider, error) {
	url := cfg.Plugins.AuthN.Tikti.IntrospectionURL
	if url == "" {
		return nil, fmt.Errorf("plugins.authn.tikti.introspection_url is required")
	}
	cacheTTL := time.Duration(cfg.Plugins.AuthN.Tikti.CacheTTLSeconds) * time.Second
	if cacheTTL <= 0 {
		cacheTTL = 60 * time.Second
	}
	return &Provider{
		url:        url,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		cacheTTL:   cacheTTL,
		cache:      make(map[string]cacheEntry),
	}, nil
}

func (p *Provider) Name() string {
	return "tikti"
}

func (p *Provider) Introspect(ctx context.Context, token string) (authz.Principal, error) {
	if token == "" {
		return authz.Principal{}, cserrors.New(cserrors.CSAuthnMissingToken, "missing bearer token")
	}
	if principal, ok := p.fromCache(token); ok {
		return principal, nil
	}

	payload, _ := json.Marshal(map[string]string{"token": token})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(payload))
	if err != nil {
		return authz.Principal{}, cserrors.Wrap(cserrors.CSAuthnInvalidToken, "failed to create introspection request", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return authz.Principal{}, cserrors.Wrap(cserrors.CSAuthnInvalidToken, "token introspection failed", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized {
			return authz.Principal{}, cserrors.New(cserrors.CSAuthnInvalidToken, "invalid token")
		}
		return authz.Principal{}, cserrors.New(cserrors.CSAuthnInvalidToken, fmt.Sprintf("unexpected introspection status: %d", resp.StatusCode))
	}

	var out struct {
		Active bool     `json:"active"`
		Sub    string   `json:"sub"`
		Tenant string   `json:"tenant"`
		Roles  []string `json:"roles"`
		Exp    int64    `json:"exp"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return authz.Principal{}, cserrors.Wrap(cserrors.CSAuthnInvalidToken, "invalid introspection response", err)
	}
	if !out.Active {
		return authz.Principal{}, cserrors.New(cserrors.CSAuthnInvalidToken, "inactive token")
	}
	if out.Exp > 0 && out.Exp < time.Now().Unix() {
		return authz.Principal{}, cserrors.New(cserrors.CSAuthnExpiredToken, "token expired")
	}
	principal := authz.Principal{Sub: out.Sub, Tenant: out.Tenant, Roles: out.Roles, Exp: out.Exp}
	p.toCache(token, principal)
	return principal, nil
}

func (p *Provider) fromCache(token string) (authz.Principal, bool) {
	if p.cacheTTL <= 0 {
		return authz.Principal{}, false
	}
	p.mu.RLock()
	entry, ok := p.cache[token]
	p.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		return authz.Principal{}, false
	}
	return entry.principal, true
}

func (p *Provider) toCache(token string, principal authz.Principal) {
	if p.cacheTTL <= 0 {
		return
	}
	p.mu.Lock()
	p.cache[token] = cacheEntry{principal: principal, expiresAt: time.Now().Add(p.cacheTTL)}
	p.mu.Unlock()
}
