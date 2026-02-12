package tikti

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/osvaldoandrade/sous/internal/authz"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
)

type Provider struct {
	url        string
	apiKey     string
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
		apiKey:     cfg.Plugins.AuthN.Tikti.APIKey,
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

	payload, _ := json.Marshal(map[string]string{"token": token, "idToken": token})
	introspectionURL, err := withAPIKey(p.url, p.apiKey)
	if err != nil {
		return authz.Principal{}, cserrors.Wrap(cserrors.CSAuthnInvalidToken, "invalid introspection url", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, introspectionURL, bytes.NewReader(payload))
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return authz.Principal{}, cserrors.Wrap(cserrors.CSAuthnInvalidToken, "failed to read introspection response", err)
	}

	principal, active, err := parseIntrospectionResponse(body, token)
	if err != nil {
		return authz.Principal{}, cserrors.Wrap(cserrors.CSAuthnInvalidToken, "invalid introspection response", err)
	}
	if !active {
		return authz.Principal{}, cserrors.New(cserrors.CSAuthnInvalidToken, "inactive token")
	}
	if principal.Exp > 0 && principal.Exp < time.Now().Unix() {
		return authz.Principal{}, cserrors.New(cserrors.CSAuthnExpiredToken, "token expired")
	}
	p.toCache(token, principal)
	return principal, nil
}

func withAPIKey(rawURL, apiKey string) (string, error) {
	if strings.TrimSpace(apiKey) == "" {
		return rawURL, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if q.Get("key") == "" {
		q.Set("key", apiKey)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func parseIntrospectionResponse(body []byte, token string) (authz.Principal, bool, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		return authz.Principal{}, false, err
	}

	claims := parseTokenClaims(token)
	expFromToken := expFromClaims(claims)
	subFromToken := claimString(claims, "sub")
	if subFromToken == "" {
		subFromToken = claimString(claims, "email")
	}
	tenantFromToken := claimString(claims, "tid")
	if tenantFromToken == "" {
		tenantFromToken = claimString(claims, "tenantId")
	}

	// Native introspection contract:
	// {"active":true,"sub":"...","tenant":"...","roles":[...],"exp":...}
	if _, ok := root["active"]; ok {
		var out struct {
			Active bool     `json:"active"`
			Sub    string   `json:"sub"`
			Tenant string   `json:"tenant"`
			Roles  []string `json:"roles"`
			Exp    int64    `json:"exp"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return authz.Principal{}, false, err
		}
		principal := authz.Principal{
			Sub:    firstNonEmpty(out.Sub, subFromToken),
			Tenant: firstNonEmpty(out.Tenant, tenantFromToken),
			Roles:  out.Roles,
			Exp:    out.Exp,
		}
		if principal.Exp == 0 {
			principal.Exp = expFromToken
		}
		return principal, out.Active, nil
	}

	// Tikti lookup contract:
	// {"users":[{"localId":"...","email":"...","role":"ADMIN","tenantId":"...","status":"ACTIVE"}]}
	if _, ok := root["users"]; ok {
		var out struct {
			Users []struct {
				LocalID string `json:"localId"`
				Email   string `json:"email"`
				Role    string `json:"role"`
				Tenant  string `json:"tenantId"`
				Status  string `json:"status"`
			} `json:"users"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return authz.Principal{}, false, err
		}
		if len(out.Users) == 0 {
			return authz.Principal{}, false, nil
		}
		u := out.Users[0]
		active := true
		if status := strings.TrimSpace(strings.ToUpper(u.Status)); status != "" && status != "ACTIVE" {
			active = false
		}
		principal := authz.Principal{
			Sub:    firstNonEmpty(u.LocalID, u.Email, subFromToken),
			Tenant: firstNonEmpty(u.Tenant, tenantFromToken),
			Roles:  rolesFromLookupRole(u.Role),
			Exp:    expFromToken,
		}
		return principal, active, nil
	}

	return authz.Principal{}, false, fmt.Errorf("unsupported response shape")
}

func rolesFromLookupRole(role string) []string {
	role = strings.TrimSpace(role)
	if role == "" {
		return nil
	}
	normalized := strings.ToLower(role)
	if normalized == "admin" {
		return []string{"admin"}
	}
	if normalized == role {
		return []string{role}
	}
	return []string{role, normalized}
}

func parseTokenClaims(token string) map[string]any {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil
	}
	payload, err := decodeBase64URL(parts[1])
	if err != nil {
		return nil
	}
	out := map[string]any{}
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil
	}
	return out
}

func decodeBase64URL(value string) ([]byte, error) {
	if m := len(value) % 4; m != 0 {
		value += strings.Repeat("=", 4-m)
	}
	return base64.URLEncoding.DecodeString(value)
}

func claimString(claims map[string]any, key string) string {
	if len(claims) == 0 {
		return ""
	}
	raw, ok := claims[key]
	if !ok {
		return ""
	}
	v, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(v)
}

func expFromClaims(claims map[string]any) int64 {
	if len(claims) == 0 {
		return 0
	}
	raw, ok := claims["exp"]
	if !ok {
		return 0
	}
	switch v := raw.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
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
