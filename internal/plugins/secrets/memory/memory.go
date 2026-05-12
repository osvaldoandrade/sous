// Package memory implements the in-process "memory" secret-provider driver.
// It is the default driver chosen by config.syncPluginConfig when no
// plugins.secrets.driver is set and is intended for local development,
// unit tests, and the cs-cli doctor scaffolding. Production deployments
// should switch to the "vault" driver — see internal/plugins/secrets/vault.
package memory

import (
	"context"
	"sync"

	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
	"github.com/osvaldoandrade/sous/internal/plugins/secrets"
)

// Provider serves SecretRef lookups out of an in-memory map seeded from
// config (plugins.secrets.memory.seed). The map is intentionally tenant-
// agnostic: the seed key is the SecretRef.Path verbatim, which keeps the
// dev-time YAML compact while still letting tests stamp tenant-scoped
// paths like "t_acme/api-key" by hand.
type Provider struct {
	mu   sync.RWMutex
	seed map[string]string
}

func init() {
	registry.RegisterSecrets("memory", NewFromConfig)
}

// NewFromConfig builds a Provider seeded from plugins.secrets.memory.seed.
func NewFromConfig(cfg config.Config) (secrets.Provider, error) {
	return New(cfg.Plugins.Secrets.Memory.Seed), nil
}

// New constructs a Provider over a copy of seed so the caller can mutate
// the original map without affecting the driver. Pass nil for an empty
// store; every Get on an empty store returns CSSecretNotFound.
func New(seed map[string]string) *Provider {
	copySeed := make(map[string]string, len(seed))
	for k, v := range seed {
		copySeed[k] = v
	}
	return &Provider{seed: copySeed}
}

// Set inserts (or replaces) a literal secret value at path. Test-only —
// production callers configure the driver through YAML. It is exported so
// the invoker integration test in cmd/cs-invoker-pool can stage fixtures
// without poking at unexported fields.
func (p *Provider) Set(path, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seed[path] = value
}

// Get returns the literal seed[Path] after applying SecretRef.Format. The
// tenant argument is accepted for interface compatibility; the memory
// driver does not enforce per-tenant isolation because operators wire
// tenant scoping into the seed keys directly when they need it.
func (p *Provider) Get(ctx context.Context, tenant string, ref secrets.SecretRef) (string, error) {
	_ = ctx
	_ = tenant
	p.mu.RLock()
	raw, ok := p.seed[ref.Path]
	p.mu.RUnlock()
	if !ok {
		return "", cserrors.New(cserrors.CSSecretNotFound, "secret not found: "+ref.Path)
	}
	value, err := secrets.ExtractValue(raw, ref.Format)
	if err != nil {
		return "", cserrors.Wrap(cserrors.CSSecretNotFound, "extract secret field", err)
	}
	return value, nil
}

// Close is a no-op for the memory driver. Provided so the cs-invoker-pool
// shutdown path can call Close on every plugin uniformly.
func (p *Provider) Close() error { return nil }
