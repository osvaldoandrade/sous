package kvrocks

import (
	"fmt"

	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/limits"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
)

func init() {
	registry.RegisterPersistence("kvrocks", NewFromConfig)
}

func NewFromConfig(cfg config.Config) (persistence.Provider, error) {
	addr := cfg.Plugins.Persistence.KVRocks.Addr
	if addr == "" {
		return nil, fmt.Errorf("plugins.persistence.kvrocks.addr is required")
	}
	store := kv.NewStore(addr, cfg.Plugins.Persistence.KVRocks.Auth.Password)
	// Project the consolidated limits view into the store so that the
	// activation write path enforces 256 KiB result and 1 MiB log caps
	// uniformly across cs-control, cs-invoker-pool, and any future
	// persistence-backed callers. See internal/limits and
	// docs/26-capacity-and-limits.md.
	store.SetLimits(limits.FromConfig(&cfg))
	return store, nil
}
