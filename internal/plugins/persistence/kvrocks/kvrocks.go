package kvrocks

import (
	"fmt"

	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/kv"
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
	return kv.NewStore(addr, cfg.Plugins.Persistence.KVRocks.Auth.Password), nil
}
