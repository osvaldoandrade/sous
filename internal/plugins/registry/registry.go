package registry

import (
	"fmt"
	"sync"

	"github.com/osvaldoandrade/sous/internal/authz"
	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/plugins/secrets"
)

type (
	AuthNFactory       func(cfg config.Config) (authz.Provider, error)
	PersistenceFactory func(cfg config.Config) (persistence.Provider, error)
	MessagingFactory   func(cfg config.Config) (messaging.Provider, error)
	// SecretsFactory mirrors the three factory shapes above for the
	// pluggable secret provider extension point introduced by E6.01.
	// Drivers register through RegisterSecrets() in their init() function
	// so cs-invoker-pool can construct the provider from operator config
	// without depending on the concrete driver at compile time. See
	// internal/plugins/secrets/secrets.go for the provider contract and
	// docs/15-security.md "Secrets" for the operator-facing model.
	SecretsFactory func(cfg config.Config) (secrets.Provider, error)
)

var (
	authnMu      sync.RWMutex
	authnDrivers = map[string]AuthNFactory{}

	persistenceMu      sync.RWMutex
	persistenceDrivers = map[string]PersistenceFactory{}

	messagingMu      sync.RWMutex
	messagingDrivers = map[string]MessagingFactory{}

	secretsMu      sync.RWMutex
	secretsDrivers = map[string]SecretsFactory{}
)

func RegisterAuthN(driver string, factory AuthNFactory) {
	authnMu.Lock()
	defer authnMu.Unlock()
	authnDrivers[driver] = factory
}

func RegisterPersistence(driver string, factory PersistenceFactory) {
	persistenceMu.Lock()
	defer persistenceMu.Unlock()
	persistenceDrivers[driver] = factory
}

func RegisterMessaging(driver string, factory MessagingFactory) {
	messagingMu.Lock()
	defer messagingMu.Unlock()
	messagingDrivers[driver] = factory
}

// RegisterSecrets installs a SecretsFactory under the supplied driver name.
// Drivers call this from their init() function (see
// internal/plugins/secrets/memory and internal/plugins/secrets/vault).
func RegisterSecrets(driver string, factory SecretsFactory) {
	secretsMu.Lock()
	defer secretsMu.Unlock()
	secretsDrivers[driver] = factory
}

func NewAuthN(cfg config.Config) (authz.Provider, error) {
	driver := cfg.Plugins.AuthN.Driver
	authnMu.RLock()
	factory, ok := authnDrivers[driver]
	authnMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("authn plugin not found: %s", driver)
	}
	return factory(cfg)
}

func NewPersistence(cfg config.Config) (persistence.Provider, error) {
	driver := cfg.Plugins.Persistence.Driver
	persistenceMu.RLock()
	factory, ok := persistenceDrivers[driver]
	persistenceMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("persistence plugin not found: %s", driver)
	}
	return factory(cfg)
}

func NewMessaging(cfg config.Config) (messaging.Provider, error) {
	driver := cfg.Plugins.Messaging.Driver
	messagingMu.RLock()
	factory, ok := messagingDrivers[driver]
	messagingMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("messaging plugin not found: %s", driver)
	}
	return factory(cfg)
}

// NewSecrets resolves the configured secrets driver and returns the
// constructed provider. cs-invoker-pool calls this once at startup and
// reuses the same provider across all activations; per-tenant isolation
// happens via the tenant argument on Provider.Get.
func NewSecrets(cfg config.Config) (secrets.Provider, error) {
	driver := cfg.Plugins.Secrets.Driver
	secretsMu.RLock()
	factory, ok := secretsDrivers[driver]
	secretsMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("secrets plugin not found: %s", driver)
	}
	return factory(cfg)
}
