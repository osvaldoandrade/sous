package registry

import (
	"fmt"
	"sync"

	"github.com/osvaldoandrade/sous/internal/authz"
	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
)

type (
	AuthNFactory       func(cfg config.Config) (authz.Provider, error)
	PersistenceFactory func(cfg config.Config) (persistence.Provider, error)
	MessagingFactory   func(cfg config.Config) (messaging.Provider, error)
)

var (
	authnMu      sync.RWMutex
	authnDrivers = map[string]AuthNFactory{}

	persistenceMu      sync.RWMutex
	persistenceDrivers = map[string]PersistenceFactory{}

	messagingMu      sync.RWMutex
	messagingDrivers = map[string]MessagingFactory{}
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
