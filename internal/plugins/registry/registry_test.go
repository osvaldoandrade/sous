package registry_test

import (
	"testing"

	"github.com/osvaldoandrade/sous/internal/config"
	_ "github.com/osvaldoandrade/sous/internal/plugins/drivers"
	"github.com/osvaldoandrade/sous/internal/plugins/registry"
)

func TestNewProvidersFromRegisteredDrivers(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.AuthN.Driver = "tikti"
	cfg.Plugins.AuthN.Tikti.IntrospectionURL = "http://tikti.example.com/introspect"
	cfg.Plugins.AuthN.Tikti.CacheTTLSeconds = 60
	cfg.Plugins.Persistence.Driver = "kvrocks"
	cfg.Plugins.Persistence.KVRocks.Addr = "localhost:6666"
	cfg.Plugins.Messaging.Driver = "codeq"
	cfg.Plugins.Messaging.CodeQ.Brokers = []string{"localhost:9092"}
	cfg.Plugins.Messaging.CodeQ.Topics.Invoke = "cs.invoke"
	cfg.Plugins.Messaging.CodeQ.Topics.Results = "cs.results"

	authnProvider, err := registry.NewAuthN(cfg)
	if err != nil {
		t.Fatalf("authn provider: %v", err)
	}
	if authnProvider.Name() != "tikti" {
		t.Fatalf("unexpected authn provider name: %s", authnProvider.Name())
	}

	persistenceProvider, err := registry.NewPersistence(cfg)
	if err != nil {
		t.Fatalf("persistence provider: %v", err)
	}
	if persistenceProvider == nil {
		t.Fatal("expected persistence provider")
	}
	_ = persistenceProvider.Close()

	messagingProvider, err := registry.NewMessaging(cfg)
	if err != nil {
		t.Fatalf("messaging provider: %v", err)
	}
	if messagingProvider == nil {
		t.Fatal("expected messaging provider")
	}
	_ = messagingProvider.Close()
}

func TestUnknownDriverReturnsError(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.AuthN.Driver = "unknown"
	if _, err := registry.NewAuthN(cfg); err == nil {
		t.Fatal("expected unknown authn driver error")
	}

	cfg.Plugins.Persistence.Driver = "unknown"
	if _, err := registry.NewPersistence(cfg); err == nil {
		t.Fatal("expected unknown persistence driver error")
	}

	cfg.Plugins.Messaging.Driver = "unknown"
	if _, err := registry.NewMessaging(cfg); err == nil {
		t.Fatal("expected unknown messaging driver error")
	}
}
