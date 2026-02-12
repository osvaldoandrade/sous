package kvrocks

import (
	"testing"

	"github.com/osvaldoandrade/sous/internal/config"
)

func TestNewFromConfigValidation(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.Persistence.KVRocks.Addr = ""
	if _, err := NewFromConfig(cfg); err == nil {
		t.Fatal("expected required addr error")
	}
}

func TestNewFromConfig(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.Persistence.KVRocks.Addr = "localhost:6666"
	provider, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	if provider == nil {
		t.Fatal("expected provider")
	}
	_ = provider.Close()
}
