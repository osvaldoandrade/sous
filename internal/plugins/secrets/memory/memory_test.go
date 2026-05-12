package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/secrets"
	"github.com/osvaldoandrade/sous/internal/plugins/secrets/memory"
)

func TestMemoryRoundTrip(t *testing.T) {
	p := memory.New(map[string]string{
		"payments/stripe_key": "sk_live_abc",
		"payments/json":       `{"api_key":"sk_live_xyz","tier":42}`,
	})
	t.Cleanup(func() { _ = p.Close() })

	got, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name: "STRIPE_KEY",
		Path: "payments/stripe_key",
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "sk_live_abc" {
		t.Fatalf("want sk_live_abc, got %q", got)
	}
}

func TestMemoryJSONField(t *testing.T) {
	p := memory.New(map[string]string{
		"payments/json": `{"api_key":"sk_live_xyz","tier":42}`,
	})
	got, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name:   "TIER",
		Path:   "payments/json",
		Format: "json-field:tier",
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "42" {
		t.Fatalf("want 42, got %q", got)
	}
}

func TestMemoryMissingPath(t *testing.T) {
	p := memory.New(nil)
	_, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{
		Name: "MISSING",
		Path: "nope/nada",
	})
	if err == nil {
		t.Fatal("expected error for missing path")
	}
	var cserr *cserrors.CSError
	if !errors.As(err, &cserr) {
		t.Fatalf("expected *CSError, got %T (%v)", err, err)
	}
	if cserr.Code != cserrors.CSSecretNotFound {
		t.Fatalf("expected CS_SECRET_NOT_FOUND, got %s", cserr.Code)
	}
}

func TestMemoryFactoryFromConfig(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.Secrets.Memory.Seed = map[string]string{"k": "v"}
	p, err := memory.NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	got, err := p.Get(context.Background(), "t_x", secrets.SecretRef{Name: "K", Path: "k"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "v" {
		t.Fatalf("got %q", got)
	}
}

func TestMemorySetUpdatesEntry(t *testing.T) {
	p := memory.New(nil)
	p.Set("api/token", "v1")
	got, err := p.Get(context.Background(), "t_acme", secrets.SecretRef{Name: "API", Path: "api/token"})
	if err != nil || got != "v1" {
		t.Fatalf("v1 get: got=%q err=%v", got, err)
	}
	p.Set("api/token", "v2")
	got, err = p.Get(context.Background(), "t_acme", secrets.SecretRef{Name: "API", Path: "api/token"})
	if err != nil || got != "v2" {
		t.Fatalf("v2 get: got=%q err=%v", got, err)
	}
}
