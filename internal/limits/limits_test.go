package limits

import (
	"testing"

	"github.com/osvaldoandrade/sous/internal/config"
)

func TestDefaultsMatchSpec(t *testing.T) {
	d := Defaults()
	// Cross-check against docs/26-capacity-and-limits.md.
	cases := map[string]struct {
		got, want int
	}{
		"MaxBundleBytes":       {d.MaxBundleBytes, 16 * 1024 * 1024},
		"MaxBodyBytes":         {d.MaxBodyBytes, 6 * 1024 * 1024},
		"MaxHeaderBytes":       {d.MaxHeaderBytes, 64 * 1024},
		"MaxQueryBytes":        {d.MaxQueryBytes, 16 * 1024},
		"MaxResultBytes":       {d.MaxResultBytes, 256 * 1024},
		"MaxErrorBytes":        {d.MaxErrorBytes, 64 * 1024},
		"MaxLogBytes":          {d.MaxLogBytes, 1024 * 1024},
		"DraftTTLSeconds":      {d.DraftTTLSeconds, 86400},
		"ActivationTTLSeconds": {d.ActivationTTLSeconds, 604800},
		"TenantMaxInflight":    {d.TenantMaxInflight, 64},
	}
	for name, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: got %d want %d", name, c.got, c.want)
		}
	}
}

func TestFromConfigAppliesDefaultsAndOverrides(t *testing.T) {
	cfg := &config.Config{}
	cfg.CSControl.Limits.MaxBundleBytes = 1234
	// Leave everything else zero so defaults are applied.

	l := FromConfig(cfg)
	if l.MaxBundleBytes != 1234 {
		t.Errorf("override not applied, got %d", l.MaxBundleBytes)
	}
	if l.MaxBodyBytes != DefaultMaxBodyBytes {
		t.Errorf("default not applied for MaxBodyBytes, got %d", l.MaxBodyBytes)
	}
	if l.ActivationTTLSeconds != DefaultActTTLSeconds {
		t.Errorf("default not applied for ActivationTTLSeconds, got %d", l.ActivationTTLSeconds)
	}
	if l.TenantMaxInflight != DefaultTenantMaxInflight {
		t.Errorf("default not applied for TenantMaxInflight, got %d", l.TenantMaxInflight)
	}
}

func TestFromConfigRejectsNegative(t *testing.T) {
	cfg := &config.Config{}
	cfg.CSHTTPGateway.Limits.MaxBodyBytes = -1
	cfg.CSHTTPGateway.RateLimits.TenantRPS = -42
	l := FromConfig(cfg)
	if l.MaxBodyBytes != DefaultMaxBodyBytes {
		t.Errorf("negative should fall back to default, got %d", l.MaxBodyBytes)
	}
	if l.TenantRPS != DefaultTenantRPS {
		t.Errorf("negative TenantRPS should fall back to default, got %d", l.TenantRPS)
	}
}
