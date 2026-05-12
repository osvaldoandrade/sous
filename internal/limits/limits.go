// Package limits exposes the v0.1 platform size and quota limits as a single
// typed value loaded from configuration. Every enforcement site in the
// platform — cs-control draft upload, cs-http-gateway invoke, cs-invoker-pool
// result/log writes, codeQ producers — is expected to consult this package so
// that the limits documented in docs/26-capacity-and-limits.md and
// docs/02-requirements.md are enforced uniformly.
//
// The package is intentionally stateless and side-effect free; configuration
// is captured at server start time via FromConfig and threaded through. See
// roadmap task E1.04 (issue #8) for the full enforcement contract.
package limits

import "github.com/osvaldoandrade/sous/internal/config"

// Default limits per docs/26-capacity-and-limits.md.
const (
	DefaultMaxBundleBytes    = 16 * 1024 * 1024 // 16 MiB
	DefaultMaxBodyBytes      = 6 * 1024 * 1024  // 6 MiB
	DefaultMaxHeaderBytes    = 64 * 1024        // 64 KiB
	DefaultMaxQueryBytes     = 16 * 1024        // 16 KiB
	DefaultMaxResultBytes    = 256 * 1024       // 256 KiB
	DefaultMaxErrorBytes     = 64 * 1024        // 64 KiB
	DefaultMaxLogBytes       = 1024 * 1024      // 1 MiB
	DefaultDraftTTLSeconds   = 86400            // 24 h
	DefaultActTTLSeconds     = 604800           // 7 d
	DefaultTenantRPS         = 200
	DefaultFunctionRPS       = 20
	DefaultTenantMaxInflight = 64
)

// Limits is the consolidated, typed view of every platform limit and quota
// knob. All fields are positive integers; defaults are applied by FromConfig
// when the corresponding config value is zero or negative.
type Limits struct {
	// Size limits (bytes).
	MaxBundleBytes int
	MaxBodyBytes   int
	MaxHeaderBytes int
	MaxQueryBytes  int
	MaxResultBytes int
	MaxErrorBytes  int
	MaxLogBytes    int

	// TTLs (seconds).
	DraftTTLSeconds      int
	ActivationTTLSeconds int

	// Quotas.
	TenantRPS         int
	FunctionRPS       int
	TenantMaxInflight int
}

// FromConfig projects a *config.Config into a Limits value, applying spec
// defaults for any unset field. It never returns an error: zero/negative
// inputs are treated as "use the documented default".
func FromConfig(cfg *config.Config) Limits {
	return Limits{
		MaxBundleBytes:       positiveOr(cfg.CSControl.Limits.MaxBundleBytes, DefaultMaxBundleBytes),
		MaxBodyBytes:         positiveOr(cfg.CSHTTPGateway.Limits.MaxBodyBytes, DefaultMaxBodyBytes),
		MaxHeaderBytes:       positiveOr(cfg.CSHTTPGateway.Limits.MaxHeaderBytes, DefaultMaxHeaderBytes),
		MaxQueryBytes:        positiveOr(cfg.CSHTTPGateway.Limits.MaxQueryBytes, DefaultMaxQueryBytes),
		MaxResultBytes:       positiveOr(cfg.CSInvokerPool.Limits.MaxResultBytes, DefaultMaxResultBytes),
		MaxErrorBytes:        positiveOr(cfg.CSInvokerPool.Limits.MaxErrorBytes, DefaultMaxErrorBytes),
		MaxLogBytes:          positiveOr(cfg.CSInvokerPool.Limits.MaxLogBytes, DefaultMaxLogBytes),
		DraftTTLSeconds:      positiveOr(cfg.CSControl.Limits.DraftTTLSeconds, DefaultDraftTTLSeconds),
		ActivationTTLSeconds: positiveOr(cfg.CSControl.Limits.ActTTLSeconds, DefaultActTTLSeconds),
		TenantRPS:            positiveOr(cfg.CSHTTPGateway.RateLimits.TenantRPS, DefaultTenantRPS),
		FunctionRPS:          positiveOr(cfg.CSHTTPGateway.RateLimits.FunctionRPS, DefaultFunctionRPS),
		TenantMaxInflight:    positiveOr(cfg.CSInvokerPool.Workers.MaxInflight, DefaultTenantMaxInflight),
	}
}

// Defaults returns a Limits value populated entirely from the documented
// spec defaults. It is convenient for tests and for services that do not
// load a config file.
func Defaults() Limits {
	return Limits{
		MaxBundleBytes:       DefaultMaxBundleBytes,
		MaxBodyBytes:         DefaultMaxBodyBytes,
		MaxHeaderBytes:       DefaultMaxHeaderBytes,
		MaxQueryBytes:        DefaultMaxQueryBytes,
		MaxResultBytes:       DefaultMaxResultBytes,
		MaxErrorBytes:        DefaultMaxErrorBytes,
		MaxLogBytes:          DefaultMaxLogBytes,
		DraftTTLSeconds:      DefaultDraftTTLSeconds,
		ActivationTTLSeconds: DefaultActTTLSeconds,
		TenantRPS:            DefaultTenantRPS,
		FunctionRPS:          DefaultFunctionRPS,
		TenantMaxInflight:    DefaultTenantMaxInflight,
	}
}

func positiveOr(v, fallback int) int {
	if v <= 0 {
		return fallback
	}
	return v
}
