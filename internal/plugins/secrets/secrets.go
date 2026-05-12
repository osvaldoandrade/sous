// Package secrets defines the pluggable secret-provider extension point used
// by the cs-invoker-pool to resolve function-level secret references at
// activation start. Drivers live under sub-packages (memory, vault) and
// register themselves with internal/plugins/registry through their package
// init() functions, mirroring the authn/persistence/messaging plugin pattern
// described in internal/plugins/registry/registry.go.
//
// The provider model intentionally avoids any persistence layer: secret
// material is fetched on demand for each activation, handed to the runtime
// in-memory through the cs.env.get host binding, and never written to the
// bundle, KVRocks, or activation logs. See docs/15-security.md "Secrets" for
// the operator-facing contract.
package secrets

import (
	"context"
	"fmt"
	"strings"
)

// Provider resolves logical secret references for a tenant. Drivers must
// honour ctx cancellation so the invoker can fail fast when an activation
// deadline elapses while a backing vault is unreachable.
type Provider interface {
	// Get resolves a single SecretRef for the supplied tenant and returns
	// the raw secret value as a string. Drivers should return a wrapped
	// CSError with code CSSecretNotFound when the underlying store has no
	// entry at the requested path; other failures (auth, transport) may
	// surface as plain errors and will be wrapped into CSSecretUnavailable
	// by the invoker.
	Get(ctx context.Context, tenant string, ref SecretRef) (string, error)
	// Close releases any pooled resources held by the driver. Memory
	// drivers are typically no-op; networked drivers should drain any
	// background lease-renewal goroutines from here.
	Close() error
}

// SecretRef describes one secret a function version wants injected at
// activation start. Name is the logical environment-variable key the
// function sees through cs.env.get(Name); Path is the provider-specific
// lookup path (e.g. a KV v2 path like "tenants/acme/stripe"); Format
// controls post-processing of the raw value returned by the driver.
//
// Supported Format values:
//   - ""          : raw bytes (default).
//   - "raw"       : explicit raw bytes.
//   - "json-field:<key>" : the driver returns a JSON document and the
//     invoker extracts <key> as a string. Used to share one Vault entry
//     across multiple env vars without leaking the JSON wrapper.
type SecretRef struct {
	Name   string
	Path   string
	Format string
}

// ParseRef converts a VersionConfig.Secrets entry into a SecretRef. The
// wire format is intentionally minimal so existing []string manifests stay
// valid without a v2 schema bump:
//
//	"DB_PASSWORD"                             -> Name=DB_PASSWORD, Path=DB_PASSWORD
//	"DB_PASSWORD=secret/data/payments/db"     -> Name=DB_PASSWORD, Path=...
//	"DB_PASSWORD=secret/data/payments/db#json-field:password"
//	                                          -> Format="json-field:password"
//
// The substring after "#" is copied verbatim into SecretRef.Format and
// later interpreted by ExtractValue or by the driver itself.
//
// Entries with empty Name (after trimming) return an error so the invoker
// can surface CS_VALIDATION_FAILED instead of silently dropping the secret.
func ParseRef(spec string) (SecretRef, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return SecretRef{}, fmt.Errorf("empty secret reference")
	}
	name, path := spec, spec
	if idx := strings.Index(spec, "="); idx >= 0 {
		name = strings.TrimSpace(spec[:idx])
		path = strings.TrimSpace(spec[idx+1:])
	}
	if name == "" {
		return SecretRef{}, fmt.Errorf("secret reference %q has empty name", spec)
	}
	if path == "" {
		path = name
	}
	format := ""
	if idx := strings.Index(path, "#"); idx >= 0 {
		format = strings.TrimSpace(path[idx+1:])
		path = strings.TrimSpace(path[:idx])
	}
	return SecretRef{Name: name, Path: path, Format: format}, nil
}

// ExtractValue applies SecretRef.Format to a raw secret payload returned by
// a driver. Drivers should call ExtractValue themselves so each one can
// short-circuit the json decode when the underlying store already exposes
// individual fields natively (Vault KV v2 does, AWS Secrets Manager does
// not). Returns the formatted value or an error compatible with the
// CS_SECRET_NOT_FOUND / CS_VALIDATION_FAILED contract.
func ExtractValue(raw string, format string) (string, error) {
	if format == "" || format == "raw" {
		return raw, nil
	}
	if strings.HasPrefix(format, "json-field:") {
		key := strings.TrimPrefix(format, "json-field:")
		if key == "" {
			return "", fmt.Errorf("json-field format requires a key")
		}
		value, ok, err := jsonField(raw, key)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", fmt.Errorf("json field %q not found", key)
		}
		return value, nil
	}
	return "", fmt.Errorf("unsupported secret format %q", format)
}
