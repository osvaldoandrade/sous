package signing

// LoadActiveTenantKey is the shared persistence-aware loader for the
// tenant's active Ed25519 public-key record. It lives here (rather
// than in cs-control's main package) so cmd/cs-control and
// cmd/cs-invoker-pool can both call it without cross-importing each
// other's main package.

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"strings"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
)

// KVReader is the read-only persistence slice this loader needs. It
// matches the KVGet shape of persistence.Provider so callers can pass
// a Provider directly.
type KVReader interface {
	KVGet(ctx context.Context, key string) (string, error)
}

// LoadActiveTenantKey reads the active signing-key record from KV. The
// notFoundCode lets callers pick the typed error they want surfaced
// when the tenant has not rotated yet: cs-control uses
// CS_SIGNATURE_KEY_NOT_FOUND (404) at publish; the invoker uses
// CS_SIGNATURE_INVALID (so a tampered/rotated key drops with the same
// code as a bad signature).
func LoadActiveTenantKey(ctx context.Context, store KVReader, tenant string, notFoundCode cserrors.Code) (api.TenantSigningKey, error) {
	raw, err := store.KVGet(ctx, kv.TenantSigningKeysKey(tenant))
	if err != nil {
		// Persistence-layer "not found" surfaces as CSValidationFailed
		// or CSKVReadFailed; translate either into the caller's chosen
		// not-found code. Other errors (CSKVUnavailable, etc.) pass
		// through so operators see the real failure mode.
		var cs *cserrors.CSError
		if stderrors.As(err, &cs) {
			switch cs.Code {
			case cserrors.CSValidationFailed, cserrors.CSKVReadFailed:
				return api.TenantSigningKey{}, cserrors.New(notFoundCode,
					fmt.Sprintf("tenant %s has no active signing key", tenant))
			}
		}
		return api.TenantSigningKey{}, err
	}
	if strings.TrimSpace(raw) == "" {
		return api.TenantSigningKey{}, cserrors.New(notFoundCode,
			fmt.Sprintf("tenant %s has no active signing key", tenant))
	}
	var rec api.TenantSigningKey
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return api.TenantSigningKey{}, cserrors.Wrap(cserrors.CSValidationFailed,
			"corrupt signing key record", err)
	}
	return rec, nil
}
