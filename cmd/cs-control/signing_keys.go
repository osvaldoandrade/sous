package main

// Signing-key management surface for E5.02. Tenants rotate an Ed25519
// public key that the publish handler verifies signatures against. The
// private half is returned exactly once on rotate; the control plane
// keeps only the public material under kv.TenantSigningKeysKey.
//
// See docs/15-security.md ("Signing") and docs/04-api-rest.md
// ("Signing keys").

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/authz"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/signing"
)

// SignatureHeader is the canonical HTTP header the publishing agent
// stamps with the base64-encoded detached signature. Documented in
// docs/04-api-rest.md "Publish".
const SignatureHeader = "X-CS-Signature"

// publishSignedVersion is the contract-bearing version stamped into
// CanonicalPayload at publish time. The publisher cannot know the
// monotonically-allocated version yet, so we sign with the sentinel
// value 0; the persisted (sha256, tenant, namespace, function) tuple
// is itself unique once stored. Documented in docs/15-security.md.
const publishSignedVersion = int64(0)

// rotateSigningKeyResponse is the rotate-endpoint return shape. The
// PrivateKey field is the *only* place the private bytes are ever
// returned by the control plane; subsequent GETs return the public
// half only. Callers must persist PrivateKey before discarding the
// response. See docs/15-security.md "Key lifecycle".
type rotateSigningKeyResponse struct {
	KID         string `json:"kid"`
	Algorithm   string `json:"algorithm"`
	PublicKey   []byte `json:"public_key"`
	PrivateKey  []byte `json:"private_key"`
	CreatedAtMS int64  `json:"created_at_ms"`
}

// rotateSigningKey generates a fresh Ed25519 key pair, persists the
// public half (and metadata) under kv.TenantSigningKeysKey, and returns
// the private half to the caller once.
func (s *server) rotateSigningKey(w http.ResponseWriter, r *http.Request) {
	principal, tenant, ok := s.authorizeTenantOnly(w, r, "cs:tenant:signing-key:rotate")
	if !ok {
		return
	}
	if err := api.ValidateTenant(tenant); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationName, err.Error()), requestID(r))
		return
	}

	pair := signing.Generate()
	rec := api.TenantSigningKey{
		KID:         shortKID(),
		Algorithm:   signing.Algorithm,
		PublicKey:   pair.Public,
		CreatedAtMS: time.Now().UnixMilli(),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "marshal signing key", err), requestID(r))
		return
	}
	// TTL=0 — signing keys are long-lived; rotation overwrites the
	// active slot rather than expiring it.
	if err := s.store.KVSet(r.Context(), kv.TenantSigningKeysKey(tenant), string(raw), 0); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	s.auditAfterCommit(r, principal, "tenant.signing_key.rotate",
		fmt.Sprintf("tenant://%s/signing-keys/%s", tenant, rec.KID),
		map[string]any{"kid": rec.KID, "algorithm": rec.Algorithm})

	api.WriteJSON(w, http.StatusOK, rotateSigningKeyResponse{
		KID:         rec.KID,
		Algorithm:   rec.Algorithm,
		PublicKey:   rec.PublicKey,
		PrivateKey:  pair.Private,
		CreatedAtMS: rec.CreatedAtMS,
	})
}

// getActiveSigningKey returns the active public-key metadata for the
// tenant. The private half is never returned — the caller had its one
// shot at rotate time.
func (s *server) getActiveSigningKey(w http.ResponseWriter, r *http.Request) {
	_, tenant, ok := s.authorizeTenantOnly(w, r, "cs:tenant:signing-key:read")
	if !ok {
		return
	}
	if err := api.ValidateTenant(tenant); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationName, err.Error()), requestID(r))
		return
	}
	rec, err := signing.LoadActiveTenantKey(r.Context(), s.store, tenant, cserrors.CSSignatureKeyNotFound)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, rec)
}

// authorizeTenantOnly is the signing-keys variant of authorize(). The
// existing authorize() reads {namespace} and {name} URL params which
// the signing endpoints do not carry — we extract only the tenant and
// run the same principal/role checks.
func (s *server) authorizeTenantOnly(w http.ResponseWriter, r *http.Request, action string) (authz.Principal, string, bool) {
	principal, ok := authz.PrincipalFromContext(r.Context())
	if !ok {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthnInvalidToken, "principal missing"), requestID(r))
		return authz.Principal{}, "", false
	}
	tenant := chi.URLParam(r, "tenant")
	if principal.Tenant != "" && principal.Tenant != tenant {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthzResourceMis, "tenant mismatch"), requestID(r))
		return authz.Principal{}, "", false
	}
	if !authz.CheckAction(principal, action) {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthzDenied, "action denied"), requestID(r))
		return authz.Principal{}, "", false
	}
	return principal, tenant, true
}

// shortKID returns the opaque key identifier the rotate endpoint stamps
// onto every new key. Length matches what the rest of the codebase
// uses for similar opaque IDs (12 hex chars from a uuid).
func shortKID() string {
	return "kid_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
}

// verifyPublishSignature centralises the publish-time signature check.
// Returns the BundleSignature record to persist (nil when the caller
// did not supply one and required=false), or a typed CSError.
//
//   - Missing header + required=true  -> CS_SIGNATURE_MISSING (400)
//   - Missing header + required=false -> (nil, nil) — backwards compat
//   - Malformed base64 / wrong length -> CS_SIGNATURE_INVALID (400)
//   - No tenant key registered        -> CS_SIGNATURE_KEY_NOT_FOUND (404)
//   - Signature does not verify       -> CS_SIGNATURE_INVALID (400)
func verifyPublishSignature(r *http.Request, store persistence.Provider, required bool, shaHex, tenant, namespace, function string) (*api.BundleSignature, error) {
	hdr := strings.TrimSpace(r.Header.Get(SignatureHeader))
	if hdr == "" {
		if required {
			return nil, cserrors.New(cserrors.CSSignatureMissing,
				"X-CS-Signature header is required (plugins.signing.required is true)")
		}
		return nil, nil
	}
	sig, err := decodeBase64Signature(hdr)
	if err != nil {
		return nil, cserrors.Wrap(cserrors.CSSignatureInvalid,
			"X-CS-Signature is not valid base64", err)
	}
	key, err := signing.LoadActiveTenantKey(r.Context(), store, tenant, cserrors.CSSignatureKeyNotFound)
	if err != nil {
		return nil, err
	}
	if key.Algorithm != signing.Algorithm {
		return nil, cserrors.New(cserrors.CSSignatureInvalid,
			fmt.Sprintf("tenant key uses unsupported algorithm %q", key.Algorithm))
	}
	payload, err := signing.CanonicalPayloadFromHexSHA(shaHex, tenant, namespace, function, publishSignedVersion)
	if err != nil {
		return nil, cserrors.Wrap(cserrors.CSValidationFailed,
			"cannot decode bundle sha256", err)
	}
	if !signing.Verify(payload, sig, key.PublicKey) {
		return nil, cserrors.New(cserrors.CSSignatureInvalid,
			"signature did not verify against tenant active key")
	}
	return &api.BundleSignature{
		KID:       key.KID,
		Algorithm: signing.Algorithm,
		Sig:       sig,
		SignedAt:  time.Now().UnixMilli(),
	}, nil
}

// decodeBase64Signature accepts both standard and URL-safe base64 with
// or without padding so an agent stamped from any of the common JS/Go
// helpers round-trips. Returns the raw bytes on success.
func decodeBase64Signature(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	for _, dec := range []func(string) ([]byte, error){
		base64.StdEncoding.DecodeString,
		base64.RawStdEncoding.DecodeString,
		base64.URLEncoding.DecodeString,
		base64.RawURLEncoding.DecodeString,
	} {
		if b, err := dec(s); err == nil {
			return b, nil
		}
	}
	return nil, fmt.Errorf("invalid base64 signature")
}
