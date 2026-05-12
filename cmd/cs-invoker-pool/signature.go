package main

// Invoker-side bundle signature verification for E5.02. We re-verify the
// publish-time signature against the tenant's currently-active public
// key on every cold load. Versions persisted without a signature
// (legacy or unsigned upload under plugins.signing.required=false) skip
// this path entirely — the caller checks Signature != nil before
// invoking us. See docs/15-security.md.

import (
	"context"
	"fmt"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
	"github.com/osvaldoandrade/sous/internal/signing"
)

// invokeSignedVersion mirrors cmd/cs-control/signing_keys.go. The
// publisher signs with version=0 because monotonic version allocation
// happens after the agent computes its signature; cs-control persists
// the signature alongside the resulting VersionRecord. Re-verifying
// here uses the same constant so signatures round-trip cleanly.
const invokeSignedVersion = int64(0)

// verifyInvokeSignature loads the tenant's active public key and checks
// that the persisted signature still verifies against the canonical
// payload derived from the version metadata. On any failure returns
// CS_SIGNATURE_INVALID so the dispatcher can reject the activation —
// even a "key not found" maps to invalid here because the runtime view
// is "this signed bundle cannot be trusted right now".
func verifyInvokeSignature(ctx context.Context, store persistence.Provider, meta api.VersionRecord, req api.InvocationRequest) error {
	if meta.Signature == nil {
		return nil
	}
	if meta.Signature.Algorithm != signing.Algorithm {
		return cserrors.New(cserrors.CSSignatureInvalid,
			fmt.Sprintf("unsupported signature algorithm %q", meta.Signature.Algorithm))
	}
	key, err := signing.LoadActiveTenantKey(ctx, store, req.Tenant, cserrors.CSSignatureInvalid)
	if err != nil {
		return err
	}
	payload, err := signing.CanonicalPayloadFromHexSHA(meta.SHA256, req.Tenant, req.Namespace, req.Ref.Function, invokeSignedVersion)
	if err != nil {
		return cserrors.Wrap(cserrors.CSSignatureInvalid,
			"cannot decode persisted bundle sha", err)
	}
	if !signing.Verify(payload, meta.Signature.Sig, key.PublicKey) {
		return cserrors.New(cserrors.CSSignatureInvalid,
			"persisted signature does not verify against active tenant key")
	}
	return nil
}
