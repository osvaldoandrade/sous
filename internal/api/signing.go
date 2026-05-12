package api

// BundleSignature is the detached signature record persisted on every
// signed VersionRecord. The shape is stable across releases — adding a
// new algorithm means a new Algorithm value, not a new struct.
//
//   - KID: short opaque identifier the control plane assigned to the
//     signing key when the tenant rotated it. The invoker uses KID to
//     look up the verification key, not to derive any trust.
//   - Algorithm: canonical algorithm tag (e.g. signing.Algorithm).
//   - Sig: the raw detached signature bytes (not base64). JSON encoding
//     handles base64 transparently via []byte semantics.
//   - SignedAt: agent-recorded UNIX millis of the signing moment.
//     Informational only; the canonical signed payload does NOT include
//     this field so clock skew between agents never invalidates a
//     signature. See docs/15-security.md.
type BundleSignature struct {
	KID       string `json:"kid"`
	Algorithm string `json:"algorithm"`
	Sig       []byte `json:"sig"`
	SignedAt  int64  `json:"signed_at_ms,omitempty"`
}

// TenantSigningKey is the record persisted under TenantSigningKeysKey.
// Only the public key bytes and metadata live in the control plane —
// the private half is returned to the rotating caller once and dropped.
type TenantSigningKey struct {
	KID         string `json:"kid"`
	Algorithm   string `json:"algorithm"`
	PublicKey   []byte `json:"public_key"`
	CreatedAtMS int64  `json:"created_at_ms"`
}
