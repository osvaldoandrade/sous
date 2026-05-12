// Package signing implements the publish-time bundle signature primitives
// used by E5.02. The control plane records tenant Ed25519 public keys,
// publish verifies the agent's signature against the active key, and the
// invoker re-verifies on bundle load (see docs/15-security.md).
//
// Only stdlib crypto is used. Ed25519 is fixed because (a) signature
// verification is a few microseconds — cheap enough to do on every cold
// bundle load — and (b) keys are 32 bytes which is small enough to ship
// in a JSON response without paging.
package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
)

// Algorithm is the canonical algorithm identifier persisted alongside
// every signature record. The control plane refuses to verify a record
// whose Algorithm differs from this constant — when we eventually add
// ECDSA P-256, the change is additive: new constant, new switch arm,
// existing records keep verifying with this one.
const Algorithm = "ed25519"

// KeyPair carries the raw Ed25519 key material. The Private field is
// returned to a caller exactly once (on rotate) and immediately dropped
// from the control plane. The Public field is the long-lived value the
// control plane stores under cs:tenant:{tenant}:signing:ed25519:active.
type KeyPair struct {
	Public  []byte
	Private []byte
}

// Generate produces a fresh Ed25519 key pair using crypto/rand. It panics
// only if the system PRNG fails, which we treat as unrecoverable.
func Generate() KeyPair {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		// crypto/rand failures are not recoverable; bubble up so the
		// caller surfaces a 5xx rather than silently issuing a weak key.
		panic("signing: ed25519.GenerateKey failed: " + err.Error())
	}
	return KeyPair{
		Public:  append([]byte(nil), pub...),
		Private: append([]byte(nil), priv...),
	}
}

// Sign returns the detached signature of payload under priv. priv must be
// a 64-byte Ed25519 private key as produced by Generate (the standard
// library layout is seed || public). Returns nil on a malformed key
// length so callers can treat the empty signature as "do not persist".
func Sign(payload, priv []byte) []byte {
	if len(priv) != ed25519.PrivateKeySize {
		return nil
	}
	return ed25519.Sign(ed25519.PrivateKey(priv), payload)
}

// Verify reports whether sig is a valid signature of payload under pub.
// Returns false (rather than panicking) on malformed key or signature
// lengths so the caller can map the result onto a typed CSError.
func Verify(payload, sig, pub []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(pub), payload, sig)
}

// ErrInvalidKeyLength is returned by helpers that need to assert key
// shape before delegating to the stdlib (which would otherwise panic).
var ErrInvalidKeyLength = errors.New("signing: invalid key length")
