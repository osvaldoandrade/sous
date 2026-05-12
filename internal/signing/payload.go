package signing

import (
	"encoding/binary"
	"encoding/hex"
)

// CanonicalPayload returns the byte sequence the publish-time signature
// commits to. The layout is intentionally byte-deterministic so a CI
// signer and the control-plane verifier reach the same bytes without
// negotiating a serializer.
//
// Layout (concatenation, no separators):
//
//	+-------------------------------------------------------------+
//	| "cs.bundle.signature.v1\x00"           (23 bytes, magic)    |
//	| uint32_be(len(bundleSHA)) | bundleSHA  (sha256 raw bytes,   |
//	|                                         32 bytes when valid)|
//	| uint32_be(len(tenant))    | tenant     (utf-8)              |
//	| uint32_be(len(namespace)) | namespace  (utf-8)              |
//	| uint32_be(len(function))  | function   (utf-8)              |
//	| int64_be(version)                                           |
//	+-------------------------------------------------------------+
//
// Why include tenant/namespace/function/version even though bundleSHA
// already pins the bytes: it prevents a tenant from re-using a
// signature against a different (tenant, namespace, function, version)
// tuple. The signature is bound to the (where, what) pair, not just the
// what. In v0.1 callers pass version=0 because the monotonic version
// number is server-allocated; cs-control re-verifies with the same
// constant so signatures round-trip cleanly.
//
// bundleSHA is accepted as raw bytes; callers that hold a hex string
// should run encoding/hex.DecodeString before invoking this function so
// the canonical region stays compact. The publish handler stores the
// SHA in hex on the VersionRecord but signs over the raw bytes.
func CanonicalPayload(bundleSHA []byte, tenant, namespace, function string, version int64) []byte {
	const magic = "cs.bundle.signature.v1\x00"
	size := len(magic) + 4 + len(bundleSHA) + 4 + len(tenant) + 4 + len(namespace) + 4 + len(function) + 8
	out := make([]byte, 0, size)
	out = append(out, magic...)
	out = appendLenPrefixed(out, bundleSHA)
	out = appendLenPrefixed(out, []byte(tenant))
	out = appendLenPrefixed(out, []byte(namespace))
	out = appendLenPrefixed(out, []byte(function))
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], uint64(version))
	out = append(out, v[:]...)
	return out
}

// CanonicalPayloadFromHexSHA is a convenience wrapper that decodes a hex
// SHA-256 string before delegating to CanonicalPayload. Returns the
// payload and any hex decode error so the caller can surface
// CS_SIGNATURE_INVALID without panicking on malformed input.
func CanonicalPayloadFromHexSHA(bundleSHAHex, tenant, namespace, function string, version int64) ([]byte, error) {
	raw, err := hex.DecodeString(bundleSHAHex)
	if err != nil {
		return nil, err
	}
	return CanonicalPayload(raw, tenant, namespace, function, version), nil
}

func appendLenPrefixed(dst, src []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(src)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, src...)
	return dst
}
