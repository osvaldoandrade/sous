package signing

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	pair := Generate()
	if len(pair.Public) == 0 || len(pair.Private) == 0 {
		t.Fatalf("Generate returned empty key material: pub=%d priv=%d",
			len(pair.Public), len(pair.Private))
	}
	payload := []byte("hello bundle")
	sig := Sign(payload, pair.Private)
	if len(sig) == 0 {
		t.Fatal("Sign returned empty signature on valid key")
	}
	if !Verify(payload, sig, pair.Public) {
		t.Fatal("Verify failed for valid signature")
	}
	if Verify([]byte("hello rundle"), sig, pair.Public) {
		t.Fatal("Verify accepted tampered payload")
	}
	tampered := append([]byte(nil), sig...)
	tampered[0] ^= 0x01
	if Verify(payload, tampered, pair.Public) {
		t.Fatal("Verify accepted tampered signature")
	}
	wrong := Generate().Public
	if Verify(payload, sig, wrong) {
		t.Fatal("Verify accepted wrong public key")
	}
}

func TestSignMalformedKey(t *testing.T) {
	if got := Sign([]byte("x"), []byte("short")); got != nil {
		t.Fatalf("Sign with short key should return nil, got %d bytes", len(got))
	}
}

func TestVerifyMalformedInputs(t *testing.T) {
	pair := Generate()
	payload := []byte("data")
	sig := Sign(payload, pair.Private)
	if Verify(payload, sig, []byte("short")) {
		t.Fatal("Verify with short public key must fail")
	}
	if Verify(payload, []byte("short"), pair.Public) {
		t.Fatal("Verify with short signature must fail")
	}
}

func TestCanonicalPayloadDeterminism(t *testing.T) {
	sha := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	a := CanonicalPayload(sha, "t_abc123", "ns1", "fn1", 7)
	b := CanonicalPayload(sha, "t_abc123", "ns1", "fn1", 7)
	if !bytes.Equal(a, b) {
		t.Fatalf("CanonicalPayload not deterministic: %x vs %x", a, b)
	}
}

func TestCanonicalPayloadFieldSeparation(t *testing.T) {
	sha := []byte{0xaa, 0xbb}
	baseline := CanonicalPayload(sha, "t_abc", "ns", "fn", 1)
	variants := []struct {
		name                        string
		sha                         []byte
		tenant, namespace, function string
		version                     int64
	}{
		{"different tenant", sha, "t_xyz", "ns", "fn", 1},
		{"different namespace", sha, "t_abc", "ns2", "fn", 1},
		{"different function", sha, "t_abc", "ns", "fn2", 1},
		{"different version", sha, "t_abc", "ns", "fn", 2},
		{"different sha", []byte{0xff}, "t_abc", "ns", "fn", 1},
	}
	for _, tc := range variants {
		out := CanonicalPayload(tc.sha, tc.tenant, tc.namespace, tc.function, tc.version)
		if bytes.Equal(baseline, out) {
			t.Fatalf("%s: payload equals baseline; field is not contributing to signature", tc.name)
		}
	}
}

func TestCanonicalPayloadHasMagic(t *testing.T) {
	out := CanonicalPayload([]byte{0x00}, "t", "n", "f", 0)
	want := "cs.bundle.signature.v1\x00"
	if string(out[:len(want)]) != want {
		t.Fatalf("payload missing magic prefix: %q", out[:len(want)])
	}
}

func TestCanonicalPayloadFromHexSHA(t *testing.T) {
	rawSHA := []byte{0x01, 0x02, 0x03, 0x04}
	hexSHA := hex.EncodeToString(rawSHA)
	a := CanonicalPayload(rawSHA, "t", "n", "f", 0)
	b, err := CanonicalPayloadFromHexSHA(hexSHA, "t", "n", "f", 0)
	if err != nil {
		t.Fatalf("unexpected hex decode error: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("hex-decoded payload mismatch:\n got=%x\nwant=%x", b, a)
	}
	if _, err := CanonicalPayloadFromHexSHA("not-hex", "t", "n", "f", 0); err == nil {
		t.Fatal("expected error for non-hex sha input")
	}
}

func TestSignVerifyOverCanonicalPayload(t *testing.T) {
	pair := Generate()
	payload := CanonicalPayload([]byte{0xde, 0xad, 0xbe, 0xef}, "t_abc123", "ns1", "fn1", 0)
	sig := Sign(payload, pair.Private)
	if !Verify(payload, sig, pair.Public) {
		t.Fatal("Verify failed on canonical payload round-trip")
	}
	otherFn := CanonicalPayload([]byte{0xde, 0xad, 0xbe, 0xef}, "t_abc123", "ns1", "fn2", 0)
	if Verify(otherFn, sig, pair.Public) {
		t.Fatal("Verify accepted signature for a different function name")
	}
}
