package cadence

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestJSONCodecRoundTrip(t *testing.T) {
	c := JSONCodec{}
	in := map[string]any{"hello": "world", "count": float64(7)}
	raw, err := c.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Ensure it's actually JSON.
	if !json.Valid(raw) {
		t.Fatalf("expected valid JSON, got %q", raw)
	}
	var out map[string]any
	if err := c.Decode(raw, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["hello"] != "world" || out["count"].(float64) != 7 {
		t.Fatalf("round-trip mismatch: %#v", out)
	}
	if got := c.ContentType(); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}
}

func TestMsgpackCodecRoundTrip(t *testing.T) {
	c := MsgpackCodec{}
	type payload struct {
		Status string `msgpack:"status"`
		Count  int    `msgpack:"count"`
	}
	in := payload{Status: "ok", Count: 42}
	raw, err := c.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("expected non-empty msgpack payload")
	}
	// Msgpack output must NOT be valid JSON for a struct of this shape —
	// this guards against a regression where MsgpackCodec silently falls
	// back to JSON.
	if json.Valid(raw) {
		t.Fatalf("msgpack payload should not be valid JSON: %q", raw)
	}
	var out payload
	if err := c.Decode(raw, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("round-trip mismatch: got %+v want %+v", out, in)
	}
	if got := c.ContentType(); got != "application/msgpack" {
		t.Fatalf("content type = %q", got)
	}
}

func TestRawBytesCodecRoundTrip(t *testing.T) {
	c := RawBytesCodec{}
	in := []byte{0x00, 0x01, 0xfe, 0xff, 'h', 'i'}

	raw, err := c.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !bytes.Equal(raw, in) {
		t.Fatalf("raw encode mutated payload: %v vs %v", raw, in)
	}

	var out []byte
	if err := c.Decode(raw, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(out, in) {
		t.Fatalf("raw decode mismatch: %v vs %v", out, in)
	}
	if got := c.ContentType(); got != "application/octet-stream" {
		t.Fatalf("content type = %q", got)
	}

	// String input is accepted (it's a common form of opaque payload).
	enc, err := c.Encode("plain")
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	if string(enc) != "plain" {
		t.Fatalf("string encode = %q", enc)
	}

	// Wrong type to Encode is an error rather than silent JSON fallback.
	if _, err := c.Encode(struct{ X int }{X: 1}); err == nil {
		t.Fatal("expected error encoding struct via raw codec")
	}
	if err := c.Decode([]byte("x"), &out); err != nil {
		t.Fatalf("re-decode: %v", err)
	}
	// Wrong type to Decode is an error.
	var notBytes int
	if err := c.Decode([]byte("x"), &notBytes); err == nil {
		t.Fatal("expected error decoding into *int via raw codec")
	}
}

func TestLookupCodecFallback(t *testing.T) {
	if _, ok := LookupCodec("").(JSONCodec); !ok {
		t.Fatal("empty name should resolve to JSONCodec")
	}
	if _, ok := LookupCodec("json").(JSONCodec); !ok {
		t.Fatal("json should resolve to JSONCodec")
	}
	if _, ok := LookupCodec("msgpack").(MsgpackCodec); !ok {
		t.Fatal("msgpack should resolve to MsgpackCodec")
	}
	if _, ok := LookupCodec("raw").(RawBytesCodec); !ok {
		t.Fatal("raw should resolve to RawBytesCodec")
	}
	if _, ok := LookupCodec("does-not-exist").(JSONCodec); !ok {
		t.Fatal("unknown codec should fall back to JSONCodec")
	}
}

func TestIsCodecRegistered(t *testing.T) {
	for _, name := range []string{"", "json", "msgpack", "raw"} {
		if !IsCodecRegistered(name) {
			t.Fatalf("expected codec %q to be registered", name)
		}
	}
	if IsCodecRegistered("totally-unknown") {
		t.Fatal("unknown codec must be reported as unregistered")
	}
}

// dummyCodec is a no-op codec used to verify RegisterCodec installs a
// codec without disturbing the built-ins.
type dummyCodec struct{}

func (dummyCodec) Encode(any) ([]byte, error) { return nil, nil }
func (dummyCodec) Decode([]byte, any) error   { return nil }
func (dummyCodec) ContentType() string        { return "application/x-dummy" }

func TestRegisterCodec(t *testing.T) {
	RegisterCodec("dummy-e802", dummyCodec{})
	if !IsCodecRegistered("dummy-e802") {
		t.Fatal("expected dummy codec to register")
	}
	if _, ok := LookupCodec("dummy-e802").(dummyCodec); !ok {
		t.Fatal("expected LookupCodec to return dummyCodec")
	}

	// Nil codec is rejected silently.
	RegisterCodec("nil-codec", nil)
	if IsCodecRegistered("nil-codec") {
		t.Fatal("nil codec should not register")
	}

	// Empty name is reserved and cannot be overwritten.
	RegisterCodec("", dummyCodec{})
	if _, ok := LookupCodec("").(JSONCodec); !ok {
		t.Fatal("empty-name registration must be a no-op")
	}
}
