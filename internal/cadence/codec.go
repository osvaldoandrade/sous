package cadence

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// Codec encodes and decodes activity payloads on the wire between
// cs-cadence-poller and a Cadence-compatible workflow service. The
// interface intentionally mirrors `encoding/json` so a binding can swap
// serialization formats without touching the poller's task-handling
// machinery — see docs/12-cadence-integration.md for the per-tasklist
// negotiation contract.
type Codec interface {
	// Encode serializes a Go value into the wire bytes shipped to Cadence.
	Encode(v any) ([]byte, error)
	// Decode populates v from the wire bytes received from Cadence. v must
	// be a non-nil pointer (with the exception of RawBytesCodec which
	// expects a *[]byte).
	Decode(data []byte, v any) error
	// ContentType reports the MIME type that describes the encoded
	// payload, used for logging and for downstream debugging tools.
	ContentType() string
}

// JSONCodec serializes payloads as UTF-8 JSON. It is the historical
// default and the fallback used when a binding does not declare a codec.
type JSONCodec struct{}

func (JSONCodec) Encode(v any) ([]byte, error)    { return json.Marshal(v) }
func (JSONCodec) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }
func (JSONCodec) ContentType() string             { return "application/json" }

// MsgpackCodec serializes payloads with github.com/vmihailenco/msgpack/v5.
// It interoperates with Java / Go workflow workers that already speak
// msgpack on a given tasklist (see issue #28).
type MsgpackCodec struct{}

func (MsgpackCodec) Encode(v any) ([]byte, error)    { return msgpack.Marshal(v) }
func (MsgpackCodec) Decode(data []byte, v any) error { return msgpack.Unmarshal(data, v) }
func (MsgpackCodec) ContentType() string             { return "application/msgpack" }

// RawBytesCodec is a passthrough codec: encode accepts a []byte (or
// *[]byte / string) and emits it verbatim; decode requires a *[]byte and
// copies the wire payload into it. RawBytesCodec is the escape hatch for
// tasklists that carry an opaque binary blob (Thrift, protobuf, compressed
// payloads) whose schema the poller never has to understand.
type RawBytesCodec struct{}

func (RawBytesCodec) Encode(v any) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		out := make([]byte, len(t))
		copy(out, t)
		return out, nil
	case *[]byte:
		if t == nil {
			return nil, nil
		}
		out := make([]byte, len(*t))
		copy(out, *t)
		return out, nil
	case string:
		return []byte(t), nil
	default:
		return nil, fmt.Errorf("raw codec: expected []byte, got %T", v)
	}
}

func (RawBytesCodec) Decode(data []byte, v any) error {
	switch t := v.(type) {
	case *[]byte:
		if t == nil {
			return fmt.Errorf("raw codec: decode target is nil *[]byte")
		}
		buf := make([]byte, len(data))
		copy(buf, data)
		*t = buf
		return nil
	case *any:
		if t == nil {
			return fmt.Errorf("raw codec: decode target is nil *any")
		}
		buf := make([]byte, len(data))
		copy(buf, data)
		*t = buf
		return nil
	default:
		return fmt.Errorf("raw codec: expected *[]byte, got %T", v)
	}
}

func (RawBytesCodec) ContentType() string { return "application/octet-stream" }

var (
	codecMu       sync.RWMutex
	codecRegistry = map[string]Codec{
		"":        JSONCodec{},
		"json":    JSONCodec{},
		"msgpack": MsgpackCodec{},
		"raw":     RawBytesCodec{},
	}
)

// RegisterCodec installs (or replaces) a codec under the given name. The
// empty name is reserved for the default JSON codec and cannot be
// overwritten; attempts to do so are a no-op.
func RegisterCodec(name string, c Codec) {
	if c == nil {
		return
	}
	codecMu.Lock()
	defer codecMu.Unlock()
	if name == "" {
		return
	}
	codecRegistry[name] = c
}

// LookupCodec returns the codec registered under name. The empty name and
// any unknown name resolve to JSONCodec so legacy bindings (which have no
// codec field) keep working unchanged.
func LookupCodec(name string) Codec {
	codecMu.RLock()
	defer codecMu.RUnlock()
	if c, ok := codecRegistry[name]; ok {
		return c
	}
	return JSONCodec{}
}

// IsCodecRegistered reports whether name maps to a known codec. The empty
// name resolves to the default JSON codec and is treated as registered.
// Callers use this at binding-create time to reject typo'd codec names
// with CS_VALIDATION_UNSUPPORTED_CODEC before any task is polled.
func IsCodecRegistered(name string) bool {
	if name == "" {
		return true
	}
	codecMu.RLock()
	defer codecMu.RUnlock()
	_, ok := codecRegistry[name]
	return ok
}
