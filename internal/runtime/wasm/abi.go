// Package wasm implements the cs-wasm runtime adapter. It executes a
// publisher-provided WebAssembly module in a sandboxed wazero runtime
// and exposes a tightly scoped host ABI that mirrors the cs.* surface
// available to cs-js functions.
//
// # Host ABI overview
//
// Every host call passes data across the wasm/host boundary as bytes
// resident in the guest's linear memory. Strings and JSON values use
// UTF-8 encoding. Two conventions cover everything:
//
//  1. Input-only calls (the host reads from guest memory) take a pair
//     of u32 arguments: (ptr, len). The host reads len bytes starting
//     at ptr and returns an i32 status code: 0 == ok, negative == one
//     of the StatusErr* constants below. Host errors never panic into
//     the guest; the guest must inspect the status code.
//
//  2. Calls that produce an output buffer return an i64 that packs a
//     (ptr, len) pair: the high 32 bits are the pointer, the low 32
//     bits are the length. A return value of 0 means "no payload"
//     (the call still succeeded or denied silently).
//
// The guest module must export the following symbols so the host can
// hand it bytes for input and read bytes back out:
//
//   - memory                          // wasm linear memory (required)
//   - cs_alloc(size: u32) -> u32      // returns a guest-owned pointer
//   - cs_free(ptr: u32, size: u32)    // optional, called when set
//   - handle(req_ptr: u32, req_len: u32) -> u64  // entry point
//
// The optional cs_free hook lets the guest reclaim memory after the
// host has consumed an output buffer; modules without it leak the
// pointer for the duration of the invocation (which is fine because
// the whole runtime is torn down after Execute returns).
//
// # Status codes
//
// Negative i32 values surface typed runtime errors back to the guest.
// They mirror the cserrors codes used by the cs-js runtime so the
// host can translate them into the same HTTP responses on the way out.
package wasm

import (
	"context"
	"fmt"
	"math"

	"github.com/tetratelabs/wazero/api"
)

// Status codes returned by input-only host calls. Positive values are
// reserved for future use; the guest should treat anything < 0 as a
// terminal error from the host.
const (
	StatusOK             int32 = 0
	StatusErrInvalidArg  int32 = -1
	StatusErrCapDenied   int32 = -2
	StatusErrHostFailure int32 = -3
	StatusErrNotFound    int32 = -4
)

// PackPtrLen packs a (ptr, len) pair into a single i64 the way the
// guest <-> host ABI expects: high 32 bits are the pointer, low 32
// bits are the length. A zero result signals "no payload".
func PackPtrLen(ptr, length uint32) uint64 {
	return (uint64(ptr) << 32) | uint64(length)
}

// UnpackPtrLen is the inverse of PackPtrLen.
func UnpackPtrLen(packed uint64) (ptr, length uint32) {
	return uint32(packed >> 32), uint32(packed & 0xFFFFFFFF)
}

// ReadMemory copies len bytes from the guest memory starting at ptr
// into a freshly allocated host buffer. It returns an error when the
// requested range is out of bounds (a malicious or buggy guest).
func ReadMemory(mem api.Memory, ptr, length uint32) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	if mem == nil {
		return nil, fmt.Errorf("wasm: memory is nil")
	}
	buf, ok := mem.Read(ptr, length)
	if !ok {
		return nil, fmt.Errorf("wasm: read out of bounds ptr=%d len=%d mem=%d", ptr, length, mem.Size())
	}
	// Defensive copy so the host can hold onto the slice across guest
	// memory mutations (memory.grow can invalidate the underlying buffer).
	out := make([]byte, length)
	copy(out, buf)
	return out, nil
}

// ReadString is a thin wrapper around ReadMemory that returns a Go
// string. The string keeps no reference to the guest memory.
func ReadString(mem api.Memory, ptr, length uint32) (string, error) {
	b, err := ReadMemory(mem, ptr, length)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// WriteToGuest allocates a buffer inside the guest by calling cs_alloc,
// copies data into it, and returns the (ptr, len) pair packed via
// PackPtrLen. When data is empty the function returns 0 without
// invoking cs_alloc.
//
// alloc must be the guest's exported cs_alloc function. mem must be
// the same Memory the function uses. The host context is passed
// through to alloc.Call so a deadline on ctx is honoured.
func WriteToGuest(ctx context.Context, alloc api.Function, mem api.Memory, data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if len(data) > math.MaxUint32 {
		return 0, fmt.Errorf("wasm: payload too large: %d bytes", len(data))
	}
	if alloc == nil {
		return 0, fmt.Errorf("wasm: guest does not export cs_alloc")
	}
	if mem == nil {
		return 0, fmt.Errorf("wasm: memory is nil")
	}
	res, err := alloc.Call(ctx, uint64(len(data)))
	if err != nil {
		return 0, fmt.Errorf("wasm: cs_alloc failed: %w", err)
	}
	if len(res) == 0 {
		return 0, fmt.Errorf("wasm: cs_alloc returned no value")
	}
	ptr := uint32(res[0])
	if ptr == 0 {
		return 0, fmt.Errorf("wasm: cs_alloc returned null pointer")
	}
	if ok := mem.Write(ptr, data); !ok {
		return 0, fmt.Errorf("wasm: write out of bounds ptr=%d len=%d mem=%d", ptr, len(data), mem.Size())
	}
	return PackPtrLen(ptr, uint32(len(data))), nil
}

// EncodeStatusU32 packs an int32 status into a uint32 with two's
// complement representation. wazero's host-function plumbing surfaces
// i32 returns as u32; the guest reinterprets the value as i32.
func EncodeStatusU32(s int32) uint32 {
	return uint32(s)
}
