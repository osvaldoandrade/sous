package wasm

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apipkg "github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	rt "github.com/osvaldoandrade/sous/internal/runtime"
)

// buildWasmBundle wraps moduleBytes + the supplied manifest in the
// canonical cs-wasm bundle layout (a tar with module.wasm and
// manifest.json). It bypasses bundle.BuildCanonical because that
// helper rejects bundles that lack function.js — cs-wasm bundles
// don't carry one.
func buildWasmBundle(t *testing.T, moduleBytes []byte, manifest apipkg.FunctionManifest) []byte {
	t.Helper()
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	files := []struct {
		name string
		data []byte
	}{
		{"manifest.json", manifestRaw},
		{entryFile, moduleBytes},
	}
	for _, f := range files {
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.data))}); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if _, err := tw.Write(f.data); err != nil {
			t.Fatalf("write data: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	return buf.Bytes()
}

// defaultManifest returns a manifest authorised to use every cs.*
// surface so individual tests can opt-in to capability tests by
// shrinking the allowlist locally.
func defaultManifest() apipkg.FunctionManifest {
	return apipkg.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: apipkg.RuntimeCSWASM,
		Entry:   entryFile,
		Handler: "default",
		Limits: apipkg.ManifestLimits{
			TimeoutMS:      3000,
			MemoryMB:       16,
			MaxConcurrency: 1,
		},
		Capabilities: apipkg.ManifestCapabilities{
			KV:    apipkg.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get", "set", "del"}},
			CodeQ: apipkg.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
			HTTP:  apipkg.ManifestHTTPCaps{AllowHosts: []string{"127.0.0.1"}, TimeoutMS: 1500},
		},
	}
}

func invocation(deadline time.Duration) apipkg.InvocationRequest {
	return apipkg.InvocationRequest{
		ActivationID: "act-1",
		RequestID:    "req-1",
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Ref:          apipkg.FunctionRef{Function: "fn", Version: 1},
		Trigger:      apipkg.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    apipkg.Principal{Sub: "user:1", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(deadline).UnixMilli(),
		Event:        map[string]any{"x": 1},
	}
}

// allocBumpBody returns a code body for a cs_alloc(size: i32) -> i32
// function that always returns a fixed bump address. This is good
// enough for tests: the host calls cs_alloc once per output buffer
// and we have plenty of headroom inside a 1 MiB linear memory.
//
//	(func (param i32) (result i32) i32.const <addr>)
func allocBumpBody(addr int32) []byte {
	body := []byte{opI32Const}
	body = append(body, sleb128i32(addr)...)
	return body
}

// constReturnHandleBody returns a code body for
// handle(req_ptr: i32, req_len: i32) -> i64 that returns the packed
// (ptr, len) of a fixed buffer baked into the module's data segment.
//
// We do this in WASM-spec hex because returning an i64 from a Go
// builder requires composing the high and low halves; the simplest
// portable form is to push two i32 constants and call
// i64.extend_i32_u then i64.shl / i64.or. Wazero accepts the same.
func constReturnHandleBody(ptr, length int32) []byte {
	var body []byte
	// high = i64.extend_i32_u (i32.const ptr) -- this gives us low 32 bits
	body = append(body, opI32Const)
	body = append(body, sleb128i32(ptr)...)
	body = append(body, opI64Extend) // i64.extend_i32_u
	// high << 32
	body = append(body, opI64Const)
	body = append(body, sleb128i32(32)...) // 32 as i64
	body = append(body, opI64Shl)
	// low = i64.extend_i32_u (i32.const length)
	body = append(body, opI32Const)
	body = append(body, sleb128i32(length)...)
	body = append(body, opI64Extend)
	// high | low
	body = append(body, opI64Or)
	return body
}

// buildEchoModule returns a wasm module that:
//   - exports memory (1 page)
//   - exports cs_alloc returning a bump pointer at offset 4096
//   - exports handle returning the packed pointer/length of a static
//     FunctionResponse JSON baked into memory at offset 0
func buildEchoModule(t *testing.T) []byte {
	t.Helper()
	b := &wasmBuilder{}
	tAlloc := b.addType([]byte{valI32}, []byte{valI32})
	tHandle := b.addType([]byte{valI32, valI32}, []byte{valI64})

	allocIdx := b.addFunc(tAlloc, allocBumpBody(4096))

	staticBody := []byte(`{"statusCode":200,"headers":{},"body":"echo-ok"}`)
	b.dataBuf = staticBody
	b.memMin = 1

	handleIdx := b.addFunc(tHandle, constReturnHandleBody(0, int32(len(staticBody))))

	b.addExport("memory", exportMem, 0)
	b.addExport("cs_alloc", exportFunc, allocIdx)
	b.addExport("handle", exportFunc, handleIdx)
	return b.build()
}

func TestRunnerEchoSuccess(t *testing.T) {
	moduleBytes := buildEchoModule(t)
	bundleBytes := buildWasmBundle(t, moduleBytes, defaultManifest())

	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), bundleBytes, invocation(2*time.Second))
	if out.Status != "success" {
		t.Fatalf("status = %s, err=%+v", out.Status, out.Error)
	}
	if out.Result == nil || out.Result.StatusCode != 200 || out.Result.Body != "echo-ok" {
		t.Fatalf("unexpected result: %+v", out.Result)
	}
}

func TestRunnerMissingModule(t *testing.T) {
	bundleBytes := buildBundleMinusEntry(t)
	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), bundleBytes, invocation(time.Second))
	if out.Status != "error" {
		t.Fatalf("expected error, got %s", out.Status)
	}
	if out.ResolvedCode != cserrors.CSValidationManifest {
		t.Fatalf("code = %s, want %s", out.ResolvedCode, cserrors.CSValidationManifest)
	}
}

// buildBundleMinusEntry returns a tar bundle that has a manifest but
// lacks module.wasm, to exercise the missing-entry branch.
func buildBundleMinusEntry(t *testing.T) []byte {
	t.Helper()
	manifestRaw, _ := json.Marshal(defaultManifest())
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	_ = tw.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0o644, Size: int64(len(manifestRaw))})
	_, _ = tw.Write(manifestRaw)
	_ = tw.Close()
	return buf.Bytes()
}

// buildKVModule returns a module that calls cs_kv_set("ctr:hits", "1")
// then cs_kv_get("ctr:hits") and returns a static success envelope.
// The KV side-effects are observed in TestRunnerKVRoundTrip via a
// shared MemoryKV provider.
func buildKVModule(t *testing.T) []byte {
	t.Helper()
	b := &wasmBuilder{}
	tAlloc := b.addType([]byte{valI32}, []byte{valI32})
	tHandle := b.addType([]byte{valI32, valI32}, []byte{valI64})
	// cs_kv_get: (key_ptr, key_len) -> i64
	tKVGet := b.addType([]byte{valI32, valI32}, []byte{valI64})
	// cs_kv_set: (kp, kl, vp, vl, ttl) -> i32
	tKVSet := b.addType([]byte{valI32, valI32, valI32, valI32, valI32}, []byte{valI32})

	kvGetImp := b.addImport(hostName, exportKVGet, tKVGet)
	kvSetImp := b.addImport(hostName, exportKVSet, tKVSet)

	// Data segment layout:
	//   offset 0..6:  key bytes "ctr:hit"
	//   offset 7..7:  value bytes "1"
	//   offset 8..:   response JSON
	key := []byte("ctr:hit")
	value := []byte("1")
	respJSON := []byte(`{"statusCode":200,"headers":{},"body":"kv-ok"}`)
	keyOff := int32(0)
	keyLen := int32(len(key))
	valOff := keyOff + keyLen
	valLen := int32(len(value))
	respOff := valOff + valLen
	respLen := int32(len(respJSON))
	data := append([]byte{}, key...)
	data = append(data, value...)
	data = append(data, respJSON...)
	b.dataBuf = data
	b.memMin = 1

	allocIdx := b.addFunc(tAlloc, allocBumpBody(4096))

	// handle body:
	//   call cs_kv_set(keyOff, keyLen, valOff, valLen, 0); drop
	//   call cs_kv_get(keyOff, keyLen); drop
	//   return packed(respOff, respLen)
	var body []byte
	push := func(v int32) {
		body = append(body, opI32Const)
		body = append(body, sleb128i32(v)...)
	}
	push(keyOff)
	push(keyLen)
	push(valOff)
	push(valLen)
	push(0)
	body = append(body, opCall)
	body = append(body, uleb128(kvSetImp)...)
	body = append(body, 0x1A) // drop
	push(keyOff)
	push(keyLen)
	body = append(body, opCall)
	body = append(body, uleb128(kvGetImp)...)
	body = append(body, 0x1A) // drop (i64)
	body = append(body, constReturnHandleBody(respOff, respLen)...)

	handleIdx := b.addFunc(tHandle, body)
	b.addExport("memory", exportMem, 0)
	b.addExport("cs_alloc", exportFunc, allocIdx)
	b.addExport("handle", exportFunc, handleIdx)
	return b.build()
}

func TestRunnerKVRoundTrip(t *testing.T) {
	moduleBytes := buildKVModule(t)
	bundleBytes := buildWasmBundle(t, moduleBytes, defaultManifest())

	kv := rt.NewMemoryKV()
	r := NewRunner(kv, nil, 0, 0, 0)
	out := r.Execute(context.Background(), bundleBytes, invocation(2*time.Second))
	if out.Status != "success" {
		t.Fatalf("status = %s err=%+v", out.Status, out.Error)
	}
	got, err := kv.Get(context.Background(), "ctr:hit")
	if err != nil {
		t.Fatalf("kv.Get: %v", err)
	}
	if got != "1" {
		t.Fatalf("kv value = %q, want %q", got, "1")
	}
}

func TestRunnerKVCapabilityDenied(t *testing.T) {
	moduleBytes := buildKVModule(t)
	manifest := defaultManifest()
	// Strip the "set" capability so the guest's cs_kv_set returns
	// StatusErrCapDenied. The module otherwise still completes
	// successfully because its handle ignores the set result.
	manifest.Capabilities.KV.Ops = []string{"get"}
	bundleBytes := buildWasmBundle(t, moduleBytes, manifest)

	kv := rt.NewMemoryKV()
	r := NewRunner(kv, nil, 0, 0, 0)
	out := r.Execute(context.Background(), bundleBytes, invocation(2*time.Second))
	if out.Status != "success" {
		t.Fatalf("status = %s err=%+v", out.Status, out.Error)
	}
	// The set call was denied so the kv store must be empty.
	got, err := kv.Get(context.Background(), "ctr:hit")
	if err != nil {
		t.Fatalf("kv.Get: %v", err)
	}
	if got != "" {
		t.Fatalf("expected kv to be empty after denied set, got %q", got)
	}
}

// buildHTTPModule returns a module that issues a single
// cs_http_fetch call with a baked-in request JSON, then returns a
// fixed response envelope.
func buildHTTPModule(t *testing.T, requestJSON []byte) []byte {
	t.Helper()
	b := &wasmBuilder{}
	tAlloc := b.addType([]byte{valI32}, []byte{valI32})
	tHandle := b.addType([]byte{valI32, valI32}, []byte{valI64})
	tFetch := b.addType([]byte{valI32, valI32}, []byte{valI64})
	fetchImp := b.addImport(hostName, exportHTTPFetch, tFetch)

	respJSON := []byte(`{"statusCode":200,"headers":{},"body":"fetch-ok"}`)
	reqOff := int32(0)
	reqLen := int32(len(requestJSON))
	respOff := reqOff + reqLen
	respLen := int32(len(respJSON))
	data := append([]byte{}, requestJSON...)
	data = append(data, respJSON...)
	b.dataBuf = data
	b.memMin = 1

	allocIdx := b.addFunc(tAlloc, allocBumpBody(4096))

	var body []byte
	push := func(v int32) {
		body = append(body, opI32Const)
		body = append(body, sleb128i32(v)...)
	}
	push(reqOff)
	push(reqLen)
	body = append(body, opCall)
	body = append(body, uleb128(fetchImp)...)
	body = append(body, 0x1A) // drop the i64 response packing
	body = append(body, constReturnHandleBody(respOff, respLen)...)
	handleIdx := b.addFunc(tHandle, body)

	b.addExport("memory", exportMem, 0)
	b.addExport("cs_alloc", exportFunc, allocIdx)
	b.addExport("handle", exportFunc, handleIdx)
	return b.build()
}

func TestRunnerHTTPFetch(t *testing.T) {
	var seenURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenURL = r.URL.Path
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("hello"))
	}))
	defer srv.Close()

	reqDoc := map[string]any{
		"url":    srv.URL + "/probe",
		"method": "GET",
	}
	requestJSON, _ := json.Marshal(reqDoc)

	moduleBytes := buildHTTPModule(t, requestJSON)
	manifest := defaultManifest()
	manifest.Capabilities.HTTP.AllowHosts = []string{"127.0.0.1"}
	bundleBytes := buildWasmBundle(t, moduleBytes, manifest)

	r := NewRunner(nil, nil, 0, 0, 0).applyOptions(disablePrivateIPCheck(), withHTTPClient(srv.Client()))
	out := r.Execute(context.Background(), bundleBytes, invocation(2*time.Second))
	if out.Status != "success" {
		t.Fatalf("status = %s err=%+v", out.Status, out.Error)
	}
	if seenURL != "/probe" {
		t.Fatalf("server did not see fetch: %q", seenURL)
	}
}

func TestRunnerHTTPCapabilityDenied(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server must not be hit when host is denied: %s", r.URL.Path)
	}))
	defer srv.Close()

	reqDoc := map[string]any{
		"url":    "http://blocked.example/data",
		"method": "GET",
	}
	requestJSON, _ := json.Marshal(reqDoc)

	moduleBytes := buildHTTPModule(t, requestJSON)
	manifest := defaultManifest()
	manifest.Capabilities.HTTP.AllowHosts = []string{"127.0.0.1"} // not the denied host
	bundleBytes := buildWasmBundle(t, moduleBytes, manifest)

	r := NewRunner(nil, nil, 0, 0, 0).applyOptions(disablePrivateIPCheck(), withHTTPClient(srv.Client()))
	out := r.Execute(context.Background(), bundleBytes, invocation(2*time.Second))
	if out.Status != "success" {
		t.Fatalf("status = %s err=%+v", out.Status, out.Error)
	}
}

// buildForbiddenImportModule returns a module that imports
// wasi_snapshot_preview1.fd_write. The runner must refuse to
// instantiate it.
func buildForbiddenImportModule(t *testing.T) []byte {
	t.Helper()
	b := &wasmBuilder{}
	tFdWrite := b.addType([]byte{valI32, valI32, valI32, valI32}, []byte{valI32})
	b.addImport("wasi_snapshot_preview1", "fd_write", tFdWrite)
	b.memMin = 1
	b.addExport("memory", exportMem, 0)
	return b.build()
}

func TestRunnerForbiddenImport(t *testing.T) {
	moduleBytes := buildForbiddenImportModule(t)
	bundleBytes := buildWasmBundle(t, moduleBytes, defaultManifest())

	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), bundleBytes, invocation(2*time.Second))
	if out.Status == "success" {
		t.Fatalf("expected forbidden-import failure, got success")
	}
	if out.ResolvedCode != cserrors.CSRuntimeCapDenied {
		t.Fatalf("code = %s, want %s (err=%+v)", out.ResolvedCode, cserrors.CSRuntimeCapDenied, out.Error)
	}
	if out.Error == nil || !strings.Contains(strings.ToLower(out.Error.Message), "fd_write") {
		t.Fatalf("expected fd_write in error message, got %+v", out.Error)
	}
}

func TestRunnerHandlesBadBundle(t *testing.T) {
	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), []byte("not-a-tar"), invocation(time.Second))
	if out.Status != "error" || out.ResolvedCode != cserrors.CSValidationManifest {
		t.Fatalf("unexpected output: %+v", out)
	}
}

func TestRunnerName(t *testing.T) {
	r := NewRunner(nil, nil, 0, 0, 0)
	if r.Name() != apipkg.RuntimeCSWASM {
		t.Fatalf("Name() = %q, want %q", r.Name(), apipkg.RuntimeCSWASM)
	}
}

func TestRunnerRegistersWithDefaultRegistry(t *testing.T) {
	if !rt.DefaultRegistry.Has(apipkg.RuntimeCSWASM) {
		t.Fatal("DefaultRegistry must contain cs-wasm after wasm package init")
	}
}

// ensure debug helpers compile even when no test exercises them
var _ = fmt.Sprintf
