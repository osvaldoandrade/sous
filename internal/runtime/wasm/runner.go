package wasm

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tetratelabs/wazero"

	apipkg "github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	rt "github.com/osvaldoandrade/sous/internal/runtime"
)

// entryFile is the canonical name of the WebAssembly module inside a
// cs-wasm bundle. The bundle tar must contain manifest.json plus this
// file; everything else is ignored at execute time. The control plane
// rejects bundles whose manifest declares "cs-wasm" but ships any
// other entry name at publish time (see manifest validation).
const entryFile = "module.wasm"

// Runner is the cs-wasm execution adapter. Each call to Execute spins
// up a fresh wazero runtime, instantiates the publisher's module
// against the cs.* host module, calls the exported handler, and
// tears the runtime down again. The Runner itself is reusable across
// invocations and safe for concurrent use because no per-invocation
// state lives on it.
type Runner struct {
	kv                  rt.KVProvider
	codeq               rt.CodeQProvider
	httpClient          *http.Client
	maxResultBytes      int
	maxErrorBytes       int
	maxLogBytes         int
	allowPrivateIPCheck bool
}

// NewRunner returns a Runner configured with the same provider
// surface as the cs-js runner. Passing nil for kv installs an
// in-memory provider; passing nil for codeq installs the no-op
// publisher; zero or negative size caps fall back to the cs-js
// defaults so cs-wasm and cs-js produce identical truncation
// behaviour for parity fixtures.
func NewRunner(kv rt.KVProvider, codeq rt.CodeQProvider, maxResultBytes, maxErrorBytes, maxLogBytes int) *Runner {
	if kv == nil {
		kv = rt.NewMemoryKV()
	}
	if codeq == nil {
		codeq = rt.NopCodeQ{}
	}
	if maxResultBytes <= 0 {
		maxResultBytes = 256 * 1024
	}
	if maxErrorBytes <= 0 {
		maxErrorBytes = 64 * 1024
	}
	if maxLogBytes <= 0 {
		maxLogBytes = 1 << 20
	}
	return &Runner{
		kv:                  kv,
		codeq:               codeq,
		httpClient:          &http.Client{Timeout: 5 * time.Second},
		maxResultBytes:      maxResultBytes,
		maxErrorBytes:       maxErrorBytes,
		maxLogBytes:         maxLogBytes,
		allowPrivateIPCheck: true,
	}
}

// Execute runs the wasm module shipped in bundleBytes. The signature
// matches the cs-js runner.Execute contract so cs-invoker-pool and
// the local CLI can route between adapters without conditional code
// in the call site. The output ExecutionOutput is the type defined
// by the runtime package and shared across every adapter.
func (r *Runner) Execute(ctx context.Context, bundleBytes []byte, request apipkg.InvocationRequest) rt.ExecutionOutput {
	start := time.Now()
	out := rt.ExecutionOutput{Status: "error"}

	files, err := extractBundle(bundleBytes)
	if err != nil {
		out.Error = &apipkg.InvocationError{Type: "BundleError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSValidationManifest
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}
	manifest, err := apipkg.ParseManifest(files["manifest.json"])
	if err != nil {
		out.Error = &apipkg.InvocationError{Type: "ManifestError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSValidationManifest
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}
	moduleBytes := files[entryFile]
	if len(moduleBytes) == 0 {
		out.Error = &apipkg.InvocationError{Type: "BundleError", Message: "bundle missing " + entryFile}
		out.ResolvedCode = cserrors.CSValidationManifest
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}

	deadline := time.UnixMilli(request.DeadlineMS)
	if deadline.IsZero() || deadline.Before(time.Now()) {
		deadline = time.Now().Add(time.Duration(manifest.Limits.TimeoutMS) * time.Millisecond)
	}
	runCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	bridge := &hostBridge{
		caps:      manifest.Capabilities,
		kv:        r.kv,
		codeq:     r.codeq,
		http:      r.httpClient,
		denyLocal: r.allowPrivateIPCheck,
		logger:    newLogBuffer(r.maxLogBytes),
	}
	bridge.SetContext(runCtx)

	res, execErr := r.runWasm(runCtx, moduleBytes, manifest, request, bridge)

	lines, truncated := bridge.logger.snapshot()
	out.Logs = lines
	out.Truncated = truncated

	if execErr != nil {
		mapped := cserrors.CSRuntimeException
		status := "error"
		errType := "Exception"
		msg := execErr.Error()
		if errors.Is(execErr, context.DeadlineExceeded) || strings.Contains(msg, "timeout") {
			mapped = cserrors.CSRuntimeTimeout
			status = "timeout"
			errType = "Timeout"
		}
		if errors.Is(execErr, errForbiddenImport) {
			mapped = cserrors.CSRuntimeCapDenied
			errType = "ForbiddenImport"
		}
		if len(msg) > r.maxErrorBytes {
			msg = msg[:r.maxErrorBytes]
			out.Truncated = true
		}
		out.Status = status
		out.Error = &apipkg.InvocationError{Type: errType, Message: msg}
		out.ResolvedCode = mapped
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}

	if err := apipkg.ValidateResultShape(res); err != nil {
		out.Status = "error"
		out.Error = &apipkg.InvocationError{Type: "ValidationError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSRuntimeException
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}
	resultRaw, _ := json.Marshal(res)
	if len(resultRaw) > r.maxResultBytes {
		resultRaw = resultRaw[:r.maxResultBytes]
		out.Truncated = true
		var truncatedRes apipkg.FunctionResponse
		if err := json.Unmarshal(resultRaw, &truncatedRes); err == nil {
			res = &truncatedRes
		}
	}
	out.Status = "success"
	out.Result = res
	out.ResolvedCode = ""
	out.DurationMS = time.Since(start).Milliseconds()
	return out
}

// errForbiddenImport is the sentinel error returned by the host when
// the guest tries to import a symbol outside the cs.* allowlist. It
// is wrapped by wazero in module-instantiation errors, so callers
// detect it via errors.Is.
var errForbiddenImport = errors.New("forbidden import")

// runWasm performs the heavy lifting: compile the module, validate
// its imports against the allowlist, instantiate, and invoke handle.
func (r *Runner) runWasm(
	ctx context.Context,
	moduleBytes []byte,
	manifest apipkg.FunctionManifest,
	request apipkg.InvocationRequest,
	bridge *hostBridge,
) (*apipkg.FunctionResponse, error) {

	config := wazero.NewRuntimeConfig().WithCloseOnContextDone(true)
	runtime := wazero.NewRuntimeWithConfig(ctx, config)
	defer runtime.Close(ctx)

	if err := registerHostModule(ctx, runtime, bridge); err != nil {
		return nil, fmt.Errorf("host module: %w", err)
	}

	compiled, err := runtime.CompileModule(ctx, moduleBytes)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	if err := validateImports(compiled); err != nil {
		return nil, err
	}

	// Memory and wall-time caps:
	//   - timeoutMs is enforced by wazero's CloseOnContextDone wiring
	//     against runCtx, set up by the caller via context.WithDeadline.
	//   - memoryMb is upper-bounded by the manifest validator (16..4096)
	//     and indirectly by the deadline (a runaway memory.grow loop
	//     will trip the deadline first). A future patch may add a
	//     hard page cap once wazero exposes one in its public API.
	modCfg := wazero.NewModuleConfig().
		WithName("cs-wasm-guest").
		WithStartFunctions(). // no implicit _start; we'll call it ourselves if present
		WithSysNanotime().
		WithSysWalltime().
		WithRandSource(nil) // deny non-deterministic randomness; user must request explicit cs.random in a future epic

	mod, err := runtime.InstantiateModule(ctx, compiled, modCfg)
	if err != nil {
		// Wazero packages forbidden-import failures as
		// "module[...] imports[name] not exported in module[env]".
		if strings.Contains(err.Error(), "not exported in module") {
			return nil, fmt.Errorf("%w: %s", errForbiddenImport, err.Error())
		}
		return nil, fmt.Errorf("instantiate: %w", err)
	}
	defer mod.Close(ctx)

	alloc := mod.ExportedFunction("cs_alloc")
	if alloc != nil {
		bridge.SetAlloc(alloc)
	}

	// Optionally call _initialize / _start when the guest exposes
	// them. AssemblyScript and TinyGo emit these to wire up runtime
	// state before user code runs.
	for _, name := range []string{"_initialize", "_start"} {
		if fn := mod.ExportedFunction(name); fn != nil {
			if _, err := fn.Call(ctx); err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
		}
	}

	handler := mod.ExportedFunction("handle")
	if handler == nil {
		// Guests that only implement _start are allowed: they're
		// assumed to have already produced a response via host
		// calls (e.g. cs_log_write) and we synthesise a 200.
		return &apipkg.FunctionResponse{StatusCode: 200, Headers: map[string]string{}, Body: ""}, nil
	}

	// Stage the request payload inside the guest. The handler
	// expects (ptr, len) tuples for both the event and the ctx
	// blob so we can stick to the input ABI rules in abi.go.
	mem := mod.Memory()
	if mem == nil {
		return nil, fmt.Errorf("guest does not export memory")
	}

	reqDoc, err := json.Marshal(map[string]any{
		"event": request.Event,
		"ctx": map[string]any{
			"activation_id": request.ActivationID,
			"deadline_ms":   request.DeadlineMS,
			"tenant":        request.Tenant,
			"namespace":     request.Namespace,
			"function":      request.Ref.Function,
			"ref": map[string]any{
				"alias":   request.Ref.Alias,
				"version": request.Ref.Version,
			},
			"trigger": map[string]any{"type": request.Trigger.Type},
			"principal": map[string]any{
				"sub":   request.Principal.Sub,
				"roles": request.Principal.Roles,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	if alloc == nil {
		return nil, fmt.Errorf("guest does not export cs_alloc")
	}
	packed, err := WriteToGuest(ctx, alloc, mem, reqDoc)
	if err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}
	ptr, length := UnpackPtrLen(packed)

	results, err := handler.Call(ctx, uint64(ptr), uint64(length))
	if err != nil {
		return nil, err
	}
	if len(results) == 0 || results[0] == 0 {
		return &apipkg.FunctionResponse{StatusCode: 200, Headers: map[string]string{}, Body: ""}, nil
	}
	respPtr, respLen := UnpackPtrLen(results[0])
	respBytes, err := ReadMemory(mem, respPtr, respLen)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	// Free the response buffer eagerly when the guest exports
	// cs_free; otherwise it lives until the module is closed.
	if free := mod.ExportedFunction("cs_free"); free != nil {
		_, _ = free.Call(ctx, uint64(respPtr), uint64(respLen))
	}

	if len(respBytes) == 0 {
		return &apipkg.FunctionResponse{StatusCode: 200, Headers: map[string]string{}, Body: ""}, nil
	}
	return decodeResponse(respBytes)
}

// decodeResponse normalises the bytes returned by the guest into a
// FunctionResponse. The guest may return either a full JSON envelope
// matching api.FunctionResponse or a raw value; in the latter case
// we wrap it as a 200 with the value JSON-encoded into body, just
// like the cs-js runner.
func decodeResponse(raw []byte) (*apipkg.FunctionResponse, error) {
	var res apipkg.FunctionResponse
	if err := json.Unmarshal(raw, &res); err == nil && res.StatusCode != 0 {
		if res.Headers == nil {
			res.Headers = map[string]string{}
		}
		return &res, nil
	}
	// Not a FunctionResponse-shaped object; treat as raw body.
	return &apipkg.FunctionResponse{
		StatusCode: 200,
		Headers:    map[string]string{},
		Body:       string(raw),
	}, nil
}

// validateImports walks the compiled module's import declarations and
// fails the instantiation if any imported function lives outside the
// "env" module or outside the cs_* allowlist. wazero will also fail
// instantiation when an import is missing, but doing the check
// up-front gives us a precise error code we can surface to operators
// via cserrors.CSRuntimeCapDenied.
//
// We intentionally do NOT register wasi_snapshot_preview1, so any
// module compiled with `tinygo build -target=wasi` that imports
// fd_write / path_open / sock_* / proc_* will fail here. That keeps
// the security model declarative (allowlist, not deny-rules).
func validateImports(compiled wazero.CompiledModule) error {
	allowedHost := map[string]bool{
		exportHTTPFetch:    true,
		exportKVGet:        true,
		exportKVSet:        true,
		exportKVDel:        true,
		exportCodeQPublish: true,
		exportLogWrite:     true,
	}
	for _, imp := range compiled.ImportedFunctions() {
		module, name, _ := imp.Import()
		if module != hostName {
			return fmt.Errorf("%w: import %q from module %q is not allowed", errForbiddenImport, name, module)
		}
		if !allowedHost[name] {
			return fmt.Errorf("%w: host function %q is not allowed", errForbiddenImport, name)
		}
	}
	// Imported memories / tables / globals are likewise denied: the
	// guest must declare its own memory and tables.
	if len(compiled.ImportedMemories()) > 0 {
		return fmt.Errorf("%w: importing memory is not allowed", errForbiddenImport)
	}
	return nil
}

// extractBundle is a wasm-aware tar reader. The shared bundle.ExtractTar
// helper insists on a function.js entry, which doesn't make sense for
// wasm bundles; we read the same canonical tar layout but require
// module.wasm + manifest.json instead.
func extractBundle(data []byte) (map[string][]byte, error) {
	tr := tar.NewReader(bytes.NewReader(data))
	out := make(map[string][]byte)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		out[hdr.Name] = content
	}
	if _, ok := out[entryFile]; !ok {
		return nil, fmt.Errorf("bundle missing %s", entryFile)
	}
	if _, ok := out["manifest.json"]; !ok {
		return nil, fmt.Errorf("bundle missing manifest.json")
	}
	return out, nil
}

// Name implements the runtime.Handler interface so the package can
// register a stable runtime identifier with runtime.DefaultRegistry.
func (r *Runner) Name() string { return apipkg.RuntimeCSWASM }

// init registers a singleton Runner with the process-wide registry so
// the control plane recognises the cs-wasm runtime at publish time.
// The default instance uses in-memory providers; cs-invoker-pool
// replaces it on boot with the wired-up kv/codeq pair via Register().
func init() {
	rt.DefaultRegistry.Register(NewRunner(nil, nil, 0, 0, 0))
}

// Compile-time sanity check: Runner must satisfy runtime.Handler so
// it can register with the DefaultRegistry.
var _ rt.Handler = (*Runner)(nil)

// Allow callers to substitute the runtime API surface in tests. These
// helpers are unexported so the public API stays narrow.
type runnerOption func(*Runner)

func disablePrivateIPCheck() runnerOption {
	return func(r *Runner) { r.allowPrivateIPCheck = false }
}

func withHTTPClient(c *http.Client) runnerOption {
	return func(r *Runner) {
		if c != nil {
			r.httpClient = c
		}
	}
}

// applyOptions is used by tests via the test-only constructor.
func (r *Runner) applyOptions(opts ...runnerOption) *Runner {
	for _, opt := range opts {
		opt(r)
	}
	return r
}
