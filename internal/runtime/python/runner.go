// Package python implements the cs-python runtime adapter.
//
// v0.1 ships a subprocess-based execution model: every activation spawns
// a fresh `python3 -I -S` child, hands it the canonical bundle layout
// extracted to a per-activation tempdir, and reads back the
// FunctionResponse over stdout. The Runner type satisfies the same
// Execute signature as the cs-js runner so cs-invoker-pool and the
// in-process CLI can route between adapters without conditional code at
// the call site (see internal/runtime/runner.go selectRunner).
//
// Why subprocess for v0.1? The CPython-embed path (linking libpython
// via cgo, or going through gopy) brings a hard build dependency that
// would block CI for every Go-only contributor. The subprocess adapter
// gets us a working cs-python end-to-end today, runs on every box that
// has `python3` on PATH, and ships zero new go.mod dependencies. The
// production adapter is tracked as a follow-up issue and will replace
// the runner body without touching the registry wiring or the manifest
// contract.
//
// Capability gates
//
//	v0.1 best-effort: a small bootstrap script (cs_audit.py) installed
//	into the activation tempdir registers a sys.addaudithook that
//	blocks os.system, subprocess.Popen, os.exec*, and socket.socket.
//	The hook lives inside the child process and provides defence-in-
//	depth, NOT a hermetic sandbox — Python introspection can usually
//	defeat an in-process hook given enough effort. The full capability
//	model (cs.kv.*, cs.codeq.publish, cs.http.fetch, egress allowlist,
//	private-IP block) is intentionally deferred to the embedded-CPython
//	adapter, where in-process host bridges replace the stdout pipe.
//
//	Operators who need stronger isolation should run cs-invoker-pool
//	under a seccomp profile or a per-activation container until the
//	follow-up adapter lands.
//
// Wire format
//
//	The child reads ONE JSON document from stdin (the api.InvocationRequest)
//	and writes ONE JSON document to stdout (an api.FunctionResponse-shaped
//	object — `{statusCode, headers, body, isBase64Encoded}` or a bare value
//	wrapped to {statusCode:200,body:<json>}). Anything the child writes to
//	stderr is captured line-by-line as log output, mirroring cs.log.* in
//	cs-js. The protocol is documented in docs/08c-runtime-cs-python.md.
package python

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	rt "github.com/osvaldoandrade/sous/internal/runtime"
)

// entryFile is the canonical name of the Python module inside a
// cs-python bundle. The bundle tar must contain manifest.json plus
// this file; everything else is ignored at execute time. The control
// plane rejects bundles whose manifest declares "cs-python" but ships
// any other entry name at publish time (manifest validation).
const entryFile = "function.py"

// auditFile is the bootstrap installed into the activation tempdir.
// It registers sys.addaudithook before importing user code so a
// best-effort capability gate is in place from the first byte the
// runtime executes. See packageDoc above for the security caveats.
const auditFile = "__cs_audit.py"

// Runner is the cs-python execution adapter. Each call to Execute
// extracts the bundle to a fresh tempdir, spawns `python3`, pipes the
// request in via stdin, and reads the response from stdout. The
// Runner itself is reusable across invocations and safe for concurrent
// use because no per-invocation state lives on it.
type Runner struct {
	kv             rt.KVProvider
	codeq          rt.CodeQProvider
	maxResultBytes int
	maxErrorBytes  int
	maxLogBytes    int
	// python3Path is the resolved python3 binary used by Execute. Tests
	// override it to point at a stub when needed; production callers
	// leave it empty so the Runner resolves "python3" from PATH at each
	// invocation (giving the operator the option to swap the binary on
	// the running pod without restarting cs-invoker-pool).
	python3Path string
}

// NewRunner returns a Runner wired with the same provider surface as
// the cs-js runner. The KV / CodeQ providers are accepted for parity
// with the cs-js / cs-wasm constructors but are NOT wired into the
// subprocess in v0.1 — see the package doc for the deferred host API
// surface. Passing nil for any provider installs the default no-op.
// Zero or negative size caps fall back to the cs-js defaults so the
// three adapters produce identical truncation behaviour for parity
// fixtures.
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
		kv:             kv,
		codeq:          codeq,
		maxResultBytes: maxResultBytes,
		maxErrorBytes:  maxErrorBytes,
		maxLogBytes:    maxLogBytes,
	}
}

// Name implements the runtime.Handler interface so the package can
// register a stable runtime identifier with runtime.DefaultRegistry.
func (r *Runner) Name() string { return api.RuntimeCSPython }

// Execute runs the function.py shipped in bundleBytes. The signature
// mirrors the cs-js Runner.Execute contract so cs-invoker-pool and the
// local CLI can dispatch by manifest.runtime without conditional code
// in the call site. ExecutionOutput is the shared shape defined by
// internal/runtime.
func (r *Runner) Execute(ctx context.Context, bundleBytes []byte, request api.InvocationRequest) rt.ExecutionOutput {
	start := time.Now()
	out := rt.ExecutionOutput{Status: "error"}

	files, err := bundle.ExtractTar(bundleBytes)
	if err != nil {
		out.Error = &api.InvocationError{Type: "BundleError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSValidationManifest
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}
	manifest, err := api.ParseManifest(files["manifest.json"])
	if err != nil {
		out.Error = &api.InvocationError{Type: "ManifestError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSValidationManifest
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}
	code := files[entryFile]
	if len(code) == 0 {
		out.Error = &api.InvocationError{Type: "BundleError", Message: "bundle missing " + entryFile}
		out.ResolvedCode = cserrors.CSValidationManifest
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}

	python3 := r.python3Path
	if python3 == "" {
		python3 = "python3"
	}
	if _, err := exec.LookPath(python3); err != nil {
		out.Error = &api.InvocationError{Type: "RuntimeUnavailable", Message: "python3 not on PATH: " + err.Error()}
		out.ResolvedCode = cserrors.CSRuntimeException
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}

	deadline := time.UnixMilli(request.DeadlineMS)
	if deadline.IsZero() || deadline.Before(time.Now()) {
		deadline = time.Now().Add(time.Duration(manifest.Limits.TimeoutMS) * time.Millisecond)
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		timeout = time.Duration(manifest.Limits.TimeoutMS) * time.Millisecond
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Per-activation tempdir. We do NOT rely on a single shared scratch
	// space: every activation gets a fresh directory rooted at
	// os.TempDir() so two concurrent activations cannot see each
	// other's bytes. The dir is removed in defer; failures to remove
	// are silently swallowed because the activation has already
	// returned a result by that point.
	dir, err := os.MkdirTemp("", "cs-py-")
	if err != nil {
		out.Error = &api.InvocationError{Type: "TempDirError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSRuntimeException
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}
	defer os.RemoveAll(dir)

	if err := writeBundleFiles(dir, files); err != nil {
		out.Error = &api.InvocationError{Type: "TempDirError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSRuntimeException
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}
	if err := os.WriteFile(filepath.Join(dir, auditFile), auditHookSource, 0o600); err != nil {
		out.Error = &api.InvocationError{Type: "TempDirError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSRuntimeException
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}

	reqJSON, err := marshalRequest(request)
	if err != nil {
		out.Error = &api.InvocationError{Type: "MarshalError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSRuntimeException
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}

	// The -I flag turns on isolated mode (ignore PYTHON* env vars,
	// ignore user site-packages). -S suppresses the implicit `import
	// site`, which would otherwise pull in arbitrary path hooks from
	// the system. Both flags are documented at
	// https://docs.python.org/3/using/cmdline.html.
	//
	// The bootstrap script (chained via -c) installs the audit hook
	// FIRST so user code never executes without the gate, then exec()s
	// function.py with the request JSON wired up to a module-level
	// `event` and `ctx` plus a real sys.stdin pipe. We deliberately
	// keep this inline rather than calling exec via a file because a
	// single -c argument prevents the user from shadowing the audit
	// bootstrap by shipping their own __cs_audit.py.
	bootstrap := `import os, sys; ` +
		`exec(open(os.path.join(os.getcwd(), '` + auditFile + `'), 'rb').read(), {'__name__': '__cs_audit__'}); ` +
		`exec(open(os.path.join(os.getcwd(), '` + entryFile + `'), 'rb').read(), {'__name__': '__main__'})`

	cmd := exec.CommandContext(runCtx, python3, "-I", "-S", "-c", bootstrap)
	cmd.Dir = dir
	cmd.Stdin = bytes.NewReader(reqJSON)
	// Scrub the environment: the child only sees PATH (so it can find
	// shared libraries via the dynamic linker on systems that need it)
	// and a TMPDIR rooted inside our scratch dir. No HOME, no PYTHON*,
	// no operator-supplied env. The cs.env.* surface is deferred to
	// the embedded adapter; surfacing the invoker-pool's secret map
	// through process env would defeat the whole point of E6.01.
	cmd.Env = childEnv(dir)

	stdout := &boundedBuffer{max: r.maxResultBytes}
	stderr := &boundedBuffer{max: r.maxLogBytes}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	execErr := cmd.Run()
	durationMS := time.Since(start).Milliseconds()

	out.Logs = stderr.Lines()
	out.Truncated = stdout.Truncated() || stderr.Truncated()
	out.DurationMS = durationMS

	// Timeout: context.WithTimeout fires first and Run returns the
	// context error; exec.CommandContext also kills the child when
	// the context is done so we don't leak a Python interpreter.
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		out.Status = "timeout"
		out.Error = &api.InvocationError{Type: "Timeout", Message: "python3 runtime timeout"}
		out.ResolvedCode = cserrors.CSRuntimeTimeout
		return out
	}

	if execErr != nil {
		// Non-zero exit. Surface the stderr tail (capped at
		// maxErrorBytes) so the operator sees a Python traceback in
		// the activation record. The audit hook raises with a string
		// containing "CS_RUNTIME_CAPABILITY_DENIED" when it trips,
		// which we map to the matching cserror code so policy denials
		// look the same across runtimes.
		tail := stderr.TailString(r.maxErrorBytes)
		if tail == "" {
			tail = execErr.Error()
		}
		mapped := cserrors.CSRuntimeException
		errType := "Exception"
		if strings.Contains(tail, string(cserrors.CSRuntimeCapDenied)) {
			mapped = cserrors.CSRuntimeCapDenied
			errType = "CapabilityDenied"
		}
		out.Status = "error"
		out.Error = &api.InvocationError{Type: errType, Message: tail}
		out.ResolvedCode = mapped
		return out
	}

	res, parseErr := parseResponse(stdout.Bytes())
	if parseErr != nil {
		out.Status = "error"
		out.Error = &api.InvocationError{Type: "ResponseDecodeError", Message: parseErr.Error()}
		out.ResolvedCode = cserrors.CSRuntimeException
		return out
	}
	if err := api.ValidateResultShape(res); err != nil {
		out.Status = "error"
		out.Error = &api.InvocationError{Type: "ValidationError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSRuntimeException
		return out
	}
	// Apply the cs-js-equivalent post-truncation re-decode for
	// FunctionResponse bodies that exceed maxResultBytes. The
	// boundedBuffer already caps stdout but the size check is
	// re-applied here so a single oversized field in the encoded JSON
	// flips out.Truncated, matching cs-js semantics.
	resultRaw, _ := json.Marshal(res)
	if len(resultRaw) > r.maxResultBytes {
		resultRaw = resultRaw[:r.maxResultBytes]
		out.Truncated = true
		var truncated api.FunctionResponse
		if err := json.Unmarshal(resultRaw, &truncated); err == nil {
			res = &truncated
		}
	}
	out.Status = "success"
	out.Result = res
	out.ResolvedCode = ""
	return out
}

// writeBundleFiles spills every file in the bundle into dir. Path
// traversal is already rejected by bundle.BuildCanonical at publish
// time (filepath.Clean returns the same string only when the input is
// safe), but we re-check here so a hand-rolled tar can't escape the
// activation scratch dir on the invoker host.
func writeBundleFiles(dir string, files map[string][]byte) error {
	for name, data := range files {
		clean := filepath.Clean(name)
		if clean != name || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
			return fmt.Errorf("invalid file path %q", name)
		}
		dst := filepath.Join(dir, clean)
		if !strings.HasPrefix(dst, dir+string(os.PathSeparator)) && dst != filepath.Join(dir, clean) {
			return fmt.Errorf("path escapes scratch dir: %q", name)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(dst, data, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// marshalRequest produces the JSON document handed to the Python child
// on stdin. The shape mirrors the cs-wasm wire format: a top-level
// {"event": ..., "ctx": {...}} so user code can write
// `import json,sys; req = json.load(sys.stdin); event = req["event"]`.
func marshalRequest(request api.InvocationRequest) ([]byte, error) {
	doc := map[string]any{
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
	}
	return json.Marshal(doc)
}

// parseResponse decodes the stdout payload into a FunctionResponse.
// Empty stdout is treated as a 200 with no body (matches cs-js when
// the user returns undefined). A full envelope is preferred; if the
// stdout is a bare scalar we wrap it as a 200 with the bytes JSON-
// encoded into body.
func parseResponse(raw []byte) (*api.FunctionResponse, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return &api.FunctionResponse{StatusCode: 200, Headers: map[string]string{}, Body: ""}, nil
	}
	// Some agent-generated Python ends its handler with print(json.dumps(...)).
	// Take only the LAST non-empty line so a function that does both a
	// progress print on stdout and then emits the envelope still works.
	lines := splitNonEmptyLines(trimmed)
	last := lines[len(lines)-1]

	var res api.FunctionResponse
	if err := json.Unmarshal(last, &res); err == nil && res.StatusCode != 0 {
		if res.Headers == nil {
			res.Headers = map[string]string{}
		}
		return &res, nil
	}
	// Not a FunctionResponse-shaped object: treat the raw bytes as the
	// body and wrap. We try to round-trip through JSON first so a bare
	// string is double-quoted consistently with cs-js JSONString.
	var anyVal any
	if err := json.Unmarshal(last, &anyVal); err == nil {
		body, _ := json.Marshal(anyVal)
		return &api.FunctionResponse{
			StatusCode: 200,
			Headers:    map[string]string{},
			Body:       string(body),
		}, nil
	}
	return nil, fmt.Errorf("invalid response: not JSON")
}

// splitNonEmptyLines splits on \n and drops blank trailing lines.
// We don't use bufio.Scanner here because it has a default 64 KiB cap
// and we'd rather fail on the parse step than silently truncate.
func splitNonEmptyLines(b []byte) [][]byte {
	parts := bytes.Split(b, []byte{'\n'})
	out := make([][]byte, 0, len(parts))
	for _, p := range parts {
		if trimmed := bytes.TrimSpace(p); len(trimmed) > 0 {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		out = append(out, bytes.TrimSpace(b))
	}
	return out
}

// childEnv returns a minimal environment for the python3 subprocess.
// PATH is preserved so the dynamic linker can resolve libpython /
// libc, TMPDIR is rooted in the scratch dir, and a sentinel
// CS_RUNTIME=cs-python lets the function introspect which runtime
// it's executing under (matches the cs-js implicit globalThis.cs).
// Nothing else is forwarded; in particular, the invoker-pool's
// secret env (E6.01) is NEVER exposed via process env — the cs.env
// surface will live in the embedded adapter follow-up.
func childEnv(scratchDir string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"TMPDIR=" + scratchDir,
		"LANG=C.UTF-8",
		"LC_ALL=C.UTF-8",
		"PYTHONDONTWRITEBYTECODE=1",
		"PYTHONUNBUFFERED=1",
		"CS_RUNTIME=cs-python",
	}
	return env
}

// boundedBuffer accumulates child stdio bytes up to a hard cap and
// records whether the cap was hit so the caller can flag truncation
// on the ExecutionOutput. It deliberately does not panic or fail the
// write — exec.Cmd will keep feeding bytes until the child exits, and
// dropping them is safer than tearing the process down mid-write.
type boundedBuffer struct {
	buf       bytes.Buffer
	max       int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.max <= 0 || b.buf.Len()+len(p) <= b.max {
		return b.buf.Write(p)
	}
	remaining := b.max - b.buf.Len()
	if remaining > 0 {
		b.buf.Write(p[:remaining])
	}
	b.truncated = true
	// Tell the caller we accepted the full slice so exec.Cmd does not
	// retry. The bytes beyond the cap are simply dropped; that matches
	// cs-js boundedBuffer semantics where Truncated() is the signal.
	return len(p), nil
}

func (b *boundedBuffer) Bytes() []byte   { return b.buf.Bytes() }
func (b *boundedBuffer) Truncated() bool { return b.truncated }

// Lines returns stderr split into log lines. Empty trailing lines are
// dropped so the activation log doesn't carry a phantom "" line after
// every newline-terminated print.
func (b *boundedBuffer) Lines() []string {
	if b.buf.Len() == 0 {
		return nil
	}
	parts := strings.Split(b.buf.String(), "\n")
	out := make([]string, 0, len(parts))
	for _, line := range parts {
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

// TailString returns the last n bytes of the buffer as a string. Used
// to bound the error message we surface in the activation record when
// the child exits non-zero.
func (b *boundedBuffer) TailString(n int) string {
	if n <= 0 || b.buf.Len() == 0 {
		return ""
	}
	if b.buf.Len() <= n {
		return strings.TrimSpace(b.buf.String())
	}
	raw := b.buf.Bytes()
	return strings.TrimSpace(string(raw[len(raw)-n:]))
}

// Compile-time sanity check: Runner must satisfy runtime.Handler so
// it can register with the DefaultRegistry.
var _ rt.Handler = (*Runner)(nil)
