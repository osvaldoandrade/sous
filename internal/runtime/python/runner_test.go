package python

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	rt "github.com/osvaldoandrade/sous/internal/runtime"
)

// skipIfNoPython3 is the gate every test in this file uses to opt out
// when the CI machine does not ship a python3 binary. The subprocess
// MVP cannot run without one; the embedded-CPython adapter follow-up
// will remove this skip.
func skipIfNoPython3(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skipf("python3 not on PATH: %v", err)
	}
}

func defaultManifest() api.FunctionManifest {
	return api.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: api.RuntimeCSPython,
		Entry:   entryFile,
		Handler: "default",
		Limits: api.ManifestLimits{
			TimeoutMS:      3000,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Capabilities: api.ManifestCapabilities{
			KV:    api.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get", "set", "del"}},
			CodeQ: api.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
			HTTP:  api.ManifestHTTPCaps{AllowHosts: []string{"127.0.0.1"}, TimeoutMS: 1500},
		},
	}
}

func defaultInvocation(deadline time.Duration) api.InvocationRequest {
	return api.InvocationRequest{
		ActivationID: "act-1",
		RequestID:    "req-1",
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    api.Principal{Sub: "user:1", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(deadline).UnixMilli(),
		Event:        map[string]any{"name": "world"},
	}
}

// buildPyBundle wraps a python source body and the supplied manifest
// in a canonical bundle tar. We go through bundle.BuildCanonical so
// the test exercises the generalised entry-file check rather than the
// adapter's private extractor.
func buildPyBundle(t *testing.T, manifest api.FunctionManifest, src string) []byte {
	t.Helper()
	mraw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	files := map[string][]byte{
		"manifest.json": mraw,
		entryFile:       []byte(src),
	}
	b, _, _, err := bundle.BuildCanonical(files)
	if err != nil {
		t.Fatalf("BuildCanonical: %v", err)
	}
	return b
}

// TestRunnerEchoFunction is the happy-path smoke test. The publisher
// reads stdin, echoes part of the event, and prints a FunctionResponse
// envelope on stdout.
func TestRunnerEchoFunction(t *testing.T) {
	skipIfNoPython3(t)
	src := `
import json, sys
req = json.load(sys.stdin)
event = req["event"]
res = {"statusCode": 200, "headers": {"x-cs-runtime": "cs-python"}, "body": "hello " + event["name"]}
print(json.dumps(res))
`
	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), buildPyBundle(t, defaultManifest(), src), defaultInvocation(2*time.Second))
	if out.Status != "success" {
		t.Fatalf("status = %q, want success; err=%+v logs=%v", out.Status, out.Error, out.Logs)
	}
	if out.Result == nil || out.Result.Body != "hello world" {
		t.Fatalf("unexpected result: %+v", out.Result)
	}
	if out.Result.Headers["x-cs-runtime"] != "cs-python" {
		t.Fatalf("missing x-cs-runtime header: %+v", out.Result.Headers)
	}
}

// TestRunnerTimeoutEnforced verifies a runaway sleep is killed inside
// the manifest TimeoutMS budget. We pick 250ms to keep the suite fast
// and a 5s sleep to make the deadline unambiguous.
func TestRunnerTimeoutEnforced(t *testing.T) {
	skipIfNoPython3(t)
	src := `
import time
time.sleep(5)
print("{}")
`
	manifest := defaultManifest()
	manifest.Limits.TimeoutMS = 250
	r := NewRunner(nil, nil, 0, 0, 0)
	start := time.Now()
	out := r.Execute(context.Background(), buildPyBundle(t, manifest, src), defaultInvocation(250*time.Millisecond))
	elapsed := time.Since(start)
	if out.Status != "timeout" {
		t.Fatalf("status = %q, want timeout; err=%+v", out.Status, out.Error)
	}
	if out.ResolvedCode != cserrors.CSRuntimeTimeout {
		t.Fatalf("resolved code = %q, want %q", out.ResolvedCode, cserrors.CSRuntimeTimeout)
	}
	// The kill should happen well within 3s; on a wedged CI machine
	// the slack is generous but still much less than the 5s sleep.
	if elapsed > 3*time.Second {
		t.Fatalf("timeout took %s, runner failed to kill child", elapsed)
	}
}

// TestRunnerNonZeroExitSurfacesError checks that a Python traceback
// reaches the activation record. We raise inside user code; the audit
// hook is bypassed (we want plain exception flow, not capability
// denial) so the runner classifies the failure as CS_RUNTIME_EXCEPTION.
func TestRunnerNonZeroExitSurfacesError(t *testing.T) {
	skipIfNoPython3(t)
	src := `
raise RuntimeError("kaboom from user code")
`
	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), buildPyBundle(t, defaultManifest(), src), defaultInvocation(2*time.Second))
	if out.Status != "error" {
		t.Fatalf("status = %q, want error", out.Status)
	}
	if out.ResolvedCode != cserrors.CSRuntimeException {
		t.Fatalf("resolved code = %q, want %q", out.ResolvedCode, cserrors.CSRuntimeException)
	}
	if out.Error == nil || !strings.Contains(out.Error.Message, "kaboom from user code") {
		t.Fatalf("expected traceback in error, got %+v", out.Error)
	}
}

// TestRunnerBannedSubprocessDenied confirms the audit hook fires on
// subprocess.Popen and the failure surfaces as
// CS_RUNTIME_CAPABILITY_DENIED. This is the v0.1 best-effort gate
// described in docs/08c-runtime-cs-python.md.
func TestRunnerBannedSubprocessDenied(t *testing.T) {
	skipIfNoPython3(t)
	src := `
import subprocess
subprocess.Popen(["/bin/echo", "hi"])
print("{}")
`
	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), buildPyBundle(t, defaultManifest(), src), defaultInvocation(2*time.Second))
	if out.Status != "error" {
		t.Fatalf("status = %q, want error; logs=%v", out.Status, out.Logs)
	}
	if out.ResolvedCode != cserrors.CSRuntimeCapDenied {
		t.Fatalf("resolved code = %q, want %q (err=%+v)", out.ResolvedCode, cserrors.CSRuntimeCapDenied, out.Error)
	}
	if out.Error == nil || !strings.Contains(out.Error.Message, "CS_RUNTIME_CAPABILITY_DENIED") {
		t.Fatalf("expected capability-denied marker in error, got %+v", out.Error)
	}
}

// TestRunnerStderrCapturedAsLogs verifies every stderr line surfaces
// as a log entry on ExecutionOutput.Logs. The function prints two
// info lines via cs_log() (provided by the audit bootstrap) before
// returning a normal response.
func TestRunnerStderrCapturedAsLogs(t *testing.T) {
	skipIfNoPython3(t)
	src := `
import json
cs_log("info", "starting up")
cs_log("warn", "halfway through")
print(json.dumps({"statusCode": 200, "body": "done"}))
`
	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), buildPyBundle(t, defaultManifest(), src), defaultInvocation(2*time.Second))
	if out.Status != "success" {
		t.Fatalf("status = %q, want success; err=%+v", out.Status, out.Error)
	}
	if len(out.Logs) < 2 {
		t.Fatalf("expected >=2 log lines, got %v", out.Logs)
	}
	joined := strings.Join(out.Logs, "\n")
	if !strings.Contains(joined, "[info] starting up") || !strings.Contains(joined, "[warn] halfway through") {
		t.Fatalf("expected info/warn log lines, got %v", out.Logs)
	}
}

// TestRunnerBundleMissingFunctionPy fails before spawning python3 so
// it does not need the skipIfNoPython3 gate. The bundle layer rejects
// a cs-python manifest that ships no function.py at BuildCanonical
// time, so we feed a hand-rolled tar to verify the runtime-side
// guard still fires defensively.
func TestRunnerBundleMissingFunctionPy(t *testing.T) {
	// Build a bundle with function.js to satisfy BuildCanonical
	// (default cs-js entry), then ParseManifest sees cs-python at
	// runtime and the python adapter rejects it.
	manifest := defaultManifest()
	mraw, _ := json.Marshal(manifest)
	// Hand-roll a bundle that has manifest+function.js but NOT
	// function.py so the python adapter's entry-file check trips.
	files := map[string][]byte{
		"manifest.json": mraw,
		"function.js":   []byte("export default async () => ({statusCode:200,body:'wrong runtime'});"),
		entryFile:       []byte(""),
	}
	// Forcing function.py to empty string is enough: BuildCanonical
	// stores the empty bytes, ExtractTar returns them, and the
	// adapter's "len(code) == 0" guard catches it.
	tarBytes, _, _, err := bundle.BuildCanonical(files)
	if err != nil {
		t.Fatalf("BuildCanonical: %v", err)
	}
	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), tarBytes, defaultInvocation(2*time.Second))
	if out.Status != "error" {
		t.Fatalf("status = %q, want error", out.Status)
	}
	if out.Error == nil || !strings.Contains(out.Error.Message, "bundle missing function.py") {
		t.Fatalf("expected missing function.py error, got %+v", out.Error)
	}
}

// TestRunnerName and TestRunnerRegisteredInDefaultRegistry pin the
// adapter into the registry contract so a future refactor cannot
// silently drop the cs-python runtime from publish-time validation.
func TestRunnerName(t *testing.T) {
	r := NewRunner(nil, nil, 0, 0, 0)
	if r.Name() != api.RuntimeCSPython {
		t.Fatalf("Name() = %q, want %q", r.Name(), api.RuntimeCSPython)
	}
}

func TestRunnerRegisteredInDefaultRegistry(t *testing.T) {
	if !rt.DefaultRegistry.Has(api.RuntimeCSPython) {
		t.Fatal("DefaultRegistry must contain cs-python after python package init")
	}
}

// TestRunnerLooksUpPython3FromPath confirms the unavailable-binary
// path produces a typed error rather than a panic. We override the
// resolved path to a name guaranteed not to exist.
func TestRunnerLooksUpPython3FromPath(t *testing.T) {
	r := NewRunner(nil, nil, 0, 0, 0)
	r.python3Path = "/definitely/not/here/python3-fake-binary-cs"
	src := `print("{}")`
	out := r.Execute(context.Background(), buildPyBundle(t, defaultManifest(), src), defaultInvocation(2*time.Second))
	if out.Status != "error" {
		t.Fatalf("status = %q, want error", out.Status)
	}
	if out.Error == nil || !strings.Contains(out.Error.Message, "python3 not on PATH") {
		t.Fatalf("expected python3-missing error, got %+v", out.Error)
	}
}

// TestParseResponseAcceptsBareValue covers the wrap-as-200 fallback:
// when the user prints a raw JSON scalar rather than a full envelope,
// the runtime wraps it in {statusCode:200, body:<json>}.
func TestParseResponseAcceptsBareValue(t *testing.T) {
	skipIfNoPython3(t)
	src := `
import json
print(json.dumps({"hello": "there"}))
`
	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), buildPyBundle(t, defaultManifest(), src), defaultInvocation(2*time.Second))
	if out.Status != "success" {
		t.Fatalf("status = %q, want success; err=%+v", out.Status, out.Error)
	}
	if out.Result.StatusCode != 200 {
		t.Fatalf("statusCode = %d, want 200", out.Result.StatusCode)
	}
	if !strings.Contains(out.Result.Body, `"hello"`) {
		t.Fatalf("body = %q, want JSON containing hello", out.Result.Body)
	}
}
