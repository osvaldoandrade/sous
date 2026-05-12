package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

func TestRegistryRegisterLookup(t *testing.T) {
	r := NewRegistry()
	if r.Has("cs-js") {
		t.Fatal("fresh registry should not contain cs-js")
	}

	r.Register(SimpleHandler{N: "cs-js"})
	if !r.Has("cs-js") {
		t.Fatal("expected cs-js to be registered")
	}

	h, ok := r.Lookup("cs-js")
	if !ok || h == nil || h.Name() != "cs-js" {
		t.Fatalf("unexpected lookup result: %+v ok=%v", h, ok)
	}

	if _, ok := r.Lookup("cs-python"); ok {
		t.Fatal("cs-python should not be registered yet")
	}
}

func TestRegistryRegisterIgnoresNilAndEmpty(t *testing.T) {
	r := NewRegistry()
	r.Register(nil)
	r.Register(SimpleHandler{N: ""})
	if names := r.Names(); len(names) != 0 {
		t.Fatalf("registry should be empty, got %v", names)
	}
}

func TestRegistryNamesSorted(t *testing.T) {
	r := NewRegistry()
	r.Register(SimpleHandler{N: "cs-wasm"})
	r.Register(SimpleHandler{N: "cs-js"})
	r.Register(SimpleHandler{N: "cs-python"})

	got := r.Names()
	want := []string{"cs-js", "cs-python", "cs-wasm"}
	if len(got) != len(want) {
		t.Fatalf("Names = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("Names[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRegistryEnsureSupported(t *testing.T) {
	r := NewRegistry()
	r.Register(SimpleHandler{N: "cs-js"})

	if err := r.EnsureSupported("cs-js"); err != nil {
		t.Fatalf("cs-js should be supported: %v", err)
	}

	err := r.EnsureSupported("cs-unknown")
	if err == nil {
		t.Fatal("expected error for unknown runtime")
	}
	var csErr *cserrors.CSError
	if !errors.As(err, &csErr) {
		t.Fatalf("expected *cserrors.CSError, got %T", err)
	}
	if csErr.Code != cserrors.CSRuntimeUnsupported {
		t.Fatalf("code = %s, want %s", csErr.Code, cserrors.CSRuntimeUnsupported)
	}
	if got := cserrors.StatusCode(csErr.Code); got != 400 {
		t.Fatalf("status = %d, want 400", got)
	}
}

func TestRegistryNilSafe(t *testing.T) {
	var r *Registry
	if r.Has("cs-js") {
		t.Fatal("nil registry should report Has=false")
	}
	if _, ok := r.Lookup("cs-js"); ok {
		t.Fatal("nil registry should report Lookup ok=false")
	}
	if names := r.Names(); names != nil {
		t.Fatalf("nil registry Names = %v, want nil", names)
	}
	// Register is a no-op on nil receiver.
	r.Register(SimpleHandler{N: "cs-js"})
}

func TestDefaultRegistryHasCSJS(t *testing.T) {
	if !DefaultRegistry.Has(api.RuntimeCSJS) {
		t.Fatal("DefaultRegistry must include cs-js out of the box")
	}
}

// TestRunnerDispatchesToRegisteredExecutor confirms the selectRunner
// helper inside Runner.Execute routes a non-cs-js bundle to the
// adapter registered for that runtime. We borrow the cs-python slot
// (the validator accepts the runtime name) and replace it with a
// fakeExecutor for the duration of the test, then assert the fake's
// Execute produced the response instead of the cs-js runJS path.
// This is the seam that lets internal/runtime/python and
// internal/runtime/wasm take over without cs-invoker-pool growing
// dispatch logic.
func TestRunnerDispatchesToRegisteredExecutor(t *testing.T) {
	original, hadOriginal := DefaultRegistry.Lookup(api.RuntimeCSPython)
	DefaultRegistry.Register(&fakeExecutor{name: api.RuntimeCSPython, body: "from-fake"})
	t.Cleanup(func() {
		if hadOriginal && original != nil {
			DefaultRegistry.Register(original)
		}
	})

	r := NewRunner(nil, nil, 0, 0, 0)

	manifest := api.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: api.RuntimeCSPython,
		Entry:   "function.py",
		Handler: "default",
		Limits: api.ManifestLimits{
			TimeoutMS:      3000,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Capabilities: api.ManifestCapabilities{
			KV:    api.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get"}},
			CodeQ: api.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
			HTTP:  api.ManifestHTTPCaps{AllowHosts: []string{"example.com"}, TimeoutMS: 1500},
		},
	}
	mraw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	tarBytes, _, _, err := bundle.BuildCanonical(map[string][]byte{
		"manifest.json": mraw,
		"function.py":   []byte("not real python; the fake adapter never reads this"),
	})
	if err != nil {
		t.Fatalf("BuildCanonical: %v", err)
	}

	out := r.Execute(context.Background(), tarBytes, api.InvocationRequest{
		ActivationID: "act-1",
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    api.Principal{Sub: "user:1", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{},
	})
	if out.Status != "success" {
		t.Fatalf("status = %q, want success; err=%+v", out.Status, out.Error)
	}
	if out.Result == nil || out.Result.Body != "from-fake" {
		t.Fatalf("dispatch did not reach fake executor: %+v", out.Result)
	}
}

// TestRunnerRejectsUnregisteredExecutor confirms that a manifest
// declaring a runtime the binary does not have an Executor for fails
// fast with CS_RUNTIME_UNSUPPORTED rather than silently running the
// cs-js path. We do this by registering a plain SimpleHandler (no
// Execute method) for a synthetic runtime name and asserting the
// invocation returns the typed error code.
func TestRunnerRejectsUnregisteredExecutor(t *testing.T) {
	// Replace the cs-wasm slot temporarily with a SimpleHandler so
	// the dispatch helper sees a Handler-without-Executor. We pick
	// cs-wasm because (a) Validate accepts it, and (b) when the wasm
	// package is not imported by this test binary, DefaultRegistry
	// already holds a SimpleHandler placeholder per init() in
	// registry.go. We assert the placeholder behaviour is preserved.
	original, hadOriginal := DefaultRegistry.Lookup(api.RuntimeCSWASM)
	DefaultRegistry.Register(SimpleHandler{N: api.RuntimeCSWASM})
	t.Cleanup(func() {
		if hadOriginal && original != nil {
			DefaultRegistry.Register(original)
		}
	})

	manifest := api.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: api.RuntimeCSWASM,
		Entry:   "module.wasm",
		Handler: "default",
		Limits: api.ManifestLimits{
			TimeoutMS:      3000,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Capabilities: api.ManifestCapabilities{
			KV:    api.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get"}},
			CodeQ: api.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
			HTTP:  api.ManifestHTTPCaps{AllowHosts: []string{"example.com"}, TimeoutMS: 1500},
		},
	}
	mraw, _ := json.Marshal(manifest)
	tarBytes, _, _, err := bundle.BuildCanonical(map[string][]byte{
		"manifest.json": mraw,
		"module.wasm":   []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00},
	})
	if err != nil {
		t.Fatalf("BuildCanonical: %v", err)
	}
	r := NewRunner(nil, nil, 0, 0, 0)
	out := r.Execute(context.Background(), tarBytes, api.InvocationRequest{
		ActivationID: "act-1",
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    api.Principal{Sub: "user:1", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{},
	})
	if out.Status != "error" {
		t.Fatalf("status = %q, want error", out.Status)
	}
	if out.ResolvedCode != cserrors.CSRuntimeUnsupported {
		t.Fatalf("resolved code = %q, want %q (err=%+v)", out.ResolvedCode, cserrors.CSRuntimeUnsupported, out.Error)
	}
}

// fakeExecutor is a test double that satisfies Executor and returns
// a deterministic ExecutionOutput. Used by the dispatch tests above
// to assert Runner.Execute routes non-cs-js bundles to the registered
// adapter rather than running cs-js code.
type fakeExecutor struct {
	name string
	body string
}

func (f *fakeExecutor) Name() string { return f.name }

func (f *fakeExecutor) Execute(_ context.Context, _ []byte, _ api.InvocationRequest) ExecutionOutput {
	body := f.body
	if body == "" {
		body = "ok"
	}
	return ExecutionOutput{
		Status: "success",
		Result: &api.FunctionResponse{
			StatusCode: 200,
			Headers:    map[string]string{},
			Body:       body,
		},
	}
}
