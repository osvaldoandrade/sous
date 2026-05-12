package runtime

import (
	"errors"
	"testing"

	"github.com/osvaldoandrade/sous/internal/api"
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
