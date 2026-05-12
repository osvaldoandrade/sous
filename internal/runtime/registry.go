package runtime

import (
	"sort"
	"sync"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

// Handler is the marker interface implemented by every runtime adapter
// (cs-js, cs-python, cs-wasm, ...). The concrete execution surface is
// intentionally narrow at this layer: the control plane only needs to know
// that a runtime is installed before it accepts a publish. The invoker
// resolves the concrete adapter (and its capability bindings) separately.
type Handler interface {
	// Name returns the canonical runtime identifier (e.g. "cs-js").
	Name() string
}

// Registry tracks the runtime adapters available to the control plane.
//
// The registry is safe for concurrent use. Adapters register themselves at
// process start via Register; the control plane queries Has / Lookup at
// publish time to reject manifests whose runtime is unknown.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register installs a handler under its declared Name. A second
// registration for the same name overwrites the previous handler so tests
// can swap adapters deterministically. Names are case-sensitive and must
// match the runtime identifier used in the manifest.
func (r *Registry) Register(h Handler) {
	if r == nil || h == nil {
		return
	}
	name := h.Name()
	if name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[name] = h
}

// Lookup returns the handler registered under name. The boolean indicates
// whether a handler is installed.
func (r *Registry) Lookup(name string) (Handler, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	return h, ok
}

// Has reports whether a handler is registered for name.
func (r *Registry) Has(name string) bool {
	_, ok := r.Lookup(name)
	return ok
}

// Names returns the sorted list of registered runtime identifiers. The
// slice is a fresh copy and safe to mutate.
func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// EnsureSupported returns a typed CSError when the runtime is not
// registered. Callers in the control plane use this on the publish path so
// the failure surfaces as HTTP 400 CS_RUNTIME_UNSUPPORTED.
func (r *Registry) EnsureSupported(name string) error {
	if r.Has(name) {
		return nil
	}
	return cserrors.New(cserrors.CSRuntimeUnsupported, "runtime not supported: "+name)
}

// DefaultRegistry is the process-wide registry. Adapter packages register
// themselves via init() functions; the control plane reads from it during
// manifest validation.
var DefaultRegistry = NewRegistry()

// SimpleHandler is a minimal Handler used by tests and by adapter packages
// that only need to advertise their name during the E3.03 slot phase. The
// concrete execution wiring lives in the adapter package itself.
type SimpleHandler struct{ N string }

// Name implements Handler.
func (s SimpleHandler) Name() string { return s.N }

func init() {
	// cs-js is always present: it's the implicit default and the
	// runtime that ships in-tree. cs-wasm has its concrete adapter in
	// internal/runtime/wasm; the slot registered here lets the
	// control plane accept the runtime name even when the wasm
	// package has not been imported (e.g. unit-test binaries). When
	// the wasm package is imported its init() overrides this slot
	// with the real Runner (Register is last-write-wins). The
	// cs-python adapter follows the same pattern when it lands.
	DefaultRegistry.Register(SimpleHandler{N: api.RuntimeCSJS})
	DefaultRegistry.Register(SimpleHandler{N: api.RuntimeCSWASM})
}
