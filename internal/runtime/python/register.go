package python

import rt "github.com/osvaldoandrade/sous/internal/runtime"

// init registers a singleton Runner with the process-wide registry so
// the control plane recognises the cs-python runtime at publish time.
// The default instance uses in-memory providers; cs-invoker-pool may
// replace it on boot once the embedded-CPython adapter follow-up
// lands and the registration carries the wired-up kv/codeq pair.
//
// This mirrors the cs-wasm registration at
// internal/runtime/wasm/runner.go init(): importing the package is
// enough to advertise the runtime, so cmd/cs-invoker-pool just needs a
// blank import.
func init() {
	rt.DefaultRegistry.Register(NewRunner(nil, nil, 0, 0, 0))
}
