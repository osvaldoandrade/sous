# Capabilities and Isolation

SOUS treats user code as untrusted.

The runtime executes functions inside an isolate, enforces time/memory/concurrency budgets, and blocks filesystem and process spawning.

Side effects are only possible through host APIs exposed as `cs.*`.

The manifest declares capabilities for those host APIs (KV prefixes and ops, codeQ publish topics, HTTP allowHosts, etc.). The invoker enforces those capabilities at runtime.

This design ensures that the privilege boundary is declared in source control (manifest) and enforced at execution time.
