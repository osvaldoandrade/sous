# Local Dev, Publish, Promote

This flow is the core SOUS workflow for agent-generated functions: create locally, validate semantics with the same runtime, publish an immutable version, then promote traffic using an alias.

## Main flow

1. Create a scaffold (`function.js`, `manifest.json`).
2. Test locally with the CLI runtime.
3. Upload a draft and publish a version.
4. Set an alias (for example, `prod`) to the new version.

### Sequence diagram

```mermaid
sequenceDiagram
  participant A as Agent/Developer
  participant CLI as cs CLI
  participant CP as cs-control
  participant KV as KVRocks

  A->>CLI: cs fn init
  A->>CLI: cs fn test
  CLI->>CP: Draft upload
  CP->>KV: Store bundle bytes + sha256
  CLI->>CP: Publish version
  CP->>KV: Persist immutable version record
  CLI->>CP: Set alias -> version
  CP->>KV: Update alias pointer
```
