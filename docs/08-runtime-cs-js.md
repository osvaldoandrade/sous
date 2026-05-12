# Runtime: cs-js

This document defines the JavaScript runtime contract.

The runtime exists to execute code that an agent can produce and publish as text.

## Runtime shape

A published function version contains two UTF-8 files:

- `function.js`
- `manifest.json`

The system stores these files as a canonical tar bundle.
The system addresses the tar bundle by `sha256`.

The runtime loads the bundle from KVRocks and executes it in an isolate.

## Why an isolate exists

User code runs in the same cluster as platform services.
The platform must treat user code as untrusted.

An isolate lets the invoker:

- cap memory
- cap wall time
- remove access to host filesystem
- remove access to process spawn
- gate side effects behind explicit host APIs

## Handler contract

`function.js` exports a default async function.

```js
export default async function handle(event, ctx) {
  return { statusCode: 200, body: "ok" }
}
```

The invoker passes:

- `event`: JSON value
- `ctx`: execution context

## Context contract

`ctx` is a JSON object with stable fields.

```json
{
  "activation_id": "uuid",
  "deadline_ms": 1730000003000,
  "tenant": "t_abc123",
  "namespace": "payments",
  "function": "reconcile",
  "version": 17,
  "ref": { "alias": "prod" },
  "trigger": { "type": "http" },
  "principal": { "sub": "user:123", "roles": ["role:app"] }
}
```

The invoker may add fields.
The invoker must not remove fields in v0.1.

## Manifest schema

`manifest.json` declares limits and capabilities.

```json
{
  "schema": "cs.function.script.v1",
  "runtime": "cs-js",
  "runtimeVersion": "cs-js@1",
  "entry": "function.js",
  "handler": "default",
  "limits": { "timeoutMs": 3000, "memoryMb": 64, "maxConcurrency": 1 },
  "capabilities": {
    "kv": { "prefixes": ["ctr:"], "ops": ["get","set"] },
    "codeq": { "publishTopics": ["jobs.*"] },
    "http": { "allowHosts": ["api.example.com"], "timeoutMs": 1500 }
  },
  "imports": {
    "zod": { "url": "https://mirror.example.com/zod@3.22.4/index.js" },
    "lib/internal": { "path": "lib/internal.js" }
  }
}
```

The control plane validates this manifest at publish time.
The invoker validates it again at execution time.

## Runtime selection

The `runtime` field selects which adapter executes the bundle.

Accepted values:

- `cs-js` (default, this document)
- `cs-python` (reserved; adapter ships in a follow-up release)
- `cs-wasm` (reserved; adapter ships in a follow-up release)

The field is optional. A manifest that omits `runtime` is treated as
`cs-js` so that v1 manifests published before the runtime selection
support landed continue to validate unchanged.

The optional `runtimeVersion` field pins a specific adapter build, for
example `cs-js@1`, `node20-goja`, `python3.12`, or `wasi-preview1`. The
control plane stores the value verbatim and the invoker uses it to pick
the matching adapter binary. When `runtimeVersion` is empty the invoker
selects the newest installed version of the declared runtime.

### Registry contract

Runtime adapters register themselves with the control plane via the
process-wide `runtime.DefaultRegistry` in `internal/runtime/registry.go`.
At publish time the control plane calls `EnsureSupported(manifest.runtime)`
and rejects manifests whose runtime has no registered handler with
`400 CS_RUNTIME_UNSUPPORTED`. Tests and embedders can construct an isolated
`Registry` instance through `runtime.NewRegistry()`.

This PR ships only the `cs-js` slot. The `cs-python` and `cs-wasm`
adapters will register themselves in subsequent releases without any
change to the manifest schema.

## Imports and import maps

A `function.js` may declare external JavaScript dependencies in the
optional `imports` block. Each entry maps a bare specifier (the string
passed to `import`) to one of two sources:

- `{ "url": "https://..." }` — fetched at publish time from a curated
  mirror. The host must appear in
  `cs_control.publish.imports.allowed_mirrors` (see
  docs/20-config-reference.md and docs/15-security.md).
- `{ "path": "..." }` — points at a file inside the uploaded bundle.

`integrity` is optional. When provided it must be a SubResource
Integrity string (`sha256-<base64>` or `sha384-<base64>`); the control
plane recomputes the digest from the bytes it actually fetched and
rejects the publish on mismatch.

Resolution runs **once in cs-control at publish time**, never at invoke
time. The control plane:

1. Fetches or copies the bytes for each specifier.
2. Verifies (or computes) the SRI digest.
3. Writes the bytes into the canonical tar under `deps/<safe-name>`.
4. Emits `import-map.json` in the bundle root with
   `schema: "cs.importmap.v1"`, mapping each specifier to its frozen
   path and integrity digest.

At invoke time the cs-js runtime reads `import-map.json` and binds a
host helper that resolves bare specifiers against the frozen bytes. The
runtime verifies the SRI digest before letting any byte reach the
isolate; a tampered bundle fails with `CS_IMPORT_NOT_FOUND`.

Supported import statement shapes in `function.js`:

```js
import x from "spec"
import * as ns from "spec"
import { a, b as c } from "spec"
import x, { a } from "spec"
import "spec"
```

Dep modules may use `export default <expr>`, `export const|let|var
NAME`, `export function NAME`, `export class NAME`, or
`export { a, b as c }`.

Hard rules:

- **No build step.** The agent uploads UTF-8 source + manifest only.
  The cs-control resolver is the only thing that touches the network
  on a publisher's behalf.
- **No network egress at invoke time.** Specifiers resolve against the
  frozen bundle. There is no fallback path.
- An undeclared specifier raises `CS_IMPORT_NOT_FOUND` (HTTP 422) at
  invoke time. See docs/21-errors.md.
- The 16 MiB bundle cap covers the *frozen* bundle (uploaded files +
  `deps/**` + `import-map.json`). See docs/26-capacity-and-limits.md.

## Host API (`cs`)

User code calls host APIs through a single object: `cs`.

### Logging

- `cs.log.info(value)`
- `cs.log.warn(value)`
- `cs.log.error(value)`

The invoker serializes `value` as JSON if possible.
The invoker writes log chunks to KVRocks.

### Key-value access

- `cs.kv.get(key)`
- `cs.kv.set(key, value, { ttlSeconds })`
- `cs.kv.del(key)`

The runtime enforces:

- `key` prefix allowlist
- operation allowlist

The runtime serializes `value` as JSON.

### codeQ publish

- `cs.codeq.publish(topic, payload)`

The runtime enforces a topic prefix allowlist.

### HTTP fetch

- `cs.http.fetch(url, { method, headers, body, timeoutMs })`

The runtime enforces:

- hostname allowlist
- private IP block
- timeout cap from manifest

The runtime returns:

```json
{
  "status": 200,
  "headers": { "content-type": "application/json" },
  "body": "base64",
  "isBase64Encoded": true
}
```

## Limits

### Wall time

The invoker enforces `timeoutMs`.

The invoker sets `deadline_ms = now_ms + timeoutMs`.
The invoker cancels the isolate at `deadline_ms`.

### Memory

The invoker configures the isolate memory budget from `memoryMb`.
The invoker terminates the activation if the isolate exceeds this budget.

### Concurrency

The invoker enforces `maxConcurrency` per function version per replica.

The invoker uses a semaphore keyed by:

- tenant
- namespace
- function
- version

## Result contract

The handler returns an object.

`cs-invoker-pool` accepts these fields:

- `statusCode` integer
- `headers` map[string]string
- `body` string
- `isBase64Encoded` boolean

If the handler returns a non-object, the invoker returns:

- `statusCode = 200`
- `body = JSON.stringify(returnValue)`

## Local runner parity

The `cs` CLI embeds the same runtime and host API definitions.

The CLI implements `cs.kv` as:

- an in-memory provider by default
- an optional remote provider when `--kv-endpoint` exists

The CLI blocks network egress unless `http.allowHosts` exists in the manifest.
