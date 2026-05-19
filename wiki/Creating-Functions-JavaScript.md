# Creating Functions: JavaScript

The cs-js runtime is the reference JavaScript execution surface for Sous. It is embedded as a [goja](https://github.com/dop251/goja) interpreter inside the `cs-invoker-pool` process — there is no separate Node binary, no worker pool, no JIT, and no out-of-process IPC. When a tenant publishes a `cs-js` function and an `InvocationRequest` arrives on `cs.invoke`, the invoker pulls the canonical tar from KVRocks, hands the bytes to `internal/runtime/runner.go`, and `Runner.Execute` evaluates the publisher's `function.js` in a fresh goja isolate. Every host capability — KV, codeQ, HTTP, secrets, Cadence — is bound into that isolate as a Go-backed function on a single global object named `cs`.

The choice of goja over a Node subprocess is deliberate. A traditional Node-subprocess model carries a per-invocation tax: process spawn, V8 warm-up, module resolution against `node_modules`, IPC framing for every host call. For Sous-sized workloads — short-lived, capability-gated handlers measured in single-digit milliseconds — that tax is a poor trade. goja is a pure-Go ECMAScript implementation that runs inside the same OS process as the invoker; cold-start is dominated by source parsing rather than process creation, host calls are direct Go function dispatch with no marshalling layer, and there is no language-level concurrency that could escape the isolate. The result is predictable cold-start (a few milliseconds for a small bundle), deterministic timing for workflow handlers (no JIT warm-up to perturb replay), and no process-spawn cost amortised across thousands of activations per second per replica.

Isolation is enforced at the interpreter boundary, not the OS boundary. goja has no syscall surface: there is no `fs`, no `child_process`, no native module loader, and no networking primitive that the runtime did not explicitly install. Every privileged operation — reading KV, publishing to codeQ, opening an outbound HTTP request, reading a secret — flows through a Go host hook bound to a property on `cs`, and every host hook consults the manifest's `capabilities` block before reaching the underlying provider. A denial surfaces as a thrown JavaScript error that propagates back to the runner as `InvocationResult.error` with `CS_RUNTIME_CAP_DENIED` (or `CS_EGRESS_DENIED` for the tenant-level egress matcher). The privilege boundary is therefore declared in source control (the manifest) and enforced at execution time by the same code that runs in the cluster and in the `cs fn test` local runner.

The rest of this page is the contract a JavaScript author writes against: the handler signature, the shape of the `cs` context, the capability model, the runtime limits, the JavaScript surface that is intentionally not present, and the worked examples. Operators looking for the tenant-level egress allowlist and signing controls should read [Operators Security](Operators-Security); authors who want to understand how the bundle is built and frozen should read [Managing Functions Packages](Managing-Functions-Packages).

## Bundle anatomy

A cs-js function lives on disk as a directory the author edits and on the wire as a canonical tar archive cs-control assembles at publish time. The minimum on-disk shape is two UTF-8 files:

```
reconcile/
├── function.js      # the handler, ES module
└── manifest.json    # the contract: limits, capabilities, imports
```

`function.js` is the entrypoint contract; the file name is fixed and cannot be overridden in v0.1. The `manifest.json` file's `entry` field is `"function.js"` for every cs-js bundle, and the schema's `entry` regex (`^[a-zA-Z0-9._/-]+$`) accepts other names but the runtime does not consult `entry` — it reads `files["function.js"]` directly in `Runner.Execute`. Authors should keep `entry: "function.js"` for forward compatibility.

When the bundle declares `imports`, cs-control adds two more pieces at publish time: a `deps/` subtree holding the frozen bytes for each declared dep, and an `import-map.json` at the bundle root mapping each specifier to a `deps/<safe-name>` path plus an SRI digest. The frozen bundle therefore looks like:

```
reconcile/
├── function.js
├── manifest.json
├── deps/
│   ├── zod.js
│   └── lib_internal.js
└── import-map.json
```

The publisher uploads only `function.js` plus `manifest.json` (and any local files the manifest's `imports` block references with `path:`). cs-control fetches `url:` deps from a curated mirror, verifies (or computes) each SRI digest, writes the bytes into the canonical tar, and emits `import-map.json`. The resulting tar is canonicalised (filenames sorted, timestamps zeroed) and signed before storage in KVRocks. The on-the-wire shape is intentionally identical to the on-disk shape so a debugging operator can `tar xf` a bundle and read it like a directory.

The 16 MiB bundle cap applies to the *frozen* bundle, including `deps/**` and `import-map.json`. Publishers should size deps with that cap in mind; a Node-style "ship `node_modules/`" approach blows the cap quickly and is not the intended pattern.

## Handler contract

A cs-js function exports exactly one default async function. The minimal HTTP handler is six lines:

```js
export default async function handle(event, ctx) {
  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ok: true }),
  };
}
```

The function name is irrelevant — the runtime resolves the handler by looking at the `default` export, not by name. `export default async function () { ... }` and `export default function handle() { ... }` are both accepted. The transform that wires `export default` to the runtime's invocation point lives in `transformESModule` (see `internal/runtime/runner.go`); it recognises named, anonymous, and `const __cs_default = ...` shapes and assigns the result to a hidden `globalThis.__cs_default` slot that the runner reads back after evaluation.

The transform is line-oriented and intentionally conservative. The three regexes it understands cover:

- `export default async function handle(` → `async function handle(` + assignment to `globalThis.__cs_default`.
- `export default async function (` (anonymous) → `const __cs_default = async function (` + assignment.
- `export default <expr>` (everything else) → `const __cs_default = <expr>` + assignment.

Anything else — re-exports of the default binding, default exports of a `class`, default exports of a destructured binding — falls back to the generic `export default <expr>` branch. The fallback usually does the right thing for simple cases (`export default myFn`, `export default { handle }`) and fails loudly with a goja syntax error for exotic shapes. Authors who hit the failure path should rewrite to one of the three canonical forms above.

The handler takes two arguments. `event` is the JSON payload that came in on the `InvocationRequest`; its shape depends entirely on the trigger that produced the request. `ctx` is the platform context — see the next section. Either argument may be ignored: a handler that returns a static health-check payload can take no arguments at all. goja passes JavaScript `undefined` for arguments the handler does not declare.

The handler is invoked through `goja.AssertFunction` and may return either a plain value or a `Promise`. A synchronous return is wrapped automatically; an `async function` returns a Promise that the runner awaits via the `awaitValue` helper in `runner.go`. A Promise that is still in the `Pending` state when control returns to Go is rejected — goja runs to completion within the calling goroutine, so a pending promise means the handler resolved by exhausting the script's synchronous microtask queue without ever entering a host call that would have driven the loop forward. In practice this means handlers must `await` every Promise they create; "fire-and-forget" patterns silently lose work.

The shape of `event` depends on the trigger:

- **HTTP trigger** (`trigger.type === "http"`): `event` is an object the gateway built from the inbound request — typically `{ method, path, query, headers, body, isBase64Encoded }`. The exact field set is defined by `cs-http-gateway` and documented in [HTTP Invoke Path](HTTP-Invoke-Path). A handler that needs to read a JSON body should `JSON.parse(event.body)` (and check `event.isBase64Encoded` for binary uploads).
- **Schedule trigger** (`trigger.type === "schedule"`): `event` is the `payload` field configured on the `ScheduleRecord`. The scheduler stamps the tick wall-clock into `trigger.source` (see [Scheduler](Scheduler)); the payload is whatever the operator pinned at create time.
- **Cadence activity** (`trigger.type === "cadence"`): `event` is the decoded Activity input. The decoder is selected by the `WorkerBinding.InputCodec` — `json` is the default and matches the v0.1 behaviour. Workflow handlers (E8.01) receive a different shape; see [Cadence Workflows](Event-Sources-Cadence-Workflows).

The runner enforces no schema on `event` beyond "must be JSON-serialisable". Validation is the function author's responsibility; the determinism linter does not look at `event`.

The handler's return value is mapped to a `FunctionResponse` by the runner. A returned object is `JSON.stringify`-ed and the result is unmarshalled into the `FunctionResponse` Go struct in `internal/api/types.go`; recognised fields are `statusCode` (integer, defaults to `200`), `headers` (string map, defaults to `{}`), `body` (string), and `isBase64Encoded` (boolean). Returning a non-object value — a string, a number, `true`, an array — is also legal: the runner JSON-encodes it into the `body` field and stamps `statusCode = 200`. Returning `undefined` or `null` yields a `200` response with an empty body.

The `statusCode` field is the source of truth for both the HTTP gateway (which forwards it as the response status) and the Cadence poller (which uses the status range to decide whether the Activity completed or failed). The convention is:

- `2xx` — success. Cadence reports `RespondActivityTaskCompleted`; the HTTP gateway returns the status verbatim.
- `4xx` — client error, not retried. Cadence reports `RespondActivityTaskFailed` with a non-retryable classification; the HTTP gateway returns the status verbatim.
- `5xx` — server error, retried for non-HTTP triggers. The retry policy in `internal/api/retry.go` is trigger-aware: HTTP triggers never retry; schedule, subscription, and Cadence triggers retry up to the configured attempt cap.

The `isBase64Encoded` flag tells the HTTP gateway to base64-decode the body before writing it onto the response. A handler that returns a binary payload (PDF, image, protobuf) should set `body` to the base64-encoded bytes and `isBase64Encoded: true`; a handler that returns text should leave `isBase64Encoded` unset (it defaults to `false`) and write plain text into `body`. The flag matches the shape of the HTTP gateway's request envelope so the round trip is symmetric.

The `headers` map's keys are forwarded verbatim — the gateway does not lowercase them, but the underlying Go `http.Header` does its own canonicalisation when writing the response, so authors should not rely on case being preserved. The gateway strips a few headers that are platform-controlled (`Connection`, `Transfer-Encoding`, `Server`) so a handler cannot smuggle them into the response.

## The ctx object

`ctx` is the platform-provided context object. The runner builds it from the `InvocationRequest` and a per-activation capability surface; the data fields are populated unconditionally, the capability namespaces are populated based on what the manifest declared. The shape lives in `runner.go` around line 302:

```js
{
  activation_id: "act_01HXY...",
  deadline_ms: 1730000003000,
  tenant: "t_abc123",
  namespace: "payments",
  function: "reconcile",
  ref: { alias: "prod", version: 17 },
  trigger: { type: "http" },
  principal: { sub: "user:123", roles: ["role:app"] },
}
```

These fields are stable: cs-invoker-pool will add new fields over time, but the v0.1 fields will never be removed without a manifest schema bump. Functions can rely on `activation_id` for log correlation, on `deadline_ms` for sub-budgeting their own work, and on `principal.roles` for in-handler authorisation checks beyond the role allowlists configured on the function version.

`activation_id` is a unique identifier the gateway, scheduler, or Cadence poller stamps onto every `InvocationRequest`. The id is durable: it survives retries (a retried Cadence activity reuses the same id), and it is the key under which the activation record is stored in KVRocks. Logs emitted through `cs.log.*` are tagged with this id; the `/activations/{id}` endpoint dereferences it. Authors who want their handler logs to correlate with an external trace ID can include the activation id in their downstream calls via a header.

`deadline_ms` is a Unix-millisecond timestamp computed by the source of the invocation. The HTTP gateway sets it to `now + min(http.timeoutMs, manifest.limits.timeoutMs)`; the scheduler sets it from the schedule's configured budget; the Cadence poller forwards Cadence's `scheduleToCloseTimeoutSeconds`. A handler that wants to budget its own work can compute `remaining = ctx.deadline_ms - Date.now()` and bail out when `remaining` falls below a safety margin. (Note: `Date.now()` is forbidden in workflow handlers — use `cs.workflow.now()` there.)

`principal.sub` is the resolved subject of the inbound caller (a user id for an HTTP trigger, the scheduler's service-account subject for a schedule, the Cadence poller's service-account for a Cadence trigger). `principal.roles` is the list of roles attached to that subject; for HTTP triggers it comes from Tikti introspection (see [IAM with Tikti](IAM-with-Tikti)), for scheduler and Cadence triggers it is the platform-configured role set for the respective service-account. The function's `VersionAuthz` configuration already gates which roles can invoke through which trigger; the values exposed to user code are informational and let a handler do finer-grained checks (e.g., "only role:admin can pass `force: true`").

`ref` carries the alias the gateway resolved (when invoked through `/v1/web/.../<alias>`) and the concrete version the alias pointed at. Functions that want to log "this is version 17 running under alias prod" can include both. `function` and `namespace` are the canonical identifiers; `tenant` is the resolved tenant id (always of the form `t_<lowercase-id>`).

`trigger.type` is one of `"http"`, `"schedule"`, `"subscription"`, or `"cadence"`. The values are stable and authors should pattern-match on them rather than infer from the shape of `event`. A defensive handler that wants to refuse non-HTTP invocations starts with `if (ctx.trigger.type !== "http") return { statusCode: 405 }`.

Layered on top of the data fields, the runner binds a single global `cs` object whose properties are capability namespaces. Each namespace is created unconditionally — the property exists even when the manifest grants no operations — and each method on the namespace performs its own permission check before reaching the underlying provider. The five namespaces below are exhaustively those installed by `bindLog`, `bindKV`, `bindCodeQ`, `bindHTTP`, `bindCadence`, and `bindEnv` in `internal/runtime/runner.go`.

The composition lives in `runJS` (`runner.go:239`): a fresh `csObj := rt.NewObject()`, then `bindLog(rt, csObj, logs)`, `bindKV(ctx, rt, csObj, manifest.Capabilities.KV)`, `bindCodeQ(ctx, rt, csObj, manifest.Capabilities.CodeQ)`, `bindHTTP(ctx, rt, csObj, manifest.Capabilities.HTTP)`, `bindCadence(rt, csObj, request)`, `bindEnv(ctx, rt, csObj)`, and finally `rt.Set("cs", csObj)`. Every binding consults the manifest before reaching for a provider, and every binding is per-activation: the closures captured by each `cs.*` method are fresh on each `Execute` call, so a misbehaving handler cannot mutate the capability surface seen by a sibling activation. The cost is a handful of pointer assignments per call, which the cold-start phase absorbs without measurable impact.

### cs.log

Structured logging. Three levels are bound: `info`, `warn`, `error`. Each takes a single argument that is JSON-serialised and appended to the activation's log buffer through `logCollector.Append`:

```js
cs.log.info("starting reconcile");
cs.log.warn({ retries: 3, account: event.accountId });
cs.log.error(new Error("downstream returned 500"));
```

The collector caps total log bytes at the runner's `maxLogBytes` (default 1 MiB; configured by `cs-invoker-pool`). When the cap is reached the in-flight line is truncated, subsequent lines are dropped, and the activation record is stamped with `result_truncated: true`. The implementation is in `logCollector` near the bottom of `runner.go`. Logs are persisted to KVRocks as part of the activation record and surface through the `GET /activations/{id}/logs` endpoint.

There is no `debug` level in v0.1. Authors who want a noisier development trace should write through `cs.log.info` and filter at retrieval time; the `level` label is preserved in each line. The serialiser is `JSONString` (defined in `internal/runtime/providers.go`), which falls back to the literal string `"null"` for any value that fails `json.Marshal` — passing a circular reference or a Symbol-keyed object yields the JSON literal rather than an error so a misbehaving log call never poisons the rest of the activation. Authors who want to log structured fields alongside a human-readable message should pass an object: `cs.log.info({ msg: "starting reconcile", account: id })` rather than concatenating strings.

### cs.kv

The per-tenant namespaced key-value store, backed by KVRocks in production and by an in-memory map in `cs fn test`. Three operations are bound:

```js
await cs.kv.get(key);                            // string | null
await cs.kv.set(key, value, { ttlSeconds: 60 }); // value JSON-encoded
await cs.kv.del(key);
```

Each call runs two guards. First the operation guard: `kv.ops` in the manifest must contain `"get"`, `"set"`, or `"del"` respectively, or the call panics with `CS_RUNTIME_CAP_DENIED`. Second the prefix guard: the key must start with one of `kv.prefixes`. The implementation in `bindKV` (`runner.go:445`) sorts the configured prefixes once at bind time and walks the list per call; an empty prefix list means "no KV access" even when ops are declared. `get` returns `null` for missing keys (so callers can distinguish "absent" from "empty string"); `set` JSON-encodes the value and accepts an optional `{ ttlSeconds }` option; `del` is idempotent. The KVRocks-backed provider lives in `internal/plugins/storage`; see [KVRocks](Enabled-Services-KVRocks) for the wire-level layout and the per-tenant namespacing convention.

The `get` path does one round trip to KVRocks, decodes the stored bytes back into a JS value (trying `JSON.parse` first, falling back to the raw string), and returns the result. A missing key returns `null`; a key that exists with the empty string returns the empty string, not null — authors who want a distinct "configured-as-empty" semantics need to encode it explicitly (e.g., store the JSON object `{ "value": "" }`). The `set` path JSON-encodes the value once via the same `JSONString` helper used by the log collector; a value that fails to marshal stores the literal `"null"` rather than aborting the activation, so a misbehaving callsite degrades to a write of `null` rather than a silent capability denial. The `ttlSeconds` option is the only `set` option in v0.1; passing `0` or omitting it means "no TTL" and the key persists until explicitly deleted.

Authors should keep prefixes short and meaningful. `"ctr:"` for counters, `"sess:"` for sessions, `"idem:"` for idempotency keys is the conventional shape; the prefix is purely a capability gate, not a storage layout, so a function with `"ctr:"` in the manifest can read or write any key under that prefix. Multi-prefix manifests are fine — `prefixes: ["ctr:", "sess:"]` lets a single handler touch both surfaces — but the upper bound is 64 prefixes per manifest (enforced by `Validate` in `internal/api/types.go`).

### cs.codeq

Outbound publish to a codeQ topic, gated by an allow-list of topic patterns:

```js
await cs.codeq.publish("jobs.reconcile.completed", { account: event.accountId });
```

The matcher in `bindCodeQ` (`runner.go:520`) supports literal topics (`"jobs.reconcile.completed"`) and trailing-wildcard patterns (`"jobs.*"`). Anything that does not match panics with `CS_RUNTIME_CAP_DENIED`. The publish itself is a single call into the `CodeQProvider` interface in `internal/runtime/providers.go`; the production wiring is the Kafka-backed `internal/plugins/messaging/codeq` package. There is no subscribe surface on `cs.codeq`: cs-js functions are leaves in the codeQ topology — they publish, they never consume. See [codeQ Protocol](codeQ-Protocol) for envelope shape and topic conventions.

The payload is serialised by the codeQ provider into the canonical envelope (`schema`, `tenant`, `namespace`, `timestamp`, `payload`). The handler should pass a JS value; the provider takes care of the envelope. A handler that wants to fan out work to a downstream consumer should publish a small, self-describing object — e.g., `{ accountId, eventVersion: 1, occurredAt: ctx.activation_id }` — and let the consumer dereference any heavy state through KV. Pushing large payloads directly through codeQ works but is rarely the right design: the consumer pays the network cost on every read and codeQ retention is bounded.

The matcher accepts at most one trailing `*`. Patterns like `"jobs.*.completed"` are *not* glob-aware in v0.1: the matcher treats the embedded `*` as a literal character, so the pattern matches only the literal string `"jobs.*.completed"`. Authors who want more expressive matching should declare multiple patterns: `["jobs.charges.completed", "jobs.refunds.completed"]` rather than `"jobs.*.completed"`. The upper bound is 64 topics per manifest.

### cs.http

Outbound HTTP with a synchronous fetch surface:

```js
const resp = await cs.http.fetch("https://api.example.com/v1/charge", {
  method: "POST",
  headers: { authorization: `Bearer ${cs.env.get("STRIPE_KEY")}` },
  body: JSON.stringify({ amount: 1000, currency: "usd" }),
  timeoutMs: 1500,
});
```

The shape returned is `{ status, headers, body, isBase64Encoded: true }` — the body is always base64-encoded so binary responses round-trip unmolested. Three guards run on every call, in order: (1) the tenant-level egress matcher (when one has been stamped onto the context by `WithEgressMatcher`) rejects any host the operator has not allow-listed for the tenant, surfacing `CS_EGRESS_DENIED`; (2) the manifest's `http.allowHosts` list rejects any host the publisher did not declare, surfacing `CS_RUNTIME_CAP_DENIED`; (3) the private-IP block (`isPrivateIP` in `runner.go:854`) rejects RFC 1918 addresses, link-local addresses, and loopback so a leaked SSRF cannot reach in-cluster services. The private-IP guard is on by default and can be disabled by the runner's owning binary for unit tests that target a loopback `httptest.Server`. The request timeout is the minimum of `http.timeoutMs` from the manifest and the optional `timeoutMs` option on the call. Operators tune the egress matcher through `EgressPolicy` documents — see [Operators Security](Operators-Security).

The supported options on the second argument are `method` (defaults to `"GET"`), `headers` (string map), `body` (string), and `timeoutMs` (clamped to the manifest cap). `body` must be a string in v0.1 — passing an object yields the JS string `"[object Object]"`, which is almost certainly not what the author intended. Handlers that want to send JSON should `JSON.stringify` themselves and set `content-type: application/json` explicitly. Multi-part uploads, streaming bodies, and HTTP/2 server-push are out of scope; the underlying transport is the standard Go `*http.Client` with a 5-second client-wide timeout that cs-invoker-pool overrides with a per-request context deadline derived from the manifest.

Response decoding is always base64. The runner does this because the byte stream is otherwise lost when goja converts a Go `[]byte` to a JS string — non-UTF-8 bytes mojibake silently. Handlers that know the response is text should `atob(resp.body)`; handlers expecting binary can pass the base64 string directly into a downstream KV write or codeQ publish without ever materialising the bytes as a string. Response headers are lower-cased and joined with commas (`Set-Cookie: a; ...` from a server that sends two headers becomes a single comma-joined string in `headers["set-cookie"]`) — this matches the conventional Go `http.Header.Get` behaviour and is the shape the gateway's response synthesiser expects.

The runner injects an `X-CS-Parent-Activation` header on every outbound request through `observability.InjectParentHeader` so a downstream cs function (reached through `cs-http-gateway`) can link its activation back to the parent. The injection is automatic; user code does not need to (and should not) set the header manually. Authors who want to suppress the header — for outbound calls to third-party APIs that reject unknown headers — can override it by setting `headers["x-cs-parent-activation"]` to the empty string, but the recommended path is to leave it alone.

### cs.env

Read a secret resolved at activation start.

```js
const apiKey = cs.env.get("STRIPE_KEY");   // string | null
const names  = cs.env.list();              // ["STRIPE_KEY", "WEBHOOK_SECRET"]
```

`cs.env` exposes the per-activation env map stamped onto `context.Context` by `runtime.WithEnv`. cs-invoker-pool resolves each `VersionConfig.Secrets` reference against the configured secret provider (Vault, AWS Secrets Manager, or the filesystem provider for local dev) once per activation; the resolved values never enter the bundle, KVRocks, or the activation result. `cs.env.get` returns `null` for unknown names so a function can distinguish "operator hasn't configured this yet" from "value is the empty string". `cs.env.list` returns the declared names sorted, so an init step can verify the secret surface without leaking material. See [Vault Secrets](Enabled-Services-Vault-Secrets) for the provider integration and the on-the-wire reference grammar.

The map is read-only by convention. `cs.env` does not expose a `set` method, and the underlying Go map is shallow-copied before being placed on the context — a misbehaving handler that tries to mutate the env through some prototype-pollution trick cannot reach the in-memory copy the invoker uses for the next activation. The map is also activation-scoped: cs-invoker-pool builds a fresh map for each invocation and discards it when `Execute` returns, so a function that wants to memoize a derived value (e.g., a parsed JWT) must do so in module scope and accept that the memo is lost between activations.

The reference grammar parsed by `internal/plugins/secrets.ParseRef` accepts three shapes:

```
NAME                              -> resolves "NAME" against the default provider/path
NAME=provider/path                -> explicit provider and path
NAME=provider/path#json-field:key -> extracts a JSON field from the resolved value
```

The `#json-field:` suffix is useful when a single secret object holds multiple values (e.g., Vault's `kv/payments/stripe` returns `{ api_key, webhook_secret }`). The handler then reads `cs.env.get("API_KEY")` and gets just the `api_key` field rather than the entire JSON blob.

Authors should not log secret material. The runner does not sanitize log output, and a `cs.log.info({ key: cs.env.get("STRIPE_KEY") })` will write the secret into the activation log buffer, which is persisted to KVRocks. Operators with the right capabilities could then read it. The convention is to never pass `cs.env.get(...)` to `cs.log.*`; if a handler needs to confirm a secret is configured, it should log its presence (`cs.log.info({ stripeConfigured: cs.env.get("STRIPE_KEY") !== null })`) rather than its value.

### cs.cadence

Cadence-aware affordances. Two methods are bound on the activity-side runtime, both with strict trigger gating.

```js
cs.cadence.heartbeat();                                        // activity only
await cs.cadence.scheduleActivity(type, args, opts);           // workflow only
```

`heartbeat` is a no-op outside the Cadence trigger and panics with `CS_RUNTIME_CAP_DENIED` for HTTP or schedule activations. `scheduleActivity` always panics from inside `cs-invoker-pool`: it is a *workflow-only* API whose real implementation lives in the workflow executor's own goja runtime under `internal/cadence/workflow/executor.go`. The denial in the activity-side binding (`runner.go:699`) exists so an author who writes `cs.cadence.scheduleActivity(...)` in an activity handler gets a clear "this is the wrong runtime" message instead of an `undefined is not a function` TypeError. See [Cadence Workflows](Event-Sources-Cadence-Workflows) for the workflow handler shape and the available `cs.workflow.*` surface.

The heartbeat surface in v0.1 is a stub — it accepts the call and returns `undefined`, but the actual Cadence `RecordActivityTaskHeartbeat` round-trip is performed by `cs-cadence-poller` on a fixed timer for the duration of the activity. Authors should call `cs.cadence.heartbeat()` at progress milestones for forward compatibility (a future runner will plumb the call through to the poller), but the v0.1 contract is that the poller is responsible for keeping the task alive. A long-running activity that exceeds `scheduleToCloseTimeoutSeconds` will be aborted by Cadence regardless of how often it calls `heartbeat`; authors who need finer-grained progress reporting should split the activity into smaller activities.

## Capability enforcement

Capabilities are declared in the function manifest at publish time and bound to the published version forever. The shape is constrained by the JSON Schema in `spec/cs.function.script.v1.json` and the strongly-typed validator in `internal/api/types.go`:

```json
{
  "capabilities": {
    "kv":    { "prefixes": ["ctr:"],            "ops": ["get", "set"] },
    "codeq": { "publishTopics": ["jobs.*"] },
    "http":  { "allowHosts": ["api.example.com"], "timeoutMs": 1500 }
  }
}
```

cs-control validates this block at publish time, rejects unknown ops, and persists it as part of the `VersionRecord`. At runtime cs-invoker-pool re-parses the manifest and threads it into `Runner.Execute`, which validates the structural shape a second time (defense in depth: a corrupted bundle in KVRocks cannot widen the privilege surface) before binding the per-call host hooks. The double-validate is intentional and is documented in [Concepts Capabilities and Isolation](Concepts-Capabilities-and-Isolation).

The semantics are default-deny. A capability namespace that is absent from the manifest binds the host hook with an empty allow-list, so the very first call from user code panics. The five denial codes are:

| Surface          | Code                       | When                                                                      |
| ---------------- | -------------------------- | ------------------------------------------------------------------------- |
| `cs.kv.*`        | `CS_RUNTIME_CAP_DENIED`    | Op not in `kv.ops`, or key prefix not in `kv.prefixes`.                   |
| `cs.codeq.*`     | `CS_RUNTIME_CAP_DENIED`    | Topic not matched by any entry in `codeq.publishTopics`.                  |
| `cs.http.fetch`  | `CS_RUNTIME_CAP_DENIED`    | Host not in `http.allowHosts`, or host resolves to a private IP.          |
| `cs.http.fetch`  | `CS_EGRESS_DENIED`         | Host blocked by the tenant-level EgressPolicy (operator-controlled).      |
| `cs.cadence.*`   | `CS_RUNTIME_CAP_DENIED`    | `heartbeat` outside a cadence trigger, or `scheduleActivity` in activity. |

Each denial is raised through `cserrors.New(code, message)` and surfaces in goja as a JavaScript `Error` whose `message` carries the code prefix. The runner's error classification block (`runner.go:175`) maps the prefix back onto `ExecutionOutput.ResolvedCode`, which cs-invoker-pool serialises into the `InvocationResult.error` envelope — see [Error Model](Error-Model) for the full mapping. Authors who want to handle a denial in user code rather than fail the activation can wrap the call in `try` / `catch`:

```js
try {
  await cs.http.fetch(url, opts);
} catch (e) {
  if (String(e).includes("CS_RUNTIME_CAP_DENIED")) {
    cs.log.warn({ skipped: url });
    return { statusCode: 204 };
  }
  throw e;
}
```

Defensive `catch` blocks are an escape hatch, not a recommendation. The intended pattern is to declare the right capabilities in the manifest and let denials fail loudly; silently swallowing a denial in production tends to hide a misconfigured publish.

## Resource limits

Three limits land in the manifest's `limits` block and one more is enforced by the runner regardless of what the manifest says.

**Wall-clock timeout** (`limits.timeoutMs`, 1–900000). cs-invoker-pool sets `request.deadline_ms = now_ms + timeoutMs` when the request is built; `Runner.Execute` derives a Go `context.WithDeadline` from that value and arms a `time.AfterFunc` that calls `rt.Interrupt("runtime timeout")` on the goja runtime when the deadline fires. The interrupt is co-operative — goja checks it between bytecode operations — so a tight CPU loop terminates within a few milliseconds of the deadline; a blocking host call (KV, HTTP) terminates as soon as its own context is cancelled. The runner classifies any `context.DeadlineExceeded` (and any error whose message contains `"timeout"`) as status `timeout` with code `CS_RUNTIME_TIMEOUT`.

**Heap cap** (`limits.memoryMb`, 16–4096). The manifest carries the budget; production cs-invoker-pool runs each invocation inside a control-group-bounded goroutine pool whose total heap is sized from the manifest. goja itself does not have a per-isolate heap cap — the V8 model does not translate — so the enforcement is at the process boundary. A function that allocates beyond the cgroup limit is killed by the OOM killer and the activation is reported as `CS_RUNTIME_OOM`.

**Concurrency** (`limits.maxConcurrency`, 1–100). cs-invoker-pool maintains a semaphore keyed by `(tenant, namespace, function, version)` and refuses to dispatch beyond the limit. The cap is per-replica; total concurrency across the invoker fleet is `maxConcurrency × replicas`. See [Capacity and Limits](Capacity-and-Limits) for the autoscaling story.

**Result and log size caps** are enforced by the runner itself, not the manifest. `Runner` carries three byte budgets — `maxResultBytes` (default 256 KiB), `maxErrorBytes` (default 64 KiB), `maxLogBytes` (default 1 MiB) — that cs-invoker-pool tunes from operator config. When the JSON-encoded `FunctionResponse` exceeds `maxResultBytes` the runner truncates the bytes, re-decodes the prefix, and stamps `out.Truncated = true`. When an error message exceeds `maxErrorBytes` the same truncation happens on the error string. The log collector enforces `maxLogBytes` line-by-line (see `cs.log` above). All three caps are independent of the manifest; the rationale is that the platform owns the persistence budget for KVRocks and the manifest cannot widen it.

The `request.DeadlineMS` field on the inbound `InvocationRequest` is the authoritative deadline; the manifest's `limits.timeoutMs` is the *publisher-declared maximum* but the gateway, scheduler, or Cadence poller may set a tighter deadline based on operator policy or the trigger's own constraints. The runner takes the minimum: `timeout = time.Until(time.UnixMilli(request.DeadlineMS))`; if that's already in the past the runner falls back to the manifest cap. Authors can read `ctx.deadline_ms` from inside the handler and budget their own work against it — a long-running sweep can stop short of the deadline and report progress rather than time out.

Concurrency is enforced *outside* the runner. `Runner.Execute` is fully re-entrant — it can be called from many goroutines concurrently — and the `maxConcurrency` semaphore lives in cs-invoker-pool's dispatcher. The reason matters: a handler that recursively invokes itself through `cs.http.fetch` does not consume its own concurrency budget at the runner layer (the recursive call is a new request through the gateway), but the dispatcher counts both invocations against the per-(tenant, namespace, function, version) semaphore. A function whose `maxConcurrency` is `1` cannot recurse against itself; cs-control rejects such configurations at publish time when the static call graph reveals a cycle.

## What is blocked

The goja interpreter does not ship the host-platform surface that a Node author would expect. The omissions are not bugs — they are the entire point of running inside an isolate.

Filesystem access is absent. There is no `require("fs")`, no `import("node:fs")`, no `Bun.file`, no synchronous `readFileSync`. The bundle's bytes are visible to the runtime through the frozen import map, but they are not visible to user code as a filesystem.

Process control is absent. There is no `require("child_process")`, no `import("node:child_process")`, no `process.spawn`. The global `process` object is not bound; references to `process.env`, `process.exit`, `process.argv`, or `process.pid` are reference errors. Secrets reach user code through `cs.env.get(name)`, not `process.env[name]`.

Native modules are absent. goja does not implement Node's binary module ABI; there is no `dlopen`, no NAPI, no FFI. The only "native" surface user code can reach is the Go-backed `cs.*` host hooks.

The networking surface is exactly `cs.http.fetch`. The global `fetch` is not bound — referencing it raises a `ReferenceError`. There are no `XMLHttpRequest`, `WebSocket`, `dgram`, `net`, or `tls` surfaces.

`setTimeout` and `setInterval` are not bound. goja runs the script to completion in a single goroutine; there is no event loop that would deliver a delayed callback. A handler that needs to wait should drive the wait through a host call (typically by polling KV or by structuring the work as a Cadence activity); functions that depend on timer-driven control flow are a poor fit for the cs-js runtime.

`Promise` and `async`/`await` *are* supported — goja implements the ES2017 surface and the runner's `awaitValue` helper drives the microtask queue to completion before reading the resolved value. The constraint is that every Promise must resolve synchronously *with respect to the host*: a Promise that depends on a `setTimeout` callback to settle will hang because there is no event loop to deliver the callback, and the runner will report `"pending promise is not supported"`. Promises returned from host calls (`cs.http.fetch`, `cs.kv.*`, `cs.codeq.publish`) are driven by Go and settle correctly.

The `globalThis` surface is minimal: the ECMAScript built-ins (`Object`, `Array`, `Map`, `Set`, `Date`, `Math`, `JSON`, `Error`, `Promise`, `RegExp`, `Symbol`, `Proxy`, `Reflect`, `BigInt`, the typed-array constructors), `console` (mapped to `cs.log` via the runtime wiring, see notes in `runner.go`), and the `cs` host object. Anything else is a reference error.

`Date` is supported but yields the host's wall clock — which is fine in an HTTP, schedule, or Cadence-activity trigger, and forbidden in a workflow handler. `Math.random` is supported but is *not* a CSPRNG; functions that need cryptographic randomness in an activity-side handler should call out to a `cs.http.fetch`-mediated downstream that owns the entropy source, or read a seeded value from `cs.env.get`. Workflows must not call `Math.random` at all — the linter rejects it at publish time.

`crypto.subtle` is *not* bound. The Go-side `crypto/*` packages are not exposed to goja, and goja does not implement the WebCrypto API. Authors who need HMAC or AEAD for an outbound API signature should pre-compute the signature in a previous step (e.g., a Cadence activity whose only job is to mint a signature) and pass the result through `cs.env.get`, or use a downstream API that accepts a bearer token instead of a signed envelope.

`TextEncoder` and `TextDecoder` are bound by goja's built-ins and work for UTF-8. `URL` and `URLSearchParams` are also bound. `Blob`, `File`, `FormData`, and `Response`/`Request`/`Headers` (the Fetch API DOM types) are *not* bound — `cs.http.fetch` returns a plain object rather than a `Response`, intentionally, so handlers do not develop a dependency on a browser-shaped surface that the runtime would have to keep emulating across goja versions.

## ES module support

The cs-js runtime is not a native ES module loader — goja is a script interpreter, not a module graph evaluator. Authors write modern `import` syntax, and cs-control freezes the dependency graph at publish time so the runtime can resolve specifiers without ever reaching the network. The pipeline lives in `internal/runtime/imports.go`.

A `function.js` may use any of the following import shapes:

```js
import x from "spec";
import * as ns from "spec";
import { a, b as c } from "spec";
import x, { a } from "spec";
import "spec";
```

At publish time, cs-control walks the `imports` block in the manifest, fetches or copies each entry's bytes, computes (or verifies) the SubResource Integrity digest, writes the bytes under `deps/<safe-name>` in the canonical tar, and emits an `import-map.json` at the bundle root that maps each specifier to its frozen path and digest. The schema for the frozen map is `cs.importmap.v1`; the bundle is signed (E5.02) so any subsequent tampering invalidates the publish.

At invoke time, `rewriteImportStatements` (in `imports.go`) rewrites the source into `const ... = __cs_require("spec")` form before evaluation, and `bindImports` installs `__cs_require` as a goja-bound host helper. The helper looks the specifier up in the frozen import map, verifies the SRI digest against the bytes in the bundle (defense in depth even though the bundle is signed), evaluates the dep module in the same goja runtime, and caches the resulting `exports` object. A specifier that is not declared in `manifest.imports` raises `CS_IMPORT_NOT_FOUND` (HTTP 422); a digest mismatch surfaces the same code with an integrity-failed message.

There are three hard rules. First, there is no build step — the publisher uploads UTF-8 source plus the manifest and cs-control is the only thing that touches the network on the publisher's behalf. Second, there is no network egress at invoke time; specifiers resolve against the frozen bundle or they fail. Third, the 16 MiB bundle cap covers the *frozen* bundle, including the `deps/` subtree and `import-map.json` (see [Capacity and Limits](Capacity-and-Limits)). For the full publish flow and the curated-mirror configuration, see [Managing Functions Packages](Managing-Functions-Packages).

Dep modules may use any of:

```js
export default <expr>;
export const NAME = <expr>;
export let NAME = <expr>;
export var NAME = <expr>;
export function NAME(...) { ... }
export async function NAME(...) { ... }
export class NAME { ... }
export { a, b as c };
```

Re-exports (`export { x } from "spec"`) and namespace re-exports (`export * from "spec"`) are out of scope for v0.1 and surface as goja parse errors. Authors who want to expose a re-export shape should write a small adapter dep that imports and re-binds.

Circular imports are tolerated by the `__cs_require` cache. When dep A imports dep B which imports dep A, the cache seeds an empty `exports` object for A before evaluating A's body; B sees a partially-initialised A and is expected to capture references to A's bindings without immediately reading their values. This is the same convention Node uses for CommonJS cycles. Authors should avoid cycles when possible — they are a code smell — but the runtime does not crash on them.

The `imports` manifest block has its own validation rules (`internal/api/types.go:574`). Each entry must declare exactly one of `url:` or `path:` (declaring both, or neither, is rejected as a manifest error). When `integrity` is present it must use the `sha256-` or `sha384-` prefix; other algorithms are rejected. The upper bound is 128 imports per manifest, and each specifier string is capped at 256 characters. cs-control computes the integrity digest for any `url:` entry that omits it (fetching once at publish time and freezing the result), so an author who fully trusts the curated mirror can leave `integrity` unspecified and let the publish step pin it. The mirror allow-list is operator-controlled — see [Operators Security](Operators-Security).

## Cold-start

A cs-js cold-start has three observable phases: bundle download, parse, and evaluation. The first phase is a single KVRocks `GET` for the canonical tar by SHA-256; cs-invoker-pool memoizes the bytes per `(function, version)` so subsequent activations on the same replica skip the network hop entirely. The second phase is `bundle.ExtractTar` plus the `transformESModule` / `rewriteImportStatements` text passes; goja's parser is the dominant cost on a fresh isolate. The third phase is `rt.RunString(compiled)` which evaluates the top-level of `function.js` and any dep modules pulled in by `__cs_require`.

Warm activations skip phase one (bundle is in memory) but not phases two and three: each activation gets a fresh goja runtime. The rationale is correctness — a long-lived isolate would accumulate global state across tenants and across versions, and the workflow determinism story requires that replays start from a clean slate. The cost is acceptable because goja's parse-and-evaluate of a small handler is in the low single-digit milliseconds; for handlers where the cost matters, the recommended pattern is to keep the module body small (push initialisation into a memoized helper) and to keep deps lean.

Concretely, the construction `rt := goja.New()` allocates a fresh runtime per call (`runner.go:240`). Each capability binding (`bindLog`, `bindKV`, `bindCodeQ`, `bindHTTP`, `bindCadence`, `bindEnv`) installs a small closure on the freshly minted `cs` object. The closures themselves are cheap (a few hundred bytes each); the dominant cost is the goja parse of the compiled source. A 5 KiB `function.js` with no deps typically parses in well under a millisecond; a 100 KiB bundle pulling in a 50 KiB dep parses in a handful of milliseconds. Operators who need to budget the cold-start tax should measure against the actual bundle, not against a synthetic micro-benchmark: the constant cost of binding the host hooks is small relative to user-code parse, but the parse cost scales with bundle size.

There is no module cache that survives across activations. Each call constructs a brand-new `goja.Runtime`, evaluates the compiled source from scratch, and discards the runtime when `Execute` returns. The `bundle.ExtractTar` results — the in-memory file map — are also rebuilt per call, although the underlying bundle bytes are memoised by cs-invoker-pool. Operators who want to amortise parse cost across many short activations should increase `maxConcurrency` on the manifest so a single replica can pipeline more work per second, rather than relying on a hot-isolate cache that does not exist by design.

cs-invoker-pool publishes per-activation timings under `cs.invoker.activation.duration_ms` labelled by `phase` (`extract`, `parse`, `evaluate`, `handler`); see [Observability](Observability) for the metric catalogue. The histograms separate phase from total duration so an operator can answer "is the function slow, or is the bundle slow to parse?" without re-instrumenting the function. A function whose `parse` phase is dominant is a candidate for splitting deps; a function whose `handler` phase is dominant is a candidate for capability or downstream-API tuning.

## Determinism for workflow handlers

A cs function whose manifest declares `cadence.kind == "workflow"` is a Cadence workflow handler. Workflows are replayed against their history on every Decision, which means *every* non-deterministic API call corrupts replay state and surfaces as a NonDeterministic-history failure hours after publish — long after the offending change has rolled out.

cs-control runs a publish-time static linter (`internal/cadence/determinism/scan.go`) against the JS source of any workflow bundle and rejects the publish with `CS_WORKFLOW_NON_DETERMINISTIC` on any violation. The banned-API table in v0.1 covers:

- `Date.now`, `Date()` (the no-arg constructor), `Math.random`, `crypto.randomUUID` — wall-clock and entropy reads.
- `setTimeout`, `setInterval`, `setImmediate`, `queueMicrotask` — async timers (these are unbound in cs-js anyway, but the linter flags the reference so the author sees the error at publish time rather than at first replay).
- bare `fetch(` — the global, which is unbound; `cs.http.fetch` is the mediated alternative and is allowed in activities but *not* in workflows (workflows must schedule activities for I/O, not perform it directly).

Authors who need wall-clock time inside a workflow read it through the workflow runtime's `cs.workflow.now()` helper, which returns the deterministic decision-task time. Authors who need randomness use `cs.workflow.sideEffect(() => ...)` to capture the value into history once.

A single line can opt out of a violation by suffixing it with `// cs-determinism-allow` — operator review at code-review time is the override gate. The scan implementation, the full table, and the override semantics are documented in [Determinism Linter](Development-Tools-Determinism-Linter).

The full v0.1 banned-API table is:

| Pattern                  | Why it is banned                                                  | Replacement                                     |
| ------------------------ | ----------------------------------------------------------------- | ----------------------------------------------- |
| `Date.now`               | Reads the host wall clock.                                        | `cs.workflow.now()`                             |
| `new Date()`             | Same; no-arg constructor reads the wall clock.                    | `cs.workflow.now()`                             |
| `Math.random`            | Host entropy source.                                              | `cs.workflow.sideEffect(() => Math.random())`   |
| `crypto.getRandomValues` | Same.                                                             | `cs.workflow.sideEffect(...)`                   |
| `setTimeout`             | Schedules real-time callbacks.                                    | `cs.workflow.sleep(durationMs)`                 |
| `setInterval`            | Same; recurrence must model through the workflow loop.            | recurrence via the workflow loop                |
| `setImmediate`           | Yields to host event loop.                                        | workflows are synchronous between awaits        |
| `performance.now`        | Reads the monotonic clock.                                        | `cs.workflow.now()`                             |
| `fetch(`                 | Unmediated network IO bypasses replay.                            | schedule an Activity that performs the fetch    |

The linter is a regex pass over `function.js` and every `.js` file under `deps/`. It does not parse the source, so a `Date.now` reference that is a comment is *not* flagged (regexes don't see comments as comments), but `Math.random123` is *not* flagged either (the `\b` anchor stops the match). The opt-out marker is a substring match on the same line as the violation, case-sensitive: `// cs-determinism-allow` opts out, `// cs-determinism-Allow` does not. The marker is intentionally noisy in code review so a reviewer sees every opt-out.

A workflow that ships without violations still has to survive replay correctness — the linter is a quick first guardrail, not a proof. A handler that imports a dep which dynamically constructs identifiers (`globalThis["Date" + ".now"]()`) will bypass the linter and fail at replay. Authors writing non-trivial workflow logic should pair the linter with replay tests; cs-control surfaces a replay-test harness in `internal/cadence/workflow` that exercises the workflow against captured histories.

## Local parity with cs fn test

The `cs` CLI ships the same `internal/runtime` package as `cs-invoker-pool`. `cs fn test` constructs a `runtime.Runner` with the in-memory `KVProvider` and `NopCodeQ` shipped from `internal/runtime/providers.go`, parses the local manifest, packs the working directory into the canonical tar shape, and calls `Runner.Execute` exactly as the cluster invoker would. The capability checks fire the same way; a denial that fails in production fails identically in `cs fn test`, and a happy-path test that passes locally does not depend on any cluster-specific affordance.

Two practical implications follow. First, the local CLI is the lowest-cost way to surface a capability misdeclaration: a handler that calls `cs.codeq.publish("jobs.foo")` with `publishTopics: ["jobs.bar"]` in the manifest fails the *first* time the test runs, not at 3 a.m. after promotion. Authors who want to validate the capability surface without exercising every code path can write a smoke test that just calls each `cs.*` method behind a feature flag and asserts no `CS_RUNTIME_CAP_DENIED` is raised. Second, the local CLI defaults the egress matcher to nil, so `cs.http.fetch` to an allow-listed host succeeds even when the tenant-level `EgressPolicy` in production would deny it. Operators who need to validate the tenant-level allow-list should run a smoke test against staging cs-control with the policy in place; the local CLI is intentionally permissive for developer iteration.

The CLI exposes a few flags that the runner honours. `--kv-endpoint` swaps the in-memory KV for the production provider so a handler can be tested against the same KVRocks instance the cluster uses; `--event` accepts a JSON payload that becomes `request.Event`; `--deadline-ms` sets `request.DeadlineMS` so the handler can be tested against the same wall-clock budget it will see in production. The result is JSON-printed to stdout and the logs are JSON-printed to stderr, both in the same `[level] value` shape `logCollector.Append` produces. See [CLI](CLI) for the full flag catalogue.

A complete local round-trip looks like:

```bash
$ cat event.json
{"path":"/charge","body":"{\"accountId\":\"acc_1\",\"amount\":1000}"}

$ cs fn test reconcile --event ./event.json --deadline-ms 3000
{
  "status": "success",
  "duration_ms": 7,
  "result": {
    "statusCode": 200,
    "headers": { "content-type": "application/json" },
    "body": "{\"ok\":true,\"count\":1}"
  },
  "logs": [
    "[info] {\"activation\":\"local-1\",\"path\":\"/charge\"}"
  ]
}
```

The local round-trip uses the same `ExecutionOutput` shape that `cs-invoker-pool` serialises onto the `cs.results` topic in production. The only difference is the transport: the CLI prints the bytes; the invoker publishes them. Authors who want to reproduce a production failure can pull the activation record from `GET /activations/{id}` and feed the captured `event` back into `cs fn test --event ...` to get a bit-identical replay against the same `function.js` they have checked out locally.

## Observability for cs-js functions

Every activation produces a deterministic emission surface. The `ActivationRecord` persisted to KVRocks carries the `activation_id`, the `tenant`, `namespace`, `function`, the resolved `ref`, the trigger envelope, the start/end timestamps, the duration, the resolved `Status` (`success`, `error`, `timeout`, `dropped`), and either the `Result` or the `Error`. The log buffer captured by `logCollector` is persisted alongside the activation under a per-activation key, retrieved through `GET /activations/{id}/logs`. The error envelope carries the resolved `cserrors.Code` so downstream alerting can pivot on a typed enum rather than free-form strings; the resolved code list lives in [Error Model](Error-Model).

Three metric families are published by cs-invoker-pool for cs-js activations. `cs.invoker.activation.duration_ms` is a histogram labelled by `tenant`, `namespace`, `function`, `status`, and `phase` (one of `extract`, `parse`, `evaluate`, `handler`). `cs.invoker.activation.bytes` is a histogram of result + log bytes labelled by `tenant`, `namespace`, `function`. `cs.invoker.activation.count` is a counter labelled by `tenant`, `namespace`, `function`, `status`, `code`. The `code` label is the empty string for success and the resolved `CS_*` code for every other outcome; alerts keyed on `code != ""` and grouped by `function` give a per-function error rate without per-error noise.

Authors who instrument their own handlers through `cs.log.*` get the lines back through the same `/logs` endpoint and through the live tail that `cs-invoker-pool` exposes for an in-flight activation. Authors should resist the temptation to over-instrument: each `cs.log.*` call consumes from the `maxLogBytes` budget, and a single `cs.log.info` on a hot path can burn the entire budget on a long-tail activation. The recommended pattern is to log at the boundaries (handler start, handler end, denied capability) and at the error sites; per-iteration logging inside a hot loop should be off by default and gated behind a `cs.env.get("DEBUG_LOGGING")` check or similar.

The `X-CS-Parent-Activation` header that the runner injects on `cs.http.fetch` (`runner.go:651`) is the glue that lets cs-control reconstruct the call graph across activations. When a cs-js handler invokes another cs function through the HTTP gateway, the downstream activation's `ParentActivationID` is automatically populated from the header; the `RootActivationID` propagates from parent to child so an entire call tree is reachable through the `/activations/{id}/tree` endpoint. The propagation is invisible to user code — the runner stamps the header automatically — but operators rely on it to debug fan-out workflows. See [Observability](Observability) for the tree-walk semantics and the sampling-policy hooks that govern when full activation records are retained versus reduced to skeleton rows.

## Worked example

A complete HTTP handler that reads a counter from KV, calls an outbound API to charge a customer, increments the counter, and returns a JSON response.

`function.js`:

```js
export default async function handle(event, ctx) {
  cs.log.info({ activation: ctx.activation_id, path: event.path });

  const body = JSON.parse(event.body || "{}");
  if (!body.accountId) {
    return { statusCode: 400, body: JSON.stringify({ error: "missing accountId" }) };
  }

  const counterKey = `ctr:charges:${body.accountId}`;
  const before = (await cs.kv.get(counterKey)) || 0;

  const resp = await cs.http.fetch("https://api.example.com/v1/charge", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${cs.env.get("STRIPE_KEY")}`,
    },
    body: JSON.stringify({ amount: body.amount, currency: "usd" }),
    timeoutMs: 1500,
  });

  if (resp.status >= 400) {
    cs.log.error({ status: resp.status, accountId: body.accountId });
    return { statusCode: 502, body: JSON.stringify({ error: "downstream failed" }) };
  }

  await cs.kv.set(counterKey, Number(before) + 1, { ttlSeconds: 86400 });
  await cs.codeq.publish("jobs.charges.completed", { accountId: body.accountId });

  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ok: true, count: Number(before) + 1 }),
  };
}
```

`manifest.json`:

```json
{
  "schema": "cs.function.script.v1",
  "runtime": "cs-js",
  "entry": "function.js",
  "handler": "default",
  "limits": {
    "timeoutMs": 3000,
    "memoryMb": 64,
    "maxConcurrency": 4
  },
  "capabilities": {
    "kv":    { "prefixes": ["ctr:"], "ops": ["get", "set"] },
    "codeq": { "publishTopics": ["jobs.charges.*"] },
    "http":  { "allowHosts": ["api.example.com"], "timeoutMs": 1500 }
  }
}
```

The function declares exactly the capabilities it uses. Two KV ops (`get`, `set`) under the `ctr:` prefix; one codeQ topic glob (`jobs.charges.*`); one outbound host (`api.example.com`) with a 1.5 s timeout. The version config (set at publish time, not in the manifest) carries the secret reference `STRIPE_KEY=vault/payments/stripe#json:api_key` — see [Vault Secrets](Enabled-Services-Vault-Secrets) for the reference grammar. Authoring this function locally and round-tripping it through `cs fn test` exercises the same `Runner.Execute` path that the cluster invoker uses; see [Local Dev Publish Promote](Use-Cases-Local-Dev-Publish-Promote) for the publish flow.

## Error propagation

A cs-js activation can fail in four ways, and each surfaces differently in the `InvocationResult`.

**Thrown user error.** A `throw` from user code (whether literal `throw new Error(...)` or an exception that propagates out of an `async` function) becomes a goja `*GoError` that the runner classifies as status `error`, type `Exception`, code `CS_RUNTIME_EXCEPTION`. The error message is the JS `Error.message` (or `Error.stack` when available) extracted by `formatPromiseRejection` (`runner.go:377`). Stack traces are not currently exposed in the `InvocationError.Stack` field — that field exists in the type but the runtime does not populate it in v0.1. The error message is truncated at `maxErrorBytes` (default 64 KiB).

**Returned malformed result.** A handler that returns a value that is not JSON-serialisable, or returns an object whose `statusCode` is outside `100..599`, or whose `headers` has empty keys, fails `api.ValidateResultShape` and surfaces as status `error`, type `ValidationError`, code `CS_RUNTIME_EXCEPTION`. The handler ran to completion; only the return value is rejected.

**Capability denial.** Any `cs.*` call that fails its capability check raises a typed `cserrors.CSError` whose message contains the code prefix. The runner's classification block (`runner.go:175`) recognises `CS_RUNTIME_CAP_DENIED`, `CS_EGRESS_DENIED`, and `CS_IMPORT_NOT_FOUND` by substring and stamps the resolved code accordingly. To the JS author the denial is just a thrown exception with a recognisable message; to the operator the activation is reported with the precise code so dashboards and alerts can fire on the right surface. See [Error Model](Error-Model) for the full code list.

**Wall-clock timeout.** When the deadline fires `rt.Interrupt("runtime timeout")` causes goja to abandon the script at the next bytecode boundary. The runner sees `context.DeadlineExceeded` (or an error whose message contains `"timeout"`) and reports status `timeout`, type `Timeout`, code `CS_RUNTIME_TIMEOUT`. Logs accumulated up to the timeout are preserved (the collector is independent of the runtime), so an activation that timed out mid-loop still surfaces every `cs.log.*` line written before the deadline.

A runtime crash — a goja panic that escapes the runner's recovery — would be reported by cs-invoker-pool as `CS_RUNTIME_PANIC` and would terminate the worker goroutine. In v0.1 this is the contract for "should never happen"; the runner's own code paths are panic-free by construction (every `panic(rt.NewGoError(...))` inside a host hook is caught by goja's call boundary and converted into a JS exception, never an OS-level crash). If a crash is ever observed in production, the activation record is preserved with the panic message and the operator runbook calls for filing an incident against `cs-invoker-pool`.

Retries are a separate concern from error classification and are decided by `internal/api/retry.go` based on the trigger type and the resolved code. HTTP triggers never retry; schedule, subscription, and Cadence triggers retry transient codes (`CS_RUNTIME_TIMEOUT`, `CS_RUNTIME_DEPENDENCY_ERROR`) up to the configured attempt cap. See [Error Model](Error-Model) and [Cadence Integration](Cadence-Integration) for the retry policy details.

## Schedule-trigger worked example

A scheduled job that sweeps a backlog from KV and republishes work to codeQ. The function runs every minute under a `ScheduleRecord` configured with `kind: "interval"`, `everySeconds: 60`, and `payload: { batchSize: 50 }`.

`function.js`:

```js
export default async function handle(event, ctx) {
  const batchSize = (event && event.batchSize) || 25;
  cs.log.info({ tick: ctx.activation_id, batchSize });

  const cursorKey = "ctr:sweep:cursor";
  let cursor = (await cs.kv.get(cursorKey)) || "0";
  let pushed = 0;

  for (let i = 0; i < batchSize; i++) {
    const itemKey = `ctr:item:${Number(cursor) + i}`;
    const item = await cs.kv.get(itemKey);
    if (item === null) break;

    await cs.codeq.publish("jobs.sweep.item", { id: itemKey, payload: item });
    pushed++;
  }

  await cs.kv.set(cursorKey, String(Number(cursor) + pushed));
  cs.log.info({ pushed, nextCursor: Number(cursor) + pushed });

  return { statusCode: 200, body: JSON.stringify({ pushed }) };
}
```

`manifest.json`:

```json
{
  "schema": "cs.function.script.v1",
  "runtime": "cs-js",
  "entry": "function.js",
  "handler": "default",
  "limits": {
    "timeoutMs": 30000,
    "memoryMb": 128,
    "maxConcurrency": 1
  },
  "capabilities": {
    "kv":    { "prefixes": ["ctr:"], "ops": ["get", "set"] },
    "codeq": { "publishTopics": ["jobs.sweep.*"] },
    "http":  { "allowHosts": [], "timeoutMs": 1000 }
  }
}
```

Two details to note. First, `maxConcurrency: 1` is paired with the scheduler's `overlap_policy: "skip"` (configured at schedule-create time, not in the manifest) so a slow sweep cannot stack invocations on top of itself. Second, the function declares `http.allowHosts: []` and `http.timeoutMs: 1000` even though it never calls `cs.http.fetch` — the manifest validator requires both fields to be present, and declaring an empty allow-list is the explicit way to say "this function does no outbound HTTP". A `cs.http.fetch` call from this function would fail the host check on the first call with `CS_RUNTIME_CAP_DENIED`.

The `event` payload (`{ batchSize: 50 }`) is the literal `payload` field configured on the `ScheduleRecord`; the scheduler passes it unchanged on every tick. `ctx.trigger.type === "schedule"` and `ctx.trigger.source` carries the scheduler-stamped tick wall-clock — see [Scheduler](Scheduler) for the tick envelope.

## Cadence activity-trigger worked example

The same KV-and-publish pattern, this time fronted by a Cadence Activity tasklist. The function is registered against a `WorkerBinding` whose `ActivityMap` maps the Activity type `chargeAccount` to `(payments.reconcile, alias=prod)`.

`function.js`:

```js
export default async function handle(event, ctx) {
  if (ctx.trigger.type !== "cadence") {
    return { statusCode: 400, body: JSON.stringify({ error: "wrong trigger" }) };
  }

  cs.log.info({ activity: event.activity_type, accountId: event.input.accountId });

  // The decoded Activity input. The codec is set on the WorkerBinding
  // (json by default); event.input is the already-decoded JS value.
  const { accountId, amount } = event.input;

  const lockKey = `ctr:lock:${accountId}`;
  const existing = await cs.kv.get(lockKey);
  if (existing !== null) {
    cs.log.warn({ accountId, locked: true });
    return { statusCode: 409, body: JSON.stringify({ error: "locked" }) };
  }
  await cs.kv.set(lockKey, "1", { ttlSeconds: 30 });

  // Heartbeat to Cadence so a long-running Activity does not time out.
  cs.cadence.heartbeat();

  const resp = await cs.http.fetch("https://api.example.com/v1/charge", {
    method: "POST",
    headers: { authorization: `Bearer ${cs.env.get("STRIPE_KEY")}` },
    body: JSON.stringify({ accountId, amount }),
    timeoutMs: 5000,
  });

  await cs.kv.del(lockKey);

  return {
    statusCode: resp.status,
    body: JSON.stringify({ accountId, status: resp.status }),
  };
}
```

`manifest.json` (the relevant capability shape):

```json
{
  "capabilities": {
    "kv":    { "prefixes": ["ctr:"],            "ops": ["get", "set", "del"] },
    "codeq": { "publishTopics": [] },
    "http":  { "allowHosts": ["api.example.com"], "timeoutMs": 5000 }
  }
}
```

Three notes. First, the function checks `ctx.trigger.type` explicitly because a Cadence-bound handler should refuse non-Cadence invocations — running the same code through the HTTP gateway would skip the at-most-once semantics Cadence provides. Second, `cs.cadence.heartbeat()` is a no-op when the trigger is `cadence` but panics for any other trigger; the explicit `if` above is redundant if heartbeat is the first call, but defensive guards on `trigger.type` are still the recommended pattern. Third, the function uses `cs.kv.del` to release the lock, which means the manifest must include `"del"` in `kv.ops` — a common manifest-and-code drift bug to watch for in code review. See [Cadence Activity Invoke](Use-Cases-Cadence-Activity-Invoke) for the binding side and the at-most-once semantics; see [Cadence Workflows](Event-Sources-Cadence-Workflows) for the workflow-handler shape (which is *not* this contract — workflows have their own runtime and their own `cs.workflow.*` surface).

## Troubleshooting common failures

A handful of failure modes recur in practice. The pattern is always the same: the runner emits a typed `CS_*` code, the activation record carries that code in `Error.Type`/`ResolvedCode`, and the dashboard or `cs activations get` surfaces both. The list below is the short version; the [Error Model](Error-Model) page is the canonical reference.

**`CS_RUNTIME_CAP_DENIED: kv prefix not allowed`** — The handler called `cs.kv.get("foo")` but the manifest's `kv.prefixes` does not contain a prefix that matches `"foo"`. The check is a literal `strings.HasPrefix`, so `"ctr:"` in the manifest matches `"ctr:account:1"` but not `"counter:account:1"`. Fix: align the manifest prefix with the key shape, or re-key the data.

**`CS_RUNTIME_CAP_DENIED: kv op not allowed`** — The handler called `cs.kv.set` (or `del`) but the manifest's `kv.ops` does not include `"set"` (or `"del"`). Fix: add the op. Read-only functions should keep `ops: ["get"]`; write-only schedulers should keep `ops: ["set"]`. The principle of least privilege applies even though widening the op list is one edit away.

**`CS_RUNTIME_CAP_DENIED: codeq topic not allowed`** — The handler called `cs.codeq.publish("jobs.foo")` but no entry in `codeq.publishTopics` matches `"jobs.foo"`. The matcher accepts trailing `*` wildcards: `"jobs.*"` matches `"jobs.foo"` and `"jobs.bar.baz"`; `"jobs.foo"` only matches the literal. Fix: align the manifest pattern with the topic. Note that topic conventions are a tenant-level concern documented in [codeQ Protocol](codeQ-Protocol); operators may add a control-plane policy that further restricts what topics a tenant can publish to, independent of the manifest.

**`CS_RUNTIME_CAP_DENIED: http host not allowed`** — The handler called `cs.http.fetch("https://api.foo.com/...")` but `http.allowHosts` does not contain `"api.foo.com"`. The host check is exact-match (case-insensitive); subdomain globbing is not supported in v0.1. Fix: enumerate the hosts. Operators who need finer-grained controls (path-based allow-list, method-based allow-list) should layer them at the per-tenant `EgressPolicy` documented in [Operators Security](Operators-Security).

**`CS_EGRESS_DENIED: egress denied: <reason>`** — The handler called `cs.http.fetch(...)` and the host *is* in the manifest's `http.allowHosts`, but the per-tenant egress matcher (operator-controlled, separate from the manifest) refused the host. Fix is operator-side: update the `EgressPolicy` for the tenant, or escalate to the operator on call. Authors cannot widen the egress matcher from inside the function.

**`CS_RUNTIME_CAP_DENIED: private ip denied`** — The handler called `cs.http.fetch(...)` against a host that resolved to an RFC 1918 address, link-local address, or loopback. This is the SSRF guard and is non-negotiable in production. The local CLI disables the guard for `httptest.Server` testing; the cluster never does. Fix: do not call internal services from a cs-js function — internal calls should go through the HTTP gateway with a service-account token, not through a leaked private-IP egress.

**`CS_IMPORT_NOT_FOUND: import "x" not declared in manifest.imports`** — A `function.js` did `import x from "x"` but the manifest's `imports` block has no `"x"` entry. Fix: add the import to the manifest (and re-upload the draft so cs-control freezes the dep into `deps/`). The author cannot bypass this with a runtime require — there is no runtime require to bypass.

**`pending promise is not supported`** — The handler returned a Promise that had not yet settled when control returned to Go. The most common cause is a `setTimeout` or `setInterval` callback the handler depended on; neither is bound by the runtime. Less common: a chain of `.then`s that resolved on a microtask the runner did not drain. Fix: `await` every host call, and remove every `setTimeout`/`setInterval` reference.

**`runtime timeout`** — The deadline fired. The handler may have run a tight CPU loop, may have stalled on a downstream that exceeded `http.timeoutMs`, or may have legitimately needed more time than `limits.timeoutMs` allows. Logs collected before the timeout are preserved. Fix: raise the manifest timeout (up to 900 s), or split the work across multiple invocations (a scheduled sweep, a Cadence workflow), or shrink the downstream `timeoutMs` so the failure path is faster than the deadline.

**`default handler not found`** — `transformESModule` could not locate an `export default` in `function.js`. The most common cause is a CommonJS-style `module.exports = ...` instead of an ES module `export default ...`. Fix: rewrite to the ES module shape. CommonJS is not supported in v0.1.

**`SyntaxError: Unexpected token ...`** — goja could not parse the source. The most common cause is a TypeScript-only construct (`interface`, `as const`, type annotations on parameters) that survived because the publisher forgot to compile to JS. Fix: cs-js is a JavaScript runtime; TypeScript is not supported in v0.1. Authors who want types should write their `function.js` with JSDoc annotations, or compile TypeScript to plain JS before upload (cs-control will not run a compiler on the publisher's behalf).

**`ReferenceError: <name> is not defined`** — A global the handler relied on is not bound by the runtime. Common offenders: `process`, `Buffer`, `__dirname`, `__filename`, `require`, `globalThis.fetch`. Fix: use the `cs.*` host surface instead. `Buffer` in particular is a Node-only API; `cs.http.fetch` returns base64-encoded bodies so a handler that needs to manipulate bytes should use `Uint8Array` plus `atob`/`btoa`.

**`CS_RUNTIME_OOM`** — The activation exceeded the cgroup heap budget. The most common cause is loading a large JSON blob into memory (e.g., `JSON.parse` on a 100 MiB response). Fix: stream the data through KV in chunks, or perform the heavy lifting in a downstream Cadence activity with a larger budget. The manifest's `limits.memoryMb` is the soft ceiling cs-invoker-pool honours.

**`CS_VALIDATION_MANIFEST: ...`** — cs-invoker-pool tried to parse the manifest and failed. This should be impossible for a manifest that already passed publish-time validation; if it happens, the bundle has been corrupted in KVRocks. Fix: re-publish the version. Operators should investigate KVRocks integrity if the failure recurs.

When in doubt the recovery procedure is the same: pull the activation record (`cs activations get <id>`), read the resolved code, cross-reference it against the table in [Error Model](Error-Model), and fix the manifest or the source. The cluster invoker and the local CLI share the same code path, so a reproduction case is always a `cs fn test --event ...` away.

## Authoring patterns

A small set of patterns recurs across well-behaved cs-js functions.

**Manifest first, code second.** The cheapest cycle is: write the manifest with the capabilities the function will need, run `cs fn test` against an empty handler, watch the runner reject the handler for missing capability declarations, then fill in the handler. The opposite order — write the handler, then chase down each `CS_RUNTIME_CAP_DENIED` — is the same amount of work spent in the wrong direction.

**Read the deadline.** Handlers that do non-trivial work should compute their remaining budget from `ctx.deadline_ms` rather than from a hard-coded constant. A handler called from HTTP with a 3 s deadline and a handler called from a schedule with a 30 s deadline can share code; the deadline is the differentiator.

**Idempotency keys live in KV.** Any handler that performs a non-idempotent downstream effect (charging, sending email, publishing once-and-only-once) should write an idempotency key into KV *before* the effect and check for the key on entry. The `cs.kv.set(key, "1", { ttlSeconds: 86400 })` pattern is the canonical shape. Authors should pair this with a `prefixes: ["idem:"]` declaration in the manifest so the keys are namespaced.

**One side effect per handler.** A handler that performs many side effects becomes hard to reason about under retry: which effects were applied before the timeout, and which were not? The recommended shape is "compute, then commit": gather all the inputs, perform a single `cs.codeq.publish` or `cs.kv.set` at the end, and let downstream consumers fan out the work. This is hard to maintain in a large handler but pays off in operational simplicity.

**Logs are not metrics.** A `cs.log.info` on a hot path is expensive (the line is persisted to KVRocks and counts against the activation's log byte budget). Authors who want to count something — invocations per second, error rate, latency — should rely on the platform metrics (`cs.invoker.activation.count` etc.) rather than log lines they parse later. Logs are for diagnostic context, not for telemetry.

**Validate the event shape.** The runner does not enforce a schema on `event`. A handler that crashes on a malformed event surfaces as `CS_RUNTIME_EXCEPTION`; a handler that returns `{ statusCode: 400, body: "..." }` surfaces as `success` with a 4xx. The second shape is easier to diagnose and is the recommended pattern: validate the event at the top of the handler, return a structured 4xx for bad input, and let the unexpected exceptions surface as 5xx-equivalent errors.

**Minimise the dependency graph.** Each dep is parsed once per cold-start; a handler that pulls in a 500 KiB dep pays the parse cost on every fresh isolate. Authors who only need `zod` to validate a payload shape should consider writing the validator by hand for the two fields that matter, rather than pulling in the full library. The 16 MiB bundle cap is a ceiling, not a target.

**Prefer JSON-friendly payloads.** `cs.kv.set`, `cs.codeq.publish`, and the handler's return value all flow through `json.Marshal`. Authors who pass values that don't round-trip cleanly (e.g., `BigInt`, `Map`, `Set`, `undefined` in an object position) get surprising behaviour. The simplest shape is "objects of strings, numbers, booleans, arrays, and nested objects"; anything else should be converted at the boundary.

**Test against the same shape production uses.** The local CLI's `cs fn test` and the cluster's `cs-invoker-pool` share `internal/runtime`, but the surrounding environment differs: local runs use the in-memory KV, no egress matcher, and no Tikti introspection. A function whose tests pass locally can still fail in production for reasons that are external to the runner (an unfamiliar `EgressPolicy`, a missing secret, a stricter `VersionAuthz`). The mitigation is to run a smoke test against staging cs-control after publish, before promoting the alias to production.

**Resist over-instrumentation.** Cs-invoker-pool already emits the metrics every handler needs (duration, status, error code, byte counts). Authors should add their own logging only when they are diagnosing a specific failure mode; logging "started", "finished" on every activation pollutes the log buffer without adding signal. The pattern that scales is "log at the boundaries and at the error sites".

## See also

- [Creating Functions](Creating-Functions) — the runtime-agnostic introduction to authoring cs functions.
- [Managing Functions Packages](Managing-Functions-Packages) — the publish pipeline, dependency freezing, and the curated mirror.
- [HTTP Invoke Path](HTTP-Invoke-Path) — how an HTTP request becomes an `InvocationRequest` and the event shape the gateway produces.
- [Scheduler](Scheduler) — the schedule-trigger envelope and the overlap-policy contract.
- [Cadence Workflows](Event-Sources-Cadence-Workflows) — the workflow-handler runtime, the `cs.workflow.*` surface, and the determinism story.
- [Cadence Activity Invoke](Use-Cases-Cadence-Activity-Invoke) — the activity-handler binding model and the at-most-once semantics.
- [KVRocks](Enabled-Services-KVRocks) — the wire-level KV protocol and the per-tenant namespacing convention.
- [Vault Secrets](Enabled-Services-Vault-Secrets) — the secret-reference grammar and provider configuration.
- [Operators Security](Operators-Security) — the tenant-level egress allow-list, bundle signing, and the curated-mirror controls.
- [Determinism Linter](Development-Tools-Determinism-Linter) — the full banned-API table and override semantics.
- [Error Model](Error-Model) — the canonical `CS_*` code list and the trigger-aware retry policy.
- [Observability](Observability) — the metric catalogue, the activation tree-walk, and sampling policies.
- [Capacity and Limits](Capacity-and-Limits) — bundle, activation, and runtime budgets.
- [Concepts Capabilities and Isolation](Concepts-Capabilities-and-Isolation) — the high-level model the rest of this page implements.
