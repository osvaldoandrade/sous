# Reference: Glossary

This page is the authoritative term list for Sous. It defines the nouns the rest of the wiki uses without re-introducing them — when a page says "draft" or "alias" or "WorkerBinding", it is using the definition recorded here. The terms are alphabetised so a reader scanning the wiki can land on the glossary, find the term, and follow the cross-link back to the page that uses it. Where a term is implemented by a concrete struct or package in the repo, the definition cites the path; the prose stays authoritative even when the code evolves.

## Terms

**Activation.** A single execution of a function version. Every activation has an `activation_id` (a UUID issued by the trigger), a tenant/namespace pair, a `ref` to a function and version, and a terminal status of `success`, `error`, or `timeout`. Activations are persisted to KVRocks under a per-tenant key prefix and surface through the activation log API. See [Concepts-Invocations-and-Activations](Concepts-Invocations-and-Activations).

**Alias.** A mutable pointer from a short name (commonly `dev`, `staging`, `prod`) to a specific `FunctionVersion`. Aliases let invokers target a moving target while versions stay immutable. The control plane writes an alias as a single KV key, so an alias swap is atomic — readers see either the old version or the new version, never a torn state. See [REST-API](REST-API) for the alias endpoints.

**Binding (WorkerBinding).** A control-plane record that ties a Cadence task list to a function. The poller in `cs-cadence-poller` reads a binding to learn which domain and task list to long-poll, which activity names map to which function refs, and which codecs decode the input and encode the output. The Go type lives at `internal/api/types.go` (`WorkerBinding`). See [Cadence-Integration](Cadence-Integration).

**Capability.** A per-version allowlist that limits what user code can do. Capabilities cover KV access (prefix and ops), codeQ publish topics, and HTTP egress hosts plus timeout. The control plane validates the capability block at publish time; the runtime enforces it at invocation time. Manifest capabilities are part of the [Reference-Schemas](Reference-Schemas) page.

**codeQ.** The data-plane message bus that carries `InvocationRequest` and `InvocationResult` messages between pollers and the invoker pool. codeQ is the only buffer between the long-poll loops (HTTP, scheduler, Cadence) and sandboxed execution. It provides backpressure, independent scaling, and crash isolation. See [codeQ-Protocol](codeQ-Protocol).

**Codec.** A pluggable encoder/decoder for Cadence activity input/output bytes. A `WorkerBinding` pins an `input_codec` and an `output_codec` per binding so different task lists can ship JSON, msgpack, or raw bytes without sharing a global wire format. Unknown codec names fail validation with `CS_VALIDATION_UNSUPPORTED_CODEC`. See [Cadence-Integration](Cadence-Integration).

**Cold start.** The latency added when the invoker has to fetch a bundle from KVRocks, initialise a runtime adapter, and warm the sandbox before user code first runs. Warm starts reuse an existing isolate so cold-start overhead amortises across activations. See [Invoker-Pool](Invoker-Pool).

**Control plane.** The half of Sous that owns lifecycle and policy: functions, drafts, versions, aliases, schedules, worker bindings, signing keys, and audit. The control plane is implemented by `cs-control` and persists state in KVRocks. Control-plane writes are the only operations that can change configuration; the data plane reads them at activation time. See [Architecture](Architecture).

**Cs CLI.** The first-party command-line client (`cs`) that talks to the control plane. The CLI handles bundle packaging, draft uploads, publishes, alias updates, schedule and binding management, and activation tail. It is also the parity harness's reference client — locally and against a remote cluster the same `cs` invocation yields the same outcome. See [CLI](CLI).

**Data plane.** The half of Sous that owns execution and result transport: `cs-http-gateway`, `cs-scheduler`, `cs-cadence-poller`, and `cs-invoker-pool`. The data plane never mutates control-plane state directly; it consumes from codeQ, runs user code under capability enforcement, and publishes results back to codeQ. See [Architecture](Architecture).

**DecisionTask.** The Cadence task surface that asks a workflow to compute its next set of decisions (timer, schedule activity, complete). A `WorkerBinding` with `kind: "workflow"` long-polls DecisionTasks and dispatches them to the workflow executor in `internal/cadence/workflow`. Distinct from ActivityTask, which carries a unit of work. See [Cadence-Integration](Cadence-Integration).

**Determinism linter.** A publish-time static check the control plane runs against workflow bundles. The linter rejects banned APIs (`Date.now`, `Math.random`, `setTimeout`, etc.) so replays remain deterministic. Failures return `CS_WORKFLOW_NON_DETERMINISTIC` with a structured `violations[]` list. See [Cadence-Integration](Cadence-Integration).

**Draft.** A temporary bundle upload used to build a `FunctionVersion`. Drafts have a per-tenant TTL (default 24h, configurable) and live in KVRocks under a draft key. Publish consumes a draft into an immutable version; expiry or explicit discard releases the slot. Drafts are isolated per upload — concurrent uploads to the same function never share a `draft_id`. See [REST-API](REST-API).

**Egress allowlist.** A per-tenant policy layered on top of the manifest's `http.allowHosts` list that the runtime consults before allowing an outbound HTTP call. Denials surface as `CS_EGRESS_DENIED` and are logged in the activation record so operators can audit blocked destinations. See [Security](Security).

**Function.** A named resource in a `(tenant, namespace)` scope that groups versions and aliases. The function record itself only carries identity (name, runtime, entry, handler) and lifecycle markers (`created_at_ms`, `deleted_at_ms`). State that changes per release lives on versions and aliases. See [Concepts-Function-Lifecycle](Concepts-Function-Lifecycle).

**Function version.** An immutable snapshot of code plus configuration, identified by a monotonic integer (`1`, `2`, `3`, ...) and a `sha256` of the bundle. The control plane allocates versions atomically; once published, a version's `(version, sha256, config)` triple never mutates. Re-publishing a draft creates a new version rather than overwriting one. See [Reference-Entity-State-Machines](Reference-Entity-State-Machines).

**ledgerDB.** The append-only audit log destination operated alongside the control plane. Audit events are emitted on the success path of every mutation and shipped to one or more configured sinks (stdout JSON, a codeQ topic, an HMAC-signed webhook). See [ledgerDB-Audit](ledgerDB-Audit).

**Manifest.** The JSON document `manifest.json` inside a bundle that declares the schema version, runtime, entry file, handler name, limits, capabilities, optional dependency imports, and optional Cadence metadata. The manifest is validated at draft upload and at publish; the wire schema is `cs.function.script.v1`. See [Reference-Schemas](Reference-Schemas).

**Namespace.** A logical boundary inside a tenant. Teams typically map namespaces to products or environments (for example `payments-prod`, `payments-staging`). The namespace is part of every resource path and KVRocks key, so cross-namespace listing is explicit, not accidental. See [Architecture](Architecture).

**Parity harness.** The integration test suite that runs the CLI and a function against both a local in-process invoker and a containerised invoker, and asserts byte-identical outcomes. The harness exists so an agent can develop locally and trust that publish-time behaviour matches the cluster. See [Testing](Testing).

**Plugin (driver).** An interface-backed swappable component the platform loads at boot — runtime adapters, secret providers, codeQ transports, KV stores, authn drivers. Plugins are registered into a typed registry; replacing one only requires implementing the interface and wiring config. See [Architecture](Architecture).

**Principal.** An authenticated identity returned by the Tikti introspection driver. A `Principal` carries a `sub` (the subject identifier), a tenant, and a role list. Every control-plane handler and every invoke path consults the principal before mutating or executing. See [IAM-with-Tikti](IAM-with-Tikti).

**Publish.** The control-plane operation that consumes a draft and produces an immutable `FunctionVersion`. Publish runs the manifest validator, the determinism linter (for workflow bundles), the import resolver, the SBOM generator, and the signing-key check, then writes the version meta, the version bundle, and (optionally) an alias pointer in a single KV transaction. See [Concepts-Function-Lifecycle](Concepts-Function-Lifecycle).

**Reference (ref).** The triple `(function, alias?, version?)` that identifies which executable a trigger wants. Exactly one of `alias` or `version` may be set; if neither is set the alias `latest` is assumed. The same shape is used by schedules, worker bindings, and invoke endpoints so callers learn the convention once. See [Reference-Schemas](Reference-Schemas).

**Result.** The terminal record of an activation, surfaced both as an `InvocationResult` message on codeQ and as the `result` field of the persisted `ActivationRecord`. A result carries a status (`success`, `error`, `timeout`), a duration in milliseconds, and either a `FunctionResponse` (status code, headers, body) or an `InvocationError`. See [Reference-Schemas](Reference-Schemas).

**Retry/DLQ.** The trigger-level policy that re-queues an `InvocationRequest` after a retryable failure and, once attempts are exhausted, forwards the original payload to a dead-letter codeQ topic. Exhaustion surfaces as `CS_RETRY_EXHAUSTED` and increments `cs_invoke_dlq_total{trigger}`. See [codeQ-Protocol](codeQ-Protocol).

**Role.** A string in Tikti that grants a permission. Functions declare per-trigger role requirements through `VersionAuthz` (`invoke_http_roles`, `invoke_schedule_roles`, `invoke_cadence_roles`); the gateway and pollers compare a principal's roles against this list before publishing an `InvocationRequest`. See [IAM-with-Tikti](IAM-with-Tikti).

**Sandbox.** The runtime isolation boundary inside which user code runs. The cs-js runtime sandboxes via a per-activation isolate; the cs-python runtime sandboxes via a subprocess with seccomp; the cs-wasm runtime sandboxes via the WASM host's capability model. All sandboxes enforce the manifest's capability block. See [Runtime-cs-js](Runtime-cs-js).

**Schedule.** A control-plane record that periodically emits an `InvocationRequest` for a function ref. A schedule has either an `every_seconds` interval (the v0.1 form) or a cron expression with timezone and jitter (the E4.01 form), an overlap policy (`skip`, `queue`, `parallel`), and an optional payload. The Go type lives at `internal/api/types.go` (`ScheduleRecord`). See [Scheduler](Scheduler).

**SBOM.** A CycloneDX software bill of materials generated at publish time and attached to the version record. The SBOM lists the bundle's frozen imports with their integrity hashes so downstream supply-chain audits can answer "what was inside version 12" without re-fetching the bytes. See [Security](Security).

**Secret.** A reference declared in `VersionConfig.Secrets` that the invoker resolves through the configured secret provider plugin before user code runs. Resolution failures surface as `CS_SECRET_NOT_FOUND` (missing) or `CS_SECRET_UNAVAILABLE` (provider down). Secret material is never persisted in the bundle, KVRocks, or activation results. See [Security](Security).

**Signing key.** A tenant-scoped ed25519 key pair the control plane uses to sign published bundles. The keystore tracks key states (`active`, `rotated`, `revoked`); the invoker verifies the signature against the recorded public key before loading the bundle. Missing or unknown signing keys surface as `CS_SIGNATURE_KEY_NOT_FOUND`. See [Security](Security).

**Tenant.** The top-level security boundary in Sous. Every resource path, every KV key, every audit event, and every metric is scoped to a tenant. Tikti ties a principal to exactly one tenant, and the control plane rejects cross-tenant access at the authn middleware. The wire form matches `t_[a-z0-9]{6,32}`. See [IAM-with-Tikti](IAM-with-Tikti).

**Tikti.** The external identity provider Sous delegates authentication and role lookup to. The platform never holds passwords or rotates user credentials directly; instead, `cs-http-gateway` and `cs-control` call Tikti's introspect endpoint to resolve a bearer token into a `Principal`. See [IAM-with-Tikti](IAM-with-Tikti).

**Trigger.** The data-plane component that creates an `InvocationRequest`. Sous ships four trigger types: `http` (via `cs-http-gateway`), `schedule` (via `cs-scheduler`), `cadence` (via `cs-cadence-poller`), and `api` (synthetic, for SDK-initiated invokes). The `trigger` field on every `InvocationRequest` carries the type and a `source` map describing the originating event. See [HTTP-Invoke-Path](HTTP-Invoke-Path).

**Version.** Shorthand for "function version". See **Function version**.
