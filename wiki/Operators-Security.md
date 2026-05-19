# Operators: Security

Sous executes untrusted code authored by tenants on shared infrastructure. The security model treats the platform as a multi-tenant compute layer that must enforce isolation across every dimension a tenant might exploit: process boundaries, network reachability, persistent storage, credential material, and the supply chain that produces the bundle in the first place. This page walks through the threat model, the defences the platform composes to mitigate each branch, and the pre-deployment checklist that operators run before opening a cluster to production traffic.

The defences fall into four categories that map cleanly onto the codebase. Process isolation lives in `internal/runtime/` (the Goja interpreter for `cs-js`, wazero for `cs-wasm`, and the Python subprocess for `cs-python`). Network isolation lives in `internal/runtime/egress/` and the per-runtime host shims. Identity and access control delegate to [Tikti IAM](Enabled-Services-Tikti-IAM). Supply-chain integrity composes SHA-256 verification, Ed25519 signing, and CycloneDX SBOMs — all detailed in [Signing and SBOM](Managing-Functions-Signing-and-SBOM).

## Threat model

Sous is designed against the following adversaries, ordered by how often they appear in real incidents.

**Malicious tenant code.** A tenant publishes a function that attempts to read another tenant's KVRocks keys, dial an internal-only service, exfiltrate the platform's signing key, or escape the sandbox into the host process. Mitigation: the runtime sandbox, capability allowlists, the per-tenant inflight semaphore, and the private-IP block.

**Credential exfiltration.** A function captures the secret material the invoker injects, then leaks it through a response body, a log line, an outbound HTTP call, or a side-channel encoded into a deliberate timing pattern. Mitigation: the secret injection contract redacts values from logs, results, and DLQ envelopes; the egress allowlist limits where leaked material can travel; the per-version egress policy default-denies new tenants.

**Supply-chain forgery.** An attacker who compromised a tenant's deploy pipeline pushes a tampered bundle. The bundle SHA matches what the publisher uploaded, but the bundle did not originate from the tenant. Mitigation: Ed25519 publish-time signatures pinned to the tenant's active public key, plus invoke-time re-verification.

**Denial of service.** A tenant — or a single misbehaving function — saturates the gateway, the invoker pool, the codeQ broker, or KVRocks. Mitigation: per-tenant rate limits at the gateway, per-tenant inflight semaphore at the invoker, body/header/query caps, max-log-bytes truncation, retry budgets, and DLQ overflow.

**Side channels.** A tenant attempts to infer another tenant's behaviour by observing shared metric labels, shared log streams, or shared timing on common infrastructure. Mitigation: per-tenant labelling on every metric and log line; activation records always stored under the tenant's KVRocks key prefix.

The threat model explicitly does not cover defence against a compromised operator. An operator who holds cluster-admin credentials can read any tenant's secrets, rotate any signing key, and bypass every runtime check. Operator access control happens through Kubernetes RBAC and the Tikti role assignments documented in [Tikti IAM](Enabled-Services-Tikti-IAM).

## Sandbox model

Every activation runs inside a per-runtime isolate. The invoker never reuses an isolate across tenants, and the runtimes share a uniform set of host functions exposed through the `cs.*` global object.

`cs-js` runs JavaScript inside a Goja interpreter. Goja is a pure-Go ECMAScript implementation with no FFI surface: there is no `require`, no `process`, no `fs`, no native module loader, and no way for user code to obtain a reference to a host pointer. The interpreter executes inside the same OS process as `cs-invoker-pool`, which means the OS does not enforce memory boundaries between the runtime and the host. The boundary is enforced at the language level by Goja: a hostile script cannot construct a runtime value that escalates outside the interpreter.

`cs-wasm` runs WebAssembly modules inside the wazero runtime. wazero is a pure-Go WebAssembly interpreter that enforces linear-memory bounds and refuses every import the host has not explicitly registered. The runtime exposes the same `cs.*` host functions as `cs-js`. User code cannot allocate memory outside the module's linear-memory region.

`cs-python` runs Python source inside a subprocess of `cs-invoker-pool`. The subprocess launches `python3` with the `-I` (isolated) and `-S` (no site init) flags, which disable user site-packages, ignore the `PYTHON*` environment variables, and skip the usual import-site bootstrap. The child inherits a minimal environment (`PATH` and a scratch directory) — neither the operator's environment nor the resolved secrets reach the subprocess; secrets are delivered exclusively through the structured stdin/stdout protocol the runner speaks. The activation deadline is enforced through `exec.CommandContext`, which kills the child when the context expires.

All three runtimes block the same host primitives:

- No filesystem access. The runtimes refuse the standard library's file operations.
- No process spawning. The runtimes refuse `fork`, `exec`, `os/exec` equivalents.
- No raw network sockets. User code cannot open TCP, UDP, or Unix sockets directly; it dials through `cs.http.fetch`, which routes through the egress matcher.
- No native module loading. User code cannot load shared libraries or invoke FFI.

User code accesses external state exclusively through the `cs.*` host functions: `cs.env.get`, `cs.http.fetch`, `cs.kv.*`, `cs.codeq.publish`. Every host function is implemented by the invoker, runs under the activation's tenant identity, and applies the capability checks described below before performing the underlying operation.

## Egress allowlists

The runtime denies outbound network egress by default. A function that needs to reach the public internet declares the allowed hosts in its manifest:

```json
{
  "http": {
    "allowHosts": ["api.partner.com", "*.example.com"]
  }
}
```

`cs.http.fetch` consults the manifest list on every call. A wildcard entry (`*.example.com`) matches any direct subdomain (`api.example.com`, `v1.api.example.com`) but never the apex (`example.com`).

The manifest list is the per-version contract. Operators run a per-tenant policy on top of it through the **EgressPolicy** record introduced by E6.02. The policy is stored at `cs:tenant:<tenant>:egress:policy` in KVRocks and is compiled into the matcher defined in `internal/runtime/egress/policy.go`:

```json
{
  "allowed_hosts": ["api.partner.com", "*.example.com"],
  "allowed_cidrs": ["203.0.113.0/24", "2001:db8::/32"],
  "denied_hosts":  ["abuse.example.com"],
  "default_deny":  true
}
```

The matcher evaluates `denied_hosts` first, then `allowed_hosts` (case-insensitive), then `allowed_cidrs` (when the host parses as a literal IP). With `default_deny: true` the matcher rejects any destination that does not match, returning `CS_EGRESS_DENIED` (HTTP 403) with a reason naming the rule that fired. With `default_deny: false` the matcher permits everything not on the deny list, suitable only for trusted internal tenants.

The control plane CRUD endpoints (`GET /v1/tenants/{tenant}/egress-policy`, `PUT .../egress-policy`) gate writes through the Tikti actions `cs:egress:policy:read` and `cs:egress:policy:write`.

### The private-IP block

The runtime applies a non-negotiable private-IP block after the allowlist resolves. The block matches the CIDR list pinned in `isPrivateIP` (`internal/runtime/runner.go`):

```
10.0.0.0/8
172.16.0.0/12
192.168.0.0/16
127.0.0.0/8
::1/128
fc00::/7
```

The list covers RFC 1918 private space, loopback, IPv6 unique-local addresses, and IPv6 loopback. The list is intentionally narrow and exhaustive: every entry is a hard-coded CIDR in `isPrivateIP`, evaluated against the resolved destination IP after DNS. Operators that need broader cuts (link-local `169.254.0.0/16` / `fe80::/10`, multicast `224.0.0.0/4`, instance-metadata endpoints) layer them through the cluster NetworkPolicy and the deployment guidance in [Deployment Kubernetes](Deployment-Kubernetes).

An EgressPolicy entry that resolves to a private range is still rejected. The allowlist cannot override the private-IP invariant — an operator who needs to dial a private service from inside a function deploys an explicit egress proxy (under an allowed hostname on a public IP) and routes the function through it.

## Capability model

User code reaches the platform through `cs.*` host functions. Each host function consults a capability allowlist before it performs the underlying operation:

- `cs.kv.get`, `cs.kv.put`, `cs.kv.delete`, `cs.kv.list` validate the operation against the function manifest's `kv` allowlist (key prefixes plus permitted ops).
- `cs.codeq.publish` validates the topic against the manifest `codeq.publishTopics` allowlist.
- `cs.http.fetch` validates the hostname against the manifest `http.allowHosts` list, then through the EgressPolicy matcher, then through the private-IP block.
- `cs.env.get` resolves only the secret names the function declared in `VersionConfig.Secrets`. Names not in the declaration return `null`.

A capability violation fails the activation with `CS_RUNTIME_CAP_DENIED` and a reason naming the rule that fired. The invoker stamps the violation onto the activation record so operators can audit it through the standard `GET /v1/tenants/{tenant}/activations/{id}` endpoint.

The capability declarations are immutable per version: changing the allowlist requires publishing a new version, which is signed and recorded under a new monotonic version number. This makes capability changes auditable through the version history rather than mutable through a separate API.

## Authentication and authorization

Every external request to a Sous cluster carries a bearer token issued by [Tikti IAM](Enabled-Services-Tikti-IAM). `cs-control` and `cs-http-gateway` introspect the token through `plugins.authn.tikti.introspection_url` and reject requests with invalid, expired, or revoked tokens.

The token resolves to a `sub` (the subject identifier) and a list of roles. `cs-control` enforces Tikti actions on every mutation; the action name follows the dotted convention (`cs:function:create`, `cs:function:publish`, `cs:schedule:create`, `cs:egress:policy:write`, `cs:audit:read`). The action map is the authoritative authorization surface; new endpoints register their action in `internal/api/authz.go` alongside the handler.

Inter-service calls inside the cluster use service principals issued by Tikti. Operators rotate the service-principal tokens through the Kubernetes Secret backing the YAML config; the deployments restart on token rotation to pick up the new values.

Cross-tenant access is denied at the authorize() layer with `CS_AUTHZ_RESOURCE_MISMATCH`. The handlers never reach into another tenant's KVRocks namespace because every key is prefixed with the requesting tenant.

## Secrets

Sous does not inject secrets by default. A function declares the secrets it needs in `VersionConfig.Secrets` and the invoker resolves them at activation start, after bundle SHA verification and before user code runs. Three reference forms are accepted:

- `"STRIPE_KEY"` — the lookup path matches the environment-variable name.
- `"STRIPE_KEY=payments/stripe_key"` — `STRIPE_KEY` is the env var the function sees through `cs.env.get("STRIPE_KEY")`; `payments/stripe_key` is the provider-specific lookup path.
- `"TIER=payments/multi#json-field:tier"` — the provider returns a JSON document and the invoker exposes the named field as a string.

The shipped providers are `memory` (development) and `vault` (production). The provider is selected through `plugins.secrets.driver`; the configuration knobs are documented in [Operators Configuration Reference](Operators-Configuration-Reference) and the production vault topology lives in [Vault Secrets](Enabled-Services-Vault-Secrets).

The injection contract guarantees:

- Secret material never lands in the bundle. The publisher only stores names.
- Secret material never lands in KVRocks. The persistence plugin is bypassed entirely; the invoker resolves through the provider and stamps the values onto the runtime context via `runtime.WithEnv`.
- Secret material never lands in `cs.results` or DLQ envelopes.
- The runtime never appends secret material to activation logs.
- `cs.env.list()` returns the declared names only; values do not appear in the output.

A missing secret fails the activation with `CS_SECRET_NOT_FOUND` (HTTP 404) before any code executes; an unreachable provider fails with `CS_SECRET_UNAVAILABLE` (HTTP 503) so the gateway can retry against another replica.

## Signing and SBOM

Every publish carries an Ed25519 signature over a canonical payload that binds the bundle digest to the (tenant, namespace, function) tuple. The publish handler verifies the signature against the tenant's active public key and persists the signature on the `VersionRecord`. The invoker re-verifies on every cold bundle load. A tampered bundle, a rotated key, or a missing key all refuse to execute.

`plugins.signing.required` controls publish-time enforcement. The default is `false`, which records signatures when supplied but accepts unsigned publishes for backward compatibility. Operators flip the knob to `true` once every active tenant has rotated a signing key, after which unsigned publishes fail with `CS_SIGNATURE_MISSING`.

Every successful publish also produces a deterministic CycloneDX 1.5 SBOM that lists the runtime, every file inside the bundle with its SHA-256 hash, the bundle digest, the signing identity, and (after E5.01) every frozen import-map dependency. The SBOM is exposed at `GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions/{version}/sbom` and is the source of truth for vulnerability scanners and license audits.

The end-to-end mechanics — canonical-payload format, key rotation endpoints, SBOM determinism — live in [Signing and SBOM](Managing-Functions-Signing-and-SBOM).

## Audit

`cs-control` emits a structured audit event for every successful control-plane mutation: function create/delete/draft, version publish, alias set, schedule create/delete, Cadence worker create/delete, signing-key rotate, egress-policy write. Events are emitted **after** the KV mutation commits — phantom mutations are never logged.

The audit stream is configured through `plugins.audit`. Three sinks ship in tree: `stdout`, `codeq`, and HMAC-signed `webhook`. Sink failures do not roll back the mutation; the recorder logs a `SinkLag` warning through the structured logger.

A per-tenant ring buffer in KVRocks (`plugins.audit.history_limit` entries, TTL tracks `cs_control.limits.activation_ttl_seconds`) backs the replay endpoint `GET /v1/tenants/{tenant}/audit?since=&actor=&action=&limit=`. The endpoint enforces `cs:audit:read` and rejects cross-tenant requests at the authorize() layer. For long-term retention, operators subscribe to the configured codeq topic or forward the webhook to their SIEM. The event schema and the long-retention plan live in [ledgerDB Audit](Enabled-Services-ledgerDB-Audit).

## Pre-deploy security checklist

The list below is the final gate operators run before opening a cluster to production traffic. Every item maps to an enforcement point in the codebase or in the deployment artifacts. Failures here block the launch.

### 1. Isolation

- [ ] User code runs inside a per-runtime isolate (`cs-js` Goja, `cs-wasm` wazero, `cs-python` subprocess).
- [ ] User code cannot import host filesystem modules.
- [ ] User code cannot spawn processes.
- [ ] User code cannot open raw sockets.
- [ ] User code cannot load native modules (`.so`, `.dll`, `.dylib`, Python C extensions).
- [ ] The Python subprocess runs `python3 -I -S` with a minimal inherited environment and an activation-deadline-bound context.

### 2. Capability enforcement

- [ ] `cs.kv.*` validates key prefixes and the allowed operation set.
- [ ] `cs.codeq.publish` validates topic prefixes.
- [ ] `cs.http.fetch` validates the manifest `http.allowHosts` list.
- [ ] `cs.http.fetch` applies the per-tenant EgressPolicy after the manifest check.
- [ ] `cs.http.fetch` applies the private-IP block after the EgressPolicy check.
- [ ] `cs.env.get` returns `null` for names not in `VersionConfig.Secrets`.

### 3. Network controls

- [ ] `cs-http-gateway` is the only public ingress.
- [ ] `cs-control` is reachable only from tenant tooling networks (CLI, CI/CD).
- [ ] Invokers reach KVRocks and codeQ only; a NetworkPolicy denies every other egress.
- [ ] Pollers reach `code-flow` (Cadence), KVRocks, codeQ only.
- [ ] Vault is reachable only from `cs-invoker-pool`.
- [ ] The metrics endpoints (`/metrics`) are reachable only from the monitoring namespace.

### 4. Token and role controls

- [ ] All external requests require a Tikti token.
- [ ] Service principals exist for every internal component (`cs-control`, `cs-http-gateway`, `cs-invoker-pool`, `cs-scheduler`, `cs-cadence-poller`).
- [ ] Service-principal tokens rotate through the Kubernetes Secret driving `plugins.authn.tikti.api_key` and the codeQ producer/worker tokens.
- [ ] Vault auth tokens are sourced from the `VAULT_TOKEN` environment variable, not from the YAML.
- [ ] The Tikti introspection cache TTL is bounded (`plugins.authn.tikti.cache_ttl_seconds <= 300`).

### 5. Data controls

- [ ] Activation logs and results store under the tenant's KVRocks key prefix.
- [ ] Log redaction strips known secret formats (JWT, AWS access keys, Vault tokens) on best-effort.
- [ ] Result truncation enforces `cs_invoker_pool.limits.max_result_bytes`.
- [ ] Log truncation enforces `cs_invoker_pool.limits.max_log_bytes` and stamps the `CS_LOG_LIMIT_EXCEEDED` sentinel.
- [ ] Activation records carry the `sampling_decision` field so operators can reconstruct retention choices.

### 6. DoS protection

- [ ] `cs-http-gateway` enforces `limits.max_body_bytes`, `limits.max_header_bytes`, `limits.max_query_bytes`.
- [ ] `cs-http-gateway` rate-limits by tenant (`rate_limits.tenant_rps`) and function ref (`rate_limits.function_rps`).
- [ ] `cs-invoker-pool` enforces `workers.max_inflight` globally and `DefaultTenantMaxInflight` per tenant.
- [ ] `cs-cadence-poller` enforces `limits.max_inflight_tasks_default` per binding.
- [ ] The retry policy is bounded (`retry.max_attempts`, `retry.max_ms`).
- [ ] Schedule interval validation rejects sub-second schedules.
- [ ] DLQ topics are wired and consumed.

### 7. Supply-chain controls

- [ ] `cs-control` computes SHA-256 for every published bundle.
- [ ] `cs-invoker-pool` verifies SHA-256 on every cold bundle load.
- [ ] Ed25519 publish-time signatures are recorded for every publish.
- [ ] `plugins.signing.required` is `true` for production clusters once tenants have rotated.
- [ ] `cs-invoker-pool` re-verifies signatures on every cold bundle load when `VersionRecord.Signature != nil`.
- [ ] Every published version has an associated CycloneDX SBOM.
- [ ] Container images pin base-image digests in the Helm values.
- [ ] `cs_control.publish.imports.allowed_mirrors` is either empty (no remote fetching) or restricted to operator-owned mirrors.

### 8. Observability and audit controls

- [ ] Metrics expose error rate, queue lag, inflight, and retry/DLQ counters.
- [ ] Logs include `request_id` and `activation_id` for correlation.
- [ ] Traces propagate `traceparent` end-to-end.
- [ ] Audit events are routed to a long-retention sink (codeq topic or SIEM webhook) — not just stdout.
- [ ] Audit events carry actor `sub`, action, resource, and outcome.
- [ ] Burn-rate alerts from `deploy/observability/alerts.rules.yaml` are loaded into Prometheus.
- [ ] Each alert's `runbook_url` annotation points at an operator-owned runbook.

## Cross-references

- [Operators Configuration Reference](Operators-Configuration-Reference) — the YAML knobs that gate the controls on this page.
- [Operators Runbooks](Operators-Runbooks) — incident playbooks for security events (signing-key compromise, audit-sink drop).
- [Operators Observability](Operators-Observability) — the signals that surface security failures (`CS_RUNTIME_CAP_DENIED`, `CS_EGRESS_DENIED`, `CS_AUTHZ_RESOURCE_MISMATCH`).
- [Tikti IAM](Enabled-Services-Tikti-IAM) — authentication and authorization.
- [Vault Secrets](Enabled-Services-Vault-Secrets) — production secret provider.
- [Signing and SBOM](Managing-Functions-Signing-and-SBOM) — Ed25519 signing and CycloneDX SBOM generation.
- [ledgerDB Audit](Enabled-Services-ledgerDB-Audit) — long-term audit retention.
- `internal/runtime/egress/policy.go` — egress matcher implementation.
- `internal/runtime/runner.go` — private-IP block list.
