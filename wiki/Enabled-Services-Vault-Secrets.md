# Enabled Services: Vault Secrets

Sous functions frequently need credentials — API keys, database passwords, third-party tokens — that should never appear in source code or bundles. The secrets subsystem solves this by resolving secret values at activation time through a pluggable driver, with the canonical production driver being HashiCorp Vault's KV v2 backend. Functions stay declarative: the manifest names what the code needs, the platform fetches the bytes, and the runtime hands them to user code through a single host binding.

The function declares the secret names it needs in its manifest under `config.secrets`; the platform exposes them as `ctx.env.get("NAME")`; the runtime fetches the value from Vault on first read (with caching) and never persists it. Each entry in the manifest is parsed by `internal/plugins/secrets.ParseRef` so the wire format can carry an optional path and an optional field selector without bumping the manifest schema. The driver chain (memory or vault) is selected once at process start from `plugins.secrets.driver` in cluster config and shared across every invocation handled by `cs-invoker-pool`.

Secrets are not stored in the bundle, not in KVRocks, and not in audit events. They flow only through a context-scoped map injected by `cs-invoker-pool` via `runtime.WithEnv`, which the runtime reads in-memory when user code calls `cs.env.get(name)`. The same context disappears when the activation completes, so no path on the codeQ envelope, on the DLQ, or on the activation log carries secret bytes. Operators rotate values directly in Vault; functions pick up the new bytes on the next activation that misses cache.

## Driver model

`internal/plugins/secrets/` defines the `Provider` interface. Every driver implements `Get(ctx, tenant, ref) (string, error)` and `Close() error`, and each driver registers itself with the plugin registry from its package `init()` so cluster config can select it by name without a compile-time wire-up. The interface intentionally takes a `tenant` argument and a `SecretRef` describing what the function asked for: drivers that support per-tenant isolation use the tenant; drivers that do not (such as the dev-only memory driver) document the gap explicitly.

In v0.1, two drivers ship:

- `memory` — secrets configured in YAML as `plugins.secrets.memory.seed`. Intended for local development, `cs-cli doctor` scaffolds, and unit tests. It serves lookups out of an in-process map and does not enforce per-tenant scoping.
- `vault` — production driver that speaks HashiCorp Vault's KV v2 HTTP API. It reads values on demand for each activation and never caches them on disk.

The driver is selected via `plugins.secrets.driver` in cluster config; when the key is absent `cs-control` falls back to `memory` with an empty seed so a fresh install can boot without a Vault dependency. Switching to Vault is a config-only operation — no rebuild, no code change in functions that were authored against the memory driver, because both drivers honour the same `SecretRef` semantics. The driver code lives under `internal/plugins/secrets/memory/` and `internal/plugins/secrets/vault/`.

The `SecretRef` shape itself is wire-friendly so manifests stay readable. `ParseRef` accepts three forms:

- `"DB_PASSWORD"` — name and path coincide; the driver looks up `DB_PASSWORD` verbatim.
- `"DB_PASSWORD=payments/db"` — name and provider path differ; the function still reads `cs.env.get("DB_PASSWORD")` but the driver looks up `payments/db`.
- `"DB_PASSWORD=payments/db#json-field:password"` — the driver fetches `payments/db`, parses the response as JSON, and returns the `password` field.

The format suffix lets one Vault entry feed multiple env vars without leaking the JSON wrapper into user code. `internal/plugins/secrets/secrets.go` carries the parser and the `ExtractValue` helper that each driver applies before handing the value back to the invoker.

Drivers are expected to be stateless past the connection or pool they hold. The Vault driver embeds a single `http.Client` so all invoker goroutines share one connection pool; the memory driver guards its seed map with a `sync.RWMutex` so concurrent activations can read in parallel. Anything heavier than that — long-lived sessions, lease renewers, background refresh loops — belongs in `Close()`, which `cs-invoker-pool` calls on shutdown so a clean stop drains every driver uniformly.

The driver also honours the activation's `context.Context`. `cs-invoker-pool` derives a context whose deadline matches the activation deadline, so a secret read that would otherwise outlast the user code's allowed wall time is cancelled before the deadline lapses. The Vault driver wires that context into `http.NewRequestWithContext`, which means a slow Vault response cooperates with cancellation rather than burning CPU on a doomed read. Drivers that ignore the context — for example, by issuing blocking calls into a non-cancellation-aware library — would violate the activation deadline contract and are rejected from the driver registry at startup; the interface comment in `secrets.go` documents the requirement explicitly.

Future drivers (AWS Secrets Manager, Google Secret Manager, on-disk encrypted files) plug into the same registry. Each implementation lives under its own subpackage with a `NewFromConfig` constructor and a registry-side `init()` registration, mirroring the pattern that `memory` and `vault` already use. Adding a new driver is intended to be a self-contained change with no edits to `cs-invoker-pool` or to runtime host bindings; the abstraction boundary is the `Provider` interface and nothing more.

## Vault driver configuration

The Vault driver is configured under `plugins.secrets.vault` in cluster YAML. Every field has a documented default so a minimal cluster only needs to set the address and the auth token:

```yaml
plugins:
  secrets:
    driver: vault
    vault:
      addr: https://vault.internal:8200
      token: ""
      token_env: VAULT_TOKEN
      kv_mount: secret
      namespace: ""
      timeout_ms: 2000
```

The fields:

- `addr` is the base URL of the Vault cluster. The driver trims trailing slashes and rejects empty values at startup with `plugins.secrets.vault.addr is required` so a misconfigured deployment fails fast.
- `token` is a static auth token. Production deployments are encouraged to leave this empty and supply the token through the environment so the YAML stays sealed; the driver reads from the env variable named by `token_env` (default `VAULT_TOKEN`) when the static value is blank.
- `token_env` selects the environment variable name. Defaults to `VAULT_TOKEN` to match upstream Vault tooling.
- `kv_mount` is the KV v2 mount path. Defaults to `secret`, matching Vault's stock mount. The driver rewrites `SecretRef.Path` to `<kv_mount>/data/<path>` before issuing the request, because KV v2 stores versioned secret bytes under the `data/` subpath.
- `namespace` is the optional Vault Enterprise namespace, forwarded as the `X-Vault-Namespace` header on every request. Open-source Vault clusters leave this empty.
- `timeout_ms` is the per-request timeout in milliseconds. Defaults to 2000 (2s). The driver respects the activation deadline, so this value is an upper bound on a single secret read, not on the whole activation.

Static-token auth is sufficient for v0.1; lease renewal, AppRole, and Kubernetes auth are out of scope for the first release and tracked as follow-up work. Operators that need richer auth modes today should rotate the static token frequently and wire that rotation through whichever secret distribution mechanism the cluster already trusts. See [Configuration Reference](Config-Reference) for the full field table.

The driver builds the request URL by joining `addr`, the literal `v1` segment, the configured mount, the literal `data` segment, and the per-request path. URL parsing happens once per request through `net/url.Parse`, so a mount or path with embedded slashes round-trips correctly without manual escaping. The driver issues a single `GET` against the resulting URL with three headers: `X-Vault-Token` for auth, `Accept: application/json` for content negotiation, and `X-Vault-Namespace` when the namespace field is set. Response bodies are read through `io.LimitReader` capped at 1 MiB so a hostile or misconfigured Vault cannot exhaust invoker memory by returning an arbitrarily large payload.

Status code handling is deliberately narrow. `200 OK` parses the KV v2 envelope and returns either the single field shortcut or the JSON-encoded data map, depending on `SecretRef.Format`. `404 Not Found` surfaces `CS_SECRET_NOT_FOUND`. `401` and `403` surface `CS_SECRET_UNAVAILABLE` with a message naming the status code so operators can distinguish "wrong token" from "wrong policy" without scraping driver logs. Any other status code surfaces `CS_SECRET_UNAVAILABLE` with the observed code. The driver does not retry on its own — the scheduler and gateway already implement retry semantics for `CS_SECRET_UNAVAILABLE`, so adding a second retry layer would just multiply tail latency on a sustained Vault outage.

Operators running Vault Enterprise typically also enable response wrapping, which returns a single-use token in place of the raw secret. Response wrapping is not implemented in the v0.1 driver; the driver expects an unwrapped KV v2 response. Tenants that need wrapping should disable it on the policy that scopes the Sous platform token, since unwrapping a wrapped response from the invoker would require a second round-trip and the trade-off does not pay off when the platform token itself is the trust boundary. Tenants on open-source Vault can ignore this section entirely.

The driver does not currently negotiate the KV v2 mount version with Vault. The configured `kv_mount` is assumed to be KV v2; pointing it at a KV v1 mount fails at request time with a 404 because the `data/` subpath does not exist on v1. The operator is expected to provision the mount correctly during cluster setup; the driver does not auto-detect. Documenting this here so the failure is easy to diagnose: "404 on every secret right after switching mounts" almost always means the mount is KV v1, not v2.

## Path scheme

The canonical production layout scopes each tenant to its own KV v2 subtree:

```
secret/data/sous/{tenant}/{secret_name}
```

The driver substitutes the tenant at lookup time so a function can declare a logical secret reference without baking the tenant identifier into the manifest. Concretely, the manifest entry `STRIPE_KEY=stripe_key` resolves under tenant `t_acme` to `secret/data/sous/t_acme/stripe_key`, and under tenant `t_widgets` to `secret/data/sous/t_widgets/stripe_key`. A function cannot read another tenant's secret even when the logical name is identical, because the path the driver issues to Vault carries the activation's tenant ID. Cross-tenant reads collapse to a Vault 404 and surface as `CS_SECRET_NOT_FOUND` at the runtime.

Tenant scoping is enforced at two layers. The driver builds the request path from the activation tenant rather than from the manifest, so a function author who tries to escape their subtree by writing `..` or `/sous/other` into the manifest still hits the resolved tenant prefix. Operator policies on the Vault side close the loop: the platform token Sous uses to read secrets should grant `read` only on `secret/data/sous/*` with a templated path so even a leaked Sous token cannot reach unrelated KV mounts.

The memory driver does not enforce this scheme — it serves whatever path appears in the seed. Tests that exercise tenant isolation stamp tenant-qualified keys into the seed directly (`t_acme/api-key`, `t_widgets/api-key`) when they need to assert isolation behaviour. The contract is documented in `internal/plugins/secrets/memory/memory.go`.

The KV v2 envelope returned by Vault wraps the actual data map under `data.data`, with an outer `data.metadata` block carrying the version number, creation timestamp, and destroyed flag. The driver unwraps the inner map so user code sees a flat `name → string` mapping. When the entry contains exactly one field and the manifest declared no format hint, the driver returns that field's value as a string — this matches Vault's idiomatic single-string secret encoding (`{"value": "..."}`), which is what most KV entries look like in practice. When the entry contains multiple fields and no format hint, the driver returns the whole map re-serialised as compact JSON so the function still receives a usable value rather than an empty string; manifests should use `#json-field:<key>` to pick a specific field in that case.

The format selector is processed in the driver rather than in user code so the wire format from driver to runtime stays a single string. This keeps `cs.env.get(name)` typed as "string or null" and avoids leaking JSON parsing into every function that wants one credential out of a multi-field entry. The cost is that the format selector is fixed at manifest publish time; a function cannot ask for a different field at runtime without republishing. That trade-off is intentional — the manifest is the audit surface, and runtime-flexible field selection would defeat the declarative-allowlist guarantee from the capability gating section below.

A worked example clarifies the layout. Suppose a payments function under tenant `t_acme` needs Stripe API credentials and a database password. The Vault layout might look like:

```
secret/data/sous/t_acme/stripe       -> { "api_key": "sk_live_...", "webhook_secret": "whsec_..." }
secret/data/sous/t_acme/payments-db  -> { "value": "p@ssw0rd" }
```

The function manifest declares three secret references:

```json
{
  "config": {
    "secrets": [
      "STRIPE_KEY=stripe#json-field:api_key",
      "STRIPE_WEBHOOK=stripe#json-field:webhook_secret",
      "DB_PASSWORD=payments-db"
    ]
  }
}
```

At activation start, the invoker resolves each reference. `STRIPE_KEY` and `STRIPE_WEBHOOK` both hit `secret/data/sous/t_acme/stripe`; the driver fetches that path once (when caching is enabled) and extracts both fields from the same response. `DB_PASSWORD` hits `secret/data/sous/t_acme/payments-db`; the single-field shortcut returns the value of `value` directly without a format suffix. User code reads three credentials through `ctx.env.get(name)` without ever knowing which paths backed which name.

## Capability gating

A function may only read secrets it has declared. The `VersionConfig.Secrets` field on the published manifest (`internal/api/types.go`) is the allowlist: it lists each entry in the wire form parsed by `ParseRef`. `cs-invoker-pool` walks the list at activation start, asks the configured driver to resolve each entry, and stamps the resulting `{name → value}` map onto the runtime context via `runtime.WithEnv`. The map only contains the names that appeared in the manifest; reads for any other name resolve to `null`.

A function that calls `cs.env.get("UNDECLARED_KEY")` does not throw; it receives `null`, which lets author code distinguish "unconfigured" from "empty string" without crashing. Declared names that fail to resolve at activation start — Vault unreachable, path missing, malformed format — fail the activation before user code runs, so user code never observes a half-injected env. The result is the principle of least authority: the runtime cannot leak a value the manifest did not name, and the activation cannot start with a partial env.

The declarative list also gives the publish path a static check surface. Reviewers and tooling can read the manifest, see exactly which credentials a function will request, and audit the bundle without running it. See [Concepts: Capabilities and Isolation](Concepts-Capabilities-and-Isolation) for how secrets fit into the broader capability model.

The platform also keeps the env channel side-stepped from the codeQ envelope on purpose. `runtime.WithEnv` (`internal/runtime/env.go`) stamps the resolved map onto the activation context immediately before `Runner.Execute`; the map never appears on `api.InvocationRequest`, which is the wire type that travels through codeQ between cs-control and cs-invoker-pool. The result is that an inspector watching the codeQ topic sees the manifest's declared names but never the resolved bytes, and a poisoned DLQ entry replayed weeks later still carries no secret material. The runtime takes a shallow copy of the env map when WithEnv runs so a misbehaving caller cannot mutate the map after the runtime started reading from it — important when the same map is shared across goroutines for one activation, as future runtime concurrency features may require.

A common operational mistake is granting the platform token broader policy than this subtree. Policies should grant `read` on `secret/data/sous/{{identity.entity.aliases...name}}/*` using Vault's templated policies, or — when templates are not feasible — `read` on `secret/data/sous/*` and rely on Sous to scope by the resolved tenant. The narrower the policy, the smaller the blast radius if the platform token leaks. Either policy still produces a working cluster, but only the templated form gives Vault a second line of defence if Sous itself has a bug in tenant resolution.

## ctx.env.get(name)

The runtime exposes secrets to user code through `cs.env`. The host binding is wired in `internal/runtime/runner.go` `bindEnv`, which pulls the per-activation env map out of the context that `cs-invoker-pool` stamped with `runtime.WithEnv`:

```js
export default async function handle(event, ctx) {
  const key = ctx.env.get("STRIPE_KEY");
  if (key === null) {
    throw new Error("STRIPE_KEY not configured");
  }
  // use key
}
```

The first call per activation hits the driver via the invoker's pre-activation resolve step; subsequent calls hit an in-process map scoped to the activation. The map is read-only by convention: a copy is taken in `WithEnv` so a misbehaving caller cannot mutate the env after the runtime started reading from it. `cs.env.list()` returns the declared names (not the values) so a function can introspect which secrets it received without leaking material into a result body. See [Runtime: cs-js](Runtime-cs-js) for the full host binding contract, and the cs-python runtime page for the parallel Python binding.

Missing names resolve to `null` rather than throwing. This is a deliberate three-state design: `null` means "declared but unset" or "not declared", `""` means "declared and explicitly empty", and a non-empty string carries the resolved value. Function authors typically guard on `null` before using a credential so a missing secret turns into a controlled application error rather than an opaque runtime crash. Tooling that lints manifests can flag every `cs.env.get` call against the declared list at publish time, catching typos before the function ever runs.

The runtime never logs the env map. Console logging goes through `internal/runtime/runner.go`'s log collector, which caps line length and total bytes; the env binding lives on a separate `cs.env` object and the collector has no path that reaches into it. A function that writes a secret to `console.log` will see that log truncated alongside every other log line, but the platform does not redact the line — secrets that user code chooses to print are still printed. The contract is "the platform does not leak; user code is responsible for not printing what it asked for."

The Python runtime mirrors the JavaScript binding exactly. A `cs-python` function reads secrets through `ctx.env.get("NAME")` — same name, same null-on-missing semantics, same per-activation cache. The cs-python adapter is documented separately, but the secrets contract is identical so a function ported between runtimes does not need a manifest change for its secret declarations. The host-side path is also identical: the invoker resolves the manifest's secret list and stamps the resulting map onto the context, and the cs-python adapter exposes that map through its own host bridge. Adding a third runtime (cs-wasm is on the roadmap) will follow the same template.

## Caching

Caching operates at two layers and the trade-off between them shapes how quickly a rotated secret propagates to running activations.

Per-activation caching is always on. `cs-invoker-pool.resolveSecrets` runs once at activation start, builds the env map, and hands it to the runtime through context. Every `cs.env.get(name)` call inside the activation reads from that map; only the first call per activation touched the driver, and even that touch happened before user code began executing. This makes secret reads free for the function regardless of how many times the code calls `cs.env.get(name)`.

Cross-activation caching is configurable and disabled by default. When enabled, the Vault driver memoises `(tenant, path) → (value, expiry)` for a TTL so back-to-back activations on the same secret avoid hammering Vault. The trade-off:

- Longer TTL — lower Vault load, faster activation start (no round-trip), but a rotated value takes up to one TTL to propagate. A function reading a rotated DB password may keep using the old value until its cache entry expires.
- Shorter TTL — faster propagation, more Vault round-trips per activation. The pathological case is TTL=0, where every cold activation re-reads Vault even when nothing changed.
- Disabled — every cold activation reads Vault. Predictable but expensive at scale.

Production deployments tune the TTL against their rotation cadence: rotating quarterly tolerates a long TTL; rotating on incident response wants a short TTL. The `cs-control` admin API exposes a rotation hint that operators can flip to invalidate cache entries cluster-wide without restarting the pool — see [Operators: Administrative Operations](Operators-Administrative-Operations) for the runbook.

The cache is keyed by `(tenant, path)` rather than by `(tenant, name)`. Two functions in the same tenant that reference the same Vault path through different manifest names share one cache entry, so rotating that path propagates to both functions on the same tick. Conversely, two tenants that reference the same logical path each get their own cache entry, because the resolved Vault path includes the tenant prefix; rotating tenant A's secret does not invalidate tenant B's cache. The format selector is applied on each read out of the cache, not stored in the cache itself, so a manifest that requests `#json-field:user` and another that requests `#json-field:password` against the same path share one underlying fetch.

Cache entries that fail with `CS_SECRET_NOT_FOUND` or `CS_SECRET_UNAVAILABLE` are not memoised. The driver only caches successful reads; a transient Vault outage does not poison the cache with negative entries that would prevent recovery once Vault returns. The trade-off is a small thundering-herd risk on a cold start against an unreachable Vault — every concurrent activation in that window will issue its own request — but that's the right trade for a security-critical path where stale `not found` decisions would silently break functions even after the operator fixed the underlying issue.

Operators worried about the thundering-herd window can warm the cache after rotation by invoking a probe function that exercises every rotated secret; that activation is the only cold one, and every subsequent activation rides the warm cache. The probe pattern is documented in [Operators: Administrative Operations](Operators-Administrative-Operations) alongside the rotation runbook.

## Audit

Secret material never appears in activation logs. The runtime's log collector strips nothing — it doesn't have to, because secrets are not iterated through stdout/stderr unless user code chooses to write them, and even then the log collector caps line length and total bytes so accidental dumps are bounded. The audit stream takes the opposite view: every secret read is logged as a `SecretRead` event with the secret NAME but not the VALUE, so operators can answer "which functions read which credentials" without ever observing the credential itself.

A `SecretRead` audit entry carries:

- `tenant` — the activation's tenant.
- `activation_id` — for correlation with the activation log and the invocation result.
- `secret_name` — the declared name from `VersionConfig.Secrets`.
- `secret_path` — the resolved Vault path (still safe; this is metadata, not material).
- `driver` — `memory` or `vault`.
- `outcome` — `ok`, `not_found`, or `unavailable`.

The event flows through the same audit sink as control-plane mutations and is signed end-to-end when the operator configures the webhook sink with an HMAC key. Cross-link to [ledgerDB Audit](Enabled-Services-ledgerDB-Audit) for the audit envelope shape and to [Security](Security) for the secrets-handling threat model. The audit sink is selected via `plugins.audit.sink` (`stdout`, `codeq`, or `webhook`) and inherits the same delivery semantics as every other audit event.

`SecretRead` events are emitted at the invoker boundary, not from inside the runtime. The invoker resolves the manifest's secret list once per activation and emits one event per resolved reference before user code starts; cache hits emit an event with a `cache_hit: true` flag so a downstream consumer can distinguish "Vault was actually touched" from "we served a cached value". This keeps the audit stream from dropping events when caching is enabled, which would otherwise create a blind spot for compliance reviewers asking "which secrets were used during incident X".

Functions that conditionally read secrets — for example, only reading `STRIPE_KEY` when the inbound request targets the payment path — still produce an audit event for every declared secret, because resolution happens before user code dispatches. This is intentional: the audit answers "which credentials were available to this activation", which is the question that matters for compliance. Detailed per-call audit would require instrumenting `cs.env.get` itself and is tracked as a follow-up enhancement for tenants that need it.

Operators tailing the audit stream during a Vault outage will observe a burst of `SecretRead` events with `outcome: unavailable` correlated with the activation failures, which makes incident attribution straightforward. The audit stream is the canonical answer to "who used what, when"; the activation log is the canonical answer to "what did the function do". The two are joined by `activation_id`, and neither stream contains the resolved secret value.

## Memory driver (dev)

The memory driver serves lookups from a hard-coded YAML map and is the default when no driver is selected. Configuration:

```yaml
plugins:
  secrets:
    driver: memory
    memory:
      seed:
        DB_PASSWORD: hunter2
        stripe_key: sk_test_xxx
        t_acme/api-key: secret-for-acme
```

The seed key matches the resolved `SecretRef.Path` verbatim, so the same manifest works against memory and Vault provided the YAML map is laid out to mirror the production path scheme. Tests can pre-stamp tenant-scoped keys (`t_acme/api-key`) when they need to assert isolation; in production, those keys live in Vault under the tenant subtree instead.

**This driver is dev-only.** It writes secret material into cluster YAML, which is a config file checked into source control by every operational baseline this project recommends. Running it in production puts plaintext credentials into the same blob that every operator with cluster-read access already has. The driver registers itself for two reasons only: to let `cs-cli doctor` scaffold a working cluster without a Vault dependency, and to let unit tests stamp deterministic fixtures.

The memory driver also exposes a `Set(path, value)` method that is not reachable from YAML — it exists for the invoker integration test in `cmd/cs-invoker-pool` to stamp fixtures without poking at unexported fields. Production code never calls it, but its existence in the public Go API is documented to discourage accidental adoption. Operators looking at the memory driver as an option should treat YAML-seeded secrets and runtime-set secrets as equally unsafe outside development.

Switching from memory to Vault is the cluster-readiness checkpoint that separates a sandbox from a production deployment. The capability gating, the path scheme, and the audit stream are all designed so the only thing that changes between dev and prod is the driver name — every function manifest, every `cs.env.get` call, and every audit consumer keeps working unchanged. That property is worth protecting; resist the temptation to seed memory-driver values for "just one production tenant".

## Failure modes

Every failure during secret resolution short-circuits the activation before user code runs, so a function never observes a partial env. The platform maps each failure to a `CS_*` code defined in `internal/errors/errors.go`:

- **Vault unreachable** — transport error, DNS failure, or 5xx response. The driver surfaces `CS_SECRET_UNAVAILABLE` (HTTP 503). The invocation result carries the code so the gateway or scheduler can retry against a healthier replica.
- **Vault auth failure** — 401 or 403 from Vault, typically because the token expired or the policy was revoked. The driver re-reads the token from `token_env` on the next request; if it still fails, the activation returns `CS_SECRET_UNAVAILABLE`. Lease renewal is not implemented in v0.1, so operators that use short-lived tokens are responsible for rotating the env value before expiry.
- **Secret not found** — Vault returns 404, or the entry exists but the `json-field:<key>` selector points at a missing field. The driver surfaces `CS_SECRET_NOT_FOUND` (HTTP 404). The activation result body names the path so the operator can fix the missing entry without grepping logs.
- **Manifest reference invalid** — `ParseRef` rejects an empty name or malformed entry. The invoker surfaces `CS_VALIDATION_FAILED`. This is a publish-time bug; the function should not have made it to invocation. The control-plane validation pass at publish time catches most of these earlier.
- **Driver not configured** — the cluster set `driver: vault` but a secret-declaring function ran before the driver could initialise. The invoker surfaces `CS_SECRET_UNAVAILABLE` with the message "secrets provider not configured".

See [Error Model](Error-Model) for the full code table and the HTTP status mapping. The `CSSecretNotFound` / `CSSecretUnavailable` split is deliberate so a health probe can distinguish "Vault is down" from "this secret is missing"; an SLO that fires on `CS_SECRET_UNAVAILABLE` flags infrastructure outages, while an SLO that fires on `CS_SECRET_NOT_FOUND` flags configuration drift.

Each failure also bubbles up to the invocation result body in the standard CS error envelope, so a synchronous HTTP invoke caller sees the same code the audit stream recorded. Asynchronous callers reading from the result topic see the same envelope, with the `request_id` carried through so an SRE can correlate the failure across the gateway log, the audit stream, and the activation log without a join key. Function authors who want to handle missing secrets gracefully — for example, falling back to an unauthenticated mode in development — can either drop the secret from the manifest entirely or use `cs.env.get(name)` and check for `null` after the activation starts, because manifest-level "optional secrets" are not a feature in v0.1.

The pre-execution failure window is bounded by `timeout_ms` per secret. With ten declared secrets and a 2-second timeout, the worst case is roughly 20 seconds of secret resolution before user code starts, although caching usually keeps the real number in single-digit milliseconds. Activations near their deadline can therefore time out during resolution; the invoker surfaces `CS_DEADLINE_EXCEEDED` in that case, and tuning the per-function `timeout_ms` allowance is the right response.

## Rotation strategy

Operators rotate secrets in Vault directly. Running activations continue to use the value they resolved at activation start; new activations pick up the new value on their next driver read. The lag between rotation and full propagation is bounded by the cross-activation cache TTL: with caching disabled, every cold activation observes the new value immediately; with a non-zero TTL, propagation completes after one TTL has elapsed.

Cross-link to [Operators: Administrative Operations](Operators-Administrative-Operations) for the full rotation runbook, which covers writing the new version to Vault, optionally invalidating the cluster cache, and verifying propagation through a probe function. The runbook also documents the emergency revoke path — flipping the Vault policy to deny reads until a leaked credential has been audited.

KV v2 keeps version history server-side, so a rotation that introduces a regression can be rolled back by writing the previous version's bytes back to the same path. Functions that need atomic multi-secret rotation should put both values under one entry and read them with `#json-field:<key>`, because writing two paths is not atomic from the consumer's perspective and a function reading mid-rotation might see one new value and one old value. The runbook covers both patterns.

Rotating the platform's Vault token (the credential `cs-invoker-pool` uses to authenticate to Vault, not the per-tenant secrets) is a different operation. It requires updating the env variable named by `token_env` and restarting the pool, because the driver loads the token once at construction time in v0.1. The operator's checklist for that rotation also lives in the administrative operations runbook; doing it in the wrong order leaves the pool unable to read any secrets until the restart completes, which manifests as a wave of `CS_SECRET_UNAVAILABLE` for every secret-dependent activation.
