# Security

This file defines the security model and enforcement points.

## Threat model

The platform runs untrusted code from tenant users.
The platform must prevent:

- cross-tenant data access
- host compromise
- data exfiltration beyond explicit allowlists
- denial of service through resource exhaustion

## Sandbox model

The invoker runs user code in an isolate.

The isolate blocks:

- filesystem access
- process spawning
- raw network sockets
- native module loading

User code accesses the outside world through `cs.*`.

## Capability allowlists

The platform enforces allowlists at runtime:

- KVRocks key prefixes
- codeQ topic prefixes
- HTTP host allowlist

The invoker denies a call that violates a capability.

## Secrets

The platform does not inject secrets by default.

A function version references secrets by **name** in
`VersionConfig.Secrets`. Three forms are accepted:

- `"STRIPE_KEY"` — the lookup path matches the env var name.
- `"STRIPE_KEY=payments/stripe_key"` — `STRIPE_KEY` is the env var the
  function sees through `cs.env.get("STRIPE_KEY")`, `payments/stripe_key`
  is the provider-specific lookup path.
- `"TIER=payments/multi#json-field:tier"` — the provider returns a JSON
  document and the invoker exposes the `tier` field as a string.

### Provider model

`plugins.secrets.driver` selects the provider implementation:

| Driver  | Backend                              | Auth                                  |
|---------|--------------------------------------|---------------------------------------|
| memory  | In-process seed map (dev + tests)    | n/a                                   |
| vault   | HashiCorp Vault KV v2                | static token (env var `VAULT_TOKEN`)  |

Each driver registers itself with `internal/plugins/registry` at init
time, mirroring the `authn`, `persistence`, and `messaging` plugin
points. New drivers (AWS Secrets Manager, GCP Secret Manager) plug in
the same way without touching the invoker.

### Injection contract

The `cs-invoker-pool` resolves the configured `VersionConfig.Secrets`
list at activation start, **after** bundle SHA verification and
**before** user code runs. The resolved `{name: value}` map is stamped
onto the runtime context via `runtime.WithEnv` and exposed to JS code
through two host bindings:

- `cs.env.get(name)` — returns the string value or `null` when the
  name is not configured.
- `cs.env.list()` — returns the declared **names** only; values never
  appear in the output so a misbehaving function cannot dump the entire
  secret set into a log line by accident.

Secret material:

- never lands in the bundle (the publisher only stores names),
- never lands in KVRocks (the persistence plugin is bypassed entirely),
- never lands in `cs.results` or DLQ envelopes,
- is not appended to activation logs by the runtime.

When a referenced secret is missing the activation fails with
`CS_SECRET_NOT_FOUND` (HTTP 404) before any code executes; when the
provider is unreachable the activation fails with `CS_SECRET_UNAVAILABLE`
(HTTP 503) so the gateway / scheduler can retry against another replica.

## Network egress

The runtime denies egress by default.

If `http.allowHosts` exists in the function manifest:

- `cs.http.fetch` allows only those hostnames.

The runtime blocks private IP ranges:

- 10.0.0.0/8
- 172.16.0.0/12
- 192.168.0.0/16
- 127.0.0.0/8
- ::1/128
- fc00::/7

The private-IP block is a non-negotiable invariant — no allowlist
override can re-enable it. See `docs/02-requirements.md` for the
contract.

### Per-tenant egress allowlist (E6.02)

A tenant-scoped **EgressPolicy** layers on top of the manifest
`http.allowHosts` list:

```json
{
  "allowed_hosts": ["api.partner.com", "*.example.com"],
  "allowed_cidrs": ["203.0.113.0/24", "2001:db8::/32"],
  "denied_hosts":  ["abuse.example.com"],
  "default_deny":  true
}
```

Semantics:

- **`default_deny: true`** (recommended) — destinations not in
  `allowed_hosts` or `allowed_cidrs` are rejected. Each invocation
  sees `CS_EGRESS_DENIED` (HTTP 403) with a reason naming the policy
  rule that fired.
- **`default_deny: false`** — destinations not in `denied_hosts` are
  permitted. Use only for trusted internal tenants that need to dial
  a broad set of upstreams.
- `allowed_hosts` entries are matched **case-insensitively**.
  `*.example.com` matches any subdomain (`api.example.com`,
  `v1.api.example.com`) but never the apex (`example.com`).
- `allowed_cidrs` accepts IPv4 or IPv6 CIDRs. Bare IPs (`8.8.8.8`,
  `2001:db8::1`) are accepted and interpreted as `/32` / `/128`.
- `denied_hosts` is evaluated **before** `allowed_hosts`, so an
  operator can carve a destination out of a wildcard
  (`allow *.example.com` + `deny abuse.example.com`).
- The private-IP block runs **after** the allowlist. A policy entry
  that resolves to `10.0.0.0/8`, `127.0.0.0/8`, `fc00::/7`, etc. is
  still rejected by the runtime — the allowlist cannot override the
  private-IP invariant.
- Missing or malformed policies fall through to the legacy
  manifest-only behaviour so existing tenants keep working until they
  opt in. cs-invoker-pool logs (at warn-level) any failure to compile
  a stored policy.

Storage: `cs:tenant:<tenant>:egress:policy` in KVRocks. The control
plane CRUD endpoints (`GET /v1/tenants/{tenant}/egress-policy`,
`PUT /v1/tenants/{tenant}/egress-policy`) gate writes through
the Tikti actions `cs:egress:policy:read` and
`cs:egress:policy:write`.

Error code: `CS_EGRESS_DENIED` (HTTP 403). See `docs/21-errors.md`.

## Request validation

The gateway enforces:

- max body size
- max header size
- max query size

The control plane enforces:

- bundle size
- manifest schema validity

## Supply chain

The control plane computes `sha256` for every published bundle.
The invoker verifies `sha256` on load.

The control plane rejects publish when:

- `sha256` mismatches draft record

## Signing (E5.02)

Tenants register an Ed25519 public key with the control plane and sign
every publish over a canonical payload bound to the bundle digest and
the publish tuple. The invoker re-verifies the signature on every cold
bundle load — a tampered bundle, a rotated key, or a missing key all
refuse to execute.

### Algorithm

Ed25519 only. Public keys are 32 bytes, signatures are 64 bytes, and
verification is a single-call stdlib operation (`crypto/ed25519.Verify`)
that adds microseconds to the bundle load path. ECDSA P-256 is reserved
for a future additive change — the `BundleSignature.Algorithm` field
already exists, the publish/invoke verifier just switches on it.

### Canonical payload

The signature commits to the byte sequence produced by
`signing.CanonicalPayload(sha, tenant, namespace, function, 0)`:

```
"cs.bundle.signature.v1\x00"
uint32_be(len(bundleSHA))   | bundleSHA            (raw bytes)
uint32_be(len(tenant))      | tenant               (utf-8)
uint32_be(len(namespace))   | namespace            (utf-8)
uint32_be(len(function))    | function             (utf-8)
int64_be(version)                                  (always 0 in v0.1)
```

- The magic prefix prevents cross-protocol reuse.
- Tenant/namespace/function are included so the same bundle bytes
  cannot be re-published under a different name with the same
  signature.
- Version is signed as `0` because the monotonic version number is
  allocated server-side after the agent has signed; cs-control persists
  the signature alongside the resulting `VersionRecord` and the invoker
  re-verifies with the same constant.
- No timestamp lives inside the canonical payload — the issue threat
  model explicitly excludes replay against a different version, which
  is already prevented by version immutability.

### Key lifecycle

| Endpoint                                          | Effect                                                |
|---------------------------------------------------|-------------------------------------------------------|
| `POST /v1/tenants/{tenant}/signing-keys/rotate`   | Generates a fresh Ed25519 keypair, stores the public  |
|                                                   | half under `cs:tenant:{tenant}:signing:ed25519:active`,|
|                                                   | returns the private key bytes **once** in the body.   |
| `GET /v1/tenants/{tenant}/signing-keys/active`    | Returns the active public key + KID + algorithm.      |

The control plane never persists the private half. A tenant that loses
its private key must rotate; old signed versions persist (they still
verify against the new key only if it produced them, otherwise the
invoker refuses to execute — operators must re-publish under the new
key).

### Publish-time enforcement

The publish handler reads the detached signature from the
`X-CS-Signature` request header (base64, standard or URL alphabet, with
or without padding). The flow is:

1. Check the header. Missing + `plugins.signing.required=true` →
   `CS_SIGNATURE_MISSING` (400). Missing + required=false → accept the
   publish without a signature (backward-compatible mode).
2. Decode base64. Malformed → `CS_SIGNATURE_INVALID` (400).
3. Load the tenant's active public key. Missing key →
   `CS_SIGNATURE_KEY_NOT_FOUND` (404).
4. Verify the signature against the canonical payload. Mismatch →
   `CS_SIGNATURE_INVALID` (400).
5. Persist the signature on the `VersionRecord` (`Signature` field).

### Invoke-time enforcement

The invoker calls `verifyInvokeSignature` immediately after the
existing `bundle.VerifySHA256` check. When `VersionRecord.Signature !=
nil`, the invoker reads the tenant's current active public key from
`cs:tenant:{tenant}:signing:ed25519:active` and re-verifies. Any
failure (algorithm mismatch, missing key, rotated key, tampered bytes)
drops the activation with `CS_SIGNATURE_INVALID` and logs the reason
through the structured logger.

### The `plugins.signing.required` knob

`plugins.signing.required` (default `false`) controls the publish-time
gate. The roll-out is:

1. Land E5.02 with `required=false` (current default).
2. Tenants rotate signing keys at their own pace; signed publishes are
   recorded but unsigned publishes still succeed.
3. Operators flip the knob to `true` once every active tenant has at
   least one rotation. Publishes without `X-CS-Signature` then 400.
4. The invoker side always re-verifies when `Signature != nil`, so
   versions published in the unsigned window keep running unchanged
   while signed versions are guarded forever.

## Supply chain artifacts

Every successful publish writes a **CycloneDX 1.5** Software Bill of
Materials next to the version record. The SBOM is generated by
`internal/sbom` from the bundle that was just persisted and lives in
KVRocks under `cs:sbom:<tenant>:<namespace>:<function>:<version>`. The
publish handler fails the request if SBOM generation or persistence
fails so regulated tenants never observe a version without
supply-chain metadata.

The SBOM lists, at minimum:

- the runtime declared by the bundle manifest (e.g. `cs-js`) as a
  `framework` component,
- every file inside the canonical bundle (e.g. `function.js`,
  `manifest.json`) as a `file` component with its SHA-256 hash and
  byte length,
- the bundle digest and signing identity (once E5.02 lands) as
  `cs:bundle.sha256`, `cs:signing.kid`, `cs:signing.algorithm`, and
  `cs:signing.fingerprint` metadata properties,
- every dependency declared by the manifest's import map (once E5.01
  lands) as a `library` component carrying its SRI hash and source URL.

The SBOM is deterministic: components are sorted by `bom-ref`,
properties are sorted by name, and the document `serialNumber` is a
UUID v4 derived from the bundle SHA-256 so replaying a publish for the
same canonical bundle yields a byte-identical SBOM. Re-generating an
older version's SBOM is therefore safe — it is a pure function of the
stored bundle plus the version metadata, with no time- or environment-
sensitive inputs.

The SBOM is exposed at
`GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions/{version}/sbom`
with `Content-Type: application/vnd.cyclonedx+json; version=1.5`. The
endpoint reuses the existing `cs:function:read` role check; see
`docs/04-api-rest.md` "SBOM" for the wire shape and error mapping.

Future work tracked separately:

- SPDX 2.3 alternate output format,
- vulnerability/CVE enrichment of the SBOM,
- per-tenant rewriting of internal registry URLs in `source` fields
  for shared-mirror deployments.

### Curated import-map mirror allowlist

When a `function.js` declares `imports` (see docs/08-runtime-cs-js.md)
and an entry references an `http(s)` URL, the resolver in cs-control
fetches the bytes at publish time. The fetch is gated by a
**curated-mirror allowlist** configured at
`cs_control.publish.imports.allowed_mirrors` (see
docs/20-config-reference.md).

Rules:

- The allowlist is **empty by default**. Out of the box no remote fetch
  is permitted; only `path:` imports (bytes already in the uploaded
  bundle) resolve.
- Only `http` and `https` URLs are allowed. Other schemes (`file:`,
  `ftp:`, `data:`) are rejected with `CS_VALIDATION_FAILED`.
- The hostname of the URL is matched case-insensitively against the
  allowlist; the port is ignored.
- A per-import size cap
  (`cs_control.publish.imports.max_bytes_per_import`, default 4 MiB)
  bounds any single dep. The 16 MiB bundle cap from
  docs/26-capacity-and-limits.md still applies to the frozen bundle as
  a whole.
- The publisher's optional `integrity` digest must match the bytes the
  resolver actually fetched, otherwise publish fails. When the
  publisher omits `integrity` the resolver computes a canonical
  `sha256-...` and freezes it into `import-map.json` so the runtime
  can verify on load.
- The runtime never re-fetches. The frozen `import-map.json` is the
  only source of truth at invoke time; a tampered bundle (bytes that
  no longer match the frozen digest) is rejected with
  `CS_IMPORT_NOT_FOUND` (HTTP 422).

## Rate limiting

The gateway rate limits by:

- tenant
- function ref
- client IP

Default policy:

- 200 rps per tenant per cluster
- 20 rps per function ref per cluster

The gateway returns `429` on limit breach.
