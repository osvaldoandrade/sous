# External vault secret provider integration

**Parent epic:** #30
**Phase:** Later
**Estimated size:** L

## Problem
Function secrets in v0.1 are resolved through a KVRocks-backed provider that lives inside the
data plane (`docs/15-security.md` "Secrets"). Enterprise tenants need to keep secret material
in their own vault (HashiCorp Vault, AWS Secrets Manager) and never ship it inside the bundle
or replicate it into KVRocks. Without a pluggable external provider, `code-sous` cannot meet
compliance requirements for centralized secret custody, rotation, and audit.

## Proposed solution
- Introduce a `plugins.secrets.driver` plugin point in `internal/plugins/registry/registry.go`
  matching the existing `plugins.authn.driver` pattern (`tikti` today). Driver factories live
  under `internal/plugins/secrets/{vault,awsSecretsManager,kvrocks}` and register through
  `RegisterSecrets` at init.
- Add a `SecretProvider` interface (`Resolve(ctx, tenant, ref) (Material, error)`,
  `Watch(ctx, refs) (<-chan Event, error)`) under `internal/plugins/secrets/secrets.go`.
  Keep `kvrocks` as the default driver for backward compatibility.
- Wire the provider into `cmd/cs-invoker-pool/main.go` so the invoker pulls secret material
  at activation start, hands it to the isolate as in-memory globals, and zeroes the buffer
  on activation end. The secret material never lands in the bundle, in logs, or in
  persistence layers.
- Update `docs/15-security.md` "Secrets" and `docs/20-config-reference.md` with the new
  `plugins.secrets` config block, env-var-only credential injection for the providers, and
  the rotation contract (cache TTL, `Watch` semantics, redaction guarantees).
- Provide a `cs secrets check <ref>` CLI verb in `cmd/cs-cli` that calls the control plane
  to confirm a secret reference resolves under the active driver (without leaking value).

## Acceptance criteria
- [ ] `plugins.secrets.driver` accepts `kvrocks`, `vault`, `aws-secrets-manager`; unknown
      drivers fail config validation with a clear error.
- [ ] `vault` driver authenticates via AppRole or Kubernetes auth (configurable), respects
      lease TTL, and renews leases until activation completion.
- [ ] `aws-secrets-manager` driver authenticates via IRSA / instance profile; ARN-style
      refs (`arn:aws:secretsmanager:...`) resolve.
- [ ] Secret material is never written to KVRocks, never serialized into the bundle,
      and is redacted in `cs.log` output and in activation logs.
- [ ] Cold-start latency overhead from secret fetch is bounded; provider caches resolved
      material per activation only (no cross-activation cache by default).
- [ ] Docs updated: `docs/15-security.md` "Secrets" section + `docs/20-config-reference.md`
      `plugins.secrets` block + example in `config.example.yaml`.
- [ ] Tests: registry round-trip, vault driver against a dev-mode Vault container,
      AWS driver against `localstack`, redaction test in invoker integration suite.

## Dependencies & risks
- Prereq: existing plugin registry refactor (already present for authn/persistence/messaging).
- External: HashiCorp Vault and AWS Secrets Manager SDKs; both add binary size — gate behind
  build tags if footprint becomes a concern.
- Risk: secret leakage through logs or panics. Mitigation: wrap material in a
  `redact.Secret` type whose `String()` returns `***`, mandatory in code review.
- Risk: provider outage stalls cold starts. Mitigation: per-driver timeout + circuit
  breaker; invocation fails fast with `SecretUnavailable` instead of hanging.
- Risk: lease renewal across long activations. Mitigation: renewal goroutine bounded by
  activation deadline; on renewal failure the invoker aborts the activation.

## Out of scope
- Tenant-managed signing keys for bundle signatures (covered by separate roadmap item).
- KMS-based envelope encryption of secret material at rest in KVRocks.
- Dynamic secrets (DB credentials brokered per-call) — phase 2.
- UI for browsing secret references.
