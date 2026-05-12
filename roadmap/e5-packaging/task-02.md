# Signed bundles with tenant keys

**Parent epic:** #21
**Phase:** Next
**Estimated size:** M

## Problem
The control plane only computes a `sha256` over the canonical bundle (`docs/15-security.md` "Supply chain"); there is no way for a tenant or auditor to prove that a published version was produced by an authorized signer rather than a compromised control-plane operator or a hijacked agent token. As we grow the surface that depends on bundles (deps in E5.01, SBOM in E5.03), we need cryptographic provenance: tenants must register signing keys, publish must require a valid signature, and the invoker must refuse to execute an unsigned or tampered bundle.

## Proposed solution
- Add a key-management surface to `cs-control`: `POST /v1/tenants/{tenant}/signing-keys` and `DELETE /v1/tenants/{tenant}/signing-keys/{kid}` that store Ed25519 (and optionally ECDSA P-256) public keys with a status (`active`, `revoked`) and a created-at timestamp.
- Extend `internal/bundle/bundle.go` with a `Canonicalize` step that produces a stable byte sequence over (`function.js`, `manifest.json`, `import-map.json`, `deps/**`) using the existing tar layout, and a `Verify(sig, pubkey, canonical)` helper using `crypto/ed25519`. Reuse `BuildCanonical` so the signing surface is the same bytes the `sha256` is computed over.
- Update the publish request body in `cmd/cs-control` to require `signature` and `signing_key_id` fields. The control plane looks up `signing_key_id` for the tenant, verifies the signature over the canonical bundle, and rejects with `CS_BUNDLE_SIG_INVALID` or `CS_BUNDLE_SIG_KEY_REVOKED` on failure. Store `signing_key_id`, `signature`, and `signed_at_ms` on the version record.
- Update `cmd/cs-invoker-pool` (and the cs CLI local runner for parity) to re-verify the signature against the stored public key on bundle load; on mismatch, drop the activation with `CS_BUNDLE_SIG_INVALID` and emit a structured audit event. Cache verified `(version, kid)` tuples in-process to keep hot path overhead low.
- Add a `cs sign` CLI subcommand that takes a built canonical bundle plus a key file and produces the signature blob, so agents can publish from CI or local without manual openssl gymnastics. Document the flow in `docs/15-security.md` and the publish API in `docs/04-api-rest.md`.

## Acceptance criteria
- [ ] Tenants can register, list, and revoke Ed25519 signing keys via the API; revoked keys cannot be used for new publishes but existing signed versions remain executable (recorded `kid` is still resolvable).
- [ ] Publish request requires `signature` + `signing_key_id`; `cs-control` rejects missing, malformed, or invalid signatures with typed errors and never persists an unverified version.
- [ ] `cs-invoker-pool` re-verifies signatures on bundle load and refuses to execute unsigned or tampered bundles; verification failure produces an audit event with `tenant`, `function`, `version`, `kid`, and `reason`.
- [ ] `cs sign` CLI subcommand signs a local bundle with a tenant key file and the resulting publish round-trips successfully against a real `cs-control`.
- [ ] `docs/15-security.md` documents the threat model addition, key lifecycle, and verification points; `docs/04-api-rest.md` documents the signing-key endpoints and the new publish fields.
- [ ] Unit tests cover sign/verify happy path, wrong-key, tampered-canonical, revoked-key, and unknown-kid; integration test exercises publish + invoke with signed bundle end-to-end.

## Dependencies & risks
- Prereqs: canonical-bundle definition stable enough to sign over; coordinate with E5.01 so `deps/**` and `import-map.json` are inside the canonical region from day one.
- Externals: tenants need a way to manage private keys; in-scope here is the public-key registry, not a hosted KMS (kept as a future extension).
- Risk: verification on every cold load adds latency. Mitigation: per-replica LRU of verified `(version, kid)` keyed by `sha256`; signature check is microseconds for Ed25519.
- Risk: a tenant losing all signing keys cannot publish. Mitigation: clear key-rotation procedure documented in `docs/24-runbooks.md`; allow multiple active keys per tenant.
- Risk: clock skew or replay of old signatures. Mitigation: bind signature to bundle `sha256` (no timestamps in the signed payload); version immutability already prevents replay against a different version.

## Out of scope
- Hosted KMS / HSM integration for tenant private keys.
- Cross-tenant key trust, signature transparency logs, or third-party attestations (e.g., Sigstore/Rekor) — tracked as a future epic.
- Code-signing for runtime binaries (`cs-control`, `cs-invoker-pool`); this task is scoped to user bundles.
- Encryption of bundle bytes at rest; this task addresses integrity and provenance, not confidentiality.
