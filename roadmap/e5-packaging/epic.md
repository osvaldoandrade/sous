# Epic: Packaging, dependencies & supply chain

**Phase:** Next (2–5 months)
**Theme:** Let functions depend on external code safely, prove provenance, and ship an SBOM for every published version.

## Why
Today a published bundle is just `function.js` + `manifest.json` with a `sha256`: there is no way to depend on external code, no signer identity on the version, and nothing machine-readable for an auditor or vulnerability scanner. Agents will not produce non-trivial functions without a dep story, and regulated tenants will not adopt the platform without provenance and an SBOM per version. This epic closes those three gaps in the same canonical-bundle pipeline so the no-build-step invariant is preserved while the supply-chain surface gets a real answer.

## Scope
- Manifest + control-plane resolver for JS deps via import maps, with a curated mirror and SRI-pinned versions, frozen into the canonical bundle.
- Tenant-managed signing keys, publish-time signature requirement, and invoker-side re-verification against `cs-control` key registry.
- CycloneDX 1.5 SBOM produced once per publish, persisted with the version, and exposed via `GET /functions/:id/versions/:v/sbom`.
- Documentation updates across `docs/08-runtime-cs-js.md`, `docs/15-security.md`, `docs/04-api-rest.md`, and `docs/20-config-reference.md` so the new surfaces are first-class.

## Outcomes / success metrics
- Dep resolve time p99 at publish under 5 s for a function with up to 10 declared deps from a warm curated-mirror cache.
- Signature verification failure rate on invoke under 0.01% of cold loads, with zero successful executions of tampered or unsigned bundles in security tests.
- SBOM coverage at 100% of newly published versions within one release of E5.03 landing; SBOM-fetch p99 under 200 ms.
- Zero regressions in the no-build-step invariant: agents continue to publish UTF-8 source + manifest only, with all resolution and SBOM work happening server-side.

## Tasks
- [ ] #12 — JS dependency bundles with import maps
- [ ] #14 — Signed bundles with tenant keys
- [ ] #15 — SBOM generation per published version

## Non-goals
- Python or WASM packaging (covered by separate runtime epics).
- Hosted KMS / HSM for tenant private keys; this epic only manages the public-key registry side.
- Vulnerability scanning, CVE enrichment, or third-party attestation (Sigstore/Rekor) on top of the SBOM.
- Encrypting bundle bytes at rest; this epic addresses integrity, provenance, and inventory, not confidentiality.
- Signing the platform's own release binaries; that belongs to a release-pipeline epic.
