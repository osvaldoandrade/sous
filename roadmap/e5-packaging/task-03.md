# SBOM generation per published version

**Parent epic:** #21
**Phase:** Next
**Estimated size:** M

## Problem
With JS deps (E5.01) and signed bundles (E5.02) landing, regulated tenants and enterprise buyers will ask "what is actually inside this version, and who signed it?" — we have no machine-readable answer today. There is no inventory of runtime, bundled deps, hashes, or signer; nothing to feed downstream vulnerability scanners or compliance reviews. We need a Software Bill of Materials produced once per published version and addressable by API.

## Proposed solution
- On every successful publish in `cmd/cs-control`, emit a CycloneDX 1.5 JSON SBOM (SPDX-2.3 considered as an alternative output format in a follow-up) describing: the `cs-js` runtime and its version, every dep declared by the manifest (name, version, SRI hash, source registry), the bundle `sha256`, the canonical size, and the signing identity (`kid`, algorithm, public-key fingerprint) from E5.02.
- Add an `internal/sbom` package that takes a resolved bundle plus version metadata and produces the SBOM bytes deterministically (sorted components, stable serial number derived from `sha256`) so repeated regenerations are byte-identical. Use a CycloneDX Go library or hand-rolled struct marshaling — no shelling out to external tools.
- Persist the SBOM alongside the version record in KVRocks under a `sbom:<tenant>:<ns>:<fn>:<version>` key with the version's `sha256` recorded for cross-check; include it in the publish audit event.
- Expose `GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions/{version}/sbom` in `internal/api/http.go` returning the stored SBOM with `Content-Type: application/vnd.cyclonedx+json; version=1.5`; support `?format=spdx` once a second serializer is added (out of scope for first cut). Authz reuses existing `cw:function:read` role check.
- Wire a `cs functions sbom <ref>` CLI subcommand into `cmd/cs-cli` that fetches and pretty-prints the SBOM, and document the new endpoint in `docs/04-api-rest.md` plus a "Supply chain artifacts" section in `docs/15-security.md`.

## Acceptance criteria
- [ ] Every successful publish writes a CycloneDX 1.5 SBOM to KVRocks; replaying publish for the same canonical bundle yields a byte-identical SBOM (deterministic ordering, deterministic serial number).
- [ ] `GET /functions/.../versions/{v}/sbom` returns the stored SBOM with the correct CycloneDX content type, `404` for unknown versions, and respects existing read-authz; an end-to-end test publishes a function with one dep and asserts the returned SBOM lists runtime + dep + signing identity.
- [ ] SBOM components include name, version, SRI hash (matching the bundle's import-map integrity), source (curated registry URL), and a `properties` block carrying `cs:bundle.sha256` and `cs:signing.kid`.
- [ ] `cs functions sbom` CLI subcommand prints the SBOM for a published version and exits non-zero on missing version or auth failure.
- [ ] `docs/04-api-rest.md` documents the new endpoint and response shape; `docs/15-security.md` adds a "Supply chain artifacts" section; `docs/25-schemas.md` references the CycloneDX 1.5 schema link.
- [ ] Unit tests cover SBOM determinism, missing-dep-metadata error, and authz denial; integration test exercises publish -> SBOM fetch -> CycloneDX schema validation.

## Dependencies & risks
- Prereqs: E5.01 must define how deps are recorded on the version (name, version, integrity, source); E5.02 must expose `kid` and key fingerprint on the version record.
- Externals: CycloneDX 1.5 JSON schema (stable). Optional SPDX-2.3 follow-up tracked separately.
- Risk: SBOMs balloon storage when versions are short-lived. Mitigation: SBOM is small (KiB-scale); apply the same retention as version metadata, no separate TTL.
- Risk: schema drift if CycloneDX bumps a minor version. Mitigation: pin to 1.5 and regenerate on demand; producing the SBOM is a pure function of stored version metadata, so backfilling older versions is trivial.
- Risk: leaking internal registry URLs through `source` fields for tenants on shared mirrors. Mitigation: map internal mirror to a public-facing label in the SBOM via config.

## Out of scope
- SPDX-2.3 output format (follow-up once CycloneDX is stable in production).
- Vulnerability scanning / CVE enrichment of the SBOM (separate downstream concern).
- SBOMs for the platform binaries themselves (`cs-control`, `cs-invoker-pool`) — those belong to a release-pipeline epic, not user-bundle packaging.
- Signing the SBOM separately from the bundle; the bundle signature already covers the inputs the SBOM is derived from.
