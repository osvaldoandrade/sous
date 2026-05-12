# JS dependency bundles with import maps

**Parent epic:** #21
**Phase:** Next
**Estimated size:** L

## Problem
Today a published function bundle contains only `function.js` and `manifest.json`; any external JS dependency must be inlined by hand, which breaks the agent UX as soon as a function needs `zod`, `date-fns`, or a tenant-internal helper. There is no build step at publish, no `node_modules`, and the invoker has no way to resolve a bare specifier. We need a way for functions to declare external JS deps and have them deterministically frozen into the bundle, without reintroducing a build step on the agent side.

## Proposed solution
- Extend the manifest schema in `docs/08-runtime-cs-js.md` and validation in `internal/bundle/bundle.go` with a new `dependencies` block: `{ "<bare-specifier>": { "version": "1.2.3", "integrity": "sha384-..." } }`, plus an optional `registry` hint pointing to a curated mirror configured in `cmd/cs-control`.
- Add a resolver in `internal/bundle` (new file, e.g. `resolve.go`) that, given a manifest, fetches each declared dep from the curated mirror, verifies the SRI hash, and emits a flat set of files under `deps/<name>@<version>/...` plus a generated `import-map.json` mapping bare specifiers to those paths. Resolution runs once in `cs-control` at publish time, never at invoke time.
- Update `cmd/cs-control` publish handler so the canonical bundle the control plane stores includes `function.js`, `manifest.json`, `import-map.json`, and the `deps/**` tree; the same `BuildCanonical` -> `sha256` flow is reused so the no-build-step invariant holds (agent ships only text + manifest).
- Update `cmd/cs-invoker-pool` and the cs-js loader to read `import-map.json` from the extracted bundle and wire it into the isolate's module resolver, so bare specifiers from `function.js` resolve to the frozen `deps/**` files without any network access at invoke time.
- Add a `cs-control` config block for the curated mirror (URL, auth, allowed registries) and a cache layer keyed by `<name>@<version>+integrity` so a second publish of the same dep set is cheap.

## Acceptance criteria
- [ ] Manifest schema accepts a `dependencies` map with version and SRI integrity; control plane rejects unknown registries and missing integrity fields with a typed error (`CS_BUNDLE_DEP_INVALID`).
- [ ] Publish flow resolves declared deps from a configured mirror, verifies SRI, and embeds them plus a generated `import-map.json` into the canonical tar bundle; resulting `sha256` is deterministic for the same input.
- [ ] Invoker pool loads the bundle, applies the import map to the isolate, and `function.js` can `import { z } from "zod"` against the frozen copy with zero network egress; an attempt to import an undeclared specifier fails with `CS_BUNDLE_DEP_MISSING`.
- [ ] No build step is required on the agent side: the agent uploads UTF-8 source + manifest only; all resolution happens server-side in `cs-control`.
- [ ] `docs/08-runtime-cs-js.md` documents the new manifest field, `docs/04-api-rest.md` notes that publish may take longer when deps are declared, and `docs/20-config-reference.md` documents the curated-mirror config.
- [ ] Unit tests in `internal/bundle` cover resolver success, SRI mismatch, registry-not-allowed, and import-map generation; integration test publishes a function with one dep and invokes it end-to-end.

## Dependencies & risks
- Prereqs: a curated JS mirror (npm-compatible, read-only) reachable from `cs-control`; ops decision on which registries are allowed.
- Externals: SRI hash format alignment with the eventual SBOM task (E5.03) so both consume the same integrity field.
- Risk: dep resolution latency on publish blows past current control-plane SLOs. Mitigation: aggressive content-addressed cache + parallel fetch with a hard budget; surface resolution time in the publish response.
- Risk: transitive dep explosion past the 16 MiB bundle limit. Mitigation: hard ceiling per bundle with a clear error, plus a `--dry-run` resolve endpoint so the agent can see size before publishing.
- Risk: import-map semantics drift between the cs-js runtime and the CLI runner. Mitigation: share the import-map applier between `cs` CLI and `cs-invoker-pool` via a common package.

## Out of scope
- Native (C/C++) addons or anything that requires compilation at the tenant side.
- Arbitrary npm registry passthrough; only curated mirrors are in scope.
- Python or WASM dependency resolution (covered by separate runtime epics).
- Dep version ranges / semver resolution; this task only accepts pinned versions with integrity.
