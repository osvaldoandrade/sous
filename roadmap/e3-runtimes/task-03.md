# Manifest changes for runtime selection at publish time

**Parent epic:** #22
**Phase:** Next
**Estimated size:** M

## Problem
The current `manifest.json` schema (`cs.function.script.v1`) hard-codes `"runtime": "cs-js"` and `"entry": "function.js"` (see `docs/08-runtime-cs-js.md`). To unlock the Python and WASM adapters, the manifest must declare which runtime to boot, which interpreter version to pin, and which capabilities the function needs — without breaking the thousands of `cs-js`-only manifests already published.

## Proposed solution
- Bump the manifest schema to `cs.function.script.v2` in `internal/api/types.go` and the validator in `internal/api/http.go`; add a `runtime` enum (`cs-js` | `cs-python` | `cs-wasm`) and a `runtimeVersion` string (e.g. `"node20-goja"`, `"python3.12"`, `"wasi-preview1"`).
- Extend `capabilities` to include runtime-specific knobs (`kv`, `codeq`, `http` stay; add optional `wasm.maxMemoryPages`, `python.allowImports`); `cs-control` rejects unknown keys.
- Add a publish-time migration: any inbound v1 manifest is auto-upgraded to v2 with `runtime: "cs-js"` and `runtimeVersion: "node20-goja"`; document the upgrade in `docs/23-migrations.md`.
- Update the publish endpoint (`POST /v1/.../versions`) and draft validator in `cmd/cs-control` to route by `runtime`, and update `cmd/cs-invoker-pool` so the dispatcher selects the right adapter from Task E3.01 / E3.02.
- Update `cs` CLI (`cmd/cs-cli`) to accept `--runtime` on `cs publish` and on `cs init <template>`, scaffolding the right `function.{js,py,wasm}` skeleton.

## Acceptance criteria
- [ ] `manifest.json` accepts `"runtime": "cs-python"` and `"runtime": "cs-wasm"`; rejects unknown values with a typed `cserrors.Code` (`manifest/unknown_runtime`).
- [ ] Existing v1 manifests continue to publish unchanged and are stored canonically as v2; round-trip tested in `internal/api/types_http_test.go`.
- [ ] `docs/08-runtime-cs-js.md` is updated to reference the new schema, `docs/04-api-rest.md` shows the new field in the publish payload example, and `docs/23-migrations.md` describes the v1→v2 upgrade.
- [ ] `cs init --runtime python` scaffolds a `function.py` + v2 manifest; `cs publish` rejects mismatched `runtime` vs. file extension with a clear error.
- [ ] `cs-invoker-pool` dispatches to the adapter named by `manifest.runtime`; an integration test exercises all three runtimes end-to-end.
- [ ] Capability validation is shared across runtimes (KV prefixes, codeQ topics, HTTP host allowlist) per `docs/15-security.md`.

## Dependencies & risks
- Soft prerequisite for Task E3.01 and Task E3.02 (the adapter dispatch needs this field) and a hard prerequisite for Task E3.04 (the parity harness loops over `runtime` values).
- Schema-version drift between CLI and server; mitigation: CLI sends both `schema` and `runtime`, server validates both and returns `400` with a remediation hint on mismatch.
- Capability surface creep; mitigation: the v2 schema is closed (additionalProperties: false) and changes require an ADR.

## Out of scope
- Multi-runtime bundles (a single bundle declaring multiple `runtime` targets).
- Inline dependency manifests (`requirements.txt`, `package.json`) — covered by the future "Packaging" epic in `docs/18-roadmap.md`.
- Signed manifests with tenant keys — also tracked under "Packaging".
