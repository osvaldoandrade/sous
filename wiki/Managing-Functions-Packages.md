# Managing Functions: Packages

This document defines how Sous accepts function code, how that code is
packaged into a bundle, and how the platform anchors a bundle's identity
without ever running a build.

## The "no build step" invariant

Sous accepts function code as UTF-8 text plus a JSON manifest. The
control plane never compiles, transpiles, links, or installs
dependencies at publish time. A publish is a write of bytes the
publisher already produced — the platform validates, hashes, and stores
them, and that is the entire publish hot path.

This invariant is load-bearing for three reasons.

The platform stays small. The control plane does not need a Node
toolchain, a Python interpreter with `pip`, or a Wasm linker installed
on the host. Operators do not have to keep those toolchains current,
and the control-plane container image does not need to ship them. The
attack surface stays narrow: there is no build step to compromise.

Publishes are deterministic. The bytes the publisher signs are the
bytes the invoker loads. There is no "the build server produced
different output than my laptop" failure mode, because there is no
build server. The canonical bundle hash on the publisher's machine and
the canonical bundle hash recorded on the `VersionRecord` are
byte-identical by construction. See `internal/bundle/bundle.go` for the
canonicalisation routine.

Cold loads are fast. A cold load is a `GET` from KVRocks plus a tar
read plus a sandbox boot. There is no `npm install`, no `pip install`,
no `cargo build`. The cold-load latency budget is dominated by the
sandbox itself, not by package fetching.

The cost of this invariant is shifted to the developer. Producing
executable artifacts — bundling JavaScript, vendoring Python wheels,
compiling Wasm modules — happens on the developer's machine or in
their CI pipeline before the publish call. Sous gives developers a
CLI ([CLI](CLI)) and a manifest schema ([Schemas](Schemas)) to make
this straightforward, but it does not own the build.

## Bundle layout

Every Sous bundle is a canonical tar archive containing two
mandatory files at its root:

- `manifest.json` — the manifest declared by the [Schemas](Schemas)
  page, runtime-specific limits and capabilities included.
- A runtime entry file — `function.js` for cs-js, `function.py` for
  cs-python, `module.wasm` for cs-wasm.

The control plane derives the entry-file name from `manifest.runtime`
in `internal/bundle/bundle.go`. A bundle whose manifest declares
`cs-python` but ships only `function.js` is rejected at publish time
with `CS_VALIDATION_FAILED`. A bundle that omits `manifest.json`
entirely is rejected with the same error.

Additional files sit alongside the entry file. Their paths are
arbitrary, modulo the platform's path-safety rules: no leading slash,
no `..` segments, no path-traversal sequences. The tar writer in
`internal/bundle/bundle.go` rejects any file whose canonical path does
not equal its declared path.

Dependencies, when the runtime supports them, live in a reserved
subtree:

- For cs-js, the resolver writes frozen deps under `deps/` and a
  generated `import-map.json` at the bundle root. See
  `internal/bundle/imports.go`.
- For cs-python, vendored modules live wherever the publisher placed
  them inside the bundle; the runtime's import path searches the
  bundle root.
- For cs-wasm, dependencies are statically linked into the `.wasm`
  module at compile time; the bundle contains a single binary.

The tar archive is built deterministically: file entries are sorted by
path, headers carry zero timestamps, and the writer emits no padding
bytes beyond what the tar format requires. Two publishers feeding the
same logical files to the canonical builder produce byte-identical
archives, which produce byte-identical sha256 digests, which produce
the same publish-time signature payload. This determinism is what
makes the publish-time signature (see
[Managing Functions: Signing and SBOM](Managing-Functions-Signing-and-SBOM))
verifiable end-to-end.

## Dependencies per runtime

The three Sous runtimes take three different dependency stances. The
platform's job is the same in each case: accept bytes, hash them, never
fetch anything at invoke time. The shape of those bytes differs.

### cs-js: frozen import maps (E5.01)

The cs-js runtime is the only one with publish-time dependency
resolution. The publisher's manifest may declare an `imports` block
that maps a bare specifier (the symbol a developer writes in
`import { z } from "zod"`) to a source — either a URL into an
allowlisted mirror or a local path to a file already in the upload.

At publish time, `cs-control` walks the manifest's `imports` and
materialises each entry. Remote entries are fetched once over HTTPS
from the curated `allowed_mirrors` list configured in
`config.example.yaml` (`cs_control.publish.imports.allowed_mirrors`).
Local-path entries are copied directly out of the uploaded file set.
Each resolved blob is hashed; the publisher may optionally pre-declare
a Subresource Integrity digest, and a mismatch fails the publish with
`CS_VALIDATION_FAILED`. The resolver lives in
`internal/bundle/imports.go`.

The resolver writes two artifacts into the canonical bundle:

- A `deps/` subtree containing one frozen file per resolved import.
- An `import-map.json` at the bundle root, declaring the
  `bare-specifier → deps/<file>` mapping along with each entry's SRI
  digest and size. The on-disk schema is `cs.importmap.v1`.

At cold load, the cs-js runtime reads `import-map.json`, verifies each
declared digest against the bytes under `deps/`, and refuses to load
the bundle if any digest disagrees. No network call is made; the
runtime serves imports out of the bundle bytes alone. This is the
"frozen" in frozen import map: by publish completion, every dependency
the function can resolve at runtime is already inside the bundle.

The per-import size cap defaults to 4 MiB
(`cs_control.publish.imports.max_bytes_per_import`); the per-mirror
fetch timeout defaults to 10 s
(`cs_control.publish.imports.timeout_ms`). An empty
`allowed_mirrors` (the default) disables remote fetching entirely,
which means a tenant must pre-vendor every dep into the upload and
declare it as a `path:` import.

### cs-python: bundle-vendored dependencies

cs-python does not have an import-map mechanism. Python's import
system already understands files inside a directory: if a publisher
ships a `vendor/requests/` tree alongside `function.py`, the runtime
can `import requests` against those files because the bundle root is
the first entry on `sys.path`.

There is therefore no `pip install` at publish time and none at invoke
time. The platform expects the publisher (or their CI) to have run
`pip wheel`, `pip install --target`, or an equivalent step on a
build machine, captured the resulting files, and folded them into the
upload. The bundle on KVRocks contains those bytes verbatim.

This means the bundle-size cap (see below) applies more aggressively
to cs-python: a function that imports a 12 MiB scientific package eats
into the 16 MiB budget before its own source code is counted. Sous
takes this trade deliberately — vendoring is the simplest contract
that does not require a package index inside the platform.

### cs-wasm: statically linked artifacts

cs-wasm bundles ship a single `module.wasm` produced by a Wasm
toolchain on the publisher's side. By the time the bytes reach the
platform, every dependency the module needs has been linked in:
Sous neither knows nor cares whether the publisher's `.wasm` came from
Rust, C, AssemblyScript, or anything else.

There is therefore no dependency declaration at all in the cs-wasm
manifest. The bundle is the dependency closure. The runtime imposes
the same bundle-size cap as the other runtimes, and the publish-time
hash covers the entire `.wasm` blob.

## Bundle size cap

The `cs_control.limits.max_bundle_bytes` setting in
`config.example.yaml` is the absolute upper bound on the canonical
bundle size. The default is `16777216` bytes (16 MiB). The cap counts
every byte in the canonical tar — manifest, entry file, every
dependency the publisher folded into the upload — not just the user's
source code.

The cap exists for three reasons.

KVRocks stores the bundle as a single value. An unbounded value size
would let one tenant evict everyone else's bundles from the cache. The
16 MiB ceiling caps memory amplification when a replica preloads
several thousand bundles into the [Invoker Pool](Invoker-Pool) cache.

Cold loads stay snappy. A 16 MiB tar deserialises in well under 100 ms
on commodity hardware. A 200 MiB tar does not.

Sandbox boot stays bounded. The cs-js isolate and the cs-python
subprocess each load the entire bundle into memory; a small cap keeps
the sandbox's resident-set ceiling predictable.

When a publish exceeds the cap, `cs-control` returns
`CS_VALIDATION_FAILED` with a message naming the byte count and the
limit. Tenants that genuinely need a larger budget should split their
function (often a sign that the work belongs in two functions chained
via codeQ), prune their vendored deps, or talk to operators about
raising the cap globally. The cap is operator-controlled, not
tenant-controlled.

## Publish-time hash

When a publish call reaches `cmd/cs-control/lifecycle.go`, the handler
in `main.go` rebuilds the canonical bundle from the draft files,
computes its sha256, and compares the result against the sha256 the
draft uploaded earlier. A mismatch fails the publish with
`CS_VALIDATION_FAILED` ("sha256 mismatch with draft"). A match makes
the canonical sha256 the bundle's permanent identity.

The sha256 is what every other piece of the platform refers to:

- The [Managing Functions: Signing and SBOM](Managing-Functions-Signing-and-SBOM)
  envelope binds the publisher's signature to this exact sha.
- The CycloneDX SBOM uses the sha to derive its `serialNumber`, so
  the SBOM bytes are also deterministic.
- The invoker pool keys its bundle cache by sha, so two versions that
  happen to ship identical bytes share a single cache entry across
  the cluster (the cache hit on the second invocation is "free").
- The activation log records the sha next to the version number, so
  post-incident triage can confirm exactly which bytes ran.

The publisher computes the same sha locally (the
`cs` CLI emits it) and uses it as the canonical signing input — see
`internal/signing/payload.go`. Determinism in `BuildCanonical` is what
makes the agent's sha and the control plane's sha agree without
explicit synchronisation. If they ever disagree, either the canonical
builder has drifted between client and server, or the draft was
tampered with in transit; both outcomes are publish-time errors.

The sha is also the unique key on the `VersionRecord`. The monotonic
integer version is allocated server-side and is the public, ordered
handle that tenants and aliases use to refer to a publish (see
[Managing Functions: Versioning and Aliases](Managing-Functions-Versioning-and-Aliases));
the sha is the immutable content-address that pins the bytes
themselves.

## Canonical bundle determinism

The `BuildCanonical` routine in `internal/bundle/bundle.go` is the
single source of truth for "what does this bundle look like on
disk". It accepts a `map[string][]byte` (paths to bytes), validates
each path against the path-safety rules above, sorts the keys, and
emits a tar archive with deterministic headers.

The headers carry zero timestamps, the user and group IDs are zero,
and the file mode is a fixed value across every entry. The tar
format's optional padding bytes are emitted exactly as the standard
library's `archive/tar` package writes them; nothing additional is
added. The output is identical down to the byte for the same input
map, and the sha256 of that output is the canonical bundle hash.

This routine is the contract between client and server. The `cs`
CLI links it as a Go package and runs it locally so the publisher's
sha256 computation matches the server's. Third-party clients that
want to reproduce the canonical sha (for offline signing) must
mirror its behaviour exactly: sorted paths, zeroed metadata, no
extended attributes, no PAX headers. Drift in any of these
guarantees breaks the signing handshake silently — the bundle would
publish, but the signature would not verify.

If a future revision changes the canonical form (for example, to
support compressed tarballs or POSIX-extended headers), it does so
under a new schema constant on the manifest. Existing bundles keep
their old canonical form forever; new bundles use the new form. The
two never mix.

## Path-safety rules

The canonical builder rejects a bundle with any of the following
file-path properties:

- An absolute path (leading `/`).
- A path that traverses outside the bundle root (`..` segment).
- A path that does not equal `filepath.Clean` applied to itself —
  this catches redundant `./`, doubled `//`, and Windows-style
  separators on a cross-platform publish.
- A path that collides with a reserved name. The reserved set in
  v0.1 is just `manifest.json` and the runtime entry name; nothing
  inside the bundle may shadow them.
- For cs-js bundles, a path under the `deps/` prefix that conflicts
  with a frozen-import target. The import resolver in
  `internal/bundle/imports.go` detects collisions and fails the
  publish with `CS_VALIDATION_FAILED`.

The rules are enforced before any byte hits KVRocks. A rejected
bundle leaves no trace at the data layer beyond the draft record,
which will expire on its TTL.

## Manifest shape, briefly

The full manifest schema lives in [Schemas](Schemas). A condensed
summary for the package context:

- `schema` — frozen at `cs.function.script.v1` across every
  supported runtime. The runtime discriminator is `runtime`, not the
  schema name.
- `runtime` — one of `cs-js`, `cs-python`, `cs-wasm` (the runtime
  catalogue lives in `internal/api/types.go`).
- `entry` — the entry file path inside the bundle; defaults match
  `entryFileForRuntime` in `internal/bundle/bundle.go`.
- `handler` — the symbol invoked at runtime (`default` for cs-js).
- `limits` — `timeoutMs`, `memoryMb`, `maxConcurrency`.
- `capabilities` — the `kv`, `codeq`, `http` allowlists enforced
  inside the sandbox.
- `imports` — present only for cs-js; the bare-specifier map the
  resolver freezes at publish time.

`cs-control` parses the manifest twice. The first parse, at
`uploadDraft` time, is a schema check that surfaces malformed
manifests early. The second parse, at `publishVersion` time, runs the
resolver, the determinism linter (for Cadence workflows; see
[Cadence Integration](Cadence-Integration)), and the canonical
sha256 computation. Both parses produce the same JSON tree; the
second parse is the one that becomes part of the immutable version.

## Drafts as the upload step

A draft is the staging slot that holds a freshly uploaded file set
until the publisher is ready to commit. Drafts have TTLs
(`cs_control.limits.draft_ttl_seconds`, default 24 hours), are
identified by a `draft_id` opaque token, and carry the sha256 of the
canonical bundle the upload would produce.

Concurrent uploads against the same `(tenant, namespace, function)`
each receive a distinct `draft_id`. There is no "one draft per
function" rule, because the platform does not promise that drafts are
exclusive. The publisher chooses which `draft_id` to publish; the
others expire on their TTL. See `cmd/cs-control/lifecycle.go` for the
`createFunctionIdempotent` and `draftExpired` helpers.

The draft → version transition is irreversible. Once a draft is
published, `MarkDraftConsumed` sets `Consumed=true` on the record, and
any subsequent publish attempt against the same `draft_id` fails with
`CS_VALIDATION_FAILED` ("draft already consumed"). See
[Managing Functions: Versioning and Aliases](Managing-Functions-Versioning-and-Aliases)
for the full lifecycle and the FSM.

## Common publish failures

Every publish failure surfaces a typed `CS_*` code from
`internal/errors`. The most common failure modes:

- `CS_VALIDATION_FAILED` ("sha256 mismatch with draft") — the
  publisher's local sha disagrees with the server-recomputed sha.
  Usually means the canonical builder on the client side drifted
  from the platform version. Re-run with the latest `cs` CLI.
- `CS_VALIDATION_FAILED` ("draft expired") — the publisher waited
  longer than `draft_ttl_seconds` between upload and publish.
  Re-upload the draft.
- `CS_VALIDATION_FAILED` ("draft already consumed") — a second
  publish call against the same `draft_id`. Drafts are
  single-redeem; allocate a new one.
- `CS_VALIDATION_FAILED` ("bundle exceeds 16777216 bytes") — the
  upload tripped the bundle-size cap. Prune deps, split the
  function, or talk to operators about raising the cap.
- `CS_VALIDATION_FAILED` ("imports[...] : ...") — the cs-js
  resolver couldn't resolve an import. Common causes: an
  un-allowlisted mirror, an SRI mismatch, or a missing `path:`
  target.
- `CS_RUNTIME_UNSUPPORTED` — the manifest declares a runtime that
  the control plane has no adapter for. The runtime catalogue lives
  in `internal/api/types.go`; the registered runtimes are
  `cs-js`, `cs-python`, and `cs-wasm`.
- `CS_IDEMPOTENCY_CONFLICT` — a `POST /functions` call against an
  existing function with a different runtime, entry, or handler.
  Resolve the conflict explicitly (rename, delete, or amend); the
  platform does not silently rewire identities.

Every failure carries a `request_id` the audit ledger
([ledgerDB Audit](ledgerDB-Audit)) records, so triage can correlate
a client-side error to the server-side record without ambiguity.

## Cross-references

- [Managing Functions: Versioning and Aliases](Managing-Functions-Versioning-and-Aliases)
  for the immutable-version-mutable-alias model and the lifecycle FSM.
- [Managing Functions: Signing and SBOM](Managing-Functions-Signing-and-SBOM)
  for how the bundle's sha256 anchors the publish-time signature and
  the CycloneDX SBOM.
- [Runtime: cs-js](Runtime-cs-js) for the runtime-side contract the
  cs-js bundle targets — handler shape, host APIs, isolate limits.
- [Schemas](Schemas) for the full JSON Schema definitions of the
  manifest and the import map.
- [REST API](REST-API) and [CLI](CLI) for the wire-level publish flow.
- [Capacity and Limits](Capacity-and-Limits) for the bundle-size cap
  in the broader context of per-tenant limits.
