# Managing Functions: Signing and SBOM

This document defines the supply-chain story for published function
versions. The platform ships two complementary primitives: an Ed25519
publish-time signature that binds the canonical bundle hash to the
tenant that produced it (E5.02), and a CycloneDX 1.5 Software Bill of
Materials emitted next to every published version (E5.03). Together
they answer two regulator-friendly questions: "did the right party
produce these bytes?" and "what is inside them?".

## Supply-chain story in two acts

At publish time, a tenant agent (a developer's CLI or a CI runner)
computes the canonical bundle's sha256 — see
[Managing Functions: Packages](Managing-Functions-Packages) — and
signs that sha (plus the tenant/namespace/function tuple) with the
tenant's Ed25519 private key. The signature travels in the
`X-CS-Signature` HTTP header on the publish request. `cs-control`
loads the tenant's active public key, verifies the signature, and
persists the result as part of the immutable `VersionRecord`. See
`cmd/cs-control/signing_keys.go` and `internal/signing/ed25519.go`.

At every cold load, the Invoker Pool re-verifies the persisted
signature against the tenant's current active public key before
booting the sandbox. A signature that no longer verifies (because the
key rotated, the record was tampered with, or the bytes were swapped
out) fails the activation with `CS_SIGNATURE_INVALID`. See
`cmd/cs-invoker-pool/signature.go`.

In parallel, `emitSBOMOnPublish` in `cmd/cs-control/sbom.go` builds a
CycloneDX 1.5 document inventorying the bundle's runtime, every file
inside the tar, and every import-map entry (when one is present), and
writes that document into KVRocks. Callers retrieve it later through
`GET /v1/.../versions/{version}/sbom`. The SBOM is part of the
version's permanent record; it is not regenerated on subsequent reads.

The two artifacts answer different audit questions and are kept on
separate evolution paths. Signing is the per-tenant identity contract;
SBOM is the per-version composition contract. Either can change
independently — a key rotation does not invalidate older SBOMs, and a
schema bump on the SBOM emitter does not invalidate older signatures.

## Signing key lifecycle

Tenants own their signing keys. The control plane stores only the
public half plus opaque metadata; the private half is generated, handed
back to the rotating caller exactly once, and immediately dropped from
control-plane memory.

### Generation

`POST /v1/tenants/{tenant}/signing-keys/rotate` generates a fresh
Ed25519 keypair via `signing.Generate` (`internal/signing/ed25519.go`).
The handler in `cmd/cs-control/signing_keys.go` writes the public-key
record under `kv.TenantSigningKeysKey(tenant)` and returns the private
half in the response body — the *only* time the private bytes ever
leave the control plane. Callers must persist the private bytes before
discarding the response; the control plane has no recovery path for a
lost private key.

The persisted `TenantSigningKey` record (see `internal/api/signing.go`)
captures `KID`, `Algorithm` (always `ed25519` in v0.1), `PublicKey`,
and `CreatedAtMS`. The `KID` is a short opaque identifier
(`kid_<12 hex chars>`) used by signature records to point back at the
key that produced them.

### Rotation

A second `rotate` call overwrites the active slot. There is one active
key per tenant at any moment; the control plane keeps no historical
key archive. The signature records on previously-published versions
still carry the old `KID`, but verification uses the current active
key — so a rotation effectively invalidates every version published
under the previous key.

This is a hard trade. The simpler model — single active key, no
archive — keeps the verification path tight: the invoker fetches one
key per tenant and re-verifies every cold load against it. The cost
is that a rotation is an opt-in re-publish event for every version a
tenant wants to keep verifiable. Operators who need long-lived
historical verification should keep rotations rare and announce them
ahead of time.

A future revision may introduce a small ring buffer of recently
retired public keys so that in-flight cold loads after a rotation
have a grace window. The current code path returns
`CS_SIGNATURE_INVALID` for any signature that does not verify against
the single active key.

### Revocation

There is no separate revoke endpoint in v0.1. A revoke is implemented
as a rotate: the operator generates a fresh keypair, persists the new
public key, and the platform refuses to verify any signature produced
under the old private key. The immediate side effect is that every
previously-signed version stops booting at the invoker until it is
re-published under the new key.

Tenants that need a softer revoke (e.g. revoke a CI signer but keep
production traffic running) should provision separate tenants for CI
identities, so the signing-key axis aligns with the trust axis. The
platform's tenancy model is the only revocation boundary v0.1
supports.

### Reading the active key

`GET /v1/tenants/{tenant}/signing-keys/active` returns the public-key
metadata (no private material). The handler (`getActiveSigningKey` in
`cmd/cs-control/signing_keys.go`) is read-only and accepts the
`cs:tenant:signing-key:read` action. External verification tools that
want to validate a Sous-produced signature offline pull the public key
from this endpoint and then run the same `signing.Verify` routine
locally.

## The signed-manifest envelope

The signature does not cover the manifest JSON literally. It covers a
canonical byte payload derived from the bundle hash plus the (tenant,
namespace, function) tuple. The exact layout lives in
`internal/signing/payload.go`:

```
+-------------------------------------------------------------+
| "cs.bundle.signature.v1\x00"           (23 bytes, magic)    |
| uint32_be(len(bundleSHA)) | bundleSHA  (sha256 raw bytes)   |
| uint32_be(len(tenant))    | tenant     (utf-8)              |
| uint32_be(len(namespace)) | namespace  (utf-8)              |
| uint32_be(len(function))  | function   (utf-8)              |
| int64_be(version)                                           |
+-------------------------------------------------------------+
```

The `version` field is the constant `0` at publish time: the
publisher cannot know its monotonic version number in advance because
that is server-allocated, and the persisted `(sha256, tenant,
namespace, function)` tuple is itself unique once stored. The invoker
re-verifies against the same constant, so signatures round-trip
cleanly. See `publishSignedVersion` in
`cmd/cs-control/signing_keys.go` and `invokeSignedVersion` in
`cmd/cs-invoker-pool/signature.go`.

### What is signed

- The 23-byte magic prefix anchors the canonical layout. A future
  version of the format will use a new magic prefix; cross-version
  forgery is structurally impossible.
- The canonical bundle sha256 binds the signature to exactly the
  bytes that produced the hash. A tampered bundle produces a
  different hash and therefore a different canonical payload, which
  fails verification.
- The tenant, namespace, and function strings prevent
  cross-resource replay. A signature stamped for
  `t_acme/payments/reconcile` cannot be replayed against
  `t_acme/payments/refund` — the payload differs, so the signature
  does not verify.

### What is not signed

- The agent's wall-clock `SignedAt`. The `BundleSignature` record
  carries `signed_at_ms` for informational use, but the canonical
  payload omits it. Clock skew between a developer's laptop, a CI
  runner, and the control plane therefore never invalidates a
  signature.
- The monotonic `version` integer. As above, this is allocated
  server-side and signed as the constant `0`.
- The manifest JSON. The signature covers the canonical bundle's
  sha256, which already covers every byte of every file in the
  bundle — including `manifest.json`. Signing the manifest
  separately would either be redundant or would create a second
  attack surface (a signed manifest that disagreed with the signed
  bundle).

### Why detached

The signature is stored as a separate `BundleSignature` record
(`internal/api/signing.go`) rather than embedded in the bundle bytes.
Two consequences flow from that choice.

The canonical bundle hash is the publisher's responsibility, and the
signature is the platform's responsibility — they are produced and
verified by different code paths and travel on different wires.

A version can carry a signature, no signature (when
`plugins.signing.required` is false), or eventually a future
signature algorithm without changing the on-disk bundle layout.
Backward compatibility is a metadata problem, not a tar-layout
problem.

## Verification semantics at cold load

The Invoker Pool re-verifies every signed version at cold load:

1. `verifyInvokeSignature` (`cmd/cs-invoker-pool/signature.go`) is
   called from the dispatcher's bundle-fetch path
   (`cmd/cs-invoker-pool/main.go`).
2. If `meta.Signature == nil`, the function returns immediately. The
   legacy / opted-out case is "skip re-verification"; the invoker
   relies on the publish-time check.
3. Otherwise the helper asserts that `meta.Signature.Algorithm`
   equals `signing.Algorithm` (`ed25519`). An unknown algorithm
   fails the cold load with `CS_SIGNATURE_INVALID`.
4. The helper loads the tenant's active public key via
   `signing.LoadActiveTenantKey`. A missing key fails with
   `CS_SIGNATURE_INVALID` ("this signed bundle cannot be trusted
   right now").
5. The canonical payload is recomputed from the persisted sha256 and
   the request tuple.
6. `signing.Verify` runs the Ed25519 verification. A `false`
   return fails the cold load with `CS_SIGNATURE_INVALID`.

Verification cost is roughly 50 microseconds on commodity hardware,
well below the cold-load floor dominated by the sandbox boot. The
re-verification on every cold load (not just on the first one) is
deliberate: a bundle cache eviction followed by a refetch is the only
moment we get a fresh authority check, and a key rotation between
cold loads must take effect immediately.

Subsequent warm invocations against the same cached bundle skip the
signature check; they rely on the Invoker Pool's bundle-cache identity
(sha-keyed) to ensure the bytes have not changed since the last
verification. See [Invoker Pool](Invoker-Pool) for the cache shape.

## SBOM format

Sous emits CycloneDX 1.5 JSON. The choice is pinned to CycloneDX
(not SPDX) because it has stronger first-class file-component
support, which lets the platform list every file inside the bundle
without straining the schema. The version is pinned to 1.5 because
it is widely supported by downstream scanners and is the version
called out in the E5.03 ticket. See
`internal/sbom/cyclonedx.go` (`SpecVersion = "1.5"`).

### Document shape

`internal/sbom/cyclonedx.go` builds the document by hand on top of
`encoding/json`. Determinism is the load-bearing property:

- Every component slice is sorted by `bom-ref` before encoding.
- Every map keyset is alphabetised.
- The document `serialNumber` is derived from the bundle's sha256, so
  replaying a publish for the same canonical bundle yields a
  byte-identical SBOM document.

The document lists, at minimum:

- The runtime declared by the manifest (e.g. `cs-js`) as an
  `application` or `framework` component.
- One CycloneDX `file` component per bundled file, each with its own
  sha256 hash.

The SBOM schema (`internal/sbom/cyclonedx.go`) also reserves two
optional slots: an `Imports` slice that becomes one CycloneDX
`library` per import-map entry (E5.01), and a `SigningIdentity`
record — KID, algorithm, and a fingerprint of the active public
key — that downstream tooling can render. The current publish path
(`emitSBOMOnPublish` in `cmd/cs-control/sbom.go`) populates the
file and runtime components from the bundle and leaves both
optional slots empty in v0.1; a future revision will weave the
import-map entries and the BundleSignature record into the same
SBOM document so the "what is inside" and "who signed it" answers
travel together.

The `ContentType` constant
(`application/vnd.cyclonedx+json; version=1.5`) is the IANA-registered
media type the SBOM endpoint uses on its response, so downstream
scanners can stream the body straight into a CycloneDX parser.

### Where it is attached

`emitSBOMOnPublish` in `cmd/cs-control/sbom.go` runs inside the
publish transaction: a publish whose SBOM emission fails is rolled
back as a whole. The platform refuses to silently store an unverified
or un-SBOMed version, because regulated tenants would rather see a
publish fail than discover a gap during an audit.

The SBOM is persisted under the version key in KVRocks
(`store.PutSBOM`). It is read by `store.GetSBOM` from the read
endpoint. There is no separate cache; the SBOM is small enough that
KVRocks itself is fast enough.

### The GET /sbom endpoint

The full URL is:

```
GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions/{version}/sbom
```

The handler `getVersionSBOM` (`cmd/cs-control/sbom.go`) requires the
`cs:function:read` action (the same role check that gates
`GET .../versions/{version}`). The response body is the raw CycloneDX
JSON document with the IANA content type, so a downstream tool such as
Grype, Trivy, or a custom regulator script can curl the URL and pipe
the bytes straight into its parser.

A missing version returns 404 `CS_VALIDATION_FAILED` rather than a
synthesised empty body. The distinction matters: "this version
predates SBOM emission" (re-publish to backfill) versus "this version
does not exist" (the caller has the wrong ref) are two different
audit conclusions.

## Threat model

The signing and SBOM machinery defends against four specific failure
modes.

### Forged manifests

Without the signature gate, an attacker who breached the publish path
could write a manifest declaring elevated capabilities (a wider HTTP
allowlist, broader KV prefixes) for a function that has none.

The signature gate blocks this because the canonical bundle sha256
covers `manifest.json` byte-for-byte. An attacker who tampered with
the manifest produces a new sha256, which produces a new canonical
signing payload, which the tenant's private key did not sign.
Verification fails at publish (`verifyPublishSignature`) or at cold
load (`verifyInvokeSignature`); either way the bytes never run.

### Key compromise

If a tenant's private key leaks, the attacker can sign arbitrary
bundles against the tenant's namespace. The defensive move is a
key rotation: a fresh `rotate` call replaces the active public key,
the leaked private key stops producing verifiable signatures, and
every version published under the compromised key stops booting at
the invoker. The trade is that legitimately-published versions also
stop verifying — re-publish is the cost of revocation.

This is a load-bearing tradeoff. The defender wants two things — fast
recovery and continuity — and v0.1 trades continuity for fast
recovery. The future ring-buffer of retired keys mentioned in the
rotation section walks that back, at the cost of the verifier needing
to consult multiple keys per cold load.

### Rollback attacks

A rollback attack swaps a current bundle for an older, vulnerable
bundle that the same key once signed. Sous mitigates this in two
ways. The version number is monotonic and operator-visible, so a
suspicious downgrade is detectable from the audit ledger. The
content-address (sha256) of the deployed version is on every
activation log, so a forensic comparison against the supposed-current
version is a single grep.

The platform does not currently include the version number in the
canonical signing payload (it signs `version=0`), which means a
signature is technically replayable across versions of the same
function — the same tenant could re-use a signature it produced
yesterday to publish the same bytes today and the platform would
accept it. The cost of this is small because the bytes are unchanged
and the bundle hash is unchanged; the audit ledger still records two
distinct publish events. A future revision that signs the real
version number would close this completely.

### Tampered storage

If KVRocks were compromised, an attacker could attempt to swap a
version's bundle bytes underneath its metadata record. The
publish-time sha256 stored on the `VersionRecord` defeats this: the
invoker recomputes the bundle's sha256 on cold load and rejects any
mismatch with `CS_SIGNATURE_INVALID` (the same path that catches
signature failures). The signature catch is downstream of the
sha-recomputation catch, so a tampered-storage event surfaces with
the same operator-visible error class.

## Configuration

Two settings govern the signing posture:

- `plugins.signing.required` — when true, publishes without
  `X-CS-Signature` fail with `CS_SIGNATURE_MISSING`. When false
  (legacy), publishes without a signature succeed and the version is
  marked `Signature == nil`. See `cmd/cs-control/signing_keys.go`.
- The tenant must call `rotate` at least once before its first
  publish if `required=true`; otherwise `CS_SIGNATURE_KEY_NOT_FOUND`
  fires at verification.

Operators rolling out signing for the first time should set
`required=false`, encourage tenants to rotate keys and start signing,
then flip the flag to `true` once telemetry shows every active tenant
has published at least one signed version.

The SBOM path has no opt-out. Every publish writes a CycloneDX
document, even for unsigned versions. The document is part of the
permanent record alongside the `VersionRecord`.

## Cross-references

- [Operators: Security](Operators-Security) for the full security
  surface — request validation, rate limiting, secrets, and the
  egress allowlist that complement the signing story. The current
  wiki publishes this material under [Security](Security); the
  bare-name link will start resolving when the renamed page lands.
- [Reference: Schemas](Reference-Schemas) for the JSON Schema
  definitions of the `BundleSignature` and `TenantSigningKey`
  records, and the CycloneDX content type. Until that page lands the
  current location is [Schemas](Schemas).
- [Managing Functions: Versioning and Aliases](Managing-Functions-Versioning-and-Aliases)
  for how a signed version interacts with the immutable-version,
  mutable-alias model.
- [Managing Functions: Packages](Managing-Functions-Packages) for
  the canonical bundle sha256 that the signature binds to.
- [Invoker Pool](Invoker-Pool) for the cold-load path that runs the
  signature re-verification.
- [ledgerDB Audit](ledgerDB-Audit) for the audit envelope that
  captures every rotate, publish, and alias move.
