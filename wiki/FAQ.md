# Frequently Asked Questions

This page collects the questions most often asked about Sous: what it is,
how it relates to other function platforms, which runtimes it supports,
what guarantees the supply chain provides, and how the moving parts of the
system behave under failure. Each answer is written for readers who have
at least skimmed the [Home](Home) page; deeper material is linked inline.

The answers below describe the system as implemented in the main
repository. Where a capability is on the roadmap rather than in the
current build, the answer says so in prose. Roadmap context is tracked in
[Reference Roadmap](Reference-Roadmap).

## What is Sous?

Sous is a function-execution platform for agent-built automation. It
accepts small pieces of code as UTF-8 text, stores them addressed by
content hash, and runs them under guarded language runtimes when an HTTP
request, a scheduled tick, or a Cadence activity task arrives. The
control plane is `cs-control`; the execution plane is `cs-invoker-pool`;
the data plane is `cs-http-gateway` for synchronous HTTP, `cs-scheduler`
for time-based triggers, and `cs-cadence-poller` for Cadence-driven
invocations. A single CLI, `cs`, drives all of these surfaces locally
and remotely.

The shape of the platform is borrowed from Apache OpenWhisk: control-plane
resources for packages, functions, versions, aliases, and triggers; a
topic-based data plane that decouples request producers from invokers;
and an explicit capability model that defines what user code may do once
it is running. The opinionated additions on top of that shape are
runtime parity (a single test harness governs all three runtimes),
text-first packaging (no build step, no container image), and an
explicit role-and-capability privilege model that is locked in at
version creation time and cannot drift under the feet of a running
deployment.

Sous is intended to be authored by agents as well as humans. The CLI,
REST API, and SDKs are designed to be self-describing enough for a model
to generate a function, run it locally, publish it, and reason about its
trigger surface without out-of-band knowledge. The platform's
contract-first stance — capability declarations, signed bundles, ledger
trails — is what makes that delegation safe.

## How does Sous compare to OpenWhisk and AWS Lambda?

Sous shares OpenWhisk's vocabulary and most of its mechanical decisions.
Both treat a function as a versioned artifact with a manifest, both route
invocations through a topic that connects gateways to a pool of invokers,
and both expose the resulting activations as queryable records. The main
practical differences are that Sous packages are text-only — there is no
Docker-based action runtime — and that Sous's capability model is more
granular: a function declares the specific side-effects it intends to
perform (KV reads, KV writes, codeQ publish, HTTP fetch with an
allowlisted hostname, ledgerDB append), and the runtime refuses any
operation outside that declaration.

The comparison with AWS Lambda is more about deployment model than
runtime. Lambda functions are bundled into zip files or container images,
deployed through a build pipeline, and invoked through a tightly coupled
set of AWS event sources. Sous functions are bundles of source files plus
a manifest, deployed by uploading those files, and invoked through HTTP,
schedules, Cadence, or topic subscriptions; the trigger surface is
portable and not tied to any single cloud provider. Sous also exposes the
activation history as a first-class resource rather than as a side-effect
of CloudWatch logs.

The biggest behavioral overlap with Lambda is the activation contract:
input and output are JSON, handlers are async, cold starts are bounded by
the runtime's warm-pool policy, and timeouts are enforced from outside
the function. Where Lambda offers provisioned concurrency, Sous offers
warm-pool sizing in `cs-invoker-pool` configuration; the operator-facing
knobs are documented under
[Operators: Capacity and Limits](Operators-Capacity-and-Limits).

## Which runtimes are supported?

Three runtimes are supported in the main build. `cs-js` is a JavaScript
runtime built on `goja`, which executes ES2015+ code inside the invoker
process without spawning a separate interpreter. It is the default
runtime and the reference implementation for the capability contract.
`cs-python` is a subprocess-based Python runtime: the invoker spawns a
Python interpreter, hands it the bundle and the request, and reads back
a JSON response over a controlled IPC channel. `cs-wasm` is a
WebAssembly runtime built on `wazero`; it accepts modules compiled from
any language that targets WASI and offers the strongest isolation of the
three.

All three runtimes implement the same handler contract — a single async
function that takes a request object and returns a response object — and
the same capability surface. A shared parity harness runs an identical
battery of tests against every runtime so that error mapping, capability
enforcement, and timeout behavior cannot diverge. See
[Creating Functions: JavaScript](Creating-Functions-JavaScript),
[Creating Functions: Python](Creating-Functions-Python), and
[Creating Functions: WebAssembly](Creating-Functions-WebAssembly) for
the per-runtime specifics, and
[Development Tools: Parity Harness](Development-Tools-Parity-Harness)
for how parity is enforced.

The roadmap considers additional runtimes (notably a JVM target for
languages like Kotlin and Scala) as future work. The procedure for
adding a runtime — registering a name, implementing the runtime
interface, wiring up capability enforcement, and extending the parity
harness — is documented under
[Contributors: Adding a Runtime](Contributors-Adding-a-Runtime).

## Does Sous support child workflows or signals?

Cadence integration in the MVP is intentionally scoped to activity
execution and decision-task scheduling. `cs-cadence-poller` polls a
Cadence service for activity tasks, hands each task off to
`cs-invoker-pool` as a normal `InvocationRequest`, and reports the
result back. A decision-task path also exists for workflows that
orchestrate activities; the MVP shipped under E8.01 implements scheduling
of activities from a workflow function, with the workflow's deterministic
replay validated by the determinism linter described in
[Development Tools: Determinism Linter](Development-Tools-Determinism-Linter).

What is not in the MVP is the rest of Cadence's surface area. Child
workflow invocation, external signals, queries, continue-as-new, and
cancellation propagation are deferred. A workflow that needs any of
those primitives can still be modeled as an activity-only sequence
today; the missing primitives are tracked under the E8 epic in
[Reference Roadmap](Reference-Roadmap).

The reason for this scope is the same as the reason Sous holds the line
on capability declarations: every new workflow primitive widens the
contract that an agent has to reason about, and a narrower MVP gives the
parity harness and the determinism linter a smaller surface to cover.
The integration is designed so that adding signals or child workflows is
additive on the existing decision-task path rather than a redesign.

## How is the supply chain hardened?

Every published version is a signed bundle. The control plane requires
that the upload include an `ed25519` signature over the canonical bundle
bytes, produced by a key registered to the tenant, and a CycloneDX SBOM
that enumerates every file and dependency in the bundle. The signature
is validated before the version is admitted; an unsigned upload, a
signature produced by a key the tenant has not registered, or a bundle
whose hash does not match the SBOM is rejected and the rejection is
appended to ledgerDB.

At invocation time, the invoker re-validates the bundle hash against the
signed manifest before loading the runtime. This catches storage
corruption and tampering between publish and execute. The capability
model layers on top: even if a signed bundle is loaded, the runtime
refuses to execute any capability the manifest did not declare. The two
checks compose into a defense-in-depth posture in which neither the
identity of the signer nor the integrity of the bytes nor the
authorization of the operation can be skipped.

The implementation lives under E5 (Packaging, dependencies and supply
chain) on the roadmap. The signing primitives shipped under E5.02 use a
tenant-scoped keyring; the SBOM enforcement and dependency manifest
validation are in the same epic. Operators who need a deeper walkthrough
of the threat model and the verification path should read
[Operators: Security](Operators-Security) and
[Managing Functions: Signing and SBOM](Managing-Functions-Signing-and-SBOM).

## Where do I file issues?

Issues, feature requests, and roadmap discussion happen in the GitHub
repository at
[github.com/osvaldoandrade/sous](https://github.com/osvaldoandrade/sous).
The roadmap epics are tracked as GitHub issues with the `roadmap` label;
individual tasks carry the `roadmap` and `enhancement` labels and
reference their parent epic in the body. Bug reports and small
enhancements should be filed without the `roadmap` label; if a request
grows into something epic-sized, it is promoted to the roadmap at
planning time.

For security-sensitive reports — vulnerabilities, supply-chain concerns,
capability bypass — the project prefers a private channel rather than a
public issue. The procedure and the current contact path are described
in [Operators: Security](Operators-Security). Public security advisories
are published through GitHub's advisory mechanism on the same
repository.

Documentation issues (errors, omissions, requests for new sample
applications) are filed in the same repository and tagged `docs`. The
source for these wiki pages lives under `wiki/` in the main repo;
editing notes for contributors are in
[Contributors: Resources](Contributors-Resources).

## Can I bring my own auth, secrets, or queue providers?

Yes. The integration points are pluggable drivers, configured under the
`plugins` section of `config.yaml`. The current drivers and their
defaults are documented in `config.example.yaml` and in
[Operators: Configuration Reference](Operators-Configuration-Reference):
`plugins.authn.driver` defaults to `tikti`,
`plugins.persistence.driver` defaults to `kvrocks`, and
`plugins.messaging.driver` defaults to `codeq` (the project's
Kafka-compatible bus). Each driver implements a small interface;
alternative implementations can be wired in by registering a new driver
name and pointing the config at it.

The same pattern extends to secrets and the audit ledger. Secrets are
read through a driver that today targets a Vault-style backend, described
in [Enabled Services: Vault Secrets](Enabled-Services-Vault-Secrets). The
audit ledger is fronted by a driver that today targets ledgerDB,
described in
[Enabled Services: ledgerDB Audit](Enabled-Services-ledgerDB-Audit).
Either can be replaced by an implementation that matches its interface,
and the choice is reflected in the manifest capabilities that user
functions are allowed to use.

Pluggability is a deliberate choice for self-hosting. Sous does not bake
in a single vendor's identity or storage; what it does bake in is the
contract — what an authn driver must answer, what a persistence driver
must guarantee for idempotency, what a messaging driver must promise
about ordering — so that a swap is a mechanical exercise rather than a
semantic one. The contract documents are listed under
[Contributors: Resources](Contributors-Resources).

## Is there a managed offering?

No. Sous is self-hosted. The repository ships everything required to run
the platform — Helm charts under `deploy/` for Kubernetes, a
`docker-compose.yml` for local and small-footprint installations, and
the CLI for both local development and remote operation. Operators are
responsible for provisioning KVRocks, a codeQ-compatible bus, a Cadence
service (if Cadence workflows are in scope), and the identity provider
behind Tikti.

The absence of a managed offering is intentional. The platform is
positioned for organizations that want their function-execution layer
under their own control plane, frequently because the functions interact
with internal systems that should not be exposed to a SaaS provider. The
deployment guides under
[Operators: Deployment Kubernetes](Operators-Deployment-Kubernetes) and
[Operators: Deployment Docker Compose](Operators-Deployment-Docker-Compose)
cover the supported topologies, and
[Operators: Capacity and Limits](Operators-Capacity-and-Limits)
documents the sizing knobs.

If a managed offering is later built, it will be developed by a separate
vendor and announced through the repository. The roadmap explicitly
excludes billing, metering, quotas-as-product, and multi-region
active/active control planes from the open-source project; those belong
to a commercial layer if and when one exists.

## How are activations retried?

Retry behavior depends on the trigger family. HTTP invocations through
`cs-http-gateway` are not retried by the platform for synchronous calls
— a 5xx response is returned to the caller, who is expected to retry.
Asynchronous HTTP invocations land on the `cs.invoke` topic and inherit
the same retry policy as scheduled and Cadence-driven invocations: on a
transient failure, the invoker republishes the request with an
incremented attempt counter, up to the limit set in the function
manifest. Once the limit is exhausted, the message is moved to a
dead-letter topic under the `cs.dlq.*` namespace and an activation
record is written with a terminal failure status.

Scheduled invocations from `cs-scheduler` use the same retry path. The
scheduler emits the request, the invoker consumes it, and the retry
counter is carried in the message envelope so that retries are visible
across the entire data plane. Cadence-driven activities use Cadence's
own retry semantics: the activity task remains owned by the Cadence
service, which decides when to re-dispatch it based on the activity's
retry policy. `cs-cadence-poller` is a translator, not a retry engine,
for that path.

Idempotency is the user function's responsibility. The platform provides
a stable activation ID per attempt and a request ID that is constant
across attempts so that downstream systems can deduplicate. The
capability model gives functions the primitives they need — conditional
KV writes, ledgerDB-keyed appends, codeQ publish-with-key — to be safely
retried. See
[Operators: Capacity and Limits](Operators-Capacity-and-Limits) for the
configurable retry bounds and
[Operators: Runbooks](Operators-Runbooks) for how to drain a poisoned
DLQ.

## What is codeQ?

codeQ is the Kafka-compatible event bus that ties the Sous data plane
together. Function invocations flow through `cs.invoke`, results flow
through `cs.results`, and exhausted retries flow through one of the
`cs.dlq.*` topics. The bus is what decouples the request producers
(`cs-http-gateway`, `cs-scheduler`, `cs-cadence-poller`) from the
invoker pool, and it is also what lets user functions emit events for
downstream consumers under the `codeq.publish` capability.

The wire format is documented under `spec/cs.invoke.v1.json` and the
related schemas; the operator-facing protocol is described in
[Event Sources: codeQ Topics](Event-Sources-codeQ-Topics) and
[Enabled Services: codeQ](Enabled-Services-codeQ). Any Kafka-compatible
broker that honors the topic, partition, and offset semantics codeQ
relies on can stand in for the reference deployment — the project's
`docker-compose.yml` uses Redpanda for local development.

User functions can subscribe to codeQ topics through the upcoming
event-trigger surface (E4 on the roadmap). Until that lands, codeQ is
best thought of as the platform's internal nervous system: required for
the platform to function, available to user functions as an outbound
publish target, and queryable through the same admin tooling that
inspects invocation history.

## How is local development kept in parity with the cluster?

The `cs` CLI links against the same runtime libraries that
`cs-invoker-pool` uses, so `cs fn test --path mine` exercises the
function under the same `goja` (or Python, or `wazero`) host that the
cluster will use. Capability enforcement, host APIs, timeout handling,
and error mapping are identical because they are the same code. The
parity harness under
[Development Tools: Parity Harness](Development-Tools-Parity-Harness)
maintains a battery of cross-runtime tests that the CI pipeline runs on
every change; a behavior that differs between local and cluster is
treated as a bug.

The deliberate parity has two corollaries. The first is that local
development does not need a Docker stack to test basic behavior — a
single CLI command suffices. The second is that anything a function can
do in the cluster, it can do locally if the operator wires up the right
back-ends. The `docker-compose.yml` at the repository root brings up
KVRocks and a Kafka-compatible bus for tests that exercise persistence
and messaging without spinning up a full cluster.

The boundary of the parity guarantee is the runtime contract itself:
handler shape, capability surface, error codes, and host APIs. It does
not extend to operational characteristics — cold-start time, concurrency
limits, observability fan-out — which are tuned on a per-deployment
basis. The local CLI is designed for fast iteration, not for
production-equivalent load.

## How are versions, aliases, and packages organized?

A package is a namespace for related functions; a function is a named
entity within a package; a version is an immutable, signed bundle
attached to a function; and an alias is a mutable pointer from a stable
name to a version. Production traffic typically targets aliases rather
than raw versions because changing an alias is an atomic, auditable
operation that does not modify history. The model is described in
detail under
[Managing Functions: Packages](Managing-Functions-Packages) and
[Managing Functions: Versioning and Aliases](Managing-Functions-Versioning-and-Aliases).

Versions are addressed by a monotonically increasing integer per
function and also by their bundle hash; either form is accepted by the
invoker. Once a version is published, neither its code nor its manifest
can change — a new version must be published instead. Aliases can be
moved between versions freely, and the move is recorded in ledgerDB so
that the alias's history is recoverable.

Promotion workflows — for example, promoting a function from `dev` to
`staging` to `prod` — are expressed as alias moves. The
[Promote Through Aliases](Tutorial-Promote-Through-Aliases) tutorial
walks through the recommended pattern, including how to gate promotion
behind the role allowlist and how to use ledgerDB queries to
reconstruct who promoted what and when.
