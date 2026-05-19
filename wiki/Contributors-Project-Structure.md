# Project structure

Sous lives in a single Git repository at `github.com/osvaldoandrade/sous`.
It ships as a single Go module declared in `go.mod`.
Every binary, library, schema, deployment artifact, and roadmap document sits inside that one module.
The single-module layout keeps imports flat, refactors atomic, and the build graph small enough to fit in one `go build ./...` invocation.

The repository follows a deliberately narrow shape.
The top level holds the `go.mod` / `go.sum` pair, a `Makefile` that pins the canonical build, test, and lint targets, a `docker-compose.yml` for local stacks, an `install.sh` script for one-shot CLI installs, and an `index.html` redirect for the project landing page.
Every other concern — binaries, internal packages, schemas, deployment manifests, product planning, integration tests, and the npm wrapper — sits under a dedicated top-level directory.
New contributors who learn this map can usually predict where a given change belongs before they open an editor.

The rest of this page walks each top-level directory in turn.
Cross-links point at the per-area wiki pages that go deeper.

## `cmd/` — service binaries

Sous is composed of six independently deployable Go binaries.
Each has its own `main.go` under `cmd/`.

The split is dictated by the [Architecture](Architecture) decomposition into a control plane and a data plane.
The control plane owns lifecycle state and policy.
The data plane owns execution and result transport.
Every binary speaks JSON over HTTP or a queue protocol.
Nothing shares an in-process import boundary, so each binary can be redeployed without coordinating with the others.

The six binaries are:

- **`cmd/cs-control`** is the control-plane API.
  It owns publish, lifecycle, audit, signing keys, subscriptions, egress policy, SBOMs, and the workflow determinism linter.
  Every state-changing route lands here and writes to KVRocks.
  The package is the largest under `cmd/` and contains the lifecycle contract tests that lock the public CRUD surface.

- **`cmd/cs-http-gateway`** is the public ingress for synchronous invokes.
  It terminates HTTP, applies tenant rate limits and inflight semaphores, deduplicates by idempotency key, then forwards to the invoker pool.
  It is intentionally thin — no business logic, just middleware and forwarding.

- **`cmd/cs-invoker-pool`** is the data-plane worker.
  It pulls activations off the dispatch queue, loads the bundle, resolves the runtime adapter, applies egress and secrets policy, runs the function, and writes the activation record back.
  Retry, sampling, signature verification, and the per-activation parent / root link all live here.

- **`cmd/cs-scheduler`** is the cron-trigger driver.
  It ticks once per second, evaluates per-tenant schedules with timezone and jitter, and enqueues invocations through the same queue path that the gateway uses.

- **`cmd/cs-cadence-poller`** bridges Sous activities into a Cadence cluster.
  It polls task lists, decodes payloads through the per-tasklist codec, dispatches DecisionTasks and ActivityTasks, and writes back results with deduplication.
  Workflow handlers run here, not in the gateway.

- **`cmd/cs-cli`** is the operator CLI (`cs`).
  It owns the developer-facing surface: `cs init`, `cs publish`, `cs invoke`, `cs fn logs`, `cs doctor`, and the typed error model documented in [Error Model](Error-Model).
  The CLI is the same binary distributed via the npm wrapper.

Each binary is built by `make build` and by the matching step in `.github/workflows/ci.yml`.
Tests live next to the binary's package — for example, `cmd/cs-control/lifecycle_contract_test.go`.

## `internal/` — shared libraries

Anything imported by more than one binary lives under `internal/`.
Go's `internal` rule keeps these packages closed to external consumers.
Third-party code cannot depend on them, so the contract surface stays at the binary level.
Each subdirectory groups a single concern.

### Runtime layer

- **`internal/runtime`** is the runtime execution layer.
  `runtime/registry.go` declares the `Handler` and `Executor` interfaces and the process-wide `DefaultRegistry`.
  Adapters register themselves through `init()`, so a blank import is enough to advertise a runtime.
  The cs-js path is implemented directly on `*Runner` in `runtime/runner.go` and uses Goja for in-process JS evaluation.
  Egress policy, environment binding, and the import-map enforcement helpers all live here.

- **`internal/runtime/python`** is the cs-python adapter.
  It runs the user's `function.py` as a `python3` subprocess in a per-activation scratch directory.
  The adapter marshals the invocation envelope over stdin and stdout and reports execution metrics back through the standard `ExecutionOutput` shape.

- **`internal/runtime/wasm`** is the cs-wasm adapter.
  It executes wasm bundles through wazero.
  The adapter validates the imported ABI against a stable allowlist and threads the same KV and CodeQ providers that cs-js uses, so capability semantics stay identical across runtimes.

- **`internal/runtime/parity`** is the cross-runtime parity harness.
  It loads JSON fixtures from `test/parity/fixtures/`, dispatches each fixture against every adapter registered with `runtime.DefaultRegistry`, and fails the build if any runtime drifts from the canonical result.
  See [Testing](Testing) for the fixture format.

### Cadence layer

- **`internal/cadence`** is the Cadence client wrapper.
  `cadence/client.go` wraps the gRPC client.
  `cadence/codec.go` implements the per-tasklist payload codec (JSON, msgpack, raw).

- **`internal/cadence/workflow`** holds the workflow executor and replay history machinery.
  The `DecisionTask` MVP that the poller dispatches lives here.

- **`internal/cadence/determinism`** holds the publish-time determinism linter.
  It scans workflow handlers for non-deterministic constructs (random, time, network) and rejects publishes that would replay incorrectly.

### Plugin layer

- **`internal/plugins/authn`** and the Tikti adapter under it implement the principal authentication contract.
  The control plane and gateway consume the resolved principal.
  The plugin layer keeps the wiring swappable.

- **`internal/plugins/persistence`** hosts the KVRocks driver and the persistence interface.
  Sous treats KVRocks as the canonical control-plane store.
  See [Storage: KVRocks](Storage-KVRocks).

- **`internal/plugins/messaging`** hosts the codeQ-backed message bus and the queue interface.
  Triggers, dispatch, and activation streaming all flow through it.

- **`internal/plugins/secrets`** ships an in-memory implementation for tests and a Vault adapter for production.
  The secrets resolver is invoked from the invoker pool before the runtime starts.

- **`internal/plugins/registry`** wires the four plugin namespaces into a single boot-time registry consumed by the binaries.

### Cross-cutting concerns

- **`internal/signing`** implements ed25519 bundle signing.
  It carries tenant keys, signed payload framing, and signature verification on the publish path.

- **`internal/audit`** is the append-only audit log.
  It defines event types, the recorder, and the persistent sink consumed by the control-plane audit endpoints.

- **`internal/observability`** holds the cross-cutting logging, metrics, tracing, and activation-sampling helpers that every binary embeds.

### Smaller helpers

Other packages under `internal/` follow the same one-concern rule.
`internal/api` holds the wire types and manifest parser.
`internal/bundle` handles tar extraction and the frozen import map.
`internal/idempotency` carries the dedup machinery.
`internal/limits` holds the per-tenant rate-limit and inflight-semaphore primitives.
`internal/sbom` writes CycloneDX 1.5 documents.
`internal/codeq` implements the codeQ protocol.
`internal/cli` hosts CLI shared types.
`internal/config` parses the YAML configuration.
`internal/errors` defines the typed `CSError` and the canonical code table.
`internal/kv` is the thin KV abstraction.
`internal/scheduler` carries the cron evaluator.
`internal/testutil` collects shared test helpers.

## `spec/` — wire-format JSON Schemas

`spec/` carries the JSON Schemas that pin the public wire format.
Today it holds three files.

`cs.function.script.v1.json` is the bundle script envelope.
`cs.invoke.v1.json` is the invocation request envelope.
`cs.results.v1.json` is the activation result envelope.

The schemas are referenced by integration tests and by the schema-rendering page.
See [Schemas](Schemas) for the rendered view.

## `deploy/` — deployment artifacts

`deploy/` collects everything an operator needs to run Sous in a real environment.

`deploy/helm/code-sous` is the Helm chart used by the Kubernetes path documented in [Deployment: Kubernetes](Deployment-Kubernetes).
The chart ships each binary as a Deployment, plus the supporting `ConfigMap`, `NetworkPolicy`, `HorizontalPodAutoscaler`, and `PodDisruptionBudget` templates under `deploy/helm/code-sous/templates/`.

`deploy/observability` ships the bundled Grafana dashboards, the SLO definitions, and the Prometheus alert rules referenced from [Observability](Observability).
The two dashboard JSON files (`control-plane.json` and `execution.json`) match the two-plane decomposition.
The `slo.yaml` and `alerts.rules.yaml` files define the burn-rate alerts validated by `make slo-validate` and `make dashboards-validate`.

`deploy/sous-deploy-template` is the reference template for the `sous-deploy` companion repository.
It documents the expected GitHub Actions workflow that builds container images from a pinned `sous` ref and runs `helm upgrade --install` against the chart under `deploy/helm/code-sous`.

## `roadmap/` — product planning

`roadmap/README.md` is the live product roadmap.
It organises work into eight epics (E1 through E8) across three phases (Now, Next, Later).
Each epic has an `epic.md` plus per-task `task-NN.md` files under `e1-v01-hardening/`, `e2-dx-cli/`, `e3-runtimes/`, `e4-triggers/`, `e5-packaging/`, `e6-security/`, `e7-observability/`, and `e8-cadence/`.

The Markdown files are written-once specifications.
GitHub issues are the source of truth for status.
See [Roadmap](Roadmap) for the rendered view and [Contributors: What to Contribute](Contributors-What-to-Contribute) for entry points.

## `test/` — integration and parity corpora

`test/integration` holds the build-tagged integration suite invoked by `make integration`.
The suite runs only under `-tags=integration`, so the default `go test ./...` stays fast and dependency-free.

`test/parity/fixtures` carries the JSON fixtures consumed by the runtime parity harness.
The current corpus is `simple-echo.json`, `kv-roundtrip.json`, `log-emission.json`, `capability-denied.json`, and `timeout.json`.
Every registered runtime is exercised against every fixture by `make test-parity`.
See [Testing](Testing) for the fixture format.

## `npm/cs/` — npm wrapper

`npm/cs` packages the `cs` CLI as an npm module.
It is a thin postinstall shim that downloads the matching prebuilt binary from the GitHub release artifacts and drops it onto `PATH`.

The `package.json` publishes as `@osvaldoandrade/cs`.
The release workflow at `.github/workflows/release.yml` keeps the wrapper version aligned with the Go binary.
A user who runs `npm install -g @osvaldoandrade/cs` ends up with the same binary that a user who runs `install.sh` from the GitHub Release page gets.

## How the pieces fit

The repository's shape mirrors the runtime architecture.
The two-plane split between control and execution shows up as two clusters of binaries under `cmd/`.
The plugin layer shows up as the swappable namespaces under `internal/plugins/`.
The capability-and-isolation contract shows up as the runtime registry and adapter packages under `internal/runtime/`.
The supply-chain story shows up as `internal/signing`, `internal/sbom`, and the SBOM endpoint on the control plane.

A contributor who needs to make a change does not usually need to touch more than one or two directories.
The exception is a change that crosses the wire — adding a manifest field, changing an envelope schema, introducing a new error code.
Those changes touch `internal/api`, `internal/errors`, the relevant binary, the parity fixtures, and the corresponding wiki page.
The single-module layout makes that fan-out cheap.

For the day-to-day flow — branches, tests, CI, releases — see [Contributors: Resources](Contributors-Resources).
For the bar a new runtime must meet, see [Contributors: Adding a Runtime](Contributors-Adding-a-Runtime).
