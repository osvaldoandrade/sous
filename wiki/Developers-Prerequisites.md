# Developers: Prerequisites

Sous is a multi-service execution layer. Before a developer can publish a function and invoke it end-to-end, the workstation must be able to compile the Go binaries that make up the control plane and data plane, run a small set of infrastructure dependencies that those binaries expect to find on localhost, and present a valid Tikti identity at every call site.

This page walks through each prerequisite, explains why Sous depends on it, and ends with a single `docker compose` command that starts the local stack. Items marked optional may be deferred until the developer first needs them.

The intended audience is a contributor or an early integrator running the platform on their own machine. Production operators who deploy Sous to Kubernetes should also read [Deployment Kubernetes](Deployment-Kubernetes); the dependency list there is the same, but the operational shape — TLS, managed Kafka, hardened Tikti, Vault HA — is different.

## Go toolchain

Sous is a Go monorepo. The top-level `go.mod` pins `go 1.25.5`, which means the toolchain version on the developer machine must satisfy that minimum.

Earlier 1.24.x toolchains compile most of the codebase but reject newer standard-library APIs the project relies on for `slog`, `sync/atomic`, and `net/http` server timeouts. Install Go 1.25 or newer from the upstream distribution, confirm with `go version`, and make sure `$GOPATH/bin` is on the shell `PATH` so the `cs` CLI ends up runnable after `make build`.

Sous does not require CGO. The default toolchain configuration is sufficient. A developer who has previously set `CGO_ENABLED=1` for unrelated projects may continue to do so; the build will still produce static binaries because none of the imported packages link against C libraries.

The Makefile target `make build` is the canonical way to produce the daemons and the CLI. It builds `cs-control`, `cs-http-gateway`, `cs-invoker-pool`, `cs-scheduler`, `cs-cadence-poller`, and the `cs` CLI binary into the `bin/` directory at the repository root.

## Docker for local dependencies

Two infrastructure components need to run alongside the daemons: a KVRocks instance for control-plane storage, and a Redpanda broker that speaks the Kafka wire protocol for codeQ message transport. Both are shipped as container images and orchestrated by the repository's `docker-compose.yml`.

### KVRocks

KVRocks is a Redis-protocol-compatible key-value store that persists to disk and supports replication. Sous uses it as the canonical store for tenants, namespaces, functions, drafts, versions, aliases, activations, and trigger bindings.

The design intentionally targets the Redis TCP wire because every Go and node client already understands it, and because KVRocks gives the platform two properties that an in-memory Redis cannot: durable writes that survive restarts, and predictable space accounting for bundles and activations.

The local image binds `localhost:6666` and uses an unauthenticated config suitable for development only. Production deployments are expected to enable password authentication and to front KVRocks with a TLS-terminating proxy; both are configured under `plugins.persistence.kvrocks.auth` in `config.example.yaml`.

### Redpanda

Redpanda is a Kafka-compatible broker that runs as a single binary with no JVM. Sous publishes `InvocationRequest`, `InvocationResult`, and DLQ envelopes onto codeQ topics, and `cs-invoker-pool` consumes them on the other side. The codeQ protocol is documented separately under [codeQ Protocol](codeQ-Protocol).

The local image binds `localhost:9092` with no SASL, so a developer can `kcat` topics directly while debugging. The topics themselves are auto-created with default settings on first publish, but operators who want explicit retention or partition counts can pre-create them through `rpk topic create`.

### Docker runtime

Docker Engine 20.10 or newer (with the `docker compose` plugin) is sufficient. On Linux a developer may use any compatible runtime that honours `docker-compose.yml` semantics.

macOS and Windows developers typically run Docker Desktop or Rancher Desktop; both have been observed to work with the bundled images. Resource defaults that ship with those products (2 CPU, 4 GB RAM) are enough for the local stack; the daemons themselves add only a few hundred megabytes of resident memory each.

## A Tikti tenant and bearer token

Sous delegates all identity decisions to Tikti. The platform never issues credentials of its own, never stores passwords, and never trusts an unverified principal.

Every authenticated request that reaches `cs-control` or `cs-http-gateway` carries a bearer token that one of those daemons resolves against Tikti's introspection endpoint. The response shapes the principal record (`sub`, `tenant`, `roles[]`) used downstream by authorization checks.

For local development a developer needs three pieces of information from a Tikti deployment:

- A tenant identifier (for example `t_dev123`) that the CLI will embed in every control-plane URL.
- A bearer token whose introspection result includes that tenant and the roles required by the actions the developer plans to exercise — `cs:function:create`, `cs:function:publish`, `cs:function:invoke:http`, and so on, as catalogued in [IAM with Tikti](IAM-with-Tikti).
- An introspection URL that the local daemons can reach over plain HTTP.

A team that already operates Tikti centrally typically provisions a dedicated `dev` tenant per engineer and pre-issues a long-lived bearer token. A team that does not yet have Tikti can stand up the open-source server and point `plugins.authn.tikti.introspection_url` in `config.yaml` at it.

The CLI command `cs auth login --tenant <t> --token <bearer>` then writes the credentials into `$XDG_CONFIG_HOME/code-sous/auth.json` for reuse by subsequent commands. The token is held only on the developer's workstation; the daemons never persist it.

## Optional: Cadence cluster

Sous integrates with Cadence as a workflow source through `cs-cadence-poller`. The poller long-polls a Cadence frontend for Activity tasks bound to registered tasklists and dispatches them through codeQ to the invoker pool, where each task becomes a regular activation.

A developer who is only exploring HTTP and schedule triggers can skip Cadence entirely; the rest of the stack runs cleanly without a Cadence frontend on the network.

For developers who do plan to exercise the Cadence path, the recommended setup is the upstream `ubercadence/cadence` Docker image (or the maintained `uber/cadence-server` derivative), exposed on `localhost:7933`. The configuration knobs that `cs-cadence-poller` consumes — domain, tasklist, worker identity, poller counts — live under `cs_cadence_poller` in `config.example.yaml`.

The integration story, including how Activity Type maps to a Sous function ref and how heartbeats translate into log chunks, is described end-to-end in [Cadence Integration](Cadence-Integration).

## Optional: HashiCorp Vault for secrets

Sous loads function secrets through a pluggable provider interface. The default driver is `memory`, which reads from a seed map embedded in `config.yaml` and is sufficient for unit tests and local exploration.

A second driver, `vault`, delegates secret resolution to a HashiCorp Vault cluster's KV v2 engine and is the recommended path for production.

A developer who wants to validate their Vault integration locally should run the official `vault` container in dev mode, populate a few paths under the configured `kv_mount`, export `VAULT_TOKEN`, and flip `plugins.secrets.driver` to `vault` in `config.yaml`. The Vault wiring, including how a manifest entry like `STRIPE_KEY=payments/stripe_key` is resolved at activation time, is documented under [Security](Security).

## Optional: A modern editor

Functions on Sous today ship as plain text bundles — `function.js` plus `manifest.json` — that the runtime parses on demand. There is no build step. Any editor that produces UTF-8 JavaScript is sufficient.

Many developers reach for VS Code with the built-in JavaScript language service because it surfaces syntax issues before `cs fn test` is invoked. Future runtimes (`cs-python`, `cs-wasm`) are described under [Runtime cs-js](Runtime-cs-js) and the language-specific pages that follow, but they share the same "text bundle, no build" contract.

## Sizing the local stack

The local stack is sized for a single developer machine. Approximate resident footprints when idle:

- `kvrocks` container: 80–150 MB.
- `redpanda` container: 600–900 MB (the broker pre-allocates memory pools).
- `cs-control`: 30–60 MB.
- `cs-http-gateway`: 30–50 MB.
- `cs-invoker-pool`: 60–120 MB (grows with cached bundles; the cache cap is `cs_invoker_pool.cache.bytes_max` in `config.example.yaml`).
- `cs-scheduler`: 20–40 MB.

A workstation with 8 GB of free RAM has substantial headroom. CPU usage is negligible at rest and bursts only while functions execute.

## Ports the local stack expects to own

Before starting the stack, a developer should confirm that nothing on the workstation already holds these ports:

- `6666` — KVRocks (Redis wire).
- `9092` — Redpanda (Kafka wire).
- `9644` — Redpanda Admin API (optional but bound by the default compose file).
- `8080` — `cs-control` HTTP API.
- `8081` — `cs-http-gateway` HTTP API.
- `8082` — `cs-invoker-pool` health and metrics.

A developer who needs to relocate any of these ports can override them in `config.yaml`. The compose file is also editable for the two container ports; the default values match the daemons' shipped config.

## Network egress from user code

Once the local stack is up, function code that calls outbound HTTP needs a path out. The runtime's egress story is gated by the manifest's capability list (`net.fetch.allowed_hosts[]`); a function with no such capability is denied at the runtime layer regardless of network topology.

A developer who wants to test a function that hits a third-party API should expect the workstation to need standard outbound HTTPS access on the usual ports. There is no inbound port mapping required for the function to make outbound calls.

## Quickstart: bring up the dependency stack

With Docker installed and the repository checked out, a developer can start the local infrastructure with a single command from the repository root:

```bash
docker compose up -d
```

That command honours `docker-compose.yml` and brings up two services:

- `kvrocks`, listening on `localhost:6666`, which serves the Redis wire protocol used by every control-plane write.
- `redpanda`, listening on `localhost:9092`, which serves the Kafka wire protocol used by codeQ.

To confirm both containers came up healthy, list their state:

```bash
docker compose ps
```

The expected output shows two services in the `running` state with their published ports bound on `0.0.0.0`. A developer who needs to reset the local state — for example after experimenting with publish-time signing or after corrupting an alias by hand — can tear the stack down and start fresh:

```bash
docker compose down -v
docker compose up -d
```

The `-v` flag deletes the anonymous volumes attached to the containers, which wipes KVRocks and Redpanda state. The Sous daemons themselves keep no on-disk state outside KVRocks, so removing the volumes is sufficient to restore a clean slate.

After the containers report healthy, the workstation is ready for the daemons described in [Developers Getting Started](Developers-Getting-Started). The next page, [Developers Using Sous](Developers-Using-Sous), explains the mental model behind the addressing scheme and identity flow before the first function is published.

## Verifying the dependency stack

A developer who wants to sanity-check the stack before bringing up the daemons can do so with two short commands that exercise the network paths the daemons will use.

To confirm KVRocks responds on the Redis wire, the `redis-cli` from any Redis distribution suffices:

```bash
redis-cli -h localhost -p 6666 PING
```

The expected response is `PONG`. A developer without `redis-cli` on hand can fall back to `docker exec`:

```bash
docker compose exec kvrocks kvrocks-cli -p 6666 PING
```

To confirm Redpanda accepts Kafka producers, `rpk` shipped inside the container is the lowest-friction option:

```bash
docker compose exec redpanda rpk cluster info
```

The output lists the broker and its advertised listener. If both commands succeed, the daemons started in [Developers Getting Started](Developers-Getting-Started) will find their dependencies on first connect.

## Common pitfalls

A small number of issues come up repeatedly when a developer brings the stack up for the first time:

- **Wrong Go version.** Builds fail with confusing errors about `slog` or `errors.Is`. The fix is to install Go 1.25 or newer.
- **Port already in use.** Docker reports a bind error. The fix is either to free the port or to remap it in `docker-compose.yml`.
- **Tikti introspection unreachable.** The daemons start, but every authenticated call returns `CS_UNAUTHORIZED`. The fix is to confirm `plugins.authn.tikti.introspection_url` resolves from inside the daemon process.
- **Wrong tenant on the token.** `cs auth login` succeeds but `cs fn create` fails with `CS_FORBIDDEN`. The fix is to confirm the token's `tenant` claim matches the value passed to `cs auth login --tenant`.

These pitfalls are covered in more detail under [Runbooks](Runbooks), which doubles as a quick-reference once the stack is in routine use.
