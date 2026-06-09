# code-sous (cs)

A serverless functions runtime. Publish a function as plain UTF-8 text, invoke it over HTTP, on a schedule, or as a Cadence Activity. Versions are immutable; aliases are mutable pointers; capabilities are explicit.

This repository contains the runtime services, the `cs` CLI, and the Kubernetes deployment manifests.

## Links

- GitHub: https://github.com/osvaldoandrade/sous
- Issues: https://github.com/osvaldoandrade/sous/issues
- Docs: https://osvaldoandrade.github.io/sous/wiki
- Landing: https://osvaldoandrade.github.io/sous

## Why code-sous

- **No build step at publish time.** Functions ship as `function.js` + `manifest.json`, both plain text. No Docker layers, no language toolchain at the registry.
- **Immutable versions and mutable aliases.** Every publish allocates a monotonic version id; rollouts switch alias pointers without rewriting history.
- **One execution fabric for three trigger families.** `cs-http-gateway`, `cs-scheduler`, and `cs-cadence-poller` all converge on `cs-invoker-pool`.
- **Capability-gated runtime.** Functions declare side effects (KV ops, codeQ publish, HTTP egress) in the manifest. The runtime denies anything not declared.
- **codeQ as the durable buffer.** Trigger ingestion and code execution run as independent loops with codeQ between them. Backpressure, independent scaling, and crash isolation come from that decoupling.
- **Local–cluster runtime parity.** The `cs` CLI embeds the same `cs-js` runtime the invoker pool uses. If `cs fn test` passes, the cluster behaves identically.
- **Multi-tenant by construction.** Tenants, namespaces, and roles are first-class in every API path. Cross-tenant access is denied at authz, not at convention.

## Get started

### Local stack with Docker Compose

```bash
git clone https://github.com/osvaldoandrade/sous
cd sous
docker compose up -d              # KVRocks + Redpanda (codeQ transport)
cp config.example.yaml config.yaml
make build
```

Run services in separate terminals:

```bash
./bin/cs-control --config config.yaml
./bin/cs-http-gateway --config config.yaml
./bin/cs-invoker-pool --config config.yaml
./bin/cs-scheduler --config config.yaml
./bin/cs-cadence-poller --config config.yaml
```

### Install the CLI

macOS, Linux, and Windows via Git Bash:

```bash
curl -fsSL https://raw.githubusercontent.com/osvaldoandrade/sous/main/install.sh | sh
```

Via npm (downloads a prebuilt Go binary from GitHub Releases):

```bash
npm install -g @osvaldoandrade/cs@latest
cs --help
```

If the source repository is private, set `GITHUB_TOKEN` or `GH_TOKEN` so the installer can authenticate.

### Author and publish your first function

```bash
cs auth login --tenant t_abc123 --token "$CS_TOKEN" --api-url http://localhost:8080
cs fn init reconcile --runtime cs-js
cs fn test reconcile --event ./event.json
cs fn draft upload reconcile --path .
cs fn publish reconcile --draft <draft_id> --timeout-ms 3000 --memory-mb 64 \
  --invoke-http-roles role:app
cs fn alias set reconcile prod --version 1
```

## Quick API flow

Invoke synchronously through the HTTP gateway:

```bash
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"orderId":"o-123"}' \
  http://localhost:8080/v1/web/t_abc123/default/reconcile/prod
```

Read an activation record:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/v1/control/t_abc123/activations/<activation_id>
```

Tail user logs for an activation:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/v1/control/t_abc123/activations/<activation_id>/logs
```

Full endpoint reference: [REST API](https://osvaldoandrade.github.io/sous/wiki/REST-API).

## Specs and docs

Start here: https://osvaldoandrade.github.io/sous/wiki

Key references:

- **Get Started** — local stack, scaffold, publish, invoke
- **Overview** — system goals and design principles
- **Architecture** — control plane, data plane, and the codeQ buffer
- **REST API** — complete endpoint contract
- **CLI** — `cs` command reference
- **Runtime: cs-js** — JavaScript runtime semantics and host APIs
- **HTTP Invoke Path**, **Scheduler**, **Cadence Integration** — trigger paths
- **IAM with Tikti** — authentication and authorization
- **codeQ Protocol** — message envelopes and topic layout
- **Storage: KVRocks** — key schema and retention
- **Security**, **Security Checklist**, **Capacity and Limits** — operational gates
- **JSON Schemas** — `cs.function.script.v1`, `cs.invoke.v1`, `cs.results.v1`

## Repo layout

- `cmd/`: services and CLI (`cs-control`, `cs-http-gateway`, `cs-scheduler`, `cs-cadence-poller`, `cs-invoker-pool`, `cs-cli`)
- `internal/`: shared libraries (API, authz, bundle, codeq, kv, runtime, observability)
- `deploy/`: Helm chart and Kubernetes manifests
- `wiki/`: full specification (single source of truth, also published at the GitHub Pages site)
- `npm/cs`: npm distribution of the `cs` CLI

## License

MIT. See `LICENSE`.
