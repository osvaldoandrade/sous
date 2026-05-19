# code-sous

`code-sous` is a function execution layer for agent-built automation.

## Documentation

Complete documentation lives at the project's GitHub wiki:
https://github.com/osvaldoandrade/sous/wiki — start with the Home page for
role-based reading paths (developers, operators, contributors). The local
`wiki/` directory in this repository is the writable source for those pages;
see `wiki/README.md` for the publishing workflow.

## Repository layout

- Control plane: `cmd/cs-control`
- Data plane ingress: `cmd/cs-http-gateway`, `cmd/cs-scheduler`, `cmd/cs-cadence-poller`
- Execution plane: `cmd/cs-invoker-pool`
- CLI: `cmd/cs-cli`
- Shared libraries under `internal/`

## Build and test

```bash
make test
make build
```

## Install CLI

Requires `git` and `go`.

macOS, Linux, and Windows via Git Bash/MSYS2/Cygwin/WSL:

```bash
curl -fsSL https://raw.githubusercontent.com/osvaldoandrade/sous/main/install.sh | sh
```

Or via npm, which installs a prebuilt binary from GitHub Releases:

```bash
npm install -g @osvaldoandrade/cs@latest
```

## Configuration

Copy and edit the example file, then point each service at it:

```bash
cp config.example.yaml config.yaml
```

Each service reads `--config config.yaml`. Plugin drivers are configured under
the `plugins` block (`plugins.authn.driver`, `plugins.persistence.driver`,
`plugins.messaging.driver`). The wiki's Operators-Configuration-Reference page
documents every field.

## Local dependencies

A local dependency stack is provided for smoke tests:

```bash
docker compose up -d
```

Services included: KVRocks and Redpanda (Kafka-compatible `codeQ`).

## Run services locally

```bash
./bin/cs-control --config config.yaml
./bin/cs-http-gateway --config config.yaml
./bin/cs-invoker-pool --config config.yaml
./bin/cs-scheduler --config config.yaml
./bin/cs-cadence-poller --config config.yaml
```

## CLI quickstart

```bash
./bin/cs auth login --tenant t_abc123 --token "$CS_TOKEN" --api-url http://localhost:8080
./bin/cs fn init reconcile
./bin/cs fn test --path reconcile
```

See the wiki's Developers-CLI page for the full command reference.
