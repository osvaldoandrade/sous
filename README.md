# code-sous

`code-sous` is a function execution layer for agent-built automation.

This repository contains:

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

macOS/Linux (and Windows via Git Bash/MSYS2/Cygwin/WSL):

```bash
curl -fsSL https://raw.githubusercontent.com/osvaldoandrade/sous/main/install.sh | sh
```

Or via npm (installs a prebuilt binary from GitHub Releases):

```bash
npm install -g @osvaldoandrade/cs@latest
```

## Configuration

Copy and edit:

```bash
cp config.example.yaml config.yaml
```

Each service reads `--config config.yaml`.

Plugin drivers are configured under `plugins` in YAML:

- `plugins.authn.driver` (default: `tikti`)
- `plugins.persistence.driver` (default: `kvrocks`)
- `plugins.messaging.driver` (default: `codeq`)
- `plugins.authn.tikti.api_key` (required when Tikti lookup endpoint is protected by `?key=...`)

Legacy top-level config blocks (`tikti`, `kvrocks`, `codeq`) are still accepted during migration.

## Local dependencies

A local dependency stack is provided for smoke tests:

```bash
docker compose up -d
```

Services included:

- KVRocks
- Redpanda (Kafka-compatible `codeQ`)

## Commands

Build binaries:

```bash
make build
```

Run services locally:

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
