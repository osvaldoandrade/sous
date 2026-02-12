# Get Started

This guide brings up a local SOUS stack, runs your first function with the CLI runtime, publishes it to the control plane, and invokes it through the HTTP gateway.

SOUS is multi-service by design. In production you scale these components independently, but locally you can run them as separate processes on one machine.

## Prerequisites

- Go 1.24+
- Docker (for KVRocks and Redpanda)
- Git (for development workflows)

## 1) Start Local Dependencies

```bash
docker compose up -d
```

This starts:

- KVRocks on `localhost:6666`
- Redpanda (Kafka-compatible) on `localhost:9092`

## 2) Create a Local Config

Start from the example:

```bash
cp config.example.yaml config.yaml
```

For local development, you typically want:

- KVRocks pointing to `localhost:6666`
- codeQ using Kafka/Redpanda (set `plugins.messaging.codeq.base_url` empty and configure `brokers`)

Example (local Kafka transport):

```yaml
plugins:
  messaging:
    driver: codeq
    codeq:
      base_url: ""
      brokers: ["localhost:9092"]
      topics:
        invoke: cs.invoke
        results: cs.results
        dlq_invoke: cs.dlq.invoke
        dlq_results: cs.dlq.results
```

Authentication is handled through Tikti introspection. For a real run you must point `plugins.authn.tikti.introspection_url` to a working introspection endpoint.

## 3) Build Binaries

```bash
make build
```

## 4) Run Services Locally

In separate terminals:

```bash
./bin/cs-control --config config.yaml
./bin/cs-http-gateway --config config.yaml
./bin/cs-invoker-pool --config config.yaml
./bin/cs-scheduler --config config.yaml
./bin/cs-cadence-poller --config config.yaml
```

## 5) Create and Test a Function Locally

SOUS functions are a small bundle:

- `function.js` (your handler)
- `manifest.json` (limits, capabilities, role allowlists)

Create a scaffold:

```bash
./bin/cs fn init reconcile --runtime cs-js
```

Test locally:

```bash
./bin/cs fn test reconcile --event ./event.json
```

This uses the same runtime contract as the cluster invoker.

## 6) Publish and Invoke

Upload a draft and publish a version:

```bash
./bin/cs fn draft upload reconcile --path .
./bin/cs fn publish reconcile --draft <draft_id> --timeout-ms 3000 --memory-mb 64 \
  --invoke-http-roles role:app \
  --invoke-schedule-roles role:worker \
  --invoke-cadence-roles role:cadence
```

Point an alias to the published version:

```bash
./bin/cs fn alias set reconcile prod --version 1
```

Invoke via HTTP gateway:

```bash
./bin/cs http invoke /v1/web/<tenant>/<namespace>/reconcile/prod -X POST -d @event.json
```

If you want the full endpoint contract and its idempotency rules, see [HTTP Invoke Path](HTTP-Invoke-Path).
