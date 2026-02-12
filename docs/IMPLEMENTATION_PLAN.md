# Implementation plan

This document defines an ordered build plan for `code-sous`.

The plan optimizes for early end-to-end execution of agent-created JavaScript.

## Delivery principle

Deliver a working vertical slice first.

A vertical slice includes:

- create function
- publish version
- invoke via HTTP
- execute in `cs-invoker-pool`
- read activation metadata and logs

Everything else builds on that slice.

## 0. Repo and build system

Create a mono-repo:

```
/cmd
  /cs-control
  /cs-http-gateway
  /cs-invoker-pool
  /cs-scheduler
  /cs-cadence-poller
  /cs-cli
/internal
  /api
  /authz
  /bundle
  /codeq
  /kv
  /runtime
  /scheduler
  /cadence
  /observability
  /errors
/deploy/helm
/spec
```

Add CI targets:

- `go test ./...`
- `golangci-lint`
- container build for each cmd
- integration tests with docker-compose

## 1. Shared primitives

### 1.1 Error catalog

Implement `internal/errors`.

Features:

- stable `CS_*` codes from `21-errors.md`
- HTTP status mapping
- JSON encoder that includes `request_id`

Gate:

- unit tests cover every mapping row

### 1.2 Config system

Implement `internal/config`.

Features:

- YAML load
- env overrides
- schema validation

Gate:

- config validation rejects missing required fields

### 1.3 KVRocks adapter

Implement `internal/kv`.

Features:

- connection pooling
- timeouts
- key builders
- Lua scripts for atomic publish and CAS updates

Gate:

- integration test runs publish transaction against KVRocks

### 1.4 codeQ adapter

Implement `internal/codeq`.

Features:

- publish
- subscribe
- consumer groups
- schema-aware envelope codec

Gate:

- integration test publishes and consumes a `cs.invoke.v1` message

## 2. Control plane (cs-control)

### 2.1 HTTP service skeleton

Implement:

- router
- request id middleware
- Tikti token validation middleware
- authz checks per endpoint

Gate:

- `/readyz` checks KVRocks and codeQ connectivity

### 2.2 Function CRUD

Implement:

- create
- read
- delete (soft delete)

Gate:

- delete sets `deleted_at_ms`
- read hides deleted by default

### 2.3 Draft upload

Implement:

- upload endpoint that accepts `files` map
- canonical tar builder
- `sha256` over tar bytes
- draft TTL record in KVRocks

Gate:

- draft expires via TTL
- publish rejects expired drafts

### 2.4 Publish version

Implement:

- atomic version assignment via Lua
- write `ver:meta` and `ver:bundle`
- enforce manifest schema validation
- optional alias update

Gate:

- concurrent publishes produce unique versions

### 2.5 Activation read API

Implement:

- activation metadata fetch
- logs chunk listing and streaming

Gate:

- invoker integration test writes an activation and API reads it

## 3. JavaScript runtime (cs-js)

### 3.1 Engine integration

Pick one engine and lock it for v0.1.

The engine must support:

- isolate per activation
- memory budget per isolate
- interrupt or cancel at deadline

Gate:

- unit test executes a module and returns response object

### 3.2 Host API surface

Implement `cs.*`:

- `cs.log`
- `cs.kv`
- `cs.codeq`
- `cs.http`

Enforcement:

- capability allowlists
- private IP block for HTTP

Gate:

- test denies disallowed key prefix
- test denies disallowed host

### 3.3 Result validation

Implement result validator:

- statusCode integer
- headers map
- body string
- isBase64Encoded boolean

Gate:

- invalid return type maps to `CS_RUNTIME_EXCEPTION`

## 4. Invoker pool (cs-invoker-pool)

### 4.1 Consumer loop and idempotency

Implement:

- subscribe to `cs.invoke`
- validate schema
- idempotency check on `activation_id`

Gate:

- repeated same activation id does not re-run code

### 4.2 Execution pipeline

Implement:

- bundle fetch and sha256 verify
- semaphore for `maxConcurrency`
- timeout enforcement
- memory enforcement
- log capture

Gate:

- timeout test yields `status=timeout`
- memory limit test yields `MemoryLimit`

### 4.3 Persistence and results

Implement:

- Activation start and terminal records
- log chunk writes
- publish `cs.results`

Gate:

- request-response test correlates by `request_id`

### 4.4 Cache

Implement:

- bundle cache by sha256
- manifest cache
- cache metrics

Gate:

- two invocations of same version increment cache hit metric

## 5. HTTP gateway (cs-http-gateway)

### 5.1 Route parsing

Implement `/v1/web/{tenant}/{namespace}/{function}/{ref}/{proxyPath*}`.

Gate:

- invalid ref returns `400 CS_VALIDATION_FAILED`

### 5.2 Authn/authz

Implement:

- Tikti token validation
- resource + action checks
- role allowlist check from version config

Gate:

- missing role returns `403`

### 5.3 Sync invoke

Implement:

- publish InvocationRequest
- wait for result by `request_id`
- map function response to HTTP

Gate:

- function sets status code and headers in HTTP response

## 6. Scheduler (cs-scheduler)

### 6.1 Leader election

Implement Kubernetes lease leader election.

Gate:

- only leader publishes schedule ticks

### 6.2 Tick state and overlap policies

Implement:

- schedule state record
- inflight marker and clearing

Gate:

- skip policy prevents overlapping activations

## 7. Cadence poller (cs-cadence-poller)

### 7.1 Binding refresh and poll loop

Implement:

- periodic load of WorkerBindings
- per-binding poller goroutines

Gate:

- enabling a binding starts polling without restart

### 7.2 Task mapping and respond path

Implement:

- ActivityTask → InvocationRequest mapping
- InvocationResult → respond mapping

Gate:

- mock Cadence server receives completion call

### 7.3 Heartbeat channel

Implement:

- `cs.cadence.heartbeat` in runtime
- `cs.cadence.heartbeat` topic in codeQ
- poller heartbeat consumer

Gate:

- heartbeat call reaches mock Cadence server

## 8. CLI (cs)

### 8.1 Local test runner

Implement:

- load local bundle
- run with embedded runtime
- print normalized result

Gate:

- local runner output matches remote output in e2e test

### 8.2 Remote commands

Implement:

- create function
- upload draft
- publish version
- set alias
- invoke and fetch activation logs

Gate:

- CLI runs against a dev cluster without manual steps

## 9. Production gates

### 9.1 Observability

Implement:

- Prometheus metrics endpoints
- structured logs with request_id and activation_id
- trace propagation in InvocationRequest

Gate:

- dashboard shows p99 invoke latency and error rate

### 9.2 Security gates

Implement:

- egress allowlist enforcement
- private IP deny list
- secret redaction

Gate:

- request to 169.254.169.254 fails inside `cs.http.fetch`

### 9.3 Load gates

Define load tests:

- sustained HTTP invoke rate
- sustained schedule tick rate
- sustained Cadence Activity rate

Gate:

- no data loss in `cs.results`
- queue lag stays under configured alert threshold
