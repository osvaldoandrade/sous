# Architecture

SOUS splits into two planes.

The control plane owns lifecycle state and policy. The data plane owns execution, isolation, and result transport.

This split mirrors large-scale serverless platforms: control-plane APIs must remain reliable and strongly validated; execution must scale independently and be resilient to user-code failures.

## Components

Control plane:

- `cs-control` provides lifecycle APIs, validates bundles/manifests, and persists function state.

Data plane ingress:

- `cs-http-gateway` terminates the HTTP invoke endpoint, authenticates via Tikti, maps HTTP to `InvocationRequest`, and optionally waits for results.
- `cs-scheduler` emits `InvocationRequest` messages at fixed intervals with overlap control.
- `cs-cadence-poller` long-polls Cadence for Activity tasks and maps them to `InvocationRequest`.

Execution:

- `cs-invoker-pool` is the execution fabric. It loads bundles, enforces isolation/capabilities, persists activations/logs, and publishes `InvocationResult`.

## Why codeQ is Between Pollers and Invokers

The platform isolates two loops:

The first loop is long-lived network I/O (HTTP waiting, scheduler ticks, Cadence long-poll). The second loop is sandboxed user-code execution.

codeQ buffers between these loops.

That buffer provides backpressure (queue depth), independent scaling (more pollers or more invokers), and failure isolation (a crashing function does not take down a long-poll loop).

## End-to-End Flow

```mermaid
graph TD
  Client[Client] -->|HTTPS| GW[cs-http-gateway]
  GW -->|publish InvocationRequest| Q[(codeQ cs.invoke)]
  Q -->|consume| INV[cs-invoker-pool]
  INV -->|publish InvocationResult| QR[(codeQ cs.results)]
  QR -->|correlate by request_id| GW
```

## HTTP Invoke (Sync)

```mermaid
sequenceDiagram
  participant U as Client
  participant G as cs-http-gateway
  participant Q as codeQ
  participant I as cs-invoker-pool
  participant K as KVRocks

  U->>G: HTTPS request + Bearer token
  G->>G: Tikti introspection + authz checks
  G->>Q: Publish InvocationRequest (request_id)
  Q->>I: Deliver InvocationRequest
  I->>K: Load bundle + persist activation/logs
  I->>Q: Publish InvocationResult (request_id)
  G->>Q: WaitForResult(request_id)
  G-->>U: HTTP response mapped from result
```

## Scheduler

```mermaid
sequenceDiagram
  participant S as cs-scheduler
  participant Q as codeQ
  participant I as cs-invoker-pool

  loop Every tick
    S->>S: Evaluate schedules + overlap policy
    S->>Q: Publish InvocationRequest
  end
  Q->>I: Deliver InvocationRequest
  I->>Q: Publish InvocationResult
```

## Cadence Activity

```mermaid
sequenceDiagram
  participant C as Cadence (code-flow)
  participant P as cs-cadence-poller
  participant Q as codeQ
  participant I as cs-invoker-pool

  P->>C: Long-poll for Activity task
  C-->>P: Activity task
  P->>Q: Publish InvocationRequest
  Q->>I: Deliver InvocationRequest
  I->>Q: Publish InvocationResult
  P->>Q: Consume result by activation_id
  P-->>C: Respond completed/failed
```

