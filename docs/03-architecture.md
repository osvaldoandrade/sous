# Architecture

This document describes the component boundaries and the execution data path.

`code-sous` splits into two planes.

- The control plane owns lifecycle state and policy.
- The data plane owns execution, isolation, and result transport.

## Component set

### Control plane

`cs-control`
- Serves lifecycle APIs.
- Validates manifests and names.
- Writes function records, drafts, versions, aliases, schedules, and WorkerBindings to KVRocks.
- Emits audit events to ledgerDB when audit is enabled.

### Data plane ingress

`cs-http-gateway`
- Terminates the generic HTTP endpoint.
- Authenticates and authorizes HTTP invokes via Tikti.
- Maps HTTP requests to InvocationRequest.

`cs-scheduler`
- Emits InvocationRequest messages on a fixed interval.
- Enforces overlap rules through an inflight marker.

`cs-cadence-poller`
- Long-polls `code-flow` for Activity tasks.
- Maps tasks to InvocationRequest messages.
- Responds to Cadence after InvocationResult arrives.

### Data plane execution

`cs-invoker-pool`
- Consumes InvocationRequest from codeQ.
- Executes `cs-js` in a sandbox.
- Persists activations and logs in KVRocks.
- Publishes InvocationResult to codeQ.

## High-level data flow

```
Clients ──HTTPS──▶ cs-http-gateway ──publish──▶ codeQ(cs.invoke) ──consume──▶ cs-invoker-pool
                                                                         │
                                                                         └──publish──▶ codeQ(cs.results)
                                                                                   ▲
                                                                                   │
                                                cs-http-gateway waits by request_id│
```

Schedule flow:

```
cs-scheduler ──publish──▶ codeQ(cs.invoke) ──consume──▶ cs-invoker-pool ──publish──▶ codeQ(cs.results)
```

Cadence Activity flow:

```
code-flow ──long poll──▶ cs-cadence-poller ──publish──▶ codeQ(cs.invoke) ──consume──▶ cs-invoker-pool
   ▲                                   │                                              │
   └──────── respond completed/failed ◀─┴──────── consume by activation_id ────────────┘
```

## Why codeQ sits between pollers and invokers

The platform isolates two loops:

- long-lived network loops (HTTP waiting, scheduler ticks, Cadence long-poll)
- sandbox execution of user code

codeQ forms a buffer between these loops.

This buffer provides:

- backpressure through queue depth
- independent scaling of pollers and invokers
- isolation of user code failures from long-poll code

## Storage boundaries

KVRocks stores:

- immutable versions and bundles
- mutable pointers (aliases, schedule state)
- activation records and logs

The invoker owns activation terminal state.
The control plane owns function lifecycle state.

## Multi-tenant isolation

Every record key includes tenant.
Every authorization check includes tenant.
The runtime enforces capability allowlists per version.

## Audit boundary

If ledgerDB audit is enabled:

- `cs-control` emits an audit event per mutation.
- `cs-invoker-pool` does not emit audit events for activations.
