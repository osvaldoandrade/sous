# code-sous specification

`code-sous` is a function execution layer for agent-built automation.

An agent generates a function as two text files:

- `function.js` contains the handler.
- `manifest.json` declares limits, capabilities, and invoke roles.

The agent runs the function locally with the `cs` CLI.
The agent publishes the same bundle to the cluster.
The cluster executes the same runtime rules inside `cs-invoker-pool`.

## Execution entry points

`code-sous` supports three trigger families.

HTTP
- A caller hits a generic endpoint.
- `cs-http-gateway` maps the HTTP request to an `InvocationRequest`.
- `cs-invoker-pool` executes the function and returns an HTTP response.

Schedule
- A schedule emits an invocation every N seconds.
- `cs-scheduler` publishes `InvocationRequest` messages.
- `cs-invoker-pool` executes them.

Cadence Activities
- `cs-cadence-poller` long-polls `code-flow` (Cadence fork) for Activity tasks.
- The poller maps each task to an `InvocationRequest`.
- `cs-invoker-pool` executes the mapped function.
- The poller responds to Cadence with completion or failure.

## Storage and transport

`code-sous` stores source, metadata, activations, and logs in KVRocks.

`code-sous` transports invocation requests and results through codeQ.

## Authorization and isolation

Tikti IAM authenticates and authorizes every operation.

The runtime enforces:

- time limits
- memory limits
- per-version capability allowlists
- per-version role allowlists

## Document map

Start with:

- `02-requirements.md` for product scope and invariants.
- `03-architecture.md` for component boundaries and data flows.
- `IMPLEMENTATION_PLAN.md` for an ordered build plan.

Use:

- `08-runtime-cs-js.md` to implement the JavaScript runner.
- `12-cadence-integration.md` to implement Cadence long-poll and response mapping.
