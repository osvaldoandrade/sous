# Requirements

This document defines product scope as invariants.

## Product intent

Agents create functions to close a task loop.

A task loop looks like this:

1. An agent receives a task.
2. The agent generates a function that encodes the task logic.
3. The agent runs the function locally with the `cs` CLI.
4. The agent publishes the function to the cluster.
5. The agent invokes the published function through HTTP, schedule, or Cadence.

The platform exists to reduce friction in steps 3 to 5.

## Core invariants

### No build step

- The platform must accept function code as UTF-8 text.
- The platform must accept a JSON manifest as UTF-8 text.
- The platform must not require a compilation step at publish time.

### Runtime parity

- The CLI must use the same runtime semantics as the server.
- The runtime must expose the same host APIs locally and remotely.
- The runtime must return the same response shape for the same input.

### Immutable versions

- A published version is immutable.
- An alias points to a specific version.
- An alias update changes the alias pointer, not the version.

### Uniform execution fabric

- HTTP, schedule, and Cadence triggers must use the same execution engine.
- `cs-invoker-pool` is that engine.

### Explicit privileges

- A function version must declare:
  - allowed roles per trigger type
  - allowed side effects through capabilities
- The runtime must enforce capabilities on every call.

## Functional requirements

### Function lifecycle

- The control plane must support create, read, delete.
- The control plane must support draft upload with TTL.
- The control plane must support publish with atomic version assignment.
- The control plane must support alias CRUD.

### Invocation

- The system must support synchronous HTTP invocation.
- The system must support asynchronous invocation via codeQ.
- The system must support interval schedules in seconds.
- The system must support Cadence Activity execution.

### Activations

- The system must persist an Activation record for every invocation.
- The system must persist user logs per Activation.
- The system must expose activation metadata and logs via API.

### Cadence worker mode

- The system must let a tenant register WorkerBindings.
- Each WorkerBinding must include:
  - domain
  - tasklist
  - worker identity
  - mapping from ActivityType to FunctionRef
- The poller must long-poll for Activity tasks.
- The poller must respond to Cadence with completion or failure.

## Non-functional requirements

### Timeouts

- Default HTTP invoke timeout: 3,000 ms.
- Max HTTP invoke timeout: 30,000 ms.
- Default worker timeout: 30,000 ms.
- Max worker timeout: 900,000 ms.

### Size limits

- Max published bundle size: 16 MiB.
- Max HTTP request body size: 6 MiB.
- Max function result size: 256 KiB.
- Max logs per activation: 1 MiB.

### Retention

- Draft TTL: 24 hours.
- Activation TTL default: 7 days.

### Availability targets

- Control plane monthly availability target: 99.9%.
- Data plane monthly availability target: 99.9%.

### Security invariants

- The runtime must block filesystem access by user code.
- The runtime must block process spawn by user code.
- The runtime must deny network egress by default.
- The runtime must block private IP ranges even when egress allowlists exist.
