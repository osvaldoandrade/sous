# Overview

SOUS is a serverless execution layer built for agent-generated automation. Like mature serverless platforms, it gives you a stable runtime contract, multiple trigger types, and a clear isolation boundary. Unlike many platforms, SOUS intentionally optimizes for "functions as text" and deterministic parity between local execution (CLI) and cluster execution (invoker pool).

The product intent is simple: an agent receives a task, generates a small function that encodes the task logic, runs that function locally to validate behavior, publishes it to the cluster, and invokes it through a trigger that matches the workflow (HTTP, schedule, or Cadence).

## The Function Contract

A SOUS function version is a bundle of UTF-8 text files:

- `function.js` exports an async handler.
- `manifest.json` declares limits, capabilities, and role allowlists.

That design removes build steps and makes publishing deterministic. The platform stores the canonical bundle addressed by `sha256` and treats published versions as immutable.

## Lifecycle: Drafts, Versions, Aliases

The lifecycle is designed to avoid ambiguity:

A draft is an upload with a TTL. It is mutable and disposable.

A version is an immutable published artifact. Once created, its code and manifest never change.

An alias is a mutable pointer to a version. Production traffic typically targets aliases, not raw version numbers.

This is why rollouts are safe: changing an alias does not change history.

## Triggers and Activations

SOUS supports three trigger families:

- HTTP invoke through `cs-http-gateway`.
- Interval schedules through `cs-scheduler`.
- Cadence Activity execution through `cs-cadence-poller`.

All triggers converge into a single execution fabric: `cs-invoker-pool` consumes `InvocationRequest` messages and produces `InvocationResult` messages.

Every invocation produces an Activation record. Activation metadata and logs are persisted (KVRocks by default) and can be queried for debugging and audit.

## Privileges: Roles and Capabilities

SOUS treats user code as untrusted. The runtime provides host APIs, but every side effect is gated behind explicit capabilities declared in the manifest.

There are two enforcement layers:

- Role allowlists decide who may invoke a version (and through which trigger type).
- Capability allowlists decide what that version is allowed to do once it runs (KV ops, codeQ publish, HTTP fetch, etc.).

This forces a clean contract: a function cannot accidentally gain new power without a version change.

## Reading Path

If you want the system from first principles, read:

1. [Architecture](Architecture)
2. [Runtime: cs-js](Runtime-cs-js)
3. [Invoker Pool](Invoker-Pool)
4. [REST API](REST-API)
