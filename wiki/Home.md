# SOUS Wiki

SOUS (code-sous) is a function execution layer for agent-built automation.

It is closer to AWS Lambda or Google Cloud Functions than to a traditional job runner, but it makes a few opinionated choices that are essential for agent-driven development:

SOUS accepts function code as plain UTF-8 text (no build step), forces runtime parity between local CLI and cluster execution, and treats privileges as explicit contracts (roles per trigger type, and capabilities per side-effect). These choices make it possible for an agent to generate a function, run it locally, publish it to a cluster, and get the same semantics and failure modes in both environments.

## Quick Start Path

1. [Get Started](Get-Started)
2. [Overview](Overview)
3. [Architecture](Architecture)
4. [Runtime: cs-js](Runtime-cs-js)
5. [REST API](REST-API)

## Documentation Map

### Start Here

- [Get Started](Get-Started)
- [Overview](Overview)
- [Architecture](Architecture)

### Concepts

- [Function Lifecycle](Concepts-Function-Lifecycle)
- [Invocation and Activations](Concepts-Invocations-and-Activations)
- [Capabilities and Isolation](Concepts-Capabilities-and-Isolation)

### Triggers

- [HTTP Invoke Path](HTTP-Invoke-Path)
- [Scheduler](Scheduler)
- [Cadence Integration](Cadence-Integration)

### Integration

- [IAM with Tikti](IAM-with-Tikti)
- [codeQ Protocol](codeQ-Protocol)
- [Storage: KVRocks](Storage-KVRocks)
- [ledgerDB Audit](ledgerDB-Audit)

### Interfaces

- [REST API](REST-API)
- [CLI](CLI)
- [Schemas](Schemas)

### Operations

- [Deployment: Kubernetes](Deployment-Kubernetes)
- [Observability](Observability)
- [Security](Security)
- [Error Model](Error-Model)
- [Runbooks](Runbooks)
- [Migrations](Migrations)
- [Capacity and Limits](Capacity-and-Limits)
- [Security Checklist](Security-Checklist)

### Use Cases

- [Use Cases](Use-Cases)

