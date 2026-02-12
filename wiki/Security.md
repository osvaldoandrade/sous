# Security

This file defines the security model and enforcement points.

## Threat model

The platform runs untrusted code from tenant users.
The platform must prevent:

- cross-tenant data access
- host compromise
- data exfiltration beyond explicit allowlists
- denial of service through resource exhaustion

## Sandbox model

The invoker runs user code in an isolate.

The isolate blocks:

- filesystem access
- process spawning
- raw network sockets
- native module loading

User code accesses the outside world through `cs.*`.

## Capability allowlists

The platform enforces allowlists at runtime:

- KVRocks key prefixes
- codeQ topic prefixes
- HTTP host allowlist

The invoker denies a call that violates a capability.

## Secrets

The platform does not inject secrets by default.

A version may reference secrets by name:

- `secrets: ["kv:tenant/payments/stripe_key"]`

The invoker resolves secrets through a secret provider.
In v0.1 the secret provider reads from KVRocks.

The invoker redacts secrets from logs.

## Network egress

The runtime denies egress by default.

If `http.allowHosts` exists:

- `cs.http.fetch` allows only those hostnames.

The runtime blocks private IP ranges:

- 10.0.0.0/8
- 172.16.0.0/12
- 192.168.0.0/16
- 127.0.0.0/8
- ::1/128
- fc00::/7

## Request validation

The gateway enforces:

- max body size
- max header size
- max query size

The control plane enforces:

- bundle size
- manifest schema validity

## Supply chain

The control plane computes `sha256` for every published bundle.
The invoker verifies `sha256` on load.

The control plane rejects publish when:

- `sha256` mismatches draft record

## Rate limiting

The gateway rate limits by:

- tenant
- function ref
- client IP

Default policy:

- 200 rps per tenant per cluster
- 20 rps per function ref per cluster

The gateway returns `429` on limit breach.
