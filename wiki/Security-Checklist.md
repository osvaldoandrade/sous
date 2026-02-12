# Security Checklist

This document defines a pre-production checklist.

## 1. Isolation

- User code runs inside an isolate per activation.
- User code cannot import host filesystem modules.
- User code cannot spawn processes.
- User code cannot open raw sockets.

## 2. Capability enforcement

- `cs.kv.*` validates key prefixes.
- `cs.kv.*` validates allowed ops.
- `cs.codeq.publish` validates topic prefixes.
- `cs.http.fetch` validates hostname allowlist.
- `cs.http.fetch` blocks private IP ranges.

## 3. Network controls

- Gateways are the only public ingress.
- Invokers can reach KVRocks and codeQ only.
- Pollers can reach code-flow, KVRocks, codeQ.

## 4. Token and role controls

- All external requests require a Tikti token.
- Service principals exist for internal components.
- Tokens rotate through Kubernetes Secrets.

## 5. Data controls

- Activation logs and results store under tenant keys.
- Log redaction removes secrets and tokens.
- Result truncation prevents oversized writes.

## 6. Denial of service controls

- HTTP gateway enforces body, header, and query size limits.
- HTTP gateway rate limits by tenant and function ref.
- Invoker enforces max inflight per replica.
- Poller enforces max inflight tasks per binding.

## 7. Supply chain controls

- Control plane computes sha256 for bundles.
- Invoker verifies sha256 before execution.
- Images pin base digests in Helm values.

## 8. Observability controls

- Metrics expose error rate and queue lag.
- Logs include request_id and activation_id.
- Traces propagate traceparent end-to-end.

## 9. Audit controls

- Control plane emits audit events for mutations when enabled.
- Audit events include actor sub and roles.
