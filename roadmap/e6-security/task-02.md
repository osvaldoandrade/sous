# Per-tenant network egress gateway with allowlist policies

**Parent epic:** #30
**Phase:** Later
**Estimated size:** L

## Problem
Today the runtime only blocks private IP ranges and applies a coarse `http.allowHosts` list
per function version (`docs/15-security.md` "Network egress"). Tenants cannot define a
destination policy that spans functions, cannot allow CIDR ranges, cannot restrict ports
or protocols, and have no central observation point for outbound traffic. This blocks
tenants who need to certify "outbound destinations are X, Y, Z only" for compliance.

## Proposed solution
- Add a `cs-egress-gateway` egress proxy (new package under `internal/runtime/egress`
  plus a thin binary launched alongside `cs-invoker-pool`). The proxy denies by default,
  enforces a per-tenant allowlist, terminates and re-originates TLS where the tenant
  policy permits, and stamps every connection with `tenant`, `function`, `version`,
  `activation_id`.
- Extend `cs.http.fetch` and any future `cs.tcp` capability in `internal/runtime` to route
  all egress through the gateway via a SOCKS5/HTTP-CONNECT bridge bound to localhost in
  each invoker worker. Direct dial paths in user code stay blocked by the existing
  isolate invariants.
- Add a tenant-scoped policy object (`EgressPolicy`) stored through the control plane:
  destinations as `{host|cidr, port_range, protocol(http|https|tcp), purpose}`,
  versioned, with `cs-control` enforcing `cs:egress:policy:*` actions through Tikti.
- Preserve the private-IP block invariant from `docs/02-requirements.md`: the gateway
  rejects any allowlist entry that resolves to a blocked range and re-checks at
  connection time against DNS rebinding.
- Update `docs/15-security.md` "Network egress" with the new architecture, the policy
  schema, the deny-by-default contract, and the metrics emitted by the gateway
  (`cs_egress_allowed_total`, `cs_egress_denied_total{reason}`,
  `cs_egress_bytes_total{direction}`).

## Acceptance criteria
- [ ] Deny-by-default: with no `EgressPolicy`, all outbound calls from user code fail
      with `EgressDenied` and a structured reason (`no-policy`, `host-not-allowed`,
      `port-not-allowed`, `protocol-not-allowed`, `private-range`).
- [ ] Allowlist supports hostnames, IPv4/IPv6 CIDRs, port ranges, and protocol selectors;
      policy validation rejects entries that collide with the private-range invariant.
- [ ] DNS resolution happens inside the gateway; resolved IPs are re-checked against the
      private-range list to defeat rebinding.
- [ ] Per-tenant rate metrics and deny metrics surface in Prometheus and are queryable per
      tenant/function/version.
- [ ] `cs egress policy {get|set|diff}` CLI verb manages policies; control plane enforces
      Tikti actions `cs:egress:policy:read` and `cs:egress:policy:write`.
- [ ] Docs updated: `docs/15-security.md` "Network egress" + a new section in
      `docs/20-config-reference.md` for `cs_egress_gateway`.
- [ ] Tests: policy validator unit tests, integration test that proves a fetch to a
      private IP and to a non-allowlisted public host both fail, and a load test
      showing p99 added latency stays under target.

## Dependencies & risks
- Prereq: stable per-tenant identity propagation from invoker workers (already in place via
  Tikti service principals).
- External: choice of egress proxy substrate (custom Go proxy vs. embedding an existing
  library). Mitigation: prototype with `golang.org/x/net/proxy` and benchmark.
- Risk: added latency on every outbound call. Mitigation: in-process gateway with
  connection pooling; SLO budget for added p99 < 5 ms.
- Risk: tenants encode brittle hostnames that change behind CDNs. Mitigation: support
  wildcard suffixes (`*.example.com`) with explicit safeguards.
- Risk: policy drift across clusters. Mitigation: policies versioned and pinned through
  control plane, same publish atomicity as function versions.

## Out of scope
- Inbound network policy (covered by gateway + cluster ingress).
- Layer-7 payload inspection / content filtering.
- Egress quotas (bytes/min) — separate roadmap item.
- mTLS to tenant-owned destinations — phase 2 once policy is stable.
