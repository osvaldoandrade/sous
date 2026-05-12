# Epic: Security hardening

**Phase:** Later (5–9 months)
**Theme:** Move beyond the baseline sandbox: integrate external secret stores, give tenants destination-level egress control, and ship an audit trail.

## Why
`code-sous` ships a strict baseline sandbox today (`docs/15-security.md`): the isolate
blocks fs, process, and raw sockets, and the runtime blocks private IPs. That baseline is
enough for individual agents, but enterprise tenants need centralized secret custody
outside the cluster, destination-level egress policy beyond a single per-version host list,
and an auditable record of every control-plane mutation. This epic raises `code-sous` from
"safe by default" to "deployable in compliance-bound environments" so agents can be
trusted with production workloads.

## Scope
- External secret providers (HashiCorp Vault, AWS Secrets Manager) wired through a plugin
  interface mirroring `plugins.authn.driver`.
- A per-tenant egress gateway that denies by default and supports host/CIDR + port +
  protocol allowlists, while preserving the existing private-IP-block invariant.
- A structured, tenant-consumable audit-event stream emitted by every mutating handler in
  `cs-control` and pluggable to codeQ topics, stdout, or webhook sinks.
- Documentation updates in `docs/15-security.md`, `docs/14-observability.md`, and
  `docs/20-config-reference.md` to match the new surfaces.

## Outcomes / success metrics
- Secret-rotation cycle: tenants can rotate a referenced secret in their vault and the
  next activation picks up the new material without a redeploy; rotation propagates in
  under one minute end-to-end on the reference workload.
- Egress-deny rate: `cs_egress_denied_total` is observable per tenant; reference tenant
  hits 100% deny on non-allowlisted hosts in conformance tests; added p99 latency from
  the gateway stays under 5 ms.
- Audit-event coverage: 100% of mutating `cs-control` handlers emit exactly one event
  on success and one on failure, verified by an integration test that diffs the handler
  registry against the emitted action set.
- Compliance posture: SOC 2 / ISO control mappings can cite specific event types,
  policy objects, and provider integrations rather than ad-hoc procedures.

## Tasks
- [ ] #23 — External vault secret provider integration
- [ ] #24 — Per-tenant network egress gateway with allowlist policies
- [ ] #25 — Audit log stream — every control-plane mutation

## Non-goals
- Replacing Tikti as the auth/authz substrate.
- Building a tenant-facing UI for secrets, egress policy, or audit exploration in this
  epic (APIs and CLI verbs only).
- Bundle signing with tenant-owned keys (tracked under the supply-chain roadmap item).
- Cryptographic tamper-evidence on the audit log (potential follow-up once the stream
  is stable).
- Egress quotas (bytes/min); this epic delivers policy, not throttling.
