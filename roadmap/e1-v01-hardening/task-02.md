# Activation persistence + log API hardening

**Parent epic:** #16
**Phase:** Now
**Estimated size:** M

## Problem
`docs/02-requirements.md` specifies activation TTL default of 7 days, 256 KiB max result, and 1 MiB max logs per activation, but the current `internal/kv` activation store and `cmd/cs-control` activation/log endpoints do not consistently enforce these caps, nor do they paginate the `GET activations/{id}` log stream. Today large logs can balloon KVRocks and the log API can return an unbounded blob.

## Proposed solution
- In `internal/kv`, add `ActivationTTLSeconds` (default 604800, configurable from `cs_control.limits.activation_ttl_seconds`) and apply it as a per-key TTL on activation metadata, log chunks, and result blobs when written by `cs-invoker-pool`. Add a background sweeper test to confirm TTL eviction.
- In `cmd/cs-control` and `internal/api`, enforce hard caps at write time: 256 KiB for `result`, 1 MiB cumulative for logs per activation; truncate cleanly on a UTF-8 boundary, append a sentinel record `{"truncated": true, "reason": "log_limit_exceeded", "limit_bytes": 1048576}`, and surface a `X-CS-Truncated: logs` (or `result`) response header on read.
- Extend `GET /v1/tenants/{tenant}/activations/{activation_id}/logs` with pagination: `?cursor=<opaque>&limit=<n>` and a `next_cursor` field in the response, backed by chunked keys (`act:{id}:log:{seq}`). Provide a streaming variant via `Accept: application/x-ndjson` that emits one log line per chunk so the CLI can tail without buffering.
- Update `cmd/cs-cli` (`cs activation logs <id> --follow`) to consume the streaming endpoint and stop at EOF or truncation sentinel.
- Document the new headers, query params, streaming media type, and TTL behavior in `docs/04-api-rest.md`, `docs/26-capacity-and-limits.md`, and `docs/24-runbooks.md` (operator runbook for "activation log volume too high").

## Acceptance criteria
- [ ] Activation records, log chunks, and result blobs all carry the configured TTL; integration test asserts a 7-day default and a configurable override (e.g., 60s in test).
- [ ] Writes that exceed 256 KiB result or 1 MiB logs are cleanly truncated; the API response includes the truncation sentinel and the `X-CS-Truncated` header, and the original execution still records a `success` status when the function itself succeeded.
- [ ] `GET .../logs` supports cursor pagination and the ndjson streaming variant; both are documented in `docs/04-api-rest.md` with examples.
- [ ] `cs activation logs <id> --follow` streams chunks and exits cleanly when truncated.
- [ ] `docs/26-capacity-and-limits.md` lists the enforced limits and `docs/24-runbooks.md` covers the operator response when activation TTL or log caps trigger alerts.

## Dependencies & risks
- Depends on KVRocks server supporting per-key TTLs (it does via `EXPIREAT`); verify with `internal/kv/store_test.go` fakes.
- Risk: TTL eviction may break audit retention if ledgerDB ingestion lags; mitigate by emitting an audit event on truncation and on TTL set, and by documenting the audit boundary in `docs/22-ledgerdb-audit.md`.
- Risk: paginated reads of very chatty functions may still be slow; add an index key `act:{id}:log:cnt` to short-circuit empty pages.

## Out of scope
- Cross-tenant log search (future observability work in `docs/14-observability.md`).
- Long-term cold storage of activation logs beyond TTL.
- Changes to runtime log capture inside `cs-js` itself.
