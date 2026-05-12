# `cs fn logs` streaming view of live activations

**Parent epic:** #17
**Phase:** Now
**Estimated size:** L

## Problem
The control plane already persists per-activation logs and exposes them at
`GET /v1/tenants/{tenant}/activations/{activation_id}/logs` (see `cmd/cs-control/main.go:93`
and the `getActivationLogs`/`ListLogChunks` path). The CLI has no equivalent — to see
what a function did, an agent has to invoke an activation, copy the activation id, and
manually craft a `cs http invoke` call against the logs URL. There is no `--follow`,
no time-range filtering, and no per-function tailing.

## Proposed solution
- Add a new `fn logs` subcommand in `cmd/cs-cli/main.go` dispatching to a `fnLogs`
  function that accepts `--function <ref>`, `--activation <id>`, `--namespace`,
  `--follow`, `--since <dur|rfc3339>`, `--until <rfc3339>`, `--limit`, and `--output
  json|pretty|compact`.
- When `--activation` is set, page through `ListLogChunks` using the existing cursor
  (`?cursor=`, `?limit=`) and stream chunks as they arrive; when `--function` is set,
  list recent activations for that function (new control-plane endpoint
  `GET /functions/{name}/activations?since=...`) and merge their log streams.
- Implement `--follow` as a poll loop with bounded backoff (start 250 ms, cap 2 s,
  reset on data) reusing the cursor returned by the logs endpoint, so resumption is
  exact-once relative to the stored chunks; abort cleanly on `SIGINT` and on
  `ctx.Err()`.
- Output modes: `pretty` (default, ANSI-coloured `LEVEL ts activation_id message`),
  `compact` (one chunk per line, no decoration), `json` (NDJSON of the raw chunk
  envelope) — gate colour on `term.IsTerminal(os.Stdout)`.
- Add control-plane support if missing: a streaming-friendly `ListLogChunks` already
  exists; add the per-function activation listing endpoint and wire authz under
  `cs:activation:read`.
- Update `docs/05-cli.md` and `docs/04-api-rest.md` with the new flags and any new
  endpoint, and add a `cs fn logs` example to `docs/24-runbooks.md`.

## Acceptance criteria
- [ ] `cs fn logs --activation <id>` prints all chunks for that activation in the
      selected format and exits 0 once the activation is terminal.
- [ ] `cs fn logs --function <name>@<alias> --follow` tails new activations + new log
      chunks, prints them in order, and exits cleanly on `Ctrl-C` (no goroutine
      leak; verified via test).
- [ ] `--since 5m`, `--since 2026-05-01T00:00:00Z`, and `--until <ts>` filter chunks
      by activation start time; invalid values surface a typed CLI error with
      `next step:` hint.
- [ ] `--output json` emits NDJSON whose schema is documented in
      `docs/04-api-rest.md` and validated by a golden test.
- [ ] Polling honours an exponential backoff capped at 2 s, with at most one
      in-flight request per active activation, asserted by an `httptest.Server` test.
- [ ] `docs/05-cli.md`, `docs/04-api-rest.md`, and `docs/24-runbooks.md` are updated
      with the command, flags, and a worked example.

## Dependencies & risks
- Depends on `cs-control`'s log-chunk pagination cursor semantics (`ListLogChunks` in
  `cmd/cs-control/main.go`); verify the cursor monotonicity contract before relying
  on it for resumption.
- Risk: a chatty function under `--follow` could exhaust client memory if chunks are
  buffered — stream chunk-by-chunk and never accumulate beyond the current page.
- Risk: clock skew between client `--since` and server-side timestamps; resolve by
  letting the server interpret `since=` as a query param instead of client-side
  filtering.
- Depends on Task E2.02's typed-error wrapper for surfacing connection / auth issues
  cleanly inside the poll loop; can ship before it but should be retrofitted after.

## Out of scope
- Server-side push (WebSocket / SSE) — polling only for the first cut.
- Log search / full-text queries — only chronological tailing is in scope.
- Multi-tenant log aggregation — single tenant from the auth config.
- Log retention / TTL configuration — owned by storage subsystem.
