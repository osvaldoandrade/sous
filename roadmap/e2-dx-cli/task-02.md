# Diagnostic CLI errors + `cs doctor`

**Parent epic:** #17
**Phase:** Now
**Estimated size:** M

## Problem
The current CLI surfaces errors as raw text (`fmt.Errorf("server error (%d): %s", ...)`
in `cmd/cs-cli/main.go:606`, and `errors.New("invalid auth config")` in
`loadAuthConfig`). When the control plane is down, a tenant header is missing, or the
local runtime panics, the user sees a single noisy line with no remediation. Agents
have no programmatic signal to differentiate "auth not configured" from "control plane
unreachable" beyond the coarse 1/2/3 exit codes in `main()`.

## Proposed solution
- Introduce a typed CLI error wrapper in a new `internal/cli/clierr` package (or
  extend `internal/errors`) exposing `Error{Code, Cause, Hint, DocsURL}`; format it
  at the top of `main()` with `cause:` and `next step:` sections and keep the 1/2/3
  exit code mapping driven by `Code` instead of substring matching on the message.
- Replace bare `errors.New` / `fmt.Errorf` sites in `handleAuth`, `handleFunction`,
  `doJSON`, `loadAuthConfig`, and `handleSchedule` with structured errors that include
  remediation hints (e.g., control-plane unreachable suggests
  `./bin/cs-control` or `--api-url`; missing `auth.json` suggests `cs auth login`).
- Add a `cs doctor` top-level subcommand that runs an ordered probe set: (1) auth
  config presence + token decode, (2) `GET {api_url}/healthz` reachability, (3) the
  control plane's runtime parity endpoint (or fall back to `cs fn test` against an
  embedded canary bundle), (4) writable `os.UserConfigDir()/code-sous/`, (5) clock
  skew vs. the control-plane response date header.
- `cs doctor` reports per-check `ok`/`warn`/`fail` rows in a fixed-width table by
  default, with `--json` for machine output; non-zero exit if any check is `fail`.
- Wire a `--verbose` (or `CS_DEBUG=1`) toggle that prints the raw response body,
  request URL, and the structured error fields when a command fails.
- Update `docs/05-cli.md` with a "Troubleshooting" section and a `cs doctor` worked
  example; cross-link from `docs/21-errors.md`.

## Acceptance criteria
- [ ] All error paths in `cmd/cs-cli/main.go` route through the new typed error and
      print both `cause:` and `next step:` lines; exit codes are driven by `Code`,
      not by `strings.Contains` on the error string.
- [ ] `cs doctor` prints at least five checks (auth, control-plane reachability,
      runtime parity, config dir writability, clock skew) and returns exit 0 on a
      healthy local stack and non-zero when any check fails.
- [ ] `cs doctor --json` emits a stable schema (array of `{name, status, detail,
      hint}`) suitable for agent parsing, asserted by a golden test.
- [ ] When the control plane is unreachable, every command (not just `doctor`)
      surfaces the message `control plane unreachable at <url> — start cs-control or
      pass --api-url` and exits with the server-error exit code (2).
- [ ] Go tests cover: (a) error formatting and exit-code mapping, (b) `cs doctor`
      against a mocked `httptest.Server` for both healthy and degraded responses.
- [ ] `docs/05-cli.md` and `docs/21-errors.md` document the new error shape and
      `cs doctor` flags.

## Dependencies & risks
- No new control-plane endpoints are strictly required, but `GET /healthz` should be
  guaranteed to exist on `cs-control`; confirm or add it under `cmd/cs-control/`.
- Risk: agent prompts depending on the current loose error strings may break — keep
  the human-readable line first and put structured detail on subsequent lines.
- Risk: clock-skew check requires the control plane to emit a `Date` header — verify
  in `cmd/cs-control/main.go` middleware before relying on it.

## Out of scope
- Auto-remediation (e.g., starting `cs-control` for the user); `doctor` only diagnoses.
- Telemetry / phone-home of diagnostic results.
- Rewriting non-CLI services to use the new error envelope (handled by `internal/errors`).
- Localized / i18n error messages.
