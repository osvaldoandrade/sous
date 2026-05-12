# CLI (`cs`)

The CLI runs function code locally and interacts with the control plane.

## Installation

The project ships `cs` as a single binary.

## Auth

### Login

```
cs auth login --tikti-url https://tikti.example.com --tenant t_abc123
```

The CLI stores the token in:

- `$HOME/.config/code-sous/auth.json`

### Whoami

```
cs auth whoami
```

## Function lifecycle

### Create

```
cs fn create reconcile --namespace payments --runtime cs-js
```

### Init scaffold

```
cs fn init reconcile
cs fn init reconcile --template scheduled-job
cs fn init --list
```

This command writes:

- `function.js`
- `manifest.json`
- `README.md` (template-specific guide)

`--template <name>` selects the scaffold (default: `http-handler`). An
unknown name exits with code `1` (client error) and prints the available
templates. `--list` prints one template per line with its one-line
description.

#### Templates

| Template | Trigger | Capability stanza |
| --- | --- | --- |
| `http-handler` (default) | HTTP gateway (`api`) | `kv: ctr:` · `codeq: jobs.*` · `http: api.example.com` |
| `scheduled-job` | `cs-scheduler` (`cs.timer`) | `kv: job:` · `codeq: jobs.scheduled.*` · `http: []` |
| `cadence-activity` | `cs-cadence-poller` (`cadence`) | `kv: act:` · `codeq: cadence.activity.*` · `http: api.example.com` |
| `codeq-consumer` | codeQ subscription | `kv: idem:` · `codeq: consumer.*` · `http: []` |

Example `--list` output:

```
$ cs fn init --list
cadence-activity        Cadence activity handler dispatched by `cs-cadence-poller`.
codeq-consumer          codeQ consumer that processes subscribed messages with idempotent state.
http-handler            HTTP handler invoked via `cs http invoke` or `cs fn invoke` with a JSON event.
scheduled-job           Scheduled job invoked by `cs-scheduler` on a fixed interval.
```

Each template's `manifest.json` ships a capability stanza tailored to the
trigger:

`http-handler`:

```json
"capabilities": {
  "kv": { "prefixes": ["ctr:"], "ops": ["get", "set", "del"] },
  "codeq": { "publishTopics": ["jobs.*"] },
  "http": { "allowHosts": ["api.example.com"], "timeoutMs": 1500 }
}
```

`scheduled-job`:

```json
"capabilities": {
  "kv": { "prefixes": ["job:"], "ops": ["get", "set", "del"] },
  "codeq": { "publishTopics": ["jobs.scheduled.*"] },
  "http": { "allowHosts": [], "timeoutMs": 1500 }
}
```

`cadence-activity`:

```json
"capabilities": {
  "kv": { "prefixes": ["act:"], "ops": ["get", "set", "del"] },
  "codeq": { "publishTopics": ["cadence.activity.*"] },
  "http": { "allowHosts": ["api.example.com"], "timeoutMs": 5000 }
}
```

`codeq-consumer`:

```json
"capabilities": {
  "kv": { "prefixes": ["idem:"], "ops": ["get", "set", "del"] },
  "codeq": { "publishTopics": ["consumer.*"] },
  "http": { "allowHosts": [], "timeoutMs": 1500 }
}
```

Every scaffold passes `cs fn test` against its sample event out of the box
(`status=success`).

Templates are packaged into the binary via `//go:embed`
(`internal/cli/templates/`); no runtime filesystem lookup or network call
is required to scaffold a function.

### Test locally

```
cs fn test reconcile --event ./event.json
```

The CLI:

- loads `function.js` and `manifest.json`
- runs the same `cs-js` runtime used by `cs-invoker-pool`
- prints `result` and exit code

### Upload draft

```
cs fn draft upload reconcile --path .
```

### Publish

```
cs fn publish reconcile \
  --draft drf_01H... \
  --timeout-ms 3000 \
  --memory-mb 64 \
  --invoke-http-roles admin \
  --invoke-schedule-roles admin \
  --invoke-cadence-roles admin
```

### Set alias

```
cs fn alias set reconcile prod --version 17
```

Alias swaps are atomic at the control plane: a concurrent invoker
resolving the alias observes either the old or the new version, never
a torn or empty value. `cs fn publish` rejects expired drafts (TTL
default 24h) with exit code `1` and a `CS_VALIDATION_FAILED` error.
See [`19-entity-state-machines.md` ("Lifecycle invariants")](19-entity-state-machines.md)
for the full contract.

## Invoke

### Invoke by alias (sync)

```
cs fn invoke reconcile@prod --event ./event.json
```

### Invoke by version (sync)

```
cs fn invoke reconcile@17 --event ./event.json
```

### HTTP invoke

```
cs http invoke /v1/web/t_abc123/payments/reconcile/prod -X POST -d @event.json
```

## Logs

### Tail activation logs

```
cs fn logs --activation <activation_id>
cs fn logs --activation <activation_id> --follow
cs fn logs --activation <activation_id> --format json
cs fn logs --activation <activation_id> --since 5m
```

The CLI fetches log chunks from `GET /v1/tenants/{tenant}/activations/{id}/logs`
using cursor pagination (`?cursor=...&limit=...`) and prints each chunk as it
arrives. With `--follow` it polls the control plane until interrupted with
`Ctrl-C`; the poll interval (default 1s) doubles up to 2s when no new chunks
arrive and resets to the configured interval as soon as data flows again.

Flags:

| Flag | Description |
| --- | --- |
| `--activation <id>` | Activation ID to tail (required for the first cut). |
| `--function <ref>` | Function reference (`name` or `name@alias`); informational while the control plane does not yet expose per-function activation listing. |
| `--namespace <ns>` | Namespace (default: `default`). |
| `--follow` | Tail new chunks instead of exiting after the first page. |
| `--since <duration>` | Drop activations whose `start_ms` is older than `<duration>` ago. Accepts any `time.ParseDuration` value (e.g. `30s`, `5m`, `1h`). |
| `--format <pretty\|json\|compact>` | Output format (default: `pretty`). |
| `--poll-interval <duration>` | Initial polling interval in `--follow` mode (default: `1s`). |
| `--limit <n>` | Maximum chunks per request (1-500, default: `100`). |
| `--api-url <url>` | Override the control-plane URL from the auth config. |

Output formats:

- `pretty` (default): `<rfc3339> <LEVEL> <message>`; ANSI colours per level
  when stdout is a TTY and `NO_COLOR` is unset.
- `json`: one chunk per line as JSON with `sequence`, `activation_id`,
  `level`, `message`, `raw`.
- `compact`: `[<level>] <message>` — strips timestamps for log scraping.

Examples:

```
# Print existing logs for an activation and exit.
cs fn logs --activation a7e1c1f0-...

# Follow a running activation; Ctrl-C exits 0.
cs fn logs --activation a7e1c1f0-... --follow

# Emit NDJSON for tooling.
cs fn logs --activation a7e1c1f0-... --format json | jq -r '.message'

# Only show output from recent activations.
cs fn logs --activation a7e1c1f0-... --since 30s
```

Errors surface with a `next step:` hint when applicable; for example,
invoking `cs fn logs --function reconcile@prod` without `--activation` exits
with a message pointing the user at `cs fn invoke` to retrieve an activation
id.

## Schedule

```
cs schedule create reconcile_30s --every 30 --fn reconcile@prod --payload payload.json
cs schedule delete reconcile_30s
```

## Cadence worker binding

```
cs cadence worker create payments-activities \
  --domain payments \
  --tasklist payments-activities \
  --worker-id cs-payments-01 \
  --activity SousInvokeActivity=reconcile@prod
```

## Doctor

`cs doctor` probes the local environment for common configuration and
connectivity issues. It is the first thing to run when a CLI command fails
unexpectedly.

```
cs doctor [--api-url <url>] [--json] [--timeout-ms <ms>]
```

Checks performed (in order):

1. **auth** — the auth config exists, is parseable, and contains
   `tenant` + `token`.
2. **control-plane** — `GET <api_url>/healthz` returns 2xx within the timeout.
3. **runtime** — the bundled `cs-js` runtime executes the same canary bundle
   used by `cs fn test`.
4. **config-dir** — the user config directory (`$XDG_CONFIG_HOME/code-sous`
   or `$HOME/.config/code-sous`) is writable.

Flags:

- `--api-url <url>` overrides the control-plane URL from the auth config.
- `--json` emits a machine-readable report (`{checks: [{name, status,
  detail, hint}]}`) suitable for agent parsing.
- `--timeout-ms <ms>` HTTP probe timeout (default 2000ms).

Example (healthy stack):

```
$ cs doctor
check          status  detail
auth           pass    tenant=t_abc123 api_url=http://localhost:8080
control-plane  pass    GET http://localhost:8080/healthz -> 200
runtime        pass    cs-js canary executed successfully
config-dir     pass    /home/me/.config/code-sous
```

Example (control plane down):

```
$ cs doctor --api-url http://localhost:65535
check          status  detail
auth           pass    tenant=t_abc123 api_url=http://localhost:8080
control-plane  fail    Get "http://localhost:65535/healthz": dial tcp ... — control plane unreachable at http://localhost:65535 — start `./bin/cs-control` or pass `--api-url`
runtime        pass    cs-js canary executed successfully
config-dir     pass    /home/me/.config/code-sous
error: doctor: control-plane probe failed
next step: control plane unreachable at http://localhost:65535 — start `./bin/cs-control` or pass `--api-url`
$ echo $?
2
```

### Exit codes

- `0` all checks passed
- `1` an `auth` or `config-dir` check failed (client-side problem)
- `2` the `control-plane` check failed (server-side problem)
- `3` the `runtime` check failed

## Diagnostic CLI errors

Errors from any `cs` subcommand follow a three-line shape so users and agents
can extract the actionable next step without parsing free-form prose:

```
<one-line summary>
cause: <wrapped error>
next step: <action the user should take>
```

For example, a control-plane outage reports:

```
error: control plane unreachable at http://localhost:8080
cause: Get "http://localhost:8080/v1/...": dial tcp 127.0.0.1:8080: connect: connection refused
next step: start `./bin/cs-control` or pass `--api-url`; run `cs doctor` for details
```

The CLI's exit code reflects the error class (see "Exit codes" below).

## Exit codes

- `0` success
- `1` client error (missing flag, missing/invalid config, validation failure)
- `2` server error (control plane unreachable or returned 4xx/5xx)
- `3` runtime error (local `cs-js` execution failed)
