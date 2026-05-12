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

## Exit codes

- `0` success
- `1` client error
- `2` server error
- `3` runtime error
