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
cs fn init reconcile --runtime cs-js
```

This command writes:

- `function.js`
- `manifest.json`

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
