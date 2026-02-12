# Entity State Machines

This file defines state transitions as invariants.

## Function

States:

- `active`
- `deleted`

Transitions:

- `active → deleted` via function delete
- no transition out of `deleted`

## Draft

States:

- `created`
- `expired`
- `consumed`

Transitions:

- `created → expired` via TTL
- `created → consumed` via publish
- no transition out of `expired`
- no transition out of `consumed`

## Version

States:

- `published`

Versions are immutable.

## Alias

Alias points to a version.

States:

- `unset`
- `set`

Transitions:

- `unset → set` via alias set
- `set → set` via alias update

## Schedule

States:

- `enabled`
- `disabled`
- `deleted`

Transitions:

- `enabled → disabled` via API
- `disabled → enabled` via API
- `enabled → deleted` via delete
- `disabled → deleted` via delete

## WorkerBinding

States:

- `enabled`
- `disabled`
- `deleted`

Transitions mirror Schedule.

## Activation

States:

- `queued`
- `running`
- terminal:
  - `success`
  - `error`
  - `timeout`

Transitions:

- `queued → running` when invoker starts
- `running → success|error|timeout` on completion
- no transition out of terminal
