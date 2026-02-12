# Testing

This file defines test layers and acceptance gates.

## Unit tests

- Manifest parsing and validation.
- KVRocks key builders.
- Tikti authorization evaluator.
- codeQ envelope marshal and unmarshal.
- HTTP event mapping.

## Integration tests

### Local docker-compose

The repo provides `docker-compose.yml` for:

- KVRocks
- codeQ
- Tikti mock
- code-flow mock (Cadence API subset)
- code-sous services

### Test cases

- Publish a version and invoke via API.
- Invoke via HTTP gateway and validate response mapping.
- Create a schedule and validate tick publishes.
- Register WorkerBinding and validate ActivityTask flow.

## End-to-end tests

- Run `cs` CLI against a real cluster.
- Create function, publish, set alias.
- Invoke by alias and read activation logs.
- Run schedule for 60 ticks and verify activation count.
- Run cadence activity and verify completion.

## Fuzz tests

- Fuzz HTTP headers and query parsing.
- Fuzz manifest JSON fields.
- Fuzz sandbox return types.

## Chaos tests

- Kill invoker pods during execution.
- Kill poller pods during inflight tasks.
- Inject codeQ delays and drops.
