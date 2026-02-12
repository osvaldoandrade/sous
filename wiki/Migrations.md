# Migrations

This file defines how to evolve storage and message formats.

## Versioning policy

The platform versions:

- REST API path (`/v1`)
- message schema strings (`cs.invoke.v1`)
- manifest schema strings (`cs.function.script.v1`)
- KVRocks record layouts (`meta.schema` fields)

## Backward compatibility

The control plane must:

- read all prior versions for at least one minor release
- write only the latest version

The invoker must:

- execute bundles with manifest schema versions listed in config
- reject unknown schema versions

## KVRocks migrations

KVRocks stores JSON records without server-side schema enforcement.

Migration strategy:

- add `schema` field to every JSON record
- use read-time upgrade functions for older schemas
- provide a `cs-migrate` job that rewrites old records when needed

## codeQ migrations

Message schemas are immutable.

Producers publish the new schema on a new `schema` string.
Consumers support both schema strings during a migration window.

## Rollback rules

A rollback requires:

- control plane writes remain compatible with old invokers
- invokers accept the manifest schemas in use
