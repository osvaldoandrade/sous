# IAM with Tikti

`code-sous` uses Tikti for authentication and authorization.

The platform uses role-based checks with explicit action names.

## Token validation

The API gateway validates bearer tokens.

Validation outputs:

- `sub`
- `tenant`
- `roles[]`
- token expiry

The gateway forwards these values to internal services.

## Actions

The platform defines these actions:

- `cs:function:create`
- `cs:function:read`
- `cs:function:delete`
- `cs:function:draft:upload`
- `cs:function:publish`
- `cs:function:alias:set`
- `cs:function:invoke:http`
- `cs:function:invoke:api`
- `cs:function:invoke:schedule`
- `cs:function:invoke:cadence`
- `cs:schedule:create`
- `cs:schedule:delete`
- `cs:schedule:read`
- `cs:cadence:worker:create`
- `cs:cadence:worker:delete`
- `cs:cadence:worker:read`
- `cs:activation:read`

## Resources

The platform uses an ARN-like string:

- `cs:{tenant}:{namespace}:function/{name}`
- `cs:{tenant}:{namespace}:function/{name}:version/{version}`
- `cs:{tenant}:{namespace}:function/{name}:alias/{alias}`
- `cs:{tenant}:{namespace}:schedule/{name}`
- `cs:{tenant}:{namespace}:cadence-worker/{name}`
- `cs:{tenant}:activation/{activation_id}`

## Enforcement points

- `cs-control` enforces lifecycle actions.
- `cs-http-gateway` enforces HTTP invoke actions.
- `cs-scheduler` enforces schedule invoke actions.
- `cs-cadence-poller` enforces cadence invoke actions.
- `cs-invoker-pool` enforces capability allowlists at runtime.

## Per-version role allowlists

Version config contains:

- `authz.invoke_http_roles[]`
- `authz.invoke_schedule_roles[]`
- `authz.invoke_cadence_roles[]`

The gateway checks:

- request principal roles intersect allowlist

If allowlist is empty:

- deny invoke

## Service principals

Internal components use Tikti service principals.

- `cs-http-gateway` uses `sp:cs-http-gateway`
- `cs-scheduler` uses `sp:cs-scheduler`
- `cs-cadence-poller` uses `sp:cs-cadence-poller`

Each service principal has scoped permissions:

- publish InvocationRequest
- read function versions
