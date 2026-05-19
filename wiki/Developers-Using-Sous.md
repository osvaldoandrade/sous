# Developers: Using Sous

A developer who has never touched Sous before tends to ask the same opening questions: where does a function live, who is allowed to invoke it, and how does the platform decide which version actually runs when a request shows up.

Those three questions map cleanly onto three concepts the platform makes explicit — the resource address, the principal, and the alias — and the rest of the developer experience is built on top of them.

This page explains the mental model. It does not enumerate every CLI flag, every REST verb, or every header on the HTTP invoke endpoint; the canonical references for those are [Developers CLI](Developers-CLI), [Developers REST API](Developers-REST-API), and [HTTP Invoke Path](HTTP-Invoke-Path).

What this page does is give a developer the lens through which those references make sense.

## The addressing model

Every artifact in Sous lives under a hierarchical path that mirrors how teams already think about service ownership. Reading from the root toward the leaves:

- `/v1/tenants/{tenant}` identifies a billing and isolation boundary. Tenants are issued by Tikti and never assigned by Sous itself.
- `/v1/tenants/{tenant}/namespaces/{namespace}` groups related functions under a name the team chose (`payments`, `growth`, `internal-tools`). Namespaces have no implicit hierarchy; they are flat under their tenant.
- `/v1/tenants/{tenant}/namespaces/{namespace}/functions/{function}` is the function record. A function is a long-lived logical name whose body — code, manifest, limits — is replaced by publishing a new version.
- `/v1/tenants/{tenant}/namespaces/{namespace}/functions/{function}/versions/{n}` is an immutable published artifact. Once `n` exists, its `function.js` and `manifest.json` cannot be modified.
- `/v1/tenants/{tenant}/namespaces/{namespace}/functions/{function}/aliases/{alias}` is a mutable pointer to a version. Production traffic typically targets aliases like `prod` or `staging`, not raw version numbers.
- `/v1/tenants/{tenant}/namespaces/{namespace}/functions/{function}/draft` is the transient upload bucket. Drafts have a TTL configured by `cs_control.limits.draft_ttl_seconds` in `config.example.yaml`; once promoted to a version they are eligible for garbage collection.

The HTTP invoke path uses a slightly different prefix that omits the lifecycle verbs and exposes only the public surface: `/v1/web/{tenant}/{namespace}/{function}/{ref}`, where `ref` is either an alias or a numeric version.

This is the surface served by `cs-http-gateway`, defined in `cmd/cs-http-gateway/main.go`, and described in [HTTP Invoke Path](HTTP-Invoke-Path).

Two consequences follow from this layout.

First, the tenant boundary is part of every URL, so an authorization mistake at the gateway cannot silently leak across tenants — the address itself disagrees with the principal.

Second, because aliases are versioned pointers rather than copy-on-write clones, a rollback is a single PUT against the alias resource, not a re-upload or a redeploy.

## Identities come from Tikti

Sous never issues credentials. Every principal that calls a control-plane endpoint or an invoke endpoint carries a bearer token, and the token is resolved against Tikti through introspection.

The response shape is documented in [IAM with Tikti](IAM-with-Tikti), but the load-bearing fields are simple:

- `sub` — the subject identifier that ends up on every activation record for audit.
- `tenant` — the tenant the principal belongs to; it must match the `{tenant}` segment of the request URL.
- `roles[]` — the role names the principal carries (for example `role:app`, `role:worker`, `role:cadence`).

### The CLI surface

`cs auth login` stores the API URL, tenant, and token in `$XDG_CONFIG_HOME/code-sous/auth.json`. Every subsequent `cs` invocation builds its request URL from the stored tenant and attaches the stored token as a `Bearer` header.

The CLI does not interpret the token; it just relays it. The CLI implementation is in `cmd/cs-cli/main.go`.

### The REST surface

`cs-control` validates every mutating request against Tikti before touching KVRocks. The action map (`cs:function:publish`, `cs:function:alias:set`, and so on) is enumerated under [IAM with Tikti](IAM-with-Tikti).

If introspection fails (network error, expired token, revoked principal), the daemon refuses the request with `CS_UNAUTHORIZED`; if introspection succeeds but the principal's roles do not authorize the action, the daemon refuses with `CS_FORBIDDEN`.

### The HTTP invoke surface

`cs-http-gateway` validates the same token, then checks that the principal's roles intersect the function version's `authz.invoke_http_roles[]` allowlist before forwarding an `InvocationRequest`.

A version published with an empty HTTP role allowlist is unreachable through the gateway by design; this is documented under [Concepts Capabilities and Isolation](Concepts-Capabilities-and-Isolation).

A developer who keeps the addressing model and the principal model side by side will find very few authorization surprises. If a request fails with `CS_FORBIDDEN`, the question is always one of three: wrong tenant in the URL, wrong role on the token, or missing role on the version's allowlist.

## Choosing a namespace

A namespace is a soft grouping that helps a team navigate its own functions and helps the platform enforce per-namespace quotas (described under [Capacity and Limits](Capacity-and-Limits)). Sous does not enforce any opinion about how namespaces map to teams, environments, or deployment units; the convention is whatever the tenant agrees on internally.

A common pattern is one namespace per bounded context (`payments`, `notifications`, `audit`) with environments distinguished by alias (`reconcile@staging`, `reconcile@prod`).

Another pattern is one namespace per environment (`payments-dev`, `payments-prod`) with aliases reserved for canary slicing. Either pattern works, because Sous treats the namespace segment as opaque after authorization is resolved.

The CLI default namespace is `default`. A developer can override it on any subcommand with `--namespace <name>` or by exporting it into a shell alias.

## Publishing a draft

The lifecycle a developer walks every time they ship a change is the same: draft, publish, alias.

### Drafts are mutable

A **draft** is a mutable upload pinned to a function name. The CLI command `cs fn draft upload <name> --path .` reads `function.js` and `manifest.json` from the current directory, base64-encodes them, and PUTs them to `/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/draft`.

`cs-control` validates the manifest, computes a canonical bundle hash, stores the draft under a TTL-bounded key, and returns a draft identifier of the shape `drf_...`.

Until the draft is promoted, it is mutable: another upload against the same function replaces it. Drafts that are never promoted expire silently after `draft_ttl_seconds`.

### Versions are immutable

A **version** is an immutable artifact produced by promoting a draft. `cs fn publish <name> --draft <id> --timeout-ms 3000 --memory-mb 64 --invoke-http-roles role:app` materialises a version record with the timeout, memory, and role allowlists embedded.

Once the version exists, its content is frozen; any change requires a new version, which produces a new monotonically increasing version number. The function record exposes both the latest published version and the full version history.

### Aliases are mutable pointers

An **alias** is a mutable pointer from a name like `prod` to a version number. `cs fn alias set <name> prod --version 17` points `prod` at version 17.

Traffic that targeted `reconcile@prod` immediately resolves to version 17 from that point forward. Aliases are the platform's promotion primitive: rollouts, rollbacks, and canary patterns are all expressed by moving aliases, not by re-uploading code.

The state machine that governs these transitions is described under [Concepts Function Lifecycle](Concepts-Function-Lifecycle), and the schema of `VersionConfig` (timeouts, memory, capabilities, allowlists) is enumerated under [Schemas](Schemas).

## Promoting through aliases

Because aliases are first-class resources rather than mutable copies of a version, promotion has a precise shape. A developer who wants to ship version 18 to production typically does it in two PUTs:

1. `cs fn alias set <name> staging --version 18` makes the new code reachable through any trigger pinned to the `staging` alias.
2. After verification, `cs fn alias set <name> prod --version 18` cuts production traffic over.

Rollback is the inverse PUT — `cs fn alias set <name> prod --version 17` — and is instantaneous in the sense that the gateway resolves alias-to-version on every invocation and caches it for a short window only. The cache TTL is documented under [HTTP Invoke Path](HTTP-Invoke-Path).

Schedule and Cadence triggers can be pinned to either an alias or a specific version. A schedule pinned to an alias follows promotions automatically; a schedule pinned to a version is locked. See [Scheduler](Scheduler) and [Cadence Integration](Cadence-Integration) for the trade-offs.

## What happens when the same identity hits three surfaces

A developer who walks the same function through three trigger paths sees the platform enforce identity consistently in each.

### HTTP invoke

`cs-http-gateway` parses the bearer token, calls Tikti introspection, builds a principal, and checks `authz.invoke_http_roles[]` on the version before forwarding the request through codeQ to the invoker pool.

The activation record records both the principal and the trigger source.

### CLI invoke

`cs fn invoke <function>@<alias>` posts to `/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}:invoke` on `cs-control`. `cs-control` enforces `cs:function:invoke:api`, then resolves the alias, then enqueues an `InvocationRequest`.

The activation records the CLI principal verbatim, including the `sub` and `roles[]` from the token used at `cs auth login` time.

### Scheduler invoke

When a schedule fires, `cs-scheduler` uses its own service principal to enqueue the invocation; the version's `authz.invoke_schedule_roles[]` allowlist must include the scheduler's role.

The activation records the schedule binding as the trigger source, not the human who created the schedule. This separation lets an operator answer two distinct questions from the same activation: who configured the trigger, and who actually fired it.

In all three cases the activation record — discoverable through `cs fn logs --activation <id>` and the `/v1/tenants/{tenant}/activations/{id}` endpoint — captures the same fields, so an operator chasing a bug does not need to know which surface produced the request to read its outcome. The activation model is documented in detail under [Concepts Invocations and Activations](Concepts-Invocations-and-Activations).

## The lifecycle of a Sous user

It is useful to walk through the experience of an engineer who joins a team that already runs Sous in production. The path is the same regardless of whether the user is human or an agent, and it touches every concept the previous sections introduced.

### Day one: receiving a token

Someone with administrative access — typically the platform team — issues the new user a Tikti bearer token bound to the right tenant and the right set of roles. The token may be long-lived or short-lived depending on policy.

The new user receives, at minimum, three values: the tenant identifier, the bearer token, and the control-plane URL. These are the three arguments `cs auth login` needs.

### Day one: configuring the CLI

The user runs `cs auth login --api-url <url> --tenant <t> --token <bearer>`. The CLI writes `auth.json` under the platform's standard config directory. From that point forward, every CLI invocation uses the stored credentials without further prompting.

`cs auth whoami` is the quickest way to confirm the credentials landed.

### Day one: picking a namespace

Before publishing anything, the user decides which namespace the new function belongs in. Most teams have a convention; new users typically inherit it. The CLI accepts `--namespace` on every subcommand, and the default value `default` works for ad-hoc experimentation.

### Day two: drafting and publishing

The user scaffolds a function with `cs fn init <name>`, edits `function.js` and `manifest.json`, and iterates locally with `cs fn test`. Each loop is offline; no daemon is involved.

When the function behaves, the user uploads it with `cs fn draft upload`, promotes the draft with `cs fn publish`, and points an alias at the new version with `cs fn alias set`.

### Day two: invoking the function

The user can invoke the function through any trigger surface the version's allowlists permit. The most direct is HTTP: a `curl` against `/v1/web/{tenant}/{namespace}/{function}/{alias}` produces a result and an activation ID.

`cs fn logs --activation <id>` prints the activation's logs and final status.

### Day three: promoting through aliases

Once the user is confident in the function, they typically wire it into the team's promotion process. The convention is that a `staging` alias rolls forward continuously while `prod` rolls forward after some verification window.

Both promotions are single PUTs on the alias resource. Rollback is the inverse PUT and is instantaneous from the gateway's perspective.

### Day four: observing activations

The activation record is the unit of audit and the unit of debugging. Every invocation produces one, every trigger family populates the same fields, and every CLI surface exposes the same read endpoints.

A user who needs to investigate a failed invocation reads the activation by ID. A user who needs to see the most recent traffic for a function reads the recent activations through the control plane. Both flows are documented in [Concepts Invocations and Activations](Concepts-Invocations-and-Activations).

## Where to go next

A developer who has internalised the addressing model and identity flow is ready to publish a function end-to-end. The walkthrough is in [Developers Getting Started](Developers-Getting-Started).

The complete CLI surface is enumerated in [Developers CLI](Developers-CLI), and the REST contract is enumerated in [Developers REST API](Developers-REST-API). Authorization is detailed in [IAM with Tikti](IAM-with-Tikti).
