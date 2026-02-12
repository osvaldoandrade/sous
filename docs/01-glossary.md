# Glossary

## Core entities

**Tenant**  
A top-level security boundary. Tikti ties a principal to one tenant.

**Namespace**  
A logical boundary inside a tenant. Teams map namespaces to products or environments.

**Function**  
A named resource that groups versions and aliases.

**FunctionVersion**  
An immutable snapshot of code + configuration. A Version has an integer `version` and a `sha256`.

**Alias**  
A mutable pointer to a FunctionVersion. Examples: `dev`, `staging`, `prod`.

**Draft**  
A temporary upload used to build a FunctionVersion. Drafts have TTL.

**Activation**  
A single execution of a FunctionVersion. An Activation has an `activation_id` and a terminal `status`.

## Triggers

**HTTP trigger**  
An invocation that starts from an HTTP request into `cs-http-gateway`.

**Schedule trigger**  
An invocation created by `cs-scheduler` at a fixed interval in seconds.

**Cadence trigger**  
An invocation created by `cs-cadence-poller` after it receives an ActivityTask from `code-flow`.

## Data plane messages

**InvocationRequest**  
A message on `codeQ` that instructs `cs-invoker-pool` to execute a function.

**InvocationResult**  
A message on `codeQ` that returns a completion, error, or timeout for an Activation.

## Cadence terms

**Domain**  
A Cadence partition that scopes workflows and task lists.

**TaskList**  
A queue name that routes Decision and Activity tasks to workers.

**ActivityTask**  
A Cadence unit of work for an Activity implementation.

## Security terms

**Principal**  
An authenticated identity from Tikti.

**Role**  
A string in Tikti that grants permissions.

**Capability**  
A per-version allowlist that limits what user code can do.
