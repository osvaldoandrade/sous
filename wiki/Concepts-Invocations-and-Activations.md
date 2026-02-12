# Invocations and Activations

An invocation is a request to execute a specific function reference.

An activation is the persisted record of what happened as a result of that invocation: status, timestamps, duration, error fields, and log pointers.

Every trigger (HTTP, schedule, Cadence) ultimately produces the same `InvocationRequest` shape and receives the same `InvocationResult` shape.

That uniformity is what makes the platform composable: triggers can evolve without changing execution semantics.

For the exact message schemas, see [Schemas](Schemas).
