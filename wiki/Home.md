# SOUS

SOUS is a serverless functions runtime. You write a small piece of code, publish it as an immutable version, and invoke it through HTTP, a fixed-interval schedule, or a Cadence Activity. The platform handles isolation, capability enforcement, persistence of activation records, and result transport.

SOUS accepts function code as plain UTF-8 text — no build step, no container image at publish time. The same execution fabric (`cs-invoker-pool`) services every trigger family, so behavior is consistent regardless of how an invocation arrives. Local execution with the `cs` CLI uses the same runtime as the cluster invoker, so the developer loop matches production.

Start with [Get Started](Get-Started).

If you are evaluating SOUS end-to-end, the fastest reading path is:

1. [Get Started](Get-Started)
2. [Overview](Overview)
3. [Architecture](Architecture)
4. [Runtime: cs-js](Runtime-cs-js)
5. [REST API](REST-API)

For scenario-based behavior, read [Use Cases](Use-Cases).
