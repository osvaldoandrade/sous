# SOUS

SOUS (code-sous) is a function execution layer for agent-built automation.

It is closer to a serverless functions platform than to a traditional job runner, but it makes a few opinionated choices that are essential for agent-driven development:

SOUS accepts function code as plain UTF-8 text (no build step), forces runtime parity between local CLI and cluster execution, and treats privileges as explicit contracts (roles per trigger type, and capabilities per side-effect). These choices make it possible for an agent to generate a function, run it locally, publish it to a cluster, and get the same semantics and failure modes in both environments.

Start with [Get Started](Get-Started).

If you are evaluating SOUS end-to-end, the fastest reading path is:

1. [Get Started](Get-Started)
2. [Overview](Overview)
3. [Architecture](Architecture)
4. [Runtime: cs-js](Runtime-cs-js)
5. [REST API](REST-API)

For scenario-based behavior, read [Use Cases](Use-Cases).
