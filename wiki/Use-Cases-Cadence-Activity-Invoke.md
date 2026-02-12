# Cadence Activity Invoke

This flow allows Cadence Activities to be executed by SOUS functions.

## Main flow

1. Operator registers a WorkerBinding mapping ActivityType -> FunctionRef.
2. Poller long-polls Cadence for Activity tasks.
3. Poller maps a task to an `InvocationRequest` and publishes it.
4. Invoker executes the function and publishes `InvocationResult`.
5. Poller responds to Cadence with completed/failed.

### Sequence diagram

```mermaid
sequenceDiagram
  participant O as Operator
  participant CP as cs-control
  participant C as Cadence
  participant P as cs-cadence-poller
  participant Q as codeQ
  participant I as cs-invoker-pool

  O->>CP: Create WorkerBinding
  P->>C: Long-poll Activity tasks
  C-->>P: Activity task
  P->>Q: Publish InvocationRequest
  Q->>I: Deliver InvocationRequest
  I->>Q: Publish InvocationResult
  P->>Q: Consume InvocationResult
  P-->>C: Respond completed/failed
```
