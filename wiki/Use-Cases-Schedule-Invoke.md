# Use Case: Schedule Invoke

This flow executes a function on a fixed interval.

## Main flow

1. Operator creates a schedule targeting a function reference.
2. Scheduler ticks and emits an invocation when due.
3. Overlap policy is enforced through an inflight marker.
4. Invoker executes the function and persists activation state.

### Sequence diagram

```mermaid
sequenceDiagram
  participant O as Operator
  participant CP as cs-control
  participant S as cs-scheduler
  participant Q as codeQ
  participant I as cs-invoker-pool

  O->>CP: Create schedule
  loop Each tick
    S->>S: Evaluate due schedules
    S->>Q: Publish InvocationRequest
  end
  Q->>I: Deliver InvocationRequest
  I-->>Q: Publish InvocationResult
```
