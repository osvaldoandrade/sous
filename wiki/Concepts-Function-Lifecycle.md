# Function Lifecycle

SOUS models function lifecycle as a sequence of immutable artifacts with a single mutable pointer.

A draft is an upload. It is meant to be temporary, it has a TTL, and it can be replaced.

A version is a publish. It is immutable and permanently addressable.

An alias is the only mutable pointer in the lifecycle. It routes production traffic to an immutable version.

This split makes rollouts safe: a promotion is an alias update, not a mutation of the published artifact.

If you want the exact REST endpoints and payloads for this lifecycle, see [REST API](REST-API) and [CLI](CLI).
