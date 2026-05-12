package observability

import (
	"context"
	"net/http"
)

// ParentActivationHeader is the request header used to propagate the
// activation ID of the calling function into the gateway when a `cs`
// function invokes another `cs` function (HTTP fan-out or codeQ enqueue).
// The receiving gateway stamps the parsed value into the new activation's
// ParentActivationID so the control plane can later materialize an agent
// decision tree. See docs/14-observability.md.
const ParentActivationHeader = "X-CS-Parent-Activation"

type parentActivationKey struct{}

// WithParent returns a context carrying the parent activation ID. Pass the
// returned context down into outbound HTTP / codeQ paths so the runtime
// egress shim can inject the X-CS-Parent-Activation header without each
// call site threading the value through manually.
func WithParent(ctx context.Context, activationID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if activationID == "" {
		return ctx
	}
	return context.WithValue(ctx, parentActivationKey{}, activationID)
}

// ParentFrom returns the parent activation ID previously stored in ctx by
// WithParent. Returns the empty string when no parent has been set — the
// signal for "this is a root activation".
func ParentFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(parentActivationKey{}).(string)
	return v
}

// InjectParentHeader copies the parent activation ID from ctx onto an
// outbound HTTP request. Safe to call with a nil request or empty context:
// the function is a no-op when there is nothing to propagate. The header
// name is the documented ParentActivationHeader constant so the receiving
// gateway can match without coordinating string literals.
func InjectParentHeader(req *http.Request, ctx context.Context) {
	if req == nil {
		return
	}
	parent := ParentFrom(ctx)
	if parent == "" {
		return
	}
	req.Header.Set(ParentActivationHeader, parent)
}
