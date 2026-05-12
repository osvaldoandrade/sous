package api

// SubscriptionBinding describes a codeQ subscription that triggers a function
// once per matching envelope on a topic. Bindings are owned by cs-control and
// consumed by an in-process subscription worker. The wire shape is documented
// in docs/07-codeq-protocol.md ("Subscription triggers").
//
// Fields:
//   - Tenant / Namespace / Name: identity. (tenant, namespace, name) is unique.
//   - Topic: codeQ topic to consume.
//   - Filter: minimal expression string (see docs/07-codeq-protocol.md);
//     empty means "match every envelope". Examples: `event.tenant == "t_x"`,
//     `body.kind != "ignored"`.
//   - FunctionRef: function to invoke per matching message.
//   - Mode: "ordered" (single goroutine per partition, blocking commit) or
//     "unordered" (pool of `max_concurrency` workers, non-blocking).
//   - GroupID: codeQ consumer group used for offset commits. When empty,
//     cs-control derives a stable group id from (tenant, namespace, name).
//   - MaxConcurrency: pool size for unordered mode. Ignored for ordered.
//     Defaults to 1 when 0; bounded to a configurable cap by cs-control.
//   - CreatedAtMS / DeletedAtMS: lifecycle timestamps.
//   - Enabled: false stops the consumer without deleting the binding.
type SubscriptionBinding struct {
	Tenant         string      `json:"tenant"`
	Namespace      string      `json:"namespace"`
	Name           string      `json:"name"`
	Topic          string      `json:"topic"`
	Filter         string      `json:"filter,omitempty"`
	FunctionRef    ScheduleRef `json:"function_ref"`
	Mode           string      `json:"mode"`
	GroupID        string      `json:"group_id,omitempty"`
	MaxConcurrency int         `json:"max_concurrency,omitempty"`
	CreatedAtMS    int64       `json:"created_at_ms"`
	DeletedAtMS    *int64      `json:"deleted_at_ms,omitempty"`
	Enabled        bool        `json:"enabled"`
}

// CreateSubscriptionRequest is the API payload accepted by cs-control when a
// subscription is created via POST /v1/.../subscriptions. Fields mirror
// SubscriptionBinding minus the timestamps and enabled flag.
type CreateSubscriptionRequest struct {
	Name           string      `json:"name"`
	Topic          string      `json:"topic"`
	Filter         string      `json:"filter,omitempty"`
	FunctionRef    ScheduleRef `json:"function_ref"`
	Mode           string      `json:"mode"`
	GroupID        string      `json:"group_id,omitempty"`
	MaxConcurrency int         `json:"max_concurrency,omitempty"`
}

// Subscription delivery modes.
const (
	SubscriptionModeOrdered   = "ordered"
	SubscriptionModeUnordered = "unordered"
)
