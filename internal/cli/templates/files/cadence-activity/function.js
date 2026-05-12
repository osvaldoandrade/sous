// Cadence activity handler. The poller (cs-cadence-poller) delivers the
// activity payload as `event` and sets ctx.trigger.type === "cadence".
// See docs/12-cadence-integration.md for the WorkerBinding contract.
export default async function handle(event, ctx) {
  cs.log.info({ template: "cadence-activity", name: "{{.Name}}", activity: event && event.activityType, activation_id: ctx.activation_id })

  // Heartbeat is only valid for cadence-triggered invocations; guard so the
  // function also runs under `cs fn test` (trigger.type === "api").
  if (ctx.trigger && ctx.trigger.type === "cadence") {
    cs.cadence.heartbeat({ activation_id: ctx.activation_id })
  }

  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ok: true, result: event && event.input ? event.input : null }),
    isBase64Encoded: false
  }
}
