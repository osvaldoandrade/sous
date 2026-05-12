// Scheduled job handler. The control plane fires this on the cadence
// configured via `cs schedule create ... --every <seconds>`; the event is the
// schedule payload merged with the timer tick metadata (cs.timer).
export default async function handle(event, ctx) {
  cs.log.info({ template: "scheduled-job", name: "{{.Name}}", trigger: ctx.trigger && ctx.trigger.type, activation_id: ctx.activation_id })
  // Idempotency anchor keyed on the activation id so retries are safe.
  cs.kv.set("job:last", { activation_id: ctx.activation_id, at: ctx.deadline_ms }, { ttlSeconds: 3600 })
  // Fan-out: publish a downstream event on codeq.
  cs.codeq.publish("jobs.scheduled.tick", { name: "{{.Name}}", event })
  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ok: true, processed: true }),
    isBase64Encoded: false
  }
}
