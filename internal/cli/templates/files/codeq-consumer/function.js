// codeQ consumer handler. The control plane forwards each subscribed message
// as `event`; idempotency is enforced via cs.kv under the `idem:` prefix so
// at-least-once delivery becomes effectively-once.
export default async function handle(event, ctx) {
  const id = (event && event.id) || ctx.activation_id
  const key = "idem:" + id

  const seen = cs.kv.get(key)
  if (seen) {
    cs.log.info({ template: "codeq-consumer", name: "{{.Name}}", deduped: true, id })
    return { statusCode: 200, headers: {}, body: JSON.stringify({ ok: true, deduped: true }), isBase64Encoded: false }
  }

  cs.log.info({ template: "codeq-consumer", name: "{{.Name}}", id, activation_id: ctx.activation_id })
  cs.kv.set(key, { at: ctx.deadline_ms }, { ttlSeconds: 86400 })
  cs.codeq.publish("consumer.processed", { name: "{{.Name}}", id })

  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ok: true, id }),
    isBase64Encoded: false
  }
}
