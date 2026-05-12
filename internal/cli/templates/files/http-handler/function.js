export default async function handle(event, ctx) {
  cs.log.info({ template: "http-handler", name: "{{.Name}}", activation_id: ctx.activation_id })
  return {
    statusCode: 200,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ ok: true, event }),
    isBase64Encoded: false
  }
}
