const { z } = require("zod");

/**
 * Meta webhook payloads vary (new message types, extra keys, timestamp as string OR number).
 * Strict Zod unions caused HTTP 422 and dropped ALL inbound traffic. We only sanity-check
 * shape; raw messages pass through untouched for waInboundWorker to normalize.
 */
function parseWebhookBody(input) {
  if (input == null || typeof input !== "object") {
    throw new Error("webhook_body_not_object");
  }
  const body = input;
  if (body.object !== "whatsapp_business_account") {
    throw new Error("webhook_unexpected_object_field");
  }
  if (!Array.isArray(body.entry) || body.entry.length < 1) {
    throw new Error("webhook_entry_missing_or_empty");
  }
  return body;
}

/**
 * Never block publishing: coerce minimal shape so the worker can still run.
 */
function coerceWebhookBodyForPublish(parsedJson) {
  const base =
    parsedJson && typeof parsedJson === "object" && !Array.isArray(parsedJson) ? { ...parsedJson } : {};
  if (base.object !== "whatsapp_business_account") {
    base.object = "whatsapp_business_account";
  }
  if (!Array.isArray(base.entry)) {
    base.entry = [];
  }
  return base;
}

/** Legacy tests / tooling: accept any object. */
const WhatsAppWebhookBodySchema = z.record(z.unknown());

module.exports = {
  WhatsAppWebhookBodySchema,
  parseWebhookBody,
  coerceWebhookBodyForPublish
};
