const { z } = require("zod");
const { resolveTenantFromPhoneNumberId } = require("../services/tenantRouter");
const { claimWamidForProcessing } = require("../services/idempotencyStore");
const { processInboundBusinessActions } = require("../services/workerActions");
const { info, warn, error } = require("../lib/logger");

const PubSubEnvelopeSchema = z
  .object({
    source: z.string().optional(),
    receivedAt: z.string().optional(),
    payload: z.unknown(),
    messageCount: z.number().optional()
  })
  .passthrough();

function parsePubSubJson(event) {
  const b64 = event?.data?.message?.data;
  if (!b64) throw new Error("Missing Pub/Sub message data");
  const jsonText = Buffer.from(b64, "base64").toString("utf8");
  return JSON.parse(jsonText);
}

function extractInboundMessages(webhookPayload) {
  const output = [];
  for (const entry of webhookPayload.entry || []) {
    for (const change of entry.changes || []) {
      const value = change?.value || {};
      const phoneNumberId = value?.metadata?.phone_number_id || null;
      const waProfileName =
        Array.isArray(value.contacts) && value.contacts[0]?.profile?.name
          ? String(value.contacts[0].profile.name).trim()
          : "";
      const messages = Array.isArray(value.messages) ? value.messages : [];
      for (const msg of messages) {
        const mid = msg?.id != null && String(msg.id).trim() !== "" ? String(msg.id).trim() : "";
        const wamid =
          mid ||
          `synthetic_${String(msg?.from || "unknown")}_${String(msg?.timestamp != null ? msg.timestamp : Date.now())}`;
        const normalized = {
          wamid,
          from: msg?.from != null ? String(msg.from) : null,
          timestamp: msg?.timestamp != null ? String(msg.timestamp) : null,
          type: msg?.type != null ? String(msg.type) : null,
          text:
            msg?.text?.body != null
              ? String(msg.text.body)
              : typeof msg?.text === "string"
                ? msg.text
                : "",
          interactive: null,
          phoneNumberId,
          profileName: waProfileName,
          raw: msg
        };
        if (msg?.type === "interactive") {
          if (msg?.interactive?.type === "button_reply") {
            normalized.interactive = {
              kind: "button_reply",
              id: msg?.interactive?.button_reply?.id || "",
              title: msg?.interactive?.button_reply?.title || ""
            };
          } else if (msg?.interactive?.type === "list_reply") {
            normalized.interactive = {
              kind: "list_reply",
              id: msg?.interactive?.list_reply?.id || "",
              title: msg?.interactive?.list_reply?.title || ""
            };
          }
        } else if (msg?.type === "button") {
          /** Meta template / legacy quick reply: `button` not nested under `interactive`. */
          const btn = msg?.button || {};
          normalized.interactive = {
            kind: "button_reply",
            id: String(btn.payload != null ? btn.payload : "").trim(),
            title: String(btn.text != null ? btn.text : "").trim()
          };
        }
        output.push(normalized);
      }
    }
  }
  return output;
}

/**
 * Firebase 2nd Gen Pub/Sub worker for "wa-inbound" topic.
 */
function createWaInboundWorker() {
  return async function waInboundWorker(event) {
    let envelope;
    try {
      const parsed = parsePubSubJson(event);
      envelope = PubSubEnvelopeSchema.parse(parsed);
    } catch (err) {
      error("WA_WORKER_ENVELOPE_INVALID", { reason: String(err) });
      return;
    }

    const rawPayload = envelope.payload;
    const webhookPayload =
      rawPayload && typeof rawPayload === "object" && !Array.isArray(rawPayload)
        ? rawPayload
        : { object: "whatsapp_business_account", entry: [] };
    if (!Array.isArray(webhookPayload.entry)) {
      webhookPayload.entry = [];
    }

    const inboundMessages = extractInboundMessages(webhookPayload);
    if (!inboundMessages.length) {
      info("WA_WORKER_NO_INBOUND_MESSAGES", {});
      return;
    }

    for (const inbound of inboundMessages) {
      try {
        const tenantContext = await resolveTenantFromPhoneNumberId(inbound.phoneNumberId);
        const tenantId = tenantContext.tenantId;

        const claim = await claimWamidForProcessing({
          tenantId,
          wamid: inbound.wamid,
          from: inbound.from,
          eventMeta: {
            routeSource: tenantContext.routeSource,
            phoneNumberId: tenantContext.phoneNumberId || "",
            pubsubMessageId: event?.data?.message?.messageId || ""
          }
        });

        if (!claim.claimed) {
          info("WA_WORKER_DUPLICATE_DROPPED", {
            tenantId,
            wamid: inbound.wamid,
            reason: claim.reason
          });
          continue;
        }

        await processInboundBusinessActions({
          tenantId,
          tenantRouteSource: tenantContext.routeSource,
          inbound
        });

        info("WA_WORKER_PROCESSED", {
          tenantId,
          wamid: inbound.wamid,
          type: inbound.type,
          interactiveId: inbound?.interactive?.id || ""
        });
      } catch (msgErr) {
        error("WA_WORKER_MESSAGE_PROCESSING_FAILED", {
          reason: String(msgErr),
          wamid: inbound?.wamid || ""
        });
        // Ack Pub/Sub — do not rethrow (infinite retries would stall all inbound).
      }
    }
  };
}

module.exports = { createWaInboundWorker };
