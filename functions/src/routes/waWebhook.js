const express = require("express");
const { ZodError } = require("zod");
const { parseWebhookBody, coerceWebhookBodyForPublish } = require("../schemas/whatsappWebhook.schema");
const { verifyMetaSignature } = require("../lib/verifyMetaSignature");
const { publishInboundWebhookEvent } = require("../services/publishInboundEvent");
const { info, warn, error } = require("../lib/logger");

function extractInboundMessagesCount(payload) {
  let count = 0;
  for (const entry of payload.entry || []) {
    for (const change of entry.changes || []) {
      const messages = change?.value?.messages || [];
      count += Array.isArray(messages) ? messages.length : 0;
    }
  }
  return count;
}

function logWebhookInboundMessages(payload) {
  try {
    for (const entry of payload.entry || []) {
      for (const change of entry.changes || []) {
        const messages = Array.isArray(change?.value?.messages) ? change.value.messages : [];
        for (const message of messages) {
          console.log("WEBHOOK_INBOUND_MSG:", JSON.stringify(message));
        }
      }
    }
  } catch (e) {
    console.log("WEBHOOK_INBOUND_MSG_LOG_ERROR:", String(e && e.message ? e.message : e));
  }
}

/**
 * Mounted on Cloud Function `whatsappWebhook` (region asia-south1).
 * Meta callback URL path: https://<function-host>/wa/webhook
 */
function createWebhookApp({ getVerifyToken, getAppSecret }) {
  const app = express();
  app.disable("x-powered-by");

  app.get("/wa/webhook", (req, res) => {
    const mode = req.query["hub.mode"];
    const token = req.query["hub.verify_token"];
    const challenge = req.query["hub.challenge"];
    const expectedToken = getVerifyToken();

    if (mode !== "subscribe" || !challenge) {
      console.error(
        "WEBHOOK_REJECT_GET_VERIFY_BAD_REQUEST",
        JSON.stringify({ mode: mode || null, hasChallenge: !!challenge })
      );
      return res.status(400).json({ error: "Invalid verification request" });
    }
    if (!expectedToken || token !== expectedToken) {
      console.error(
        "WEBHOOK_REJECT_VERIFY_TOKEN_MISMATCH",
        JSON.stringify({
          hasExpectedToken: !!(expectedToken && String(expectedToken).trim()),
          expectedLen: expectedToken ? String(expectedToken).length : 0,
          hasQueryToken: !!(token && String(token).trim()),
          queryLen: token ? String(token).length : 0
        })
      );
      return res.status(403).json({ error: "Verification token mismatch" });
    }
    return res.status(200).send(challenge);
  });

  app.post("/wa/webhook", express.raw({ type: "application/json", limit: "1mb" }), async (req, res) => {
    const signature = req.get("x-hub-signature-256");
    const appSecret = getAppSecret();
    const rawBody = req.body;

    if (!verifyMetaSignature(rawBody, signature, appSecret)) {
      const sigStr = signature ? String(signature) : "";
      console.error(
        "WEBHOOK_REJECT_SIGNATURE",
        JSON.stringify({
          hasSignatureHeader: !!sigStr,
          signaturePrefix: sigStr.slice(0, 24),
          bodyIsBuffer: Buffer.isBuffer(rawBody),
          bodyLength: Buffer.isBuffer(rawBody) ? rawBody.length : 0,
          appSecretConfigured: !!(appSecret && String(appSecret).trim()),
          appSecretLength: appSecret ? String(appSecret).length : 0
        })
      );
      warn("Invalid X-Hub-Signature-256", { hasSignature: !!signature });
      return res.status(403).json({ error: "Invalid signature" });
    }

    let parsedJson;
    try {
      parsedJson = JSON.parse(rawBody.toString("utf8"));
    } catch (parseErr) {
      console.error(
        "WEBHOOK_REJECT_JSON_PARSE",
        JSON.stringify({
          reason: String(parseErr && parseErr.message ? parseErr.message : parseErr),
          bodyLength: Buffer.isBuffer(rawBody) ? rawBody.length : 0
        })
      );
      warn("Webhook JSON parse failed", { reason: String(parseErr) });
      return res.status(400).json({ error: "Invalid JSON payload" });
    }

    let payload;
    try {
      payload = parseWebhookBody(parsedJson);
    } catch (validationErr) {
      if (validationErr instanceof ZodError) {
        warn("Webhook payload validation failed", {
          issues: validationErr.issues.map((i) => ({
            path: i.path.join("."),
            code: i.code,
            message: i.message
          }))
        });
      } else {
        warn("Webhook payload sanity check failed — using relaxed coerce (still ack Meta)", {
          reason: String(validationErr && validationErr.message ? validationErr.message : validationErr)
        });
      }
      payload = coerceWebhookBodyForPublish(parsedJson);
    }

    // Ack fast to Meta; heavy operations should be async.
    res.status(200).send("EVENT_RECEIVED");

    logWebhookInboundMessages(payload);

    try {
      const inboundCount = extractInboundMessagesCount(payload);
      if (inboundCount === 0) {
        info("Webhook status event received (no inbound messages)", {});
        return;
      }

      await publishInboundWebhookEvent({
        payload,
        messageCount: inboundCount,
        receivedAt: new Date().toISOString()
      });

      console.log("WEBHOOK_OK_PUBLISHED_TO_PUBSUB", JSON.stringify({ messageCount: inboundCount }));

      info("Inbound webhook accepted", {
        messageCount: inboundCount
      });
    } catch (processingErr) {
      const msg = String(processingErr && processingErr.message ? processingErr.message : processingErr);
      console.error("WEBHOOK_ASYNC_PUBLISH_FAILED", JSON.stringify({ reason: msg }));
      error("Webhook async processing failed", { reason: String(processingErr) });
    }
  });

  app.use((err, req, res, next) => {
    error("Unhandled webhook route error", { reason: String(err) });
    return res.status(500).json({ error: "Internal server error" });
  });

  return app;
}

module.exports = { createWebhookApp };
