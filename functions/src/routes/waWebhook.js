const express = require("express");
const { ZodError } = require("zod");
const { parseWebhookBody } = require("../schemas/whatsappWebhook.schema");
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

function createWebhookApp({ getVerifyToken, getAppSecret }) {
  const app = express();
  app.disable("x-powered-by");

  app.get("/wa/webhook", (req, res) => {
    const mode = req.query["hub.mode"];
    const token = req.query["hub.verify_token"];
    const challenge = req.query["hub.challenge"];
    const expectedToken = getVerifyToken();

    if (mode !== "subscribe" || !challenge) {
      return res.status(400).json({ error: "Invalid verification request" });
    }
    if (!expectedToken || token !== expectedToken) {
      return res.status(403).json({ error: "Verification token mismatch" });
    }
    return res.status(200).send(challenge);
  });

  app.post("/wa/webhook", express.raw({ type: "application/json", limit: "1mb" }), async (req, res) => {
    const signature = req.get("x-hub-signature-256");
    const appSecret = getAppSecret();
    const rawBody = req.body;

    if (!verifyMetaSignature(rawBody, signature, appSecret)) {
      warn("Invalid X-Hub-Signature-256", { hasSignature: !!signature });
      return res.status(403).json({ error: "Invalid signature" });
    }

    let parsedJson;
    try {
      parsedJson = JSON.parse(rawBody.toString("utf8"));
    } catch (parseErr) {
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
        return res.status(422).json({ error: "Payload validation failed" });
      }
      throw validationErr;
    }

    // Ack fast to Meta; heavy operations should be async.
    res.status(200).send("EVENT_RECEIVED");

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

      info("Inbound webhook accepted", {
        messageCount: inboundCount
      });
    } catch (processingErr) {
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
