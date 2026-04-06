const { PubSub } = require("@google-cloud/pubsub");
const admin = require("firebase-admin");
const { info } = require("../lib/logger");

const pubsub = new PubSub();
const WA_INBOUND_TOPIC = "wa-inbound";

/**
 * Publish inbound webhook payload to Pub/Sub for async processing.
 * Keep this publisher lightweight because webhook route must ack quickly.
 */
async function publishInboundWebhookEvent(event) {
  const payload = {
    source: "meta_whatsapp_webhook",
    receivedAt: new Date().toISOString(),
    ...event
  };

  if (process.env.WA_INBOUND_PUBLISH_MOCK === "1") {
    const db = admin.firestore();
    const ref = db.collection("_test_runtime").doc("wa_inbound_mock").collection("events").doc();
    await ref.set({
      payload,
      created_at: admin.firestore.FieldValue.serverTimestamp()
    });
    info("WA_INBOUND_MOCK_STORED", {
      mockId: ref.id,
      messageCount: payload?.messageCount || 0
    });
    return { queued: true, pubsubMessageId: ref.id, mode: "mock" };
  }

  const topic = pubsub.topic(WA_INBOUND_TOPIC);
  const dataBuffer = Buffer.from(JSON.stringify(payload), "utf8");
  const messageId = await topic.publishMessage({
    data: dataBuffer,
    attributes: {
      source: "meta_whatsapp",
      hasMessages: String(payload?.messageCount > 0)
    }
  });

  info("WA_INBOUND_PUBLISHED", {
    pubsubMessageId: messageId,
    messageCount: payload?.messageCount || 0
  });

  return { queued: true, pubsubMessageId: messageId };
}

module.exports = { publishInboundWebhookEvent, WA_INBOUND_TOPIC };
