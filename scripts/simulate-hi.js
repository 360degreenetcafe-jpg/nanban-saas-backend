/**
 * Injects a standard Meta-style "Hi" text message through the same code path as
 * waInboundWorker (Pub/Sub handler), bypassing HTTP signature + Pub/Sub transport.
 *
 * Usage (repo root):
 *   node scripts/simulate-hi.js
 *
 * Requires: Application Default Credentials for Firestore (same as emergency scripts).
 * Sets WA_OUTBOUND_MOCK=1 so enqueueWaOutboundSend writes to tenants/{tid}/wa_outbound_mock.
 */
process.env.WA_OUTBOUND_MOCK = "1";
process.env.GCLOUD_PROJECT = process.env.GCLOUD_PROJECT || "nanban-driving-school-d7b20";
process.env.GOOGLE_CLOUD_PROJECT = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;

const path = require("path");
const admin = require(path.join(__dirname, "..", "functions", "node_modules", "firebase-admin"));

async function main() {
  if (!admin.apps.length) {
    try {
      admin.initializeApp({
        credential: admin.credential.applicationDefault(),
        projectId: process.env.GCLOUD_PROJECT
      });
    } catch (_) {
      admin.initializeApp({ projectId: process.env.GCLOUD_PROJECT });
    }
  }

  const { createWaInboundWorker } = require(path.join(__dirname, "..", "functions", "src", "workers", "waInboundWorker"));
  const handler = createWaInboundWorker();

  const wamid = `wamid.SIMULATE_HI_${Date.now()}`;
  const webhookPayload = {
    object: "whatsapp_business_account",
    entry: [
      {
        id: "WABA_SIM",
        changes: [
          {
            field: "messages",
            value: {
              messaging_product: "whatsapp",
              metadata: { phone_number_id: "978781185326220" },
              contacts: [{ profile: { name: "Sim User" } }],
              messages: [
                {
                  from: "919876543210",
                  id: wamid,
                  timestamp: String(Math.floor(Date.now() / 1000)),
                  type: "text",
                  text: { body: "Hi" }
                }
              ]
            }
          }
        ]
      }
    ]
  };

  const envelope = {
    source: "simulate-hi",
    payload: webhookPayload,
    messageCount: 1,
    receivedAt: new Date().toISOString()
  };

  const event = {
    data: {
      message: {
        data: Buffer.from(JSON.stringify(envelope), "utf8").toString("base64"),
        messageId: `sim-hi-${Date.now()}`
      }
    }
  };

  console.log("SIMULATE_HI: running waInboundWorker handler (wamid=%s)...", wamid);
  await handler(event);

  const db = admin.firestore();
  const col = db.collection("tenants").doc("nanban_main").collection("wa_outbound_mock");
  const qs = await col.limit(10).get();

  console.log("SIMULATE_HI: wa_outbound_mock docs (up to 10, unordered):", qs.size);
  qs.forEach((doc) => {
    const d = doc.data() || {};
    console.log("  doc", doc.id, "| to=", d.to, "| messageType=", d.messageType, "| messagePreview=", String(d.message || "").slice(0, 100).replace(/\n/g, " "));
  });

  if (qs.empty) {
    console.error("SIMULATE_HI: FAIL — no rows in tenants/nanban_main/wa_outbound_mock (nothing was enqueued).");
    console.error("SIMULATE_HI: Direct router probe (bypass idempotency claim)...");

    const { processInboundBusinessActions } = require(path.join(__dirname, "..", "functions", "src", "services", "workerActions"));
    const inbound = {
      wamid,
      from: "919876543210",
      timestamp: String(Math.floor(Date.now() / 1000)),
      type: "text",
      text: "Hi",
      interactive: null,
      phoneNumberId: "978781185326220",
      profileName: "Sim User",
      raw: {}
    };
    const r = await processInboundBusinessActions({
      tenantId: "nanban_main",
      tenantRouteSource: "simulate",
      inbound
    });
    console.log("SIMULATE_HI: processInboundBusinessActions result:", JSON.stringify(r));

    const qs2 = await col.limit(10).get();
    if (qs2.empty) {
      console.error("SIMULATE_HI: FAIL — still no outbound mock after direct processInboundBusinessActions.");
      process.exit(1);
    }
    qs2.forEach((doc) => {
      const d = doc.data() || {};
      console.log("  after direct:", doc.id, "| to=", d.to, "| messageType=", d.messageType);
    });
  }

  console.log("SIMULATE_HI: done.");
}

main().catch((e) => {
  console.error("SIMULATE_HI: FATAL", e);
  process.exit(1);
});
