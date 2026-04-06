const fs = require("fs");
const path = require("path");
const admin = require("firebase-admin");
const { createWaInboundWorker } = require("../src/workers/waInboundWorker");

process.env.WA_OUTBOUND_MOCK = "1";

if (!process.env.FIRESTORE_EMULATOR_HOST) {
  console.error("FIRESTORE_EMULATOR_HOST is required. Start emulator first.");
  process.exit(1);
}

if (!admin.apps.length) {
  admin.initializeApp({
    projectId: process.env.GCLOUD_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || "nanban-driving-school-d7b20"
  });
}

const db = admin.firestore();

function assertTrue(cond, msg) {
  if (!cond) throw new Error(msg);
}

function loadPayloads() {
  const p = path.join(__dirname, "wa.integration.payloads.json");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function toPubSubEvent(webhookPayload, messageId) {
  const envelope = {
    source: "integration_test",
    receivedAt: new Date().toISOString(),
    payload: webhookPayload,
    messageCount: (((webhookPayload || {}).entry || [])
      .flatMap((e) => e.changes || [])
      .flatMap((c) => (c.value && c.value.messages) ? c.value.messages : [])).length
  };

  return {
    data: {
      message: {
        messageId: String(messageId || ""),
        data: Buffer.from(JSON.stringify(envelope), "utf8").toString("base64")
      }
    }
  };
}

async function setBackendMode(tenantId, mode) {
  await db.collection("platform_tenants").doc(tenantId).set({
    backend_mode: mode,
    active: true,
    updated_at: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
}

async function getOutboundMocksByPhone(tenantId, phone) {
  const snap = await db
    .collection("tenants")
    .doc(tenantId)
    .collection("wa_outbound_mock")
    .where("to", "==", String(phone))
    .get();
  return snap.docs.map((d) => ({ id: d.id, ...d.data() }));
}

async function run() {
  const payloads = loadPayloads();
  const worker = createWaInboundWorker({
    getLegacyBridgeUrl: () => "mock://success",
    getLegacyBridgeKey: () => "mock_key"
  });

  const tenantId = "nanban_main";

  // Flow 1: Hi -> welcome with MENU_FEES hint
  await setBackendMode(tenantId, "firebase");
  await worker(toPubSubEvent(payloads.flows.flow1_hi_welcome.webhookPayload, "flow1-msg"));
  const f1 = await getOutboundMocksByPhone(tenantId, "919900000001");
  assertTrue(f1.length >= 1, "Flow1 failed: no outbound queued");
  assertTrue(String(f1[f1.length - 1].message || "").includes("MENU_FEES"), "Flow1 failed: MENU_FEES hint missing");

  // Flow 2: select FW training -> dynamic ₹3000
  await setBackendMode(tenantId, "firebase");
  await worker(toPubSubEvent(payloads.flows.flow2_select_service_dynamic_pricing.webhookPayload, "flow2-msg"));
  const f2 = await getOutboundMocksByPhone(tenantId, "919900000002");
  assertTrue(f2.length >= 1, "Flow2 failed: no outbound queued");
  assertTrue(String(f2[f2.length - 1].message || "").includes("₹3000"), "Flow2 failed: expected ₹3000 missing");

  // Flow 3: duplicate wamid should be dropped
  await setBackendMode(tenantId, "firebase");
  const before3 = (await getOutboundMocksByPhone(tenantId, "919900000003")).length;
  await worker(toPubSubEvent(payloads.flows.flow3_idempotency_duplicate.webhookPayload, "flow3-msg-a"));
  await worker(toPubSubEvent(payloads.flows.flow3_idempotency_duplicate.webhookPayload, "flow3-msg-b"));
  const after3 = (await getOutboundMocksByPhone(tenantId, "919900000003")).length;
  assertTrue(after3 === before3 + 1, `Flow3 failed: expected +1 outbound, got ${after3 - before3}`);

  // Flow 4: cutover toggle gas -> firebase
  const flow4Gas = JSON.parse(JSON.stringify(payloads.flows.flow4_cutover_toggle.webhookPayload));
  await setBackendMode(tenantId, "gas");
  await worker(toPubSubEvent(flow4Gas, "flow4-msg-gas"));
  const f4Gas = await getOutboundMocksByPhone(tenantId, "919900000004");
  assertTrue(f4Gas.length >= 1, "Flow4(gas) failed: no outbound queued");
  assertTrue((f4Gas[f4Gas.length - 1].metadata || {}).mode === "gas", "Flow4(gas) failed: mode is not gas");

  const flow4Fb = JSON.parse(JSON.stringify(payloads.flows.flow4_cutover_toggle.webhookPayload));
  flow4Fb.entry[0].changes[0].value.messages[0].id = "wamid.flow4.toggle.002";
  await setBackendMode(tenantId, "firebase");
  await worker(toPubSubEvent(flow4Fb, "flow4-msg-firebase"));
  const f4Fb = await getOutboundMocksByPhone(tenantId, "919900000004");
  assertTrue(f4Fb.length >= 2, "Flow4(firebase) failed: expected second outbound");
  assertTrue((f4Fb[f4Fb.length - 1].metadata || {}).mode === "firebase", "Flow4(firebase) failed: mode is not firebase");

  console.log("All integration mock flows passed ✅");
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("Integration mock suite failed ❌", err);
    process.exit(1);
  });
