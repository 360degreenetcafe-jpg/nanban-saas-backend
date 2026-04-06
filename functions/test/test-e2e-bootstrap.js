const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const admin = require("firebase-admin");
const { createWebhookApp } = require("../src/routes/waWebhook");
const { createWaInboundWorker } = require("../src/workers/waInboundWorker");
const { createWaOutboundWorker } = require("../src/services/waOutboundQueue");
const { createDlqDashboardApp } = require("../src/routes/dlqDashboardApi");

const VERIFY_TOKEN = "verify_local_token";
const APP_SECRET = "local_app_secret";
const TENANT_ID = "nanban_main";

process.env.WA_INBOUND_PUBLISH_MOCK = "1";
process.env.WA_OUTBOUND_MOCK = "1";

function resolveProjectId() {
  if (process.env.GCLOUD_PROJECT) return process.env.GCLOUD_PROJECT;
  if (process.env.GOOGLE_CLOUD_PROJECT) return process.env.GOOGLE_CLOUD_PROJECT;
  if (process.env.FIREBASE_CONFIG) {
    try {
      const cfg = JSON.parse(process.env.FIREBASE_CONFIG);
      if (cfg?.projectId) return String(cfg.projectId);
    } catch (_e) {
      // Ignore invalid FIREBASE_CONFIG in tests.
    }
  }
  return "nanban-driving-school-d7b20";
}

const PROJECT_ID = resolveProjectId();
process.env.GCLOUD_PROJECT = PROJECT_ID;
process.env.GOOGLE_CLOUD_PROJECT = PROJECT_ID;

function assertTrue(condition, message) {
  if (!condition) throw new Error(message);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function loadPayloads() {
  const p = path.join(__dirname, "wa.integration.payloads.json");
  return JSON.parse(fs.readFileSync(p, "utf8")).flows;
}

async function startExpressServer(app) {
  return await new Promise((resolve, reject) => {
    const server = app.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      resolve({
        server,
        baseUrl: `http://127.0.0.1:${addr.port}`
      });
    });
    server.on("error", reject);
  });
}

async function stopExpressServer(server) {
  if (!server) return;
  await new Promise((resolve) => server.close(resolve));
}

function signWebhook(rawBody) {
  const digest = crypto.createHmac("sha256", APP_SECRET).update(Buffer.from(rawBody, "utf8")).digest("hex");
  return `sha256=${digest}`;
}

async function postWebhook(baseUrl, webhookPayload) {
  const raw = JSON.stringify(webhookPayload);
  const sig = signWebhook(raw);
  const res = await fetch(`${baseUrl}/wa/webhook`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-hub-signature-256": sig
    },
    body: raw
  });
  const text = await res.text();
  assertTrue(res.status === 200, `Webhook POST failed: ${res.status} ${text}`);
}

async function createOrUpdateUser({ email, password, displayName, claims }) {
  const auth = admin.auth();
  let user;
  try {
    user = await auth.getUserByEmail(email);
    await auth.updateUser(user.uid, { password, displayName });
  } catch (e) {
    user = await auth.createUser({ email, password, displayName });
  }
  await auth.setCustomUserClaims(user.uid, claims || {});
  return user;
}

async function signInForIdToken(email, password) {
  const host = process.env.FIREBASE_AUTH_EMULATOR_HOST;
  assertTrue(!!host, "FIREBASE_AUTH_EMULATOR_HOST not set");
  const url = `http://${host}/identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=fake-api-key`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-firebase-project": PROJECT_ID
    },
    body: JSON.stringify({ email, password, returnSecureToken: true })
  });
  const data = await res.json();
  assertTrue(res.ok && data.idToken, `Auth sign-in failed: ${JSON.stringify(data)}`);
  return data.idToken;
}

async function seedCoreData(adminUid, staffUid) {
  const db = admin.firestore();
  await db.collection("platform_tenants").doc(TENANT_ID).set(
    {
      backend_mode: "firebase",
      active: true,
      admin_phones: ["919092036666", "919942391870"],
      updated_at: admin.firestore.FieldValue.serverTimestamp()
    },
    { merge: true }
  );

  await db.collection("tenants").doc(TENANT_ID).collection("users").doc(adminUid).set(
    {
      role: "owner",
      active: true
    },
    { merge: true }
  );
  await db.collection("tenants").doc(TENANT_ID).collection("users").doc(staffUid).set(
    {
      role: "staff",
      active: true
    },
    { merge: true }
  );
}

function toPubSubEventFromStoredPayload(storedPayload, messageId) {
  return {
    data: {
      message: {
        messageId: String(messageId || ""),
        data: Buffer.from(JSON.stringify(storedPayload), "utf8").toString("base64")
      }
    }
  };
}

async function drainInboundMockEventsAndRunWorker(inboundWorker) {
  const db = admin.firestore();
  const col = db.collection("_test_runtime").doc("wa_inbound_mock").collection("events");
  const maxRounds = 12;

  for (let round = 0; round < maxRounds; round += 1) {
    const snap = await col.limit(50).get();
    if (snap.empty) return;

    for (const doc of snap.docs) {
      const payload = doc.data()?.payload;
      if (payload) {
        await inboundWorker(toPubSubEventFromStoredPayload(payload, doc.id));
      }
      await doc.ref.delete();
    }

    // Allow async mock publish writes to settle between rounds.
    await sleep(60);
  }
}

async function getOutboundMockCountForPhone(phone) {
  const db = admin.firestore();
  const snap = await db
    .collection("tenants")
    .doc(TENANT_ID)
    .collection("wa_outbound_mock")
    .where("to", "==", String(phone))
    .get();
  return snap.size;
}

async function getLatestOutboundMockForPhone(phone) {
  const db = admin.firestore();
  const snap = await db
    .collection("tenants")
    .doc(TENANT_ID)
    .collection("wa_outbound_mock")
    .where("to", "==", String(phone))
    .orderBy("created_at", "desc")
    .limit(1)
    .get();
  if (snap.empty) return null;
  return { id: snap.docs[0].id, ...snap.docs[0].data() };
}

async function waitForLatestOutboundMockForPhone(phone, timeoutMs = 5000) {
  const started = Date.now();
  while (Date.now() - started <= timeoutMs) {
    const found = await getLatestOutboundMockForPhone(phone);
    if (found) return found;
    await sleep(120);
  }
  return null;
}

async function setBackendMode(mode) {
  const db = admin.firestore();
  await db.collection("platform_tenants").doc(TENANT_ID).set(
    { backend_mode: mode, updated_at: admin.firestore.FieldValue.serverTimestamp() },
    { merge: true }
  );
}

async function run() {
  assertTrue(!!process.env.FIRESTORE_EMULATOR_HOST, "FIRESTORE_EMULATOR_HOST not set");
  assertTrue(!!process.env.FIREBASE_AUTH_EMULATOR_HOST, "FIREBASE_AUTH_EMULATOR_HOST not set");

  if (!admin.apps.length) {
    admin.initializeApp({
      projectId: PROJECT_ID
    });
  }

  const payloads = loadPayloads();

  const adminUser = await createOrUpdateUser({
    email: "owner.nanban@example.com",
    password: "Pass@1234",
    displayName: "Nanban Owner",
    claims: { tenant_id: TENANT_ID, role: "owner", roles: ["owner"] }
  });
  const staffUser = await createOrUpdateUser({
    email: "staff.nanban@example.com",
    password: "Pass@1234",
    displayName: "Nanban Staff",
    claims: { tenant_id: TENANT_ID, role: "staff", roles: ["staff"] }
  });
  await seedCoreData(adminUser.uid, staffUser.uid);

  const inboundWorker = createWaInboundWorker();
  const outboundWorker = createWaOutboundWorker({
    getWaToken: () => "mock_token",
    getWaPhoneId: () => "mock_phone"
  });

  const webhookSrv = await startExpressServer(
    createWebhookApp({
      getVerifyToken: () => VERIFY_TOKEN,
      getAppSecret: () => APP_SECRET
    })
  );
  const dlqSrv = await startExpressServer(createDlqDashboardApp());

  try {
    // Flow 1: Hi -> Welcome + MENU_FEES hint
    await postWebhook(webhookSrv.baseUrl, payloads.flow1_hi_welcome.webhookPayload);
    await drainInboundMockEventsAndRunWorker(inboundWorker);
    const flow1Msg = await getLatestOutboundMockForPhone("919900000001");
    assertTrue(!!flow1Msg, "Flow1: outbound not generated");
    assertTrue(String(flow1Msg.message || "").includes("MENU_FEES"), "Flow1: MENU_FEES hint missing");

    // Flow 2: Specific service select
    await postWebhook(webhookSrv.baseUrl, payloads.flow2_select_service_dynamic_pricing.webhookPayload);
    await drainInboundMockEventsAndRunWorker(inboundWorker);
    const flow2Msg = await waitForLatestOutboundMockForPhone("919900000002");
    assertTrue(!!flow2Msg, "Flow2: outbound not generated");
    assertTrue(String(flow2Msg.message || "").includes("₹3000"), "Flow2: expected ₹3000 missing");

    // Flow 3: Idempotency duplicate check
    const before3 = await getOutboundMockCountForPhone("919900000003");
    await postWebhook(webhookSrv.baseUrl, payloads.flow3_idempotency_duplicate.webhookPayload);
    await postWebhook(webhookSrv.baseUrl, payloads.flow3_idempotency_duplicate.webhookPayload);
    await drainInboundMockEventsAndRunWorker(inboundWorker);
    const after3 = await getOutboundMockCountForPhone("919900000003");
    assertTrue(after3 === before3 + 1, `Flow3: duplicate not dropped (delta=${after3 - before3})`);

    // Flow 4: GAS bridge removed — always Firebase engine
    await setBackendMode("gas");
    const flow4a = JSON.parse(JSON.stringify(payloads.flow4_cutover_toggle.webhookPayload));
    flow4a.entry[0].changes[0].value.messages[0].id = "wamid.flow4.native.001";
    await postWebhook(webhookSrv.baseUrl, flow4a);
    await drainInboundMockEventsAndRunWorker(inboundWorker);
    const flow4First = await getLatestOutboundMockForPhone("919900000004");
    assertTrue((flow4First?.metadata || {}).mode === "firebase", "Flow4 expected firebase mode");

    await setBackendMode("firebase");
    const flow4b = JSON.parse(JSON.stringify(payloads.flow4_cutover_toggle.webhookPayload));
    flow4b.entry[0].changes[0].value.messages[0].id = "wamid.flow4.native.002";
    await postWebhook(webhookSrv.baseUrl, flow4b);
    await drainInboundMockEventsAndRunWorker(inboundWorker);
    const flow4Second = await getLatestOutboundMockForPhone("919900000004");
    assertTrue((flow4Second?.metadata || {}).mode === "firebase", "Flow4 second message firebase mismatch");

    // DLQ fallback simulation (final retry exhausted)
    await outboundWorker({
      data: {
        tenantId: TENANT_ID,
        to: "919977776666",
        message: "Forced fail to test DLQ",
        messageType: "text",
        metadata: { force_fail: true }
      },
      headers: {
        "x-cloudtasks-taskretrycount": "9",
        "x-cloudtasks-taskexecutioncount": "10",
        "x-cloudtasks-taskname": "projects/demo/locations/asia-south1/queues/waOutboundWorker/tasks/task-final-001",
        "x-cloudtasks-queuename": "waOutboundWorker"
      }
    });

    const db = admin.firestore();
    const dlqSnap = await db.collection("tenants").doc(TENANT_ID).collection("wa_dlq").limit(1).get();
    assertTrue(!dlqSnap.empty, "DLQ simulation failed: no DLQ entry found");

    // RBAC test on secure dashboard endpoint
    const adminToken = await signInForIdToken("owner.nanban@example.com", "Pass@1234");
    const staffToken = await signInForIdToken("staff.nanban@example.com", "Pass@1234");

    const adminRes = await fetch(`${dlqSrv.baseUrl}/api/v1/tenants/${TENANT_ID}/dlq/stats?limit=20`, {
      headers: { authorization: `Bearer ${adminToken}` }
    });
    const adminJson = await adminRes.json();
    assertTrue(adminRes.status === 200, `DLQ dashboard admin auth failed: ${adminRes.status}`);
    assertTrue(adminJson?.stats?.pending_replay >= 1, "DLQ dashboard stats incorrect");

    const staffRes = await fetch(`${dlqSrv.baseUrl}/api/v1/tenants/${TENANT_ID}/dlq/stats?limit=20`, {
      headers: { authorization: `Bearer ${staffToken}` }
    });
    assertTrue(staffRes.status === 403, `DLQ dashboard RBAC failed (expected 403, got ${staffRes.status})`);

    console.log("E2E Bootstrap Test Passed ✅");
    console.log("Verified: webhook->inbound worker->idempotency->cutover toggle->DLQ->dashboard RBAC");
  } finally {
    await stopExpressServer(webhookSrv.server);
    await stopExpressServer(dlqSrv.server);
  }
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("E2E Bootstrap Test Failed ❌", err);
    process.exit(1);
  });
