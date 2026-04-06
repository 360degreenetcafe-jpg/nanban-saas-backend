const admin = require("firebase-admin");
const { onRequest } = require("firebase-functions/v2/https");
const { onMessagePublished } = require("firebase-functions/v2/pubsub");
const { onTaskDispatched } = require("firebase-functions/v2/tasks");
const { defineSecret } = require("firebase-functions/params");
const { createWebhookApp } = require("./routes/waWebhook");
const { createDlqReplayApp } = require("./routes/opsDlqReplay");
const { createDlqDashboardApp } = require("./routes/dlqDashboardApi");
const { createWaInboundWorker } = require("./workers/waInboundWorker");
const { createWaOutboundWorker, WA_OUTBOUND_MAX_ATTEMPTS } = require("./services/waOutboundQueue");

if (!admin.apps.length) {
  admin.initializeApp();
}

const WHATSAPP_VERIFY_TOKEN = defineSecret("WHATSAPP_VERIFY_TOKEN");
const WHATSAPP_APP_SECRET = defineSecret("WHATSAPP_APP_SECRET");
const WHATSAPP_GRAPH_TOKEN = defineSecret("WHATSAPP_GRAPH_TOKEN");
const WHATSAPP_PHONE_NUMBER_ID = defineSecret("WHATSAPP_PHONE_NUMBER_ID");
const LEGACY_GAS_BRIDGE_URL = defineSecret("LEGACY_GAS_BRIDGE_URL");
const LEGACY_GAS_BRIDGE_KEY = defineSecret("LEGACY_GAS_BRIDGE_KEY");
const INTERNAL_OPS_KEY = defineSecret("INTERNAL_OPS_KEY");

const webhookApp = createWebhookApp({
  getVerifyToken: () => WHATSAPP_VERIFY_TOKEN.value(),
  getAppSecret: () => WHATSAPP_APP_SECRET.value()
});
const dlqReplayApp = createDlqReplayApp({
  getOpsKey: () => INTERNAL_OPS_KEY.value()
});
const dlqDashboardApp = createDlqDashboardApp();

const waInboundWorkerHandler = createWaInboundWorker({
  getLegacyBridgeUrl: () => LEGACY_GAS_BRIDGE_URL.value(),
  getLegacyBridgeKey: () => LEGACY_GAS_BRIDGE_KEY.value()
});

const waOutboundWorkerHandler = createWaOutboundWorker({
  getWaToken: () => WHATSAPP_GRAPH_TOKEN.value(),
  getWaPhoneId: () => WHATSAPP_PHONE_NUMBER_ID.value()
});

exports.whatsappWebhook = onRequest(
  {
    region: "asia-south1",
    memory: "256MiB",
    timeoutSeconds: 60,
    secrets: [WHATSAPP_VERIFY_TOKEN, WHATSAPP_APP_SECRET]
  },
  webhookApp
);

exports.waDlqReplay = onRequest(
  {
    region: "asia-south1",
    memory: "256MiB",
    timeoutSeconds: 60,
    secrets: [INTERNAL_OPS_KEY]
  },
  dlqReplayApp
);

exports.dlqDashboardApi = onRequest(
  {
    region: "asia-south1",
    memory: "256MiB",
    timeoutSeconds: 60
  },
  dlqDashboardApp
);

exports.waInboundWorker = onMessagePublished(
  {
    topic: "wa-inbound",
    region: "asia-south1",
    memory: "256MiB",
    timeoutSeconds: 120,
    retry: true,
    secrets: [LEGACY_GAS_BRIDGE_URL, LEGACY_GAS_BRIDGE_KEY]
  },
  waInboundWorkerHandler
);

exports.waOutboundWorker = onTaskDispatched(
  {
    region: "asia-south1",
    retryConfig: {
      maxAttempts: WA_OUTBOUND_MAX_ATTEMPTS,
      minBackoffSeconds: 5,
      maxBackoffSeconds: 300,
      maxRetrySeconds: 3600,
      maxDoublings: 5
    },
    rateLimits: {
      maxConcurrentDispatches: 25,
      maxDispatchesPerSecond: 8
    },
    secrets: [WHATSAPP_GRAPH_TOKEN, WHATSAPP_PHONE_NUMBER_ID]
  },
  waOutboundWorkerHandler
);
