const admin = require("firebase-admin");
const { onRequest } = require("firebase-functions/v2/https");
const { onMessagePublished } = require("firebase-functions/v2/pubsub");
const { onTaskDispatched } = require("firebase-functions/v2/tasks");
const { defineSecret } = require("firebase-functions/params");
const { createWebhookApp } = require("./routes/waWebhook");
const { createDlqReplayApp } = require("./routes/opsDlqReplay");
const { createDlqDashboardApp } = require("./routes/dlqDashboardApi");
const { createNanbanWebIntegrationApp } = require("./routes/nanbanWebIntegration");
const { createWaInboundWorker } = require("./workers/waInboundWorker");
const { createWaOutboundWorker, WA_OUTBOUND_MAX_ATTEMPTS } = require("./services/waOutboundQueue");
const { registerWaNativeJobCreated } = require("./triggers/waNativeJobsTrigger");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const {
  runNanbanDailyMorning,
  runNanbanDailyEvening,
  runChitAutoReminder,
  runESevaiAppointmentReminderCron,
  runESevaiDeliveryReminderCron,
  runESevaiAgentLlrReminderCron
} = require("./jobs/scheduledCrons");

if (!admin.apps.length) {
  admin.initializeApp();
}

const WHATSAPP_VERIFY_TOKEN = defineSecret("WHATSAPP_VERIFY_TOKEN");
const WHATSAPP_APP_SECRET = defineSecret("WHATSAPP_APP_SECRET");
const WHATSAPP_GRAPH_TOKEN = defineSecret("WHATSAPP_GRAPH_TOKEN");
const WHATSAPP_PHONE_NUMBER_ID = defineSecret("WHATSAPP_PHONE_NUMBER_ID");
const INTERNAL_OPS_KEY = defineSecret("INTERNAL_OPS_KEY");
const NANBAN_WEB_WRITE_KEY = defineSecret("NANBAN_WEB_WRITE_KEY");

const webhookApp = createWebhookApp({
  getVerifyToken: () => WHATSAPP_VERIFY_TOKEN.value(),
  getAppSecret: () => WHATSAPP_APP_SECRET.value()
});
const dlqReplayApp = createDlqReplayApp({
  getOpsKey: () => INTERNAL_OPS_KEY.value()
});
const dlqDashboardApp = createDlqDashboardApp();

const waInboundWorkerHandler = createWaInboundWorker();

const waOutboundWorkerHandler = createWaOutboundWorker({
  getWaToken: () => WHATSAPP_GRAPH_TOKEN.value(),
  getWaPhoneId: () => WHATSAPP_PHONE_NUMBER_ID.value()
});

const nanbanWebIntegrationApp = createNanbanWebIntegrationApp({
  getWriteKey: () => NANBAN_WEB_WRITE_KEY.value()
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

exports.nanbanWebIntegration = onRequest(
  {
    region: "asia-south1",
    memory: "512MiB",
    timeoutSeconds: 120,
    secrets: [NANBAN_WEB_WRITE_KEY]
  },
  nanbanWebIntegrationApp
);

exports.waInboundWorker = onMessagePublished(
  {
    topic: "wa-inbound",
    region: "asia-south1",
    memory: "256MiB",
    timeoutSeconds: 120,
    retry: true
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

exports.waNativeJobWorker = registerWaNativeJobCreated({ region: "asia-south1" });

exports.nanbanCronDailyMorning = onSchedule(
  {
    schedule: "0 7 * * *",
    timeZone: "Asia/Kolkata",
    region: "asia-south1",
    memory: "512MiB",
    timeoutSeconds: 540
  },
  async () => {
    await runNanbanDailyMorning();
  }
);

exports.nanbanCronDailyEvening = onSchedule(
  {
    schedule: "0 19 * * *",
    timeZone: "Asia/Kolkata",
    region: "asia-south1",
    memory: "512MiB",
    timeoutSeconds: 300
  },
  async () => {
    await runNanbanDailyEvening();
  }
);

exports.nanbanCronChitReminders = onSchedule(
  {
    schedule: "0 9 * * *",
    timeZone: "Asia/Kolkata",
    region: "asia-south1",
    memory: "256MiB",
    timeoutSeconds: 300
  },
  async () => {
    await runChitAutoReminder();
  }
);

exports.esevaiCronAppointmentHourly = onSchedule(
  {
    schedule: "0 * * * *",
    timeZone: "Asia/Kolkata",
    region: "asia-south1",
    memory: "256MiB",
    timeoutSeconds: 120
  },
  async () => {
    await runESevaiAppointmentReminderCron();
  }
);

exports.esevaiCronDelivery = onSchedule(
  {
    schedule: "30 */2 * * *",
    timeZone: "Asia/Kolkata",
    region: "asia-south1",
    memory: "256MiB",
    timeoutSeconds: 120
  },
  async () => {
    await runESevaiDeliveryReminderCron();
  }
);

exports.esevaiCronAgentLlr = onSchedule(
  {
    schedule: "0 */6 * * *",
    timeZone: "Asia/Kolkata",
    region: "asia-south1",
    memory: "256MiB",
    timeoutSeconds: 120
  },
  async () => {
    await runESevaiAgentLlrReminderCron();
  }
);
