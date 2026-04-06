const admin = require("firebase-admin");
const { info, warn } = require("../lib/logger");
const { runDynamicPricingFromInbound } = require("./dynamicPricingEngine");
const { getTenantBackendMode } = require("./tenantBackendMode");
const { invokeLegacyGasDynamicPricing } = require("./legacyGasAdapter");
const { enqueueWaOutboundSend } = require("./waOutboundQueue");

function cleanPhoneKey(phone) {
  return String(phone || "").replace(/[^0-9]/g, "").slice(0, 20) || "unknown";
}

async function loadChatbotState(tenantId, phone) {
  const db = admin.firestore();
  const key = cleanPhoneKey(phone);
  const ref = db.collection("tenants").doc(tenantId).collection("chatbot_state").doc(key);
  const snap = await ref.get();
  if (!snap.exists) return { selected_services: [] };
  const data = snap.data() || {};
  return {
    selected_services: Array.isArray(data.selected_services) ? data.selected_services : []
  };
}

async function saveChatbotState(tenantId, phone, state) {
  const db = admin.firestore();
  const key = cleanPhoneKey(phone);
  const ref = db.collection("tenants").doc(tenantId).collection("chatbot_state").doc(key);
  await ref.set(
    {
      selected_services: Array.isArray(state?.selected_services) ? state.selected_services : [],
      updated_at: admin.firestore.FieldValue.serverTimestamp()
    },
    { merge: true }
  );
}

async function queueOutboundMessage(tenantId, to, message, meta) {
  await enqueueWaOutboundSend(
    {
      tenantId,
      to,
      message,
      messageType: "text",
      metadata: meta || {}
    },
    { delaySeconds: 0 }
  );
  info("OUTBOUND_TASK_QUEUED_FROM_WORKER_ACTIONS", { tenantId, to });
}

/**
 * TODO(ADMIN ALERT INTEGRATION):
 * Replace this logger with real alert dispatch (WhatsApp template/email/push).
 */
async function queueAdminLeadAlert(tenantId, from, clicked) {
  const alertText = `User ${from} clicked ${clicked} - Potential Lead`;
  info("ADMIN_LEAD_ALERT_PLACEHOLDER", {
    tenantId,
    alertText
  });
}

/**
 * Canary-safe business action router.
 *
 * backend_mode in Firestore:
 * - platform_tenants/{tenantId}.backend_mode = "firebase" | "gas"
 *
 * firebase => new dynamic pricing engine in Cloud Functions
 * gas      => fallback to legacy GAS adapter during cutover
 */
async function processInboundBusinessActions(ctx) {
  const { tenantId, inbound, bridgeConfig } = ctx;
  const backendMode = await getTenantBackendMode(tenantId);
  const clicked = inbound?.interactive?.id || inbound?.interactive?.title || inbound?.text || "";

  // Admin lead alert should happen in both modes.
  if (clicked) {
    await queueAdminLeadAlert(tenantId, inbound?.from || "", clicked);
  }

  if (backendMode === "gas") {
    const legacy = await invokeLegacyGasDynamicPricing({
      bridgeUrl: bridgeConfig?.url || "",
      bridgeKey: bridgeConfig?.key || "",
      inbound
    });

    if (legacy.ok && legacy.message) {
      await queueOutboundMessage(tenantId, inbound?.from || "", legacy.message, { mode: "gas" });
      return { mode: "gas", handled: true };
    }

    // Safety fallback: if GAS path fails, use firebase engine instead.
    warn("GAS_FALLBACK_FAILED_SWITCH_TO_FIREBASE_ENGINE", { tenantId, wamid: inbound?.wamid || "" });
  }

  const state = await loadChatbotState(tenantId, inbound?.from || "");
  const result = runDynamicPricingFromInbound(inbound, state.selected_services || []);
  if (!result.handled) {
    info("DYNAMIC_PRICING_NOT_APPLICABLE", { tenantId, wamid: inbound?.wamid || "" });
    return { mode: "firebase", handled: false };
  }

  await saveChatbotState(tenantId, inbound?.from || "", {
    selected_services: result.selectedServices || []
  });

  await queueOutboundMessage(tenantId, inbound?.from || "", result.message, {
    mode: "firebase",
    selectedServices: result.selectedServices || []
  });

  return { mode: "firebase", handled: true };
}

module.exports = { processInboundBusinessActions };
