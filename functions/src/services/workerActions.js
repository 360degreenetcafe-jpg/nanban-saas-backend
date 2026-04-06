const admin = require("firebase-admin");
const { info } = require("../lib/logger");
const { runDynamicPricingFromInbound } = require("./dynamicPricingEngine");
const { enqueueWaOutboundSend } = require("./waOutboundQueue");
const { resolveChatbotOutboundTemplate } = require("./waTemplateConfig");

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

/**
 * Firebase chatbot: Meta template first (enquiry welcome + optional fee summary), then text fallback.
 */
async function queueOutboundFirebaseChatbot(tenantId, to, result, meta) {
  const textBody = String(result?.message || "").trim();
  const { template } = await resolveChatbotOutboundTemplate(tenantId, result?.outboundKind || "noop", {
    messageText: textBody,
    selectedServices: result.selectedServices || [],
    displayName: meta?.displayName || ""
  });

  await enqueueWaOutboundSend(
    {
      tenantId,
      to,
      message: textBody,
      messageType: template ? "template_with_text_fallback" : "text",
      template,
      metadata: Object.assign({}, meta || {}, {
        outbound_kind: result?.outboundKind || "",
        template_name: template ? template.name : ""
      })
    },
    { delaySeconds: 0 }
  );
  info("OUTBOUND_TASK_QUEUED_FROM_WORKER_ACTIONS", { tenantId, to, kind: result?.outboundKind || "" });
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
 * Business action router — Firebase engine only (no GAS bridge).
 */
async function processInboundBusinessActions(ctx) {
  const { tenantId, inbound } = ctx;
  const clicked = inbound?.interactive?.id || inbound?.interactive?.title || inbound?.text || "";

  if (clicked) {
    await queueAdminLeadAlert(tenantId, inbound?.from || "", clicked);
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

  await queueOutboundFirebaseChatbot(tenantId, inbound?.from || "", result, {
    mode: "firebase",
    selectedServices: result.selectedServices || [],
    displayName: inbound?.profileName || ""
  });

  return { mode: "firebase", handled: true };
}

module.exports = { processInboundBusinessActions };
