const admin = require("firebase-admin");
const { sanitizeTemplateParamText } = require("../lib/sanitizeTemplateParam");
const {
  buildEnquiryWelcomTwoParams,
  buildEnquiryStyleTemplateParams
} = require("./dynamicPricingEngine");

/**
 * Mirrors gode.gs getTemplateAndReminderConfig() names (Meta must approve the same keys).
 * Firebase chatbot uses enquiry (+ optional fee_summary). ERP WhatsApp is triggered via wa_native_jobs / nanbanWebIntegration.
 *
 * Firestore: platform_tenants/{tenantId}
 * ─────────────────────────────────────
 * wa_use_template_for_firebase_chatbot  (boolean, default true)
 *
 * Option A — nested object (recommended):
 *   wa_templates: {
 *     enquiry:      { name: "enquiry_welcom", language: "ta", body_param_count: 2 }
 *                   // body_param_count: 2 = {{1}} name + {{2}} service (Tamil line, e.g. கார் பயிற்சி);
 *                   // 4 = legacy fee-style; 1 = name-only fallback
 *     fee_summary:  { name: "bulk_announcement", language: "ta" }  // {{1}} = full fee text; name "" = text only
 *   }
 *
 * Option B — flat (backward compatible):
 *   wa_enquiry_template_name, wa_enquiry_template_language
 *   wa_fee_summary_template_name, wa_fee_summary_template_language
 *
 * If Meta uses different names/languages (e.g. en_US), set them here — otherwise templates fail and text fallback sends.
 */
/** Default fee line uses Meta template bulk_announcement (முக்கிய அறிவிப்பு: {{1}}) */
const DEFAULT_FEE_SLOT = { name: "bulk_announcement", language: "ta" };

const GAS_TEMPLATE_DEFAULT_NAMES = {
  /** Meta UI spelling (missing trailing "e"); body is typically {{1}} = name */
  enquiry: "enquiry_welcom",
  welcome_admission: "welcome_admission",
  llr: "welcome_admission",
  rto: "rto_test_reminder",
  rto_tomorrow: "rto_test_tomorrow",
  daily_class: "daily_class_alert",
  chit_auction: "chit_auction_alert",
  chit_due: "chit_due_reminder",
  chit_receipt: "chit_payment_receipt",
  bulk: "bulk_announcement",
  admin_alert: "admin_universal_alert",
  payment_reminder: "payment_reminder_nds",
  day_close: "day_close_report",
  quiz: "daily_quiz_btn"
};

function mergeSlot(base, patch) {
  const p = patch && typeof patch === "object" ? patch : {};
  const name = String(p.name !== undefined ? p.name : base.name).trim();
  const language = String(p.language !== undefined ? p.language : base.language).trim() || "ta";
  const valid = (n) => n === 1 || n === 2 || n === 4;
  const bodyParamCount = valid(p.body_param_count)
    ? p.body_param_count
    : base.body_param_count !== undefined && valid(base.body_param_count)
      ? base.body_param_count
      : undefined;
  return { name, language, bodyParamCount };
}

function inferEnquiryBodyParamCount(enquirySlot, data) {
  const flat = Number(data.wa_enquiry_body_param_count);
  if (flat === 1 || flat === 2 || flat === 4) return flat;
  const n = Number(enquirySlot.bodyParamCount);
  if (n === 1 || n === 2 || n === 4) return n;
  const name = String(enquirySlot.name || "").toLowerCase();
  // "enquiry_welcome" starts with "enquiry_welcom" — match 4-param name first
  if (name.includes("enquiry_welcome")) return 4;
  if (name.includes("enquiry_welcom")) return 2;
  return 2;
}

async function loadTenantDoc(tenantId) {
  const tid = String(tenantId || "").trim() || "nanban_main";
  const db = admin.firestore();
  const snap = await db.collection("platform_tenants").doc(tid).get();
  return { tid, data: snap.exists ? snap.data() || {} : {} };
}

/**
 * Full registry for UI/docs; chatbot reads enquiry + fee_summary slots.
 */
async function getTenantWaTemplateRegistry(tenantId) {
  const { tid, data } = await loadTenantDoc(tenantId);
  const wt = data.wa_templates && typeof data.wa_templates === "object" ? data.wa_templates : {};

  const enquiry = mergeSlot(
    { name: GAS_TEMPLATE_DEFAULT_NAMES.enquiry, language: "ta", bodyParamCount: 2 },
    wt.enquiry || {}
  );
  if (data.wa_enquiry_template_name) {
    enquiry.name = String(data.wa_enquiry_template_name).trim() || enquiry.name;
  }
  if (data.wa_enquiry_template_language) {
    enquiry.language = String(data.wa_enquiry_template_language).trim() || enquiry.language;
  }
  enquiry.bodyParamCount = inferEnquiryBodyParamCount(enquiry, data);

  const fee_summary = mergeSlot(DEFAULT_FEE_SLOT, wt.fee_summary || {});
  if (data.wa_fee_summary_template_name) {
    fee_summary.name = String(data.wa_fee_summary_template_name).trim();
  }
  if (data.wa_fee_summary_template_language) {
    fee_summary.language = String(data.wa_fee_summary_template_language).trim() || fee_summary.language;
  }

  return {
    tenantId: tid,
    useTemplateForChatbot: data.wa_use_template_for_firebase_chatbot !== false,
    enquiry,
    fee_summary,
    /** Reference template names (Meta); also used by native WA job processor */
    gasTemplateKeys: { ...GAS_TEMPLATE_DEFAULT_NAMES }
  };
}

const MAX_BODY_SINGLE = 1020;

/**
 * Build Graph API template payload for chatbot outbound, or null to use text only.
 */
async function resolveChatbotOutboundTemplate(tenantId, outboundKind, ctx) {
  const reg = await getTenantWaTemplateRegistry(tenantId);
  if (!reg.useTemplateForChatbot) {
    return { template: null };
  }

  const messageText = String(ctx?.messageText || "").trim();
  const selectedServices = ctx?.selectedServices || [];

  if (outboundKind === "welcome" && reg.enquiry.name) {
    const slots = reg.enquiry.bodyParamCount;
    const displayName = String(ctx?.displayName || "").trim() || "நண்பர்";
    const bodyParams =
      slots === 4
        ? buildEnquiryStyleTemplateParams(selectedServices, displayName).map(sanitizeTemplateParamText)
        : slots === 2
          ? buildEnquiryWelcomTwoParams(selectedServices, displayName).map(sanitizeTemplateParamText)
          : [sanitizeTemplateParamText(displayName)];
    return {
      template: {
        name: reg.enquiry.name,
        languageCode: reg.enquiry.language,
        bodyParams,
        tryFirst: true
      }
    };
  }

  if (
    (outboundKind === "fee_detail" || outboundKind === "fee_select") &&
    reg.fee_summary.name &&
    messageText
  ) {
    const one = sanitizeTemplateParamText(messageText).slice(0, MAX_BODY_SINGLE);
    return {
      template: {
        name: reg.fee_summary.name,
        languageCode: reg.fee_summary.language,
        bodyParams: [one],
        tryFirst: true
      }
    };
  }

  return { template: null };
}

/** @deprecated use getTenantWaTemplateRegistry */
async function getTenantWaChatbotTemplatePolicy(tenantId) {
  const reg = await getTenantWaTemplateRegistry(tenantId);
  return {
    useTemplateForChatbot: reg.useTemplateForChatbot,
    enquiryTemplateName: reg.enquiry.name,
    enquiryTemplateLanguage: reg.enquiry.language
  };
}

module.exports = {
  getTenantWaTemplateRegistry,
  resolveChatbotOutboundTemplate,
  getTenantWaChatbotTemplatePolicy,
  GAS_TEMPLATE_DEFAULT_NAMES
};
