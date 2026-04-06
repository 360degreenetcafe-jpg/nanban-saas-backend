const { enqueueWaOutboundSend } = require("./waOutboundQueue");

const DEFAULT_ADMIN_PHONES = {
  nanban_main: ["919092036666", "919942391870"]
};

function cleanPhone(phone) {
  return String(phone || "").replace(/\D/g, "");
}

async function getTenantAdminPhones(tenantId) {
  const admin = require("firebase-admin");
  const tid = String(tenantId || "").trim() || "nanban_main";
  try {
    const snap = await admin.firestore().collection("platform_tenants").doc(tid).get();
    if (snap.exists) {
      const data = snap.data() || {};
      if (Array.isArray(data.admin_phones) && data.admin_phones.length) {
        return data.admin_phones.map(cleanPhone).filter(Boolean);
      }
    }
  } catch (e) {}
  return (DEFAULT_ADMIN_PHONES[tid] || []).map(cleanPhone).filter(Boolean);
}

async function notifyAdminsText(tenantId, message) {
  const msg = String(message || "").trim();
  if (!msg) return;
  const phones = await getTenantAdminPhones(tenantId);
  for (const phone of phones) {
    try {
      await enqueueWaOutboundSend(
        {
          tenantId: String(tenantId || "").trim() || "nanban_main",
          to: phone,
          message: msg,
          messageType: "text",
          metadata: { kind: "admin_notify" }
        },
        { delaySeconds: 0 }
      );
    } catch (e) {}
  }
}

module.exports = { notifyAdminsText, getTenantAdminPhones, cleanPhone };
