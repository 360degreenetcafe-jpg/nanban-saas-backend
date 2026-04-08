const { enqueueWaOutboundSend } = require("./waOutboundQueue");
const { getResolvedAdminPhonesForTenant, cleanDigits } = require("./adminPhoneResolve");

function cleanPhone(phone) {
  return cleanDigits(phone);
}

async function getTenantAdminPhones(tenantId) {
  return getResolvedAdminPhonesForTenant(tenantId);
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
    } catch (e) {
      console.warn(`ADMIN_NOTIFY_ENQUEUE_FAIL to=${phone} ${String(e && e.message ? e.message : e)}`);
    }
  }
}

module.exports = { notifyAdminsText, getTenantAdminPhones, cleanPhone };
