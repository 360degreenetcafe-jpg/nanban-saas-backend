const admin = require("firebase-admin");

const PRIMARY_PHONE_TO_TENANT = {
  // Current Nanban production WA number -> main tenant
  "978781185326220": "nanban_main"
};

/**
 * Resolve tenant from WhatsApp business phone_number_id.
 *
 * Priority:
 * 1) hardcoded primary mapping (safe bootstrap)
 * 2) Firestore mapping: platform/wa_phone_map/{phoneNumberId}
 * 3) fallback -> nanban_main
 */
async function resolveTenantFromPhoneNumberId(phoneNumberId) {
  const phoneId = String(phoneNumberId || "").trim();

  if (phoneId && PRIMARY_PHONE_TO_TENANT[phoneId]) {
    return {
      tenantId: PRIMARY_PHONE_TO_TENANT[phoneId],
      routeSource: "bootstrap_primary_mapping",
      phoneNumberId: phoneId
    };
  }

  if (phoneId) {
    const db = admin.firestore();
    const mapRef = db.collection("platform").doc("wa_phone_map").collection("numbers").doc(phoneId);
    const snap = await mapRef.get();
    if (snap.exists) {
      const data = snap.data() || {};
      if (data.active !== false && data.tenant_id) {
        return {
          tenantId: String(data.tenant_id),
          routeSource: "firestore_mapping",
          phoneNumberId: phoneId
        };
      }
    }
  }

  return {
    tenantId: "nanban_main",
    routeSource: "fallback_default",
    phoneNumberId: phoneId || null
  };
}

module.exports = { resolveTenantFromPhoneNumberId };
