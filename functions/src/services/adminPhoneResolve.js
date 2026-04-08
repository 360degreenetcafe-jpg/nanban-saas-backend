const admin = require("firebase-admin");
const { getBusinessSnapshotDoc } = require("./snapshotStore");

/** When no phones in Nanban snapshot or platform_tenants */
const DEFAULT_TENANT_ADMIN_PHONES = {
  nanban_main: ["919092036666", "919942391870"]
};

function cleanDigits(phone) {
  return String(phone || "").replace(/\D/g, "");
}

/**
 * Meta WhatsApp Cloud API: India numbers as 91 + 10 digits.
 */
function normalizeIndiaWaPhone_(input) {
  let d = cleanDigits(input);
  if (d.length === 10 && /^[6-9]/.test(d)) d = `91${d}`;
  return d.length >= 10 ? d : "";
}

/**
 * Split CSV / semicolon / newline; normalize each token.
 */
function parseAdminPhoneTokens_(raw) {
  if (raw == null) return [];
  if (Array.isArray(raw)) {
    return raw.flatMap((item) => parseAdminPhoneTokens_(item));
  }
  const s = String(raw).trim();
  if (!s) return [];
  return s
    .split(/[,;\n]+/)
    .map((x) => normalizeIndiaWaPhone_(x))
    .filter(Boolean);
}

/**
 * Read admin list from businesses/Nanban snapshot `appSettings` document shape
 * (matches hosted UI: appSettings.appSettings.trainerAlertPhone, optional adminPhone, etc.).
 */
function collectFromAppSettingsRoot_(root) {
  const out = [];
  const push = (raw) => {
    for (const t of parseAdminPhoneTokens_(raw)) out.push(t);
  };
  if (!root || typeof root !== "object") return [...new Set(out)];
  const inner = root.appSettings && typeof root.appSettings === "object" ? root.appSettings : {};
  push(inner.adminPhone);
  push(root.adminPhone);
  push(inner.trainerAlertPhone);
  push(root.trainerAlertPhone);
  for (const key of ["adminPhones", "admin_phones"]) {
    if (Array.isArray(inner[key])) push(inner[key]);
    if (Array.isArray(root[key])) push(root[key]);
  }
  return [...new Set(out)];
}

/**
 * All admin alert destinations for a tenant: Nanban snapshot appSettings first,
 * then platform_tenants.admin_phones, then code defaults.
 */
async function getResolvedAdminPhonesForTenant(tenantId) {
  const tid = String(tenantId || "").trim() || "nanban_main";
  const merged = new Set();

  try {
    const snap = await getBusinessSnapshotDoc("Nanban");
    const as = snap.appSettings && typeof snap.appSettings === "object" ? snap.appSettings : {};
    for (const p of collectFromAppSettingsRoot_(as)) merged.add(p);
  } catch (_e) {
    /* ignore */
  }

  try {
    const doc = await admin.firestore().collection("platform_tenants").doc(tid).get();
    if (doc.exists) {
      const data = doc.data() || {};
      if (Array.isArray(data.admin_phones) && data.admin_phones.length) {
        for (const p of data.admin_phones) {
          const n = normalizeIndiaWaPhone_(p);
          if (n) merged.add(n);
        }
      }
    }
  } catch (_e) {
    /* ignore */
  }

  if (merged.size) return [...merged];
  const fallback = DEFAULT_TENANT_ADMIN_PHONES[tid] || DEFAULT_TENANT_ADMIN_PHONES.nanban_main || [];
  return [...new Set(fallback.map((p) => normalizeIndiaWaPhone_(p)).filter(Boolean))];
}

module.exports = {
  getResolvedAdminPhonesForTenant,
  normalizeIndiaWaPhone_,
  cleanDigits,
  DEFAULT_TENANT_ADMIN_PHONES
};
