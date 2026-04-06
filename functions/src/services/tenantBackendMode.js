const admin = require("firebase-admin");

/**
 * Resolve per-tenant backend mode for cutover.
 *
 * Expected document:
 * platform/tenants/{tenantId}
 * {
 *   backend_mode: "firebase" | "gas",
 *   active: true
 * }
 */
async function getTenantBackendMode(tenantId) {
  const tid = String(tenantId || "").trim() || "nanban_main";
  const db = admin.firestore();
  const snap = await db.collection("platform_tenants").doc(tid).get();
  if (!snap.exists) return "firebase";
  const data = snap.data() || {};
  const mode = String(data.backend_mode || "firebase").toLowerCase();
  return mode === "gas" ? "gas" : "firebase";
}

module.exports = { getTenantBackendMode };
