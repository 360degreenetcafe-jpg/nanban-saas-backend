const admin = require("firebase-admin");

function safeDocIdFromWamid(wamid) {
  return String(wamid || "")
    .trim()
    .replace(/[^a-zA-Z0-9:_-]/g, "_")
    .slice(0, 250);
}

/**
 * Idempotency gate:
 * - create marker doc under tenant scope using wamid
 * - if create succeeds => first-time processing
 * - if already exists => duplicate, skip processing
 */
async function claimWamidForProcessing({ tenantId, wamid, from, eventMeta }) {
  const db = admin.firestore();
  const safeId = safeDocIdFromWamid(wamid);
  if (!safeId) {
    return { claimed: false, reason: "missing_wamid" };
  }

  const ref = db
    .collection("tenants")
    .doc(String(tenantId))
    .collection("wa_processed")
    .doc(safeId);

  try {
    await ref.create({
      wamid: String(wamid),
      from: String(from || ""),
      processed_at: admin.firestore.FieldValue.serverTimestamp(),
      event_meta: eventMeta || {}
    });
    return { claimed: true };
  } catch (err) {
    const code = err && (err.code || err.status);
    const alreadyExists = code === 6 || code === "already-exists" || String(err).toLowerCase().includes("already exists");
    if (alreadyExists) return { claimed: false, reason: "duplicate" };
    throw err;
  }
}

module.exports = { claimWamidForProcessing };
