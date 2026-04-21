const admin = require("firebase-admin");
const { warn } = require("../lib/logger");
const { getISTDateString, getISTTimeHHMMSS } = require("../lib/istTime");

function tenantDocId_(tenantId) {
  const t = String(tenantId || "").trim();
  return t || "nanban_main";
}

function filingCol_(tenantId) {
  return admin.firestore().collection("tenants").doc(tenantDocId_(tenantId)).collection("nanban_filing_log");
}

/**
 * Append one filing row (parity with gode.gs Filing_Index sheet + logFilingEntry).
 * Hosted PDFs should call this after a successful Storage upload.
 */
async function appendNanbanFilingEntry(tenantId, { monthKey, reportType, url, metaObj, actor }) {
  const now = new Date();
  let metaStr = "";
  try {
    metaStr =
      metaObj && typeof metaObj === "object" ? JSON.stringify(metaObj) : String(metaObj || "");
  } catch (_) {
    metaStr = "";
  }
  await filingCol_(tenantId).add({
    monthKey: String(monthKey || "").trim(),
    reportType: String(reportType || "Report").trim(),
    url: String(url || "").trim(),
    meta: metaStr,
    actor: String(actor || "").trim(),
    istDate: getISTDateString(now),
    istTime: getISTTimeHHMMSS(now),
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  });
}

/**
 * List filing entries for a calendar month key YYYY-MM (same shape as getFilingEntriesAction in gode.gs).
 */
async function listNanbanFilingEntries(tenantId, monthKey) {
  const mk = String(monthKey || "").trim();
  let snap;
  try {
    snap = await filingCol_(tenantId)
      .where("monthKey", "==", mk)
      .orderBy("createdAt", "desc")
      .limit(250)
      .get();
  } catch (e) {
    const msg = String(e && e.message ? e.message : e);
    if (msg.includes("index") || msg.includes("FAILED_PRECONDITION")) {
      snap = await filingCol_(tenantId)
        .where("monthKey", "==", mk)
        .limit(250)
        .get();
    } else {
      throw e;
    }
  }

  const rows = snap.docs.map((d) => {
    const data = d.data() || {};
    const ts = data.createdAt && data.createdAt.toMillis ? data.createdAt.toMillis() : 0;
    return {
      _ts: ts,
      date: String(data.istDate || ""),
      time: String(data.istTime || ""),
      month: String(data.monthKey || ""),
      type: String(data.reportType || ""),
      url: String(data.url || ""),
      meta: String(data.meta || ""),
      actor: String(data.actor || "")
    };
  });
  rows.sort((a, b) => b._ts - a._ts);
  rows.forEach((r) => {
    delete r._ts;
  });
  return { status: "success", items: rows };
}

async function appendNanbanFilingEntryQuiet(tenantId, payload) {
  try {
    await appendNanbanFilingEntry(tenantId, payload);
  } catch (e) {
    warn("NANBAN_FILING_APPEND_FAILED", {
      tenantId: tenantDocId_(tenantId),
      reason: String(e && e.message ? e.message : e)
    });
  }
}

module.exports = {
  appendNanbanFilingEntry,
  appendNanbanFilingEntryQuiet,
  listNanbanFilingEntries
};
