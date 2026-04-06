const admin = require("firebase-admin");
const { onDocumentCreated } = require("firebase-functions/v2/firestore");
const { error, info } = require("../lib/logger");
const { processWaNativeJob } = require("../services/waNativeJobProcessor");

function createWaNativeJobCreatedHandler() {
  return async (event) => {
    const snap = event.data;
    if (!snap) return;
    const tenantId = String(event.params?.tenantId || "").trim();
    const jobId = String(event.params?.jobId || "").trim();
    const data = snap.data() || {};
    if (String(data.status || "") !== "pending") {
      return;
    }

    const ref = snap.ref;
    try {
      await ref.update({
        status: "processing",
        processing_at: admin.firestore.FieldValue.serverTimestamp()
      });
    } catch (e) {
      error("WA_NATIVE_JOB_STATUS_UPDATE_FAILED", { tenantId, jobId, reason: String(e) });
    }

    try {
      await processWaNativeJob(tenantId, data, ref);
      await ref.update({
        status: "sent",
        sent_at: admin.firestore.FieldValue.serverTimestamp()
      });
      info("WA_NATIVE_JOB_SENT", { tenantId, jobId, kind: data.kind || "" });
    } catch (e) {
      error("WA_NATIVE_JOB_FAILED", { tenantId, jobId, reason: String(e) });
      try {
        await ref.update({
          status: "failed",
          error: String(e?.message || e),
          failed_at: admin.firestore.FieldValue.serverTimestamp()
        });
      } catch (e2) {
        error("WA_NATIVE_JOB_FAIL_PERSIST_FAILED", { tenantId, jobId, reason: String(e2) });
      }
    }
  };
}

function registerWaNativeJobCreated(options) {
  return onDocumentCreated(
    {
      document: "tenants/{tenantId}/wa_native_jobs/{jobId}",
      region: options?.region || "asia-south1",
      retry: false
    },
    createWaNativeJobCreatedHandler()
  );
}

module.exports = { registerWaNativeJobCreated, createWaNativeJobCreatedHandler };
