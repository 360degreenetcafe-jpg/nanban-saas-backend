const admin = require("firebase-admin");
const { getFunctions } = require("firebase-admin/functions");
const { info, warn, error } = require("../lib/logger");
const { sanitizeTemplateParamText } = require("../lib/sanitizeTemplateParam");

const DEFAULT_OUTBOUND_DELAY_SECONDS = 0;
const TENANT_PER_MINUTE_LIMIT = 45;
const WA_OUTBOUND_MAX_ATTEMPTS = 10;
const DEFAULT_DLQ_ALERT_ADMIN_PHONES = {
  nanban_main: ["919092036666", "919942391870"]
};

function cleanPhone(phone) {
  return String(phone || "").replace(/[^\d]/g, "");
}

function minuteBucketKey(now) {
  const d = now || new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  return `${y}${m}${day}${hh}${mm}`;
}

function safeDocId(input) {
  return String(input || "")
    .replace(/[^a-zA-Z0-9:_-]/g, "_")
    .slice(0, 240) || `dlq_${Date.now()}`;
}

function getHeaderValue(headers, key) {
  if (!headers || typeof headers !== "object") return "";
  const keys = Object.keys(headers);
  const found = keys.find((k) => String(k).toLowerCase() === String(key).toLowerCase());
  if (!found) return "";
  return String(headers[found] || "");
}

function getTaskAttemptMeta(request) {
  const headers = request?.headers || {};
  const retryCount = Number(getHeaderValue(headers, "x-cloudtasks-taskretrycount") || 0);
  const executionCount = Number(getHeaderValue(headers, "x-cloudtasks-taskexecutioncount") || 0);
  const taskNameRaw = getHeaderValue(headers, "x-cloudtasks-taskname");
  const queueName = getHeaderValue(headers, "x-cloudtasks-queuename");
  const taskName = taskNameRaw ? taskNameRaw.split("/").pop() : "";
  const isFinalAttempt = retryCount >= (WA_OUTBOUND_MAX_ATTEMPTS - 1);
  return {
    retryCount: Number.isFinite(retryCount) ? retryCount : 0,
    executionCount: Number.isFinite(executionCount) ? executionCount : 0,
    taskName,
    queueName,
    isFinalAttempt
  };
}

async function enqueueWaOutboundSend(taskPayload, options) {
  const payload = {
    tenantId: String(taskPayload?.tenantId || "").trim(),
    to: cleanPhone(taskPayload?.to || ""),
    message: String(taskPayload?.message || "").trim(),
    messageType: String(taskPayload?.messageType || "text").trim(),
    metadata: taskPayload?.metadata || {},
    queuedAt: new Date().toISOString()
  };

  if (!payload.tenantId) throw new Error("enqueueWaOutboundSend: tenantId required");
  if (!payload.to) throw new Error("enqueueWaOutboundSend: recipient phone required");
  const hasTemplate =
    payload.template &&
    String(payload.template.name || "").trim() &&
    Array.isArray(payload.template.bodyParams);
  if (!String(payload.message || "").trim() && !hasTemplate) {
    throw new Error("enqueueWaOutboundSend: message or template with bodyParams required");
  }

  // Local/mock mode for integration tests (no Cloud Tasks dependency).
  if (process.env.WA_OUTBOUND_MOCK === "1") {
    const db = admin.firestore();
    const ref = db
      .collection("tenants")
      .doc(payload.tenantId)
      .collection("wa_outbound_mock")
      .doc();

    await ref.set({
      ...payload,
      created_at: admin.firestore.FieldValue.serverTimestamp()
    });
    info("WA_OUTBOUND_MOCK_ENQUEUED", { tenantId: payload.tenantId, to: payload.to });
    return { enqueued: true, mode: "mock", id: ref.id };
  }

  const delaySec = Number(options?.delaySeconds ?? DEFAULT_OUTBOUND_DELAY_SECONDS);
  const scheduleDelaySeconds = Number.isFinite(delaySec) && delaySec > 0 ? Math.floor(delaySec) : 0;

  const queue = getFunctions().taskQueue("waOutboundWorker");
  const enqueueOptions = {
    dispatchDeadlineSeconds: 30
  };
  if (scheduleDelaySeconds > 0) {
    enqueueOptions.scheduleDelaySeconds = scheduleDelaySeconds;
  }

  await queue.enqueue(payload, enqueueOptions);
  info("WA_OUTBOUND_TASK_ENQUEUED", {
    tenantId: payload.tenantId,
    to: payload.to,
    scheduleDelaySeconds
  });
  return { enqueued: true, mode: "cloud_tasks" };
}

async function enforceTenantRateLimit_(tenantId) {
  const db = admin.firestore();
  const bucket = minuteBucketKey(new Date());
  const ref = db
    .collection("tenants")
    .doc(String(tenantId))
    .collection("runtime")
    .doc(`wa_rate_${bucket}`);

  const countAfter = await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const current = snap.exists ? Number(snap.data()?.count || 0) : 0;
    const next = current + 1;
    tx.set(
      ref,
      {
        count: next,
        minute_bucket: bucket,
        updated_at: admin.firestore.FieldValue.serverTimestamp()
      },
      { merge: true }
    );
    return next;
  });

  if (countAfter > TENANT_PER_MINUTE_LIMIT) {
    throw new Error(`TENANT_RATE_LIMIT_EXCEEDED:${tenantId}:${countAfter}`);
  }
}

async function persistOutboundEvent_(taskPayload, responseObj) {
  const db = admin.firestore();
  const tenantId = String(taskPayload?.tenantId || "");
  if (!tenantId) return;

  await db
    .collection("tenants")
    .doc(tenantId)
    .collection("wa_events")
    .add({
      direction: "outbound",
      to: String(taskPayload?.to || ""),
      message_type: String(taskPayload?.messageType || "text"),
      message: String(taskPayload?.message || ""),
      delivery_status: responseObj?.ok ? "accepted" : "failed",
      provider_status: Number(responseObj?.status || 0),
      provider_response: responseObj?.body || null,
      metadata: taskPayload?.metadata || {},
      created_at: admin.firestore.FieldValue.serverTimestamp()
    });
}

async function getTenantAdminPhones_(tenantId) {
  const tid = String(tenantId || "").trim();
  if (!tid) return [];
  const db = admin.firestore();
  try {
    const snap = await db.collection("platform_tenants").doc(tid).get();
    if (snap.exists) {
      const data = snap.data() || {};
      if (Array.isArray(data.admin_phones) && data.admin_phones.length) {
        return data.admin_phones.map(cleanPhone).filter(Boolean);
      }
    }
  } catch (e) {}
  return (DEFAULT_DLQ_ALERT_ADMIN_PHONES[tid] || []).map(cleanPhone).filter(Boolean);
}

async function persistDlqRecord_({ tenantId, taskPayload, reason, attemptMeta }) {
  const db = admin.firestore();
  const failedTaskId = safeDocId(attemptMeta?.taskName || `failed_${Date.now()}`);
  const ref = db
    .collection("tenants")
    .doc(String(tenantId))
    .collection("wa_dlq")
    .doc(failedTaskId);

  const doc = {
    failed_task_id: failedTaskId,
    tenant_id: String(tenantId),
    to: String(taskPayload?.to || ""),
    message: String(taskPayload?.message || ""),
    message_type: String(taskPayload?.messageType || "text"),
    template: taskPayload?.template || null,
    metadata: taskPayload?.metadata || {},
    failure_reason: String(reason || "unknown_error"),
    retry_count: Number(attemptMeta?.retryCount || 0),
    execution_count: Number(attemptMeta?.executionCount || 0),
    queue_name: String(attemptMeta?.queueName || ""),
    source_task_name: String(attemptMeta?.taskName || ""),
    status: "pending_replay",
    created_at: admin.firestore.FieldValue.serverTimestamp(),
    updated_at: admin.firestore.FieldValue.serverTimestamp()
  };

  await ref.set(doc, { merge: true });
  return { dlqId: failedTaskId, ref };
}

async function triggerDlqAdminEscalation_({ tenantId, failedTo, reason, dlqId, skip }) {
  if (skip) return;
  const phones = await getTenantAdminPhones_(tenantId);
  if (!phones.length) {
    warn("DLQ_ADMIN_ESCALATION_NO_PHONES", { tenantId, dlqId });
    return;
  }

  const msg =
    "🚨 WhatsApp Delivery Failure (Permanent)\n" +
    `Tenant: ${tenantId}\n` +
    `Phone: ${failedTo || "-"}\n` +
    `Reason: ${reason || "-"}\n` +
    `DLQ ID: ${dlqId}\n` +
    "Action: Please review and replay after fixing the issue.";

  for (const phone of phones) {
    try {
      await enqueueWaOutboundSend(
        {
          tenantId,
          to: phone,
          message: msg,
          messageType: "text",
          metadata: { kind: "dlq_alert", dlq_id: dlqId, failed_to: failedTo || "" }
        },
        { delaySeconds: 0 }
      );
    } catch (e) {
      error("DLQ_ADMIN_ESCALATION_ENQUEUE_FAILED", { tenantId, phone, reason: String(e) });
    }
  }
}

async function sendTextViaMeta_(taskPayload, waToken, waPhoneId) {
  const url = `https://graph.facebook.com/v20.0/${waPhoneId}/messages`;
  const body = {
    messaging_product: "whatsapp",
    to: cleanPhone(taskPayload.to),
    type: "text",
    text: { body: String(taskPayload.message || "") }
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${waToken}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const text = await res.text();
  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    parsed = { raw: text };
  }
  return { ok: res.ok, status: res.status, body: parsed };
}

async function sendTemplateViaMeta_(taskPayload, waToken, waPhoneId) {
  const tpl = taskPayload.template || {};
  const name = String(tpl.name || "").trim();
  const lang = String(tpl.languageCode || "ta").trim() || "ta";
  const bodyParams = Array.isArray(tpl.bodyParams) ? tpl.bodyParams : [];
  const components = [];
  if (bodyParams.length) {
    components.push({
      type: "body",
      parameters: bodyParams.map((p) => ({
        type: "text",
        text: sanitizeTemplateParamText(p)
      }))
    });
  }

  const url = `https://graph.facebook.com/v20.0/${waPhoneId}/messages`;
  const body = {
    messaging_product: "whatsapp",
    to: cleanPhone(taskPayload.to),
    type: "template",
    template: {
      name,
      language: { code: lang },
      components
    }
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${waToken}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const text = await res.text();
  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    parsed = { raw: text };
  }
  return { ok: res.ok, status: res.status, body: parsed };
}

/**
 * Cloud Tasks worker handler.
 * Retries/backoff are configured in index.js onTaskDispatched retryConfig.
 */
function createWaOutboundWorker({ getWaToken, getWaPhoneId }) {
  return async (request) => {
    const taskPayload = request?.data || {};
    const tenantId = String(taskPayload?.tenantId || "").trim();
    const attemptMeta = getTaskAttemptMeta(request);
    const forceFail = taskPayload?.metadata?.force_fail === true;

    if (!tenantId) {
      warn("WA_OUTBOUND_SKIP_NO_TENANT", {});
      return;
    }

    try {
      await enforceTenantRateLimit_(tenantId);

      // Mock mode for local/emulator assertions.
      if (process.env.WA_OUTBOUND_MOCK === "1") {
        if (forceFail) throw new Error("MOCK_FORCED_FAILURE");
        await persistOutboundEvent_(taskPayload, { ok: true, status: 202, body: { mocked: true } });
        info("WA_OUTBOUND_WORKER_MOCK_SENT", { tenantId, to: taskPayload?.to || "" });
        return;
      }

      const waToken = String(getWaToken?.() || "").trim();
      const waPhoneId = String(getWaPhoneId?.() || "").trim();
      if (!waToken || !waPhoneId) {
        throw new Error("WA_OUTBOUND_SECRET_MISSING");
      }

      const tpl = taskPayload.template;
      const tryTemplateFirst = !!(tpl && String(tpl.name || "").trim() && tpl.tryFirst !== false);
      const textBody = String(taskPayload.message || "").trim();

      let result;
      let usedKind = "text";

      if (tryTemplateFirst) {
        result = await sendTemplateViaMeta_(taskPayload, waToken, waPhoneId);
        if (result.ok) {
          usedKind = "template";
        } else {
          warn("WA_OUTBOUND_TEMPLATE_FAILED_TRY_TEXT", {
            tenantId,
            to: taskPayload?.to || "",
            status: result.status,
            template: tpl.name
          });
          if (!textBody) {
            error("WA_OUTBOUND_TEMPLATE_FAILED_NO_TEXT_FALLBACK", {
              tenantId,
              status: result.status
            });
            throw new Error(`WA_TEMPLATE_ERROR:${result.status}`);
          }
          result = await sendTextViaMeta_(taskPayload, waToken, waPhoneId);
        }
      } else {
        result = await sendTextViaMeta_(taskPayload, waToken, waPhoneId);
      }

      const persistPayload = Object.assign({}, taskPayload, {
        messageType: usedKind,
        metadata: Object.assign({}, taskPayload.metadata || {}, { wa_send_kind: usedKind })
      });
      await persistOutboundEvent_(persistPayload, result);

      if (!result.ok) {
        error("WA_OUTBOUND_PROVIDER_FAILED", {
          tenantId,
          status: result.status,
          to: taskPayload?.to || ""
        });
        throw new Error(`WA_PROVIDER_ERROR:${result.status}`);
      }

      info("WA_OUTBOUND_SENT", {
        tenantId,
        to: taskPayload?.to || "",
        providerStatus: result.status,
        kind: usedKind
      });
    } catch (err) {
      const reason = String(err && err.message ? err.message : err);
      const skipEscalation = String(taskPayload?.metadata?.kind || "") === "dlq_alert";

      if (attemptMeta.isFinalAttempt) {
        const dlq = await persistDlqRecord_({
          tenantId,
          taskPayload,
          reason,
          attemptMeta
        });
        await triggerDlqAdminEscalation_({
          tenantId,
          failedTo: taskPayload?.to || "",
          reason,
          dlqId: dlq.dlqId,
          skip: skipEscalation
        });

        error("WA_OUTBOUND_MOVED_TO_DLQ", {
          tenantId,
          dlqId: dlq.dlqId,
          reason,
          retryCount: attemptMeta.retryCount
        });
        // Final failure already captured and escalated; acknowledge task.
        return;
      }

      // Non-final attempt -> allow Cloud Tasks retry/backoff.
      throw err;
    }
  };
}

async function replayDlqMessage({ tenantId, dlqId, replayedBy }) {
  const tid = String(tenantId || "").trim();
  const id = String(dlqId || "").trim();
  if (!tid || !id) throw new Error("tenantId and dlqId are required");

  const db = admin.firestore();
  const ref = db.collection("tenants").doc(tid).collection("wa_dlq").doc(id);
  const snap = await ref.get();
  if (!snap.exists) throw new Error("DLQ message not found");
  const data = snap.data() || {};

  const taskPayload = {
    tenantId: tid,
    to: String(data.to || ""),
    message: String(data.message || ""),
    messageType: String(data.message_type || "text"),
    metadata: Object.assign({}, data.metadata || {}, {
      replay_of_dlq_id: id,
      replayed_by: String(replayedBy || "manual_ops")
    })
  };
  if (data.template && typeof data.template === "object") {
    taskPayload.template = data.template;
  }

  await enqueueWaOutboundSend(taskPayload, { delaySeconds: 0 });
  await ref.set(
    {
      status: "requeued",
      replayed_at: admin.firestore.FieldValue.serverTimestamp(),
      replayed_by: String(replayedBy || "manual_ops"),
      replay_count: admin.firestore.FieldValue.increment(1),
      updated_at: admin.firestore.FieldValue.serverTimestamp()
    },
    { merge: true }
  );

  return { status: "success", dlqId: id, tenantId: tid };
}

module.exports = {
  enqueueWaOutboundSend,
  createWaOutboundWorker,
  replayDlqMessage,
  TENANT_PER_MINUTE_LIMIT,
  WA_OUTBOUND_MAX_ATTEMPTS
};
