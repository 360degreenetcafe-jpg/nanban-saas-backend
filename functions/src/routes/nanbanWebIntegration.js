const express = require("express");
const admin = require("firebase-admin");
const { info, warn } = require("../lib/logger");
const { inferJobKindFromStudent } = require("../services/waNativeJobProcessor");
const { handleErpRpc } = require("../services/erpRpcDispatch");

const MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024;

function corsHeaders(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, x-nanban-write-key");
  res.setHeader("Access-Control-Max-Age", "86400");
}

function readWriteKey(req) {
  return String(req.get("x-nanban-write-key") || req.query?.key || req.body?.writeKey || "").trim();
}

function createNanbanWebIntegrationApp({ getWriteKey }) {
  const app = express();
  app.disable("x-powered-by");
  app.use((req, res, next) => {
    corsHeaders(res);
    if (req.method === "OPTIONS") {
      res.status(204).send("");
      return;
    }
    next();
  });
  app.use(express.json({ limit: "8mb" }));

  function auth(req, res) {
    const expected = String(getWriteKey?.() || "").trim();
    const got = readWriteKey(req);
    if (!expected || got !== expected) {
      res.status(401).json({ ok: false, error: "unauthorized" });
      return false;
    }
    return true;
  }

  app.get("/health", (_req, res) => {
    res.json({ ok: true, service: "nanbanWebIntegration" });
  });

  /**
   * GET /v1/snapshot?business=Nanban&key=...
   */
  app.get("/v1/snapshot", async (req, res) => {
    if (!auth(req, res)) return;
    const business = String(req.query.business || "Nanban").trim() || "Nanban";
    try {
      const db = admin.firestore();
      const snap = await db.collection("businesses").doc(business).collection("snapshot").doc("main").get();
      if (!snap.exists) {
        res.json({ ok: true, students: [], expenses: [], appSettings: null, chitData: null });
        return;
      }
      const data = snap.data() || {};
      res.json({
        ok: true,
        students: Array.isArray(data.students) ? data.students : [],
        expenses: Array.isArray(data.expenses) ? data.expenses : [],
        appSettings: data.appSettings || null,
        chitData: data.chitData || null,
        updated_at: data.updated_at || null
      });
    } catch (e) {
      warn("NANBAN_WEB_SNAPSHOT_GET_FAILED", { reason: String(e) });
      res.status(500).json({ ok: false, error: "snapshot_read_failed" });
    }
  });

  /**
   * POST /v1/snapshot
   * body: { writeKey?, tenantId?, business?, students, expenses, appSettings?, chitData? }
   */
  app.post("/v1/snapshot", async (req, res) => {
    if (!auth(req, res)) return;
    const business = String(req.body?.business || "Nanban").trim() || "Nanban";
    try {
      const payload = {
        students: Array.isArray(req.body?.students) ? req.body.students : [],
        expenses: Array.isArray(req.body?.expenses) ? req.body.expenses : [],
        updated_at: admin.firestore.FieldValue.serverTimestamp()
      };
      if (req.body?.appSettings && typeof req.body.appSettings === "object") {
        payload.appSettings = req.body.appSettings;
      }
      if (req.body?.chitData && typeof req.body.chitData === "object") {
        payload.chitData = req.body.chitData;
      }
      const json = JSON.stringify(payload);
      if (json.length > MAX_SNAPSHOT_BYTES) {
        res.status(413).json({ ok: false, error: "payload_too_large" });
        return;
      }
      const db = admin.firestore();
      await db.collection("businesses").doc(business).collection("snapshot").doc("main").set(payload, { merge: true });
      info("NANBAN_WEB_SNAPSHOT_SAVED", { business, students: payload.students.length });
      res.json({ ok: true });
    } catch (e) {
      warn("NANBAN_WEB_SNAPSHOT_POST_FAILED", { reason: String(e) });
      res.status(500).json({ ok: false, error: "snapshot_write_failed" });
    }
  });

  /**
   * POST /v1/wa/job
   * body: { writeKey?, tenantId?, kind?, student }
   * Creates Firestore job; trigger sends WhatsApp.
   */
  /**
   * POST /v1/rpc
   * body: { action, args? } — GAS-shaped server functions for hosted ERP UI.
   */
  app.post("/v1/rpc", async (req, res) => {
    if (!auth(req, res)) return;
    const action = String(req.body?.action || "").trim();
    const args = Array.isArray(req.body?.args) ? req.body.args : [];
    if (!action) {
      res.status(400).json({ ok: false, error: "action_required" });
      return;
    }
    try {
      const result = await handleErpRpc(action, args);
      const msg = result && String(result.message || "");
      if (result && result.status === "error" && msg.includes("not implemented")) {
        res.status(501).json({ ok: false, error: "rpc_not_implemented", result });
        return;
      }
      res.json({ ok: true, result });
    } catch (e) {
      warn("NANBAN_WEB_RPC_FAILED", { action, reason: String(e) });
      res.status(500).json({ ok: false, error: "rpc_failed", message: String(e && e.message ? e.message : e) });
    }
  });

  app.post("/v1/wa/job", async (req, res) => {
    if (!auth(req, res)) return;
    const tenantId = String(req.body?.tenantId || "nanban_main").trim() || "nanban_main";
    const student = req.body?.student;
    if (!student || typeof student !== "object") {
      res.status(400).json({ ok: false, error: "student_required" });
      return;
    }
    const kind = String(req.body?.kind || "").trim() || inferJobKindFromStudent(student);
    try {
      const db = admin.firestore();
      const ref = await db.collection("tenants").doc(tenantId).collection("wa_native_jobs").add({
        status: "pending",
        kind,
        student,
        source: "nanban_web_integration",
        created_at: admin.firestore.FieldValue.serverTimestamp()
      });
      info("NANBAN_WEB_WA_JOB_QUEUED", { tenantId, jobId: ref.id, kind });
      res.json({ ok: true, jobId: ref.id });
    } catch (e) {
      warn("NANBAN_WEB_WA_JOB_FAILED", { reason: String(e) });
      res.status(500).json({ ok: false, error: "wa_job_create_failed" });
    }
  });

  return app;
}

module.exports = { createNanbanWebIntegrationApp };
