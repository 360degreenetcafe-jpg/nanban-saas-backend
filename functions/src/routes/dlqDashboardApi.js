const express = require("express");
const admin = require("firebase-admin");
const { warn, error } = require("../lib/logger");

const ALLOWED_ADMIN_ROLES = new Set(["admin", "owner"]);

function normalizeRole(v) {
  return String(v || "").trim().toLowerCase();
}

function extractBearerToken(req) {
  const auth = String(req.get("authorization") || "").trim();
  if (!auth.toLowerCase().startsWith("bearer ")) return "";
  return auth.slice(7).trim();
}

async function verifyTenantAdminAccessOrThrow(req, tenantId) {
  const token = extractBearerToken(req);
  if (!token) {
    const err = new Error("Missing Bearer token");
    err.statusCode = 401;
    throw err;
  }

  let decoded;
  try {
    decoded = await admin.auth().verifyIdToken(token);
  } catch (e) {
    const err = new Error("Invalid Firebase ID token");
    err.statusCode = 401;
    throw err;
  }

  const uid = String(decoded.uid || "").trim();
  const claimTenant = String(decoded.tenant_id || "").trim();
  const claimRole = normalizeRole(decoded.role || "");
  const claimRoles = Array.isArray(decoded.roles) ? decoded.roles.map(normalizeRole) : [];

  const claimHasAdminRole = ALLOWED_ADMIN_ROLES.has(claimRole) || claimRoles.some((r) => ALLOWED_ADMIN_ROLES.has(r));
  if (claimTenant === tenantId && claimHasAdminRole) {
    return { uid, via: "token_claims" };
  }

  // Fallback: validate using tenant membership document
  const db = admin.firestore();
  const memberSnap = await db.collection("tenants").doc(tenantId).collection("users").doc(uid).get();
  if (!memberSnap.exists) {
    const err = new Error("No tenant membership");
    err.statusCode = 403;
    throw err;
  }
  const member = memberSnap.data() || {};
  const memberRole = normalizeRole(member.role || "");
  if (member.active === false || !ALLOWED_ADMIN_ROLES.has(memberRole)) {
    const err = new Error("Insufficient role");
    err.statusCode = 403;
    throw err;
  }
  return { uid, via: "membership_doc" };
}

function toIso(ts) {
  if (!ts) return "";
  if (typeof ts.toDate === "function") {
    try {
      return ts.toDate().toISOString();
    } catch (e) {
      return "";
    }
  }
  try {
    return new Date(ts).toISOString();
  } catch (e) {
    return "";
  }
}

async function getDlqStatusCount(db, tenantId, status) {
  const q = db
    .collection("tenants")
    .doc(tenantId)
    .collection("wa_dlq")
    .where("status", "==", status);
  const agg = await q.count().get();
  return Number(agg.data().count || 0);
}

function createDlqDashboardApp() {
  const app = express();
  app.disable("x-powered-by");

  /**
   * GET /api/v1/tenants/:tenantId/dlq/stats?limit=20
   *
   * Security:
   * - Requires Firebase ID token (Bearer)
   * - User must be Admin/Owner for target tenant
   *
   * Response:
   * {
   *   tenantId,
   *   stats: { pending_replay, requeued, total },
   *   recentFailures: [ ... ]
   * }
   */
  app.get("/api/v1/tenants/:tenantId/dlq/stats", async (req, res) => {
    const tenantId = String(req.params.tenantId || "").trim();
    if (!tenantId) {
      return res.status(400).json({ status: "error", message: "tenantId is required" });
    }

    try {
      await verifyTenantAdminAccessOrThrow(req, tenantId);
    } catch (authErr) {
      const code = Number(authErr.statusCode || 403);
      warn("DLQ_STATS_AUTH_DENIED", { tenantId, reason: String(authErr.message || authErr) });
      return res.status(code).json({ status: "error", message: "Unauthorized" });
    }

    const rawLimit = Number(req.query.limit || 20);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(100, rawLimit)) : 20;

    try {
      const db = admin.firestore();
      const [pendingReplay, requeued, recentSnap] = await Promise.all([
        getDlqStatusCount(db, tenantId, "pending_replay"),
        getDlqStatusCount(db, tenantId, "requeued"),
        db
          .collection("tenants")
          .doc(tenantId)
          .collection("wa_dlq")
          .orderBy("created_at", "desc")
          .limit(limit)
          .get()
      ]);

      const recentFailures = recentSnap.docs.map((d) => {
        const x = d.data() || {};
        return {
          dlqId: d.id,
          to: String(x.to || ""),
          failureReason: String(x.failure_reason || ""),
          status: String(x.status || ""),
          retryCount: Number(x.retry_count || 0),
          executionCount: Number(x.execution_count || 0),
          createdAt: toIso(x.created_at),
          updatedAt: toIso(x.updated_at)
        };
      });

      return res.status(200).json({
        status: "success",
        tenantId,
        stats: {
          pending_replay: pendingReplay,
          requeued: requeued,
          total: pendingReplay + requeued
        },
        recentFailures
      });
    } catch (e) {
      error("DLQ_STATS_FETCH_FAILED", { tenantId, reason: String(e) });
      return res.status(500).json({ status: "error", message: "Failed to fetch DLQ stats" });
    }
  });

  return app;
}

module.exports = { createDlqDashboardApp };
