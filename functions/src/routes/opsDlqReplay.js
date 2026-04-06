const express = require("express");
const { replayDlqMessage } = require("../services/waOutboundQueue");
const { warn } = require("../lib/logger");

function getOpsKeyFromRequest(req) {
  const headerKey = String(req.get("x-ops-key") || "").trim();
  const auth = String(req.get("authorization") || "").trim();
  const bearer = auth.toLowerCase().startsWith("bearer ") ? auth.slice(7).trim() : "";
  const bodyKey = String(req.body?.opsKey || "").trim();
  return headerKey || bearer || bodyKey;
}

function createDlqReplayApp({ getOpsKey }) {
  const app = express();
  app.disable("x-powered-by");
  app.use(express.json({ limit: "256kb" }));

  app.post("/ops/dlq/replay", async (req, res) => {
    const expected = String(getOpsKey?.() || "").trim();
    const provided = getOpsKeyFromRequest(req);
    if (!expected || !provided || provided !== expected) {
      warn("OPS_DQL_REPLAY_UNAUTHORIZED", {});
      return res.status(403).json({ status: "error", message: "Unauthorized" });
    }

    const tenantId = String(req.body?.tenantId || "").trim();
    const dlqId = String(req.body?.dlqId || "").trim();
    const replayedBy = String(req.body?.replayedBy || "ops").trim();
    if (!tenantId || !dlqId) {
      return res.status(400).json({ status: "error", message: "tenantId and dlqId are required" });
    }

    try {
      const result = await replayDlqMessage({ tenantId, dlqId, replayedBy });
      return res.status(200).json(result);
    } catch (e) {
      return res.status(500).json({ status: "error", message: String(e) });
    }
  });

  return app;
}

module.exports = { createDlqReplayApp };
