const { info, warn } = require("../lib/logger");

async function postJsonWithTimeout(url, body, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs || 8000);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body || {}),
      signal: controller.signal
    });
    const text = await res.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch (e) {
      json = { raw: text };
    }
    return { ok: res.ok, status: res.status, body: json };
  } finally {
    clearTimeout(timer);
  }
}

function extractMessageFromLegacyResult(payload) {
  const result = payload?.result || payload;
  if (!result) return "";
  if (typeof result.message === "string" && result.message.trim()) return result.message;

  const outbound = Array.isArray(result.outbound) ? result.outbound : [];
  const firstText = outbound.find((x) => x?.type === "text" && typeof x.text === "string");
  if (firstText) return firstText.text;
  const firstButtons = outbound.find((x) => x?.type === "buttons" && typeof x.body === "string");
  if (firstButtons) return firstButtons.body;
  const firstList = outbound.find((x) => x?.type === "list" && typeof x.body === "string");
  if (firstList) return firstList.body;

  return "";
}

/**
 * Canary-safe fallback adapter to existing GAS chatbot simulation endpoint.
 * NOTE: this does NOT send WhatsApp directly. It asks GAS to compute message.
 */
async function invokeLegacyGasDynamicPricing({
  bridgeUrl,
  bridgeKey,
  inbound
}) {
  if (!bridgeUrl || !bridgeKey) {
    warn("LEGACY_GAS_ADAPTER_DISABLED_MISSING_SECRET", {});
    return { ok: false, message: "" };
  }

  const url = String(bridgeUrl || "").trim();
  if (url.startsWith("mock://")) {
    const clicked = inbound?.interactive?.id || inbound?.interactive?.title || inbound?.text || "MENU_FEES";
    return {
      ok: true,
      message: `[LEGACY GAS MOCK] ${clicked} response`
    };
  }

  const reqBody = {
    action: "api_bridge",
    key: bridgeKey,
    fn: "simulateChatbotWebhookMockAction",
    args: [
      {
        from: inbound?.from || "",
        pid: inbound?.interactive?.id || "",
        rawMsg: inbound?.interactive?.title || inbound?.text || ""
      }
    ]
  };

  const res = await postJsonWithTimeout(url, reqBody, 8000);
  if (!res.ok || res.body?.status === "error") {
    warn("LEGACY_GAS_ADAPTER_CALL_FAILED", {
      status: res.status,
      error: res.body?.message || ""
    });
    return { ok: false, message: "" };
  }

  const msg = extractMessageFromLegacyResult(res.body);
  info("LEGACY_GAS_ADAPTER_SUCCESS", {
    hasMessage: !!msg
  });
  return { ok: true, message: msg };
}

module.exports = { invokeLegacyGasDynamicPricing };
