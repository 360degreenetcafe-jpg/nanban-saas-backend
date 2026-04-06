const SERVICE_CATALOG = {
  TW_FULL: {
    key: "TW_FULL",
    titleTa: "இருசக்கர வாகனம் - முழு பேக்கேஜ்",
    pricing: { llr: 1000, license: 1800 }
  },
  FW_LICENSE_ONLY: {
    key: "FW_LICENSE_ONLY",
    titleTa: "நான்கு சக்கர வாகனம் - லைசென்ஸ் மட்டும்",
    pricing: { llr: 2000, license: 2500 }
  },
  FW_TRAINING_ONLY: {
    key: "FW_TRAINING_ONLY",
    titleTa: "நான்கு சக்கர வாகனம் - பயிற்சி மட்டும்",
    pricing: { training_per_day: 200, training_days: 15 }
  },
  FW_LICENSE_TRAINING: {
    key: "FW_LICENSE_TRAINING",
    titleTa: "நான்கு சக்கர வாகனம் - லைசென்ஸ் + பயிற்சி",
    pricing: { llr: 2000, license: 2500, training_per_day: 200, training_days: 15 }
  },
  COMBO_2W_4W: {
    key: "COMBO_2W_4W",
    titleTa: "காம்போ (2W + 4W)",
    includes: ["TW_FULL", "FW_LICENSE_ONLY"]
  }
};

const DEFAULT_SERVICE = "TW_FULL";

function formatInr(amount) {
  return `₹${Math.round(Number(amount) || 0)}`;
}

function normalizeServiceKey(input) {
  const txt = String(input || "").trim().toUpperCase();
  if (txt === "FW_TRAINING") return "FW_TRAINING_ONLY";
  if (txt === "FEE_SEL::FW_TRAINING") return "FW_TRAINING_ONLY";
  if (txt && SERVICE_CATALOG[txt]) return txt;

  const low = String(input || "").toLowerCase();
  if (low.includes("fee_sel::")) return normalizeServiceKey(low.split("fee_sel::")[1] || "");
  if (low.includes("two") || low.includes("bike") || low.includes("2w") || low.includes("இருசக்கர")) return "TW_FULL";
  if (low.includes("combo") || low.includes("2+4")) return "COMBO_2W_4W";
  if (low.includes("training only") || low.includes("பயிற்சி மட்டும்")) return "FW_TRAINING_ONLY";
  if (low.includes("license + training") || low.includes("லைசென்ஸ் + பயிற்சி")) return "FW_LICENSE_TRAINING";
  if (low.includes("license only") || low.includes("லைசென்ஸ் மட்டும்")) return "FW_LICENSE_ONLY";
  if (low.includes("four") || low.includes("car") || low.includes("4w") || low.includes("நான்கு")) return "FW_LICENSE_ONLY";
  return DEFAULT_SERVICE;
}

function computeBreakdown(key, visited) {
  const serviceKey = normalizeServiceKey(key);
  const svc = SERVICE_CATALOG[serviceKey] || SERVICE_CATALOG[DEFAULT_SERVICE];
  const stack = visited || {};
  if (stack[serviceKey]) {
    return { key: serviceKey, titleTa: svc.titleTa, lines: [], total: 0 };
  }
  stack[serviceKey] = true;

  let lines = [];
  let total = 0;
  if (Array.isArray(svc.includes) && svc.includes.length) {
    for (const childKey of svc.includes) {
      const child = computeBreakdown(childKey, stack);
      lines.push(`  - ${child.titleTa}: ${formatInr(child.total)}`);
      total += child.total;
    }
  } else {
    const p = svc.pricing || {};
    if (Number(p.llr) > 0) {
      lines.push(`  - LLR: ${formatInr(p.llr)}`);
      total += Number(p.llr);
    }
    if (Number(p.license) > 0) {
      lines.push(`  - License: ${formatInr(p.license)}`);
      total += Number(p.license);
    }
    if (Number(p.training_per_day) > 0 && Number(p.training_days) > 0) {
      const trainTotal = Number(p.training_per_day) * Number(p.training_days);
      lines.push(`  - Training: ${formatInr(p.training_per_day)} x ${Number(p.training_days)} days = ${formatInr(trainTotal)}`);
      total += trainTotal;
    }
  }

  return {
    key: serviceKey,
    titleTa: svc.titleTa,
    lines,
    total
  };
}

function unique(list) {
  const out = [];
  const seen = {};
  for (const item of list || []) {
    const key = normalizeServiceKey(item);
    if (!seen[key] && SERVICE_CATALOG[key]) {
      seen[key] = true;
      out.push(key);
    }
  }
  return out;
}

function buildDynamicPricingMessage(serviceKeys, heading) {
  const keys = unique(serviceKeys);
  const finalKeys = keys.length ? keys : [DEFAULT_SERVICE];

  const blocks = [];
  let grandTotal = 0;
  for (const key of finalKeys) {
    const b = computeBreakdown(key, {});
    blocks.push(b);
    grandTotal += Number(b.total) || 0;
  }

  let text = `${heading || "💰 கட்டண விவரம் (Dynamic Pricing)"}`;
  for (const block of blocks) {
    text += `\n\n• *${block.titleTa}*\n`;
    text += block.lines.length ? `${block.lines.join("\n")}\n` : "  - தகவல் இல்லை\n";
    text += `  = *${formatInr(block.total)}*`;
  }
  if (blocks.length > 1) text += `\n\n🧮 *Grand Total:* ${formatInr(grandTotal)}`;
  return text.trim();
}

function deriveSelectionFromInbound(inbound) {
  const pid = String(inbound?.interactive?.id || "").trim();
  const text = String(inbound?.interactive?.title || inbound?.text || "").trim();
  const lowerText = text.toLowerCase();

  if (!pid && (lowerText === "hi" || lowerText === "hello" || lowerText === "hey" || lowerText === "hai" || lowerText === "hlo" || lowerText.includes("வணக்கம்"))) {
    return { action: "welcome", serviceKeys: [DEFAULT_SERVICE] };
  }

  if (pid.startsWith("FEE_SEL::")) {
    return { action: "select", serviceKeys: [normalizeServiceKey(pid)] };
  }
  if (pid === "MENU_FEES" || /fee|fees|கட்டண/i.test(text)) {
    return { action: "show_fees", serviceKeys: [normalizeServiceKey(text)] };
  }
  if (pid === "MENU_BIKE") {
    return { action: "show_fees", serviceKeys: ["TW_FULL"] };
  }
  if (pid === "MENU_CAR") {
    return { action: "show_fees", serviceKeys: ["FW_LICENSE_ONLY"] };
  }
  if (pid === "FEE_SHOW_TOTAL") {
    return { action: "show_fees", serviceKeys: [DEFAULT_SERVICE] };
  }
  return { action: "noop", serviceKeys: [] };
}

function runDynamicPricingFromInbound(inbound, selectedServices) {
  const derived = deriveSelectionFromInbound(inbound);
  const base = unique(selectedServices || []);

  if (derived.action === "select") {
    const key = derived.serviceKeys[0];
    const exists = base.includes(key);
    const next = exists ? base.filter((k) => k !== key) : base.concat([key]);
    return {
      handled: true,
      selectedServices: next.length ? next : [key],
      message: buildDynamicPricingMessage(next.length ? next : [key], "✅ தேர்வு புதுப்பிக்கப்பட்டது")
    };
  }

  if (derived.action === "show_fees") {
    const merged = unique(base.concat(derived.serviceKeys));
    return {
      handled: true,
      selectedServices: merged.length ? merged : [DEFAULT_SERVICE],
      message: buildDynamicPricingMessage(merged.length ? merged : [DEFAULT_SERVICE], "📣 உங்கள் தேர்வுக்கான கட்டண விவரம்")
    };
  }

  if (derived.action === "welcome") {
    const welcome =
      "வணக்கம்! 🙏 நண்பன் டிரைவிங் ஸ்கூலுக்கு வரவேற்கிறோம்.\n" +
      "👇 கட்டண விவரம் பார்க்க *MENU_FEES* தேர்வு செய்யலாம்.\n\n" +
      buildDynamicPricingMessage([DEFAULT_SERVICE], "📌 Starter Package Preview");
    return { handled: true, selectedServices: [DEFAULT_SERVICE], message: welcome };
  }

  return { handled: false, selectedServices: base, message: "" };
}

module.exports = {
  SERVICE_CATALOG,
  normalizeServiceKey,
  buildDynamicPricingMessage,
  runDynamicPricingFromInbound
};
