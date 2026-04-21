/**
 * LLR / Indian driving licence — OCR (Cloud Vision) + heuristic field extraction.
 * Best results: clear photo scan, JPG/PNG. Digital PDFs: text extraction via pdf-parse.
 */

const vision = require("@google-cloud/vision");

let _client;

function getVisionClient_() {
  if (!_client) _client = new vision.ImageAnnotatorClient();
  return _client;
}

/** Plausible learner DOB years (OCR noise outside band ignored unless near DOB keyword). */
function isPlausibleDobYear_(year) {
  return year >= 1935 && year <= 2025;
}

/** Normalize OCR text for matching */
function normalizeOcrText_(raw) {
  return String(raw || "")
    .replace(/\r/g, "\n")
    .replace(/[|Il]{1,2}/g, "1")
    .replace(/O(?=\d)/g, "0")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Extract Indian DL-style number (Sarathi / state formats).
 */
function extractDlNumber_(text) {
  const t = String(text || "");
  const compact = t.replace(/\s/g, "").toUpperCase();
  const patterns = [
    /\b([A-Z]{2}\d{2}\d{4}\d{7})\b/,
    /\b([A-Z]{2}\d{14})\b/,
    /\b([A-Z]{2}\d{13})\b/,
    /\b([A-Z]{2}\d{2}[0-9]{11})\b/
  ];
  for (const re of patterns) {
    const m = compact.match(re);
    if (m) return m[1].slice(0, 20);
  }
  const spaced = t.toUpperCase().match(/\b([A-Z]{2}\s*\d{2}\s*\d{4}\s*\d{7})\b/);
  if (spaced) return spaced[1].replace(/\s/g, "");
  const spacedLoose = t.toUpperCase().match(/\b([A-Z]{2})\s+(\d{2})\s+(\d{4})\s+(\d{7})\b/);
  if (spacedLoose) return `${spacedLoose[1]}${spacedLoose[2]}${spacedLoose[3]}${spacedLoose[4]}`.slice(0, 20);
  return "";
}

/** Sarathi-style id when OCR removes spaces between state and digits (common on phone scans). */
function extractSarathiCompactLoose_(text) {
  const c = String(text || "")
    .replace(/\s/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
  const re = /([A-Z]{2}\d{13,16})/g;
  let best = "";
  let m;
  while ((m = re.exec(c)) !== null) {
    const cand = m[1];
    const yy = parseInt(cand.slice(4, 8), 10);
    if (yy >= 1990 && yy <= 2035 && cand.length > best.length) best = cand;
  }
  return best ? best.slice(0, 20) : "";
}

/**
 * LLR / application ref on same or next line as label (Tamil + English Sarathi PDFs).
 */
function extractLlrNumberNearLabels_(rawText) {
  const norm = String(rawText || "").replace(/\r/g, "\n");
  const lines = norm.split("\n").map((l) => l.trim());
  const labelRe =
    /(LLR|L\.?\s*L\.?\s*R\.?|Learner|LEARNER|Application\s*Ref|FORM\s*NO|REFERENCE|REF\.?\s*NO|DL\s*NO|D\.?L\.?\s*NO|Licen[cs]e\s*No|குறிப்பு\s*எண்|இணைப்பு|பதிவு\s*எண்)/i;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const next = lines[i + 1] || "";
    const prev = lines[i - 1] || "";
    if (!labelRe.test(prev) && !labelRe.test(line) && !labelRe.test(next)) continue;
    const chunk = `${prev} ${line} ${next}`;
    let hit = extractDlNumber_(chunk) || extractSarathiCompactLoose_(chunk);
    if (hit) return hit;
    const tail = line.replace(/^.*?:/g, " ").trim();
    hit = extractDlNumber_(tail) || extractSarathiCompactLoose_(tail);
    if (hit) return hit;
  }
  return "";
}

/** Parse DD/MM/YYYY or DD-MM-YYYY or YYYY-MM-DD → YYYY-MM-DD */
function toYmd_(d, m, y) {
  let dd = parseInt(d, 10);
  let mm = parseInt(m, 10);
  let yy = parseInt(y, 10);
  if (yy < 100) yy += yy < 50 ? 2000 : 1900;
  if (!isFinite(dd) || !isFinite(mm) || !isFinite(yy)) return "";
  if (mm < 1 || mm > 12 || dd < 1 || dd > 31) return "";
  return `${String(yy).padStart(4, "0")}-${String(mm).padStart(2, "0")}-${String(dd).padStart(2, "0")}`;
}

/**
 * Prefer DOB near keywords; avoid issue/expiry dates when possible.
 */
function extractDobYmd_(text) {
  const lines = text.split("\n");
  let best = "";
  let bestScore = -1;

  const keywordRe = /(DOB|D\.?\s*O\.?\s*B|DATE\s+OF\s+BIRTH|பிறப்பு|பிறந்த\s*தேதி|जन्म)/i;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const joined = (lines[i - 1] || "") + " " + line + " " + (lines[i + 1] || "");
    const hasKw = keywordRe.test(line) || keywordRe.test(joined);
    const dates = [];
    const re = /(\d{1,2})[\/.\-](\d{1,2})[\/.\-](\d{2,4})/g;
    let m;
    while ((m = re.exec(line)) !== null) {
      dates.push({ d: m[1], mo: m[2], y: m[3], idx: m.index });
    }
    const isoRe = /(\d{4})-(\d{1,2})-(\d{1,2})/g;
    while ((m = isoRe.exec(line)) !== null) {
      const ymd = `${String(m[1]).padStart(4, "0")}-${String(m[2]).padStart(2, "0")}-${String(m[3]).padStart(2, "0")}`;
      const year = parseInt(m[1], 10);
      if (isPlausibleDobYear_(year)) dates.push({ iso: ymd });
    }
    for (const d of dates) {
      let ymd = "";
      if (d.iso) ymd = d.iso;
      else ymd = toYmd_(d.d, d.mo, d.y);
      if (!ymd) continue;
      const year = parseInt(ymd.slice(0, 4), 10);
      let score = hasKw ? 50 : 0;
      if (isPlausibleDobYear_(year)) score += 30;
      if (hasKw && /DOB|பிறப்பு|பிறந்த/i.test(line)) score += 20;
      if (/(ISSUED|ISSUE|VALID|VALIDITY|EXPIR|பிரிண்ட்)/i.test(line)) score -= 40;
      if (score > bestScore) {
        bestScore = score;
        best = ymd;
      }
    }
  }

  if (best) return best;

  const globalRe = /(\d{1,2})[\/.\-](\d{1,2})[\/.\-](\d{4})/g;
  let gm;
  const candidates = [];
  while ((gm = globalRe.exec(text)) !== null) {
    const ymd = toYmd_(gm[1], gm[2], gm[3]);
    if (!ymd) continue;
    const year = parseInt(ymd.slice(0, 4), 10);
    if (isPlausibleDobYear_(year)) candidates.push(ymd);
  }
  const isoGlobal = /(\d{4})-(\d{2})-(\d{2})/g;
  while ((gm = isoGlobal.exec(text)) !== null) {
    const y = parseInt(gm[1], 10);
    if (!isPlausibleDobYear_(y)) continue;
    const ymd = `${gm[1]}-${gm[2]}-${gm[3]}`;
    candidates.push(ymd);
  }
  if (candidates.length === 1) return candidates[0];
  if (candidates.length > 1) {
    const mid = candidates.filter((c) => {
      const y = parseInt(c.slice(0, 4), 10);
      return y >= 1950 && y <= 2015;
    });
    if (mid.length === 1) return mid[0];
    return candidates.sort()[0];
  }
  return "";
}

/**
 * Best-effort holder name from DL / LLR OCR text (layout varies by state).
 */
/**
 * Extract blood group from Indian LLR / DL OCR text.
 * Covers: A+, B-, O+, AB+, A+ve, B-ve, O Positive, AB Negative, ரத்த வகை, रक्त समूह, etc.
 * Returns canonical uppercase string like "A+", "B-", "O+", "AB+", "" if not found.
 */
function extractBloodGroup_(rawText) {
  const text = String(rawText || "");
  const BG_VALS = ["AB+", "AB-", "A+", "A-", "B+", "B-", "O+", "O-"];

  const keywordRe =
    /(?:BLOOD\s*(?:GROUP|TYPE|GRP)|B\.?\s*G\.?|BG|RH|ரத்த\s*வகை|இரத்த|रक्त\s*समूह|रक्तगट|Blood\s*Grp)/i;

  function normalizeGroup_(raw) {
    let s = String(raw || "")
      .toUpperCase()
      .replace(/\s+/g, "")
      .replace(/VE\b/g, "")
      .replace(/POSITIVE/g, "+")
      .replace(/NEGATIVE/g, "-")
      .replace(/POS\b/g, "+")
      .replace(/NEG\b/g, "-");
    for (const v of BG_VALS) {
      if (s === v) return v;
    }
    return "";
  }

  /* After A+/O+ the char '+' is not \\w, so a trailing \\b fails — use lookahead instead. */
  const inlineRe =
    /\b(AB|A|B|O)[+\-](?:VE|POSITIVE|NEGATIVE|POS|NEG)?(?=\s|$|[,;.:\-])|\b(AB|A|B|O)\s*(?:POSITIVE|NEGATIVE|POS|NEG)\b/gi;
  const lines = text.split("\n");

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const nearby = (lines[i - 1] || "") + " " + line + " " + (lines[i + 1] || "");
    if (!keywordRe.test(nearby)) continue;
    let m;
    inlineRe.lastIndex = 0;
    while ((m = inlineRe.exec(line)) !== null) {
      const norm = normalizeGroup_(m[0]);
      if (norm) return norm;
    }
    const nextLine = (lines[i + 1] || "").trim();
    if (nextLine) {
      const norm = normalizeGroup_(nextLine.split(/\s+/)[0]);
      if (norm) return norm;
    }
  }

  inlineRe.lastIndex = 0;
  let m;
  while ((m = inlineRe.exec(text)) !== null) {
    const norm = normalizeGroup_(m[0]);
    if (norm) return norm;
  }

  const lineBlood = /^\s*(AB|A|B|O)\s*[+\-]\s*(?:ve|pos|neg)?\s*$/i;
  for (const line of text.split("\n")) {
    const t = line.trim();
    const lm = t.match(lineBlood);
    if (lm) {
      const norm = normalizeGroup_(lm[0]);
      if (norm) return norm;
    }
  }
  return "";
}

function extractHolderName_(rawText) {
  const text = String(rawText || "");
  const lines = text.split("\n").map((l) => l.trim()).filter(Boolean);
  const inline = /^(?:NAME|Name|பெயர்|Holder|HOLDER|SON\/W\/D|D\/O)[\s:.\-—]+(.{2,100})$/i;
  for (const line of lines) {
    const m = line.match(inline);
    if (m && m[1]) {
      const n = m[1].replace(/\s+/g, " ").trim();
      if (n.length >= 2 && !/^\d+$/.test(n) && !/^[A-Z]{2}\d{2}/i.test(n)) return n.slice(0, 100);
    }
  }
  for (let i = 0; i < lines.length - 1; i++) {
    if (/^(NAME|பெயர்|Holder)$/i.test(lines[i])) {
      const n = lines[i + 1];
      if (n && n.length > 2 && !/^[A-Z]{2}\d{2}\d{4}/i.test(n.replace(/\s/g, ""))) return n.replace(/\s+/g, " ").trim().slice(0, 100);
    }
  }
  return "";
}

function parseLlrFieldsFromPlainText_(rawText) {
  const raw = String(rawText || "");
  const text = normalizeOcrText_(raw);
  const llrNumber =
    extractDlNumber_(text) ||
    extractDlNumber_(raw) ||
    extractLlrNumberNearLabels_(raw) ||
    extractLlrNumberNearLabels_(text) ||
    extractSarathiCompactLoose_(raw) ||
    extractSarathiCompactLoose_(text);
  const dobYmd = extractDobYmd_(raw) || extractDobYmd_(text);
  const holderName = extractHolderName_(raw) || extractHolderName_(text);
  const bloodGroup = extractBloodGroup_(raw) || extractBloodGroup_(text);
  return {
    llrNumber: llrNumber || "",
    dobYmd: dobYmd || "",
    holderName: holderName || "",
    bloodGroup: bloodGroup || "",
    rawTextSnippet: raw ? raw.replace(/\s+/g, " ").trim().slice(0, 400) : ""
  };
}

async function extractLlrFieldsFromBuffer_(buf) {
  const client = getVisionClient_();
  /** Prefer DOCUMENT_TEXT_DETECTION for licence-style dense layouts (often beats plain textDetection on scans). */
  let rawText = "";
  try {
    const [docRes] = await client.documentTextDetection({ image: { content: buf } });
    const ann = docRes.fullTextAnnotation;
    rawText = ann && ann.text ? ann.text : "";
  } catch (_e) {
    rawText = "";
  }
  let parsed = parseLlrFieldsFromPlainText_(rawText);
  if (parsed.llrNumber || parsed.dobYmd || parsed.bloodGroup) return parsed;

  try {
    const [result] = await client.textDetection({ image: { content: buf } });
    const ann = result.fullTextAnnotation;
    const alt = ann && ann.text ? ann.text : "";
    const p2 = parseLlrFieldsFromPlainText_(alt);
    const merged = {
      llrNumber: parsed.llrNumber || p2.llrNumber || "",
      dobYmd: parsed.dobYmd || p2.dobYmd || "",
      bloodGroup: parsed.bloodGroup || p2.bloodGroup || "",
      holderName: parsed.holderName || p2.holderName || "",
      rawTextSnippet: `${rawText}\n${alt}`.replace(/\s+/g, " ").trim().slice(0, 400)
    };
    if (merged.llrNumber || merged.dobYmd || merged.bloodGroup) return merged;
    return parseLlrFieldsFromPlainText_(`${rawText}\n${alt}`);
  } catch (_e2) {
    return parsed;
  }
}

/**
 * Digital PDF with embedded text (not scanned image pages).
 */
async function extractLlrFieldsFromPdfBuffer_(buf) {
  const pdfParse = require("pdf-parse");
  const data = await pdfParse(buf);
  const raw = data && data.text ? String(data.text) : "";
  if (!raw || raw.replace(/\s/g, "").length < 8) {
    const err = new Error("pdf_no_extractable_text");
    err.code = "PDF_SCANNED_NO_TEXT";
    throw err;
  }
  return parseLlrFieldsFromPlainText_(raw);
}

function isPdfMagicBytes_(buf) {
  return buf && buf.length >= 5 && buf[0] === 0x25 && buf[1] === 0x50 && buf[2] === 0x44 && buf[3] === 0x46;
}

module.exports = {
  extractLlrFieldsFromBuffer_,
  extractLlrFieldsFromPdfBuffer_,
  parseLlrFieldsFromPlainText_,
  isPdfMagicBytes_,
  extractBloodGroup_
};
