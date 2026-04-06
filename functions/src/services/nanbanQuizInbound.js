const admin = require("firebase-admin");
const { info } = require("../lib/logger");
const { getBusinessSnapshotDoc, setBusinessSnapshotDoc } = require("./snapshotStore");
const { enqueueWaOutboundSend } = require("./waOutboundQueue");
const { getISTDateString } = require("../lib/istTime");

const TENANT_DEFAULT = "nanban_main";

function digitsPhone(p) {
  let d = String(p || "").replace(/\D/g, "");
  if (d.length > 10) d = d.slice(-10);
  return d;
}

function waE164FromInbound(from) {
  const d = digitsPhone(from);
  return d.length === 10 ? `91${d}` : "";
}

function cleanQuizInboundText(msg) {
  return String(msg || "")
    .replace(/[*_~`]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function parseQuizChoiceFromText(msg) {
  const t = cleanQuizInboundText(msg);
  if (t === "1" || t === "1️⃣" || t.includes("முதல் விடை") || (t.includes("முதல்") && t.includes("விடை")))
    return 1;
  if (
    t === "2" ||
    t === "2️⃣" ||
    t.includes("இரண்டாம் விடை") ||
    t.includes("இரண்டாம்") ||
    t.includes("இரண்டு")
  )
    return 2;
  if (t === "3" || t === "3️⃣" || t.includes("மூன்றாம் விடை") || t.includes("மூன்றாம்") || t.includes("மூன்று"))
    return 3;
  return 0;
}

function parseQuizChoiceFromButton(id) {
  const s = String(id || "").trim();
  const m = s.match(/QUIZ_(?:CORRECT|WRONG)_(\d+)_(\d+)/i);
  if (m) {
    const userPick = parseInt(m[2], 10);
    if (userPick >= 1 && userPick <= 3) return userPick;
  }
  if (/^[123]$/.test(s)) return parseInt(s, 10);
  const low = s.toLowerCase();
  if (low === "btn_1" || low === "option_1" || low.endsWith("_1") || low === "answer_1") return 1;
  if (low === "btn_2" || low === "option_2" || low.endsWith("_2") || low === "answer_2") return 2;
  if (low === "btn_3" || low === "option_3" || low.endsWith("_3") || low === "answer_3") return 3;
  return 0;
}

/** Button replies have empty `text.body`; use interactive title + id. */
function resolveQuizChoiceFromInbound(inbound) {
  if (inbound?.interactive?.kind === "button_reply") {
    const idPick = parseQuizChoiceFromButton(inbound.interactive.id);
    if (idPick) return idPick;
    const titlePick = parseQuizChoiceFromText(inbound.interactive.title || "");
    if (titlePick) return titlePick;
  }
  return parseQuizChoiceFromText(inbound?.text || "");
}

async function persistNanbanStudents(students) {
  const snap = await getBusinessSnapshotDoc("Nanban");
  await setBusinessSnapshotDoc(
    "Nanban",
    Object.assign({}, snap, {
      students,
      updated_at: admin.firestore.FieldValue.serverTimestamp()
    }),
    true
  );
}

/**
 * WhatsApp quiz replies: FIFO against student.quizPendingQueue (filled by morning cron).
 * Correct answer → +1 marks / quizMarks (₹1 wallet) + quizStats + adminRemarks → Firestore.
 */
async function tryHandleNanbanQuizInbound({ tenantId, inbound }) {
  const tid = String(tenantId || "").trim() || TENANT_DEFAULT;

  const fromWa = inbound?.from || "";
  const phone10 = digitsPhone(fromWa);
  if (!phone10) return { handled: false };

  const choice = resolveQuizChoiceFromInbound(inbound);
  if (!choice) return { handled: false };

  const snap = await getBusinessSnapshotDoc("Nanban");
  let students = Array.isArray(snap.students) ? snap.students.map((x) => ({ ...x })) : [];
  const ix = students.findIndex((s) => digitsPhone(s.phone) === phone10);
  if (ix < 0) return { handled: false };

  const s = { ...students[ix] };
  const q = Array.isArray(s.quizPendingQueue) ? [...s.quizPendingQueue] : [];
  if (!q.length) return { handled: false };

  const pending = q[0];
  const correctNo = parseInt(pending.correctNo, 10) || 1;
  const isCorrect = choice === correctNo;

  q.shift();
  s.quizPendingQueue = q;

  if (!s.quizStats || typeof s.quizStats !== "object") s.quizStats = { total: 0, correct: 0 };
  s.quizStats.total = (parseInt(s.quizStats.total, 10) || 0) + 1;

  const today = getISTDateString();
  if (!Array.isArray(s.adminRemarks)) s.adminRemarks = [];

  if (isCorrect) {
    s.quizStats.correct = (parseInt(s.quizStats.correct, 10) || 0) + 1;
    s.marks = (parseInt(s.marks, 10) || 0) + 1;
    s.quizMarks = (parseInt(s.quizMarks, 10) || 0) + 1;
    s.adminRemarks.unshift({
      date: today,
      text: `✅ Quiz correct (+₹1 wallet / marks). Day ${pending.quizDay || "-"}`
    });
  } else {
    s.adminRemarks.unshift({
      date: today,
      text: `❌ Quiz wrong (choice ${choice}, expected ${correctNo}). Day ${pending.quizDay || "-"}`
    });
  }

  students[ix] = s;
  await persistNanbanStudents(students);

  const to = waE164FromInbound(fromWa);
  if (to) {
    const okMsg = "சரியான விடை! உங்கள் வாலட்டில் ₹1 வரவு வைக்கப்பட்டது.";
    const badMsg =
      "❌ தவறான விடை.\n" +
      "பரவாயில்லை, நாளை மீண்டும் முயற்சி செய்வோம்! விழிப்புணர்வுடன் ஓட்டுங்கள். 🚦\n" +
      "- நண்பன் டிரைவிங் ஸ்கூல்";
    await enqueueWaOutboundSend(
      {
        tenantId: tid,
        to,
        message: isCorrect ? okMsg : badMsg,
        messageType: "text",
        metadata: { kind: "quiz_reply_feedback", student_id: String(s.id), correct: isCorrect }
      },
      { delaySeconds: 0 }
    );
  }

  info("NANBAN_QUIZ_INBOUND_HANDLED", { phone10, isCorrect, remaining: q.length });
  return { handled: true, isCorrect };
}

module.exports = { tryHandleNanbanQuizInbound };
