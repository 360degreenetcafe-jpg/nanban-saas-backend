const admin = require("firebase-admin");

/**
 * Firestore sometimes stores sheet-style rows as JSON strings or map objects.
 * The hosted UI expects real arrays for students / expenses / E‑Sevai lists.
 */
function coerceFirestoreArray(v) {
  if (Array.isArray(v)) return v;
  if (v === undefined || v === null) return [];
  if (typeof v === "string") {
    const s = v.trim();
    if (!s) return [];
    try {
      const p = JSON.parse(s);
      return Array.isArray(p) ? p : [];
    } catch (_) {
      return [];
    }
  }
  if (typeof v === "object") {
    const keys = Object.keys(v);
    if (keys.length && keys.every((k) => /^\d+$/.test(k))) {
      return keys.sort((a, b) => Number(a) - Number(b)).map((k) => v[k]);
    }
    // Firestore map keyed by id (e.g. { "1734...": { id, name, ... }, ... })
    const vals = Object.values(v);
    if (
      vals.length &&
      vals.every((item) => item && typeof item === "object" && !Array.isArray(item))
    ) {
      return vals;
    }
  }
  return [];
}

function coerceChitData(raw) {
  if (typeof raw === "string") {
    try {
      const p = JSON.parse(raw);
      return coerceChitData(p);
    } catch (_) {
      return { groups: [], members: [], auctions: [], payments: [], bids: [], schedule: [] };
    }
  }
  const base = raw && typeof raw === "object" && !Array.isArray(raw) ? { ...raw } : {};
  for (const k of ["groups", "members", "auctions", "payments", "bids", "schedule"]) {
    base[k] = coerceFirestoreArray(base[k] !== undefined ? base[k] : []);
  }
  return base;
}

function normalizeSnapshotDocForRead(businessName, raw) {
  const id = String(businessName || "Nanban").trim() || "Nanban";
  const data = raw && typeof raw === "object" ? { ...raw } : {};
  if (id === "ESevai") {
    for (const k of [
      "services",
      "customers",
      "agents",
      "ledgerEntries",
      "enquiries",
      "works",
      "transactions",
      "reminders"
    ]) {
      data[k] = coerceFirestoreArray(data[k]);
    }
    return data;
  }
  data.students = coerceFirestoreArray(data.students);
  data.expenses = coerceFirestoreArray(data.expenses);
  if (data.chitData !== undefined && data.chitData !== null) {
    data.chitData = coerceChitData(data.chitData);
  }
  return data;
}

async function getBusinessSnapshotDoc(businessName) {
  const name = String(businessName || "Nanban").trim() || "Nanban";
  const db = admin.firestore();
  const ref = db.collection("businesses").doc(name).collection("snapshot").doc("main");
  const snap = await ref.get();
  const raw = snap.exists ? snap.data() || {} : {};
  return normalizeSnapshotDocForRead(name, raw);
}

async function setBusinessSnapshotDoc(businessName, data, merge = true) {
  const name = String(businessName || "Nanban").trim() || "Nanban";
  const db = admin.firestore();
  const ref = db.collection("businesses").doc(name).collection("snapshot").doc("main");
  const payload = Object.assign({}, data, {
    updated_at: admin.firestore.FieldValue.serverTimestamp()
  });
  await ref.set(payload, { merge });
}

async function getRuntimeDoc(businessName, docId) {
  const name = String(businessName || "Nanban").trim() || "Nanban";
  const db = admin.firestore();
  const ref = db.collection("businesses").doc(name).collection("runtime").doc(String(docId || "main"));
  const snap = await ref.get();
  return snap.exists ? snap.data() || {} : {};
}

async function setRuntimeDoc(businessName, docId, data, merge = true) {
  const name = String(businessName || "Nanban").trim() || "Nanban";
  const db = admin.firestore();
  const ref = db.collection("businesses").doc(name).collection("runtime").doc(String(docId || "main"));
  await ref.set(
    Object.assign({}, data, { updated_at: admin.firestore.FieldValue.serverTimestamp() }),
    { merge }
  );
}

/** Firestore forbids nested arrays; rows may be stored as JSON strings per line. */
function materializeQuizRow(entry) {
  if (Array.isArray(entry)) return entry;
  if (typeof entry === "string") {
    try {
      const a = JSON.parse(entry);
      return Array.isArray(a) ? a : [];
    } catch (_) {
      return [];
    }
  }
  if (entry && typeof entry === "object") {
    const o = entry;
    if (o.cat != null || o.day != null) {
      return [
        String(o.cat ?? ""),
        String(o.day ?? ""),
        String(o.question ?? o.ques ?? ""),
        String(o.opt1 ?? o.o1 ?? ""),
        String(o.opt2 ?? o.o2 ?? ""),
        String(o.opt3 ?? o.o3 ?? ""),
        String(o.answer ?? o.ans ?? ""),
        String(o.imageUrl ?? o.img ?? ""),
        String(o.explanation ?? o.exp ?? "")
      ];
    }
  }
  return [];
}

async function getQuizBankRows() {
  const db = admin.firestore();
  const ref = db.collection("businesses").doc("Nanban").collection("meta").doc("quiz_bank");
  const snap = await ref.get();
  if (!snap.exists) return [];
  const rows = snap.data()?.rows;
  if (!Array.isArray(rows)) return [];
  return rows.map(materializeQuizRow).filter((r) => r.length >= 6);
}

module.exports = {
  getBusinessSnapshotDoc,
  setBusinessSnapshotDoc,
  getRuntimeDoc,
  setRuntimeDoc,
  getQuizBankRows,
  coerceFirestoreArray,
  normalizeSnapshotDocForRead
};
