const admin = require("firebase-admin");

async function getBusinessSnapshotDoc(businessName) {
  const name = String(businessName || "Nanban").trim() || "Nanban";
  const db = admin.firestore();
  const ref = db.collection("businesses").doc(name).collection("snapshot").doc("main");
  const snap = await ref.get();
  return snap.exists ? snap.data() || {} : {};
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
  getQuizBankRows
};
