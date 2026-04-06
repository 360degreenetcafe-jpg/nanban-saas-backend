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

async function getQuizBankRows() {
  const db = admin.firestore();
  const ref = db.collection("businesses").doc("Nanban").collection("meta").doc("quiz_bank");
  const snap = await ref.get();
  if (!snap.exists) return [];
  const rows = snap.data()?.rows;
  return Array.isArray(rows) ? rows : [];
}

module.exports = {
  getBusinessSnapshotDoc,
  setBusinessSnapshotDoc,
  getRuntimeDoc,
  setRuntimeDoc,
  getQuizBankRows
};
