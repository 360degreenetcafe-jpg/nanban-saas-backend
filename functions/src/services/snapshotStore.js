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
    // Wrapped shapes seen in exports / legacy writes
    if (Array.isArray(v.items)) return coerceFirestoreArray(v.items);
    if (Array.isArray(v.rows)) return coerceFirestoreArray(v.rows);
    if (Array.isArray(v.list)) return coerceFirestoreArray(v.list);
    const keys = Object.keys(v);
    if (keys.length && keys.every((k) => /^\d+$/.test(k))) {
      return keys
        .sort((a, b) => Number(a) - Number(b))
        .map((k) => v[k])
        .map((item) => {
          if (item && typeof item === "object" && !Array.isArray(item)) return item;
          if (typeof item === "string") {
            try {
              const p = JSON.parse(item);
              return p && typeof p === "object" ? p : null;
            } catch (_) {
              return null;
            }
          }
          return null;
        })
        .filter((item) => item != null);
    }
    // Map keyed by id, or mixed maps: keep only object rows (one bad leaf must not wipe all rows).
    const vals = Object.values(v).filter(
      (item) => item && typeof item === "object" && !Array.isArray(item)
    );
    if (vals.length) return vals;
    const stringRows = Object.values(v).filter((item) => typeof item === "string");
    if (stringRows.length === keys.length && keys.length) {
      const parsed = stringRows
        .map((s) => {
          try {
            const p = JSON.parse(String(s).trim());
            return p && typeof p === "object" ? p : null;
          } catch (_) {
            return null;
          }
        })
        .filter(Boolean);
      if (parsed.length) return parsed;
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
  const pay = data.payload && typeof data.payload === "object" ? data.payload : {};
  const rawStu =
    data.students !== undefined && data.students !== null
      ? data.students
      : data.Students !== undefined
        ? data.Students
        : data.studentList !== undefined
          ? data.studentList
          : pay.students !== undefined
            ? pay.students
            : pay.Students;
  const rawExp =
    data.expenses !== undefined && data.expenses !== null
      ? data.expenses
      : data.Expenses !== undefined
        ? data.Expenses
        : data.expenseList !== undefined
          ? data.expenseList
          : pay.expenses !== undefined
            ? pay.expenses
            : pay.Expenses;
  data.students = coerceFirestoreArray(rawStu);
  data.expenses = coerceFirestoreArray(rawExp);
  delete data.Students;
  delete data.Expenses;
  if (data.chitData !== undefined && data.chitData !== null) {
    data.chitData = coerceChitData(data.chitData);
  }
  return data;
}

/**
 * Data may live on `businesses/{id}` (parent) and/or `.../snapshot/main`.
 * Subdoc merge alone drops parent-only rows; pick the longer raw students/expenses.
 */
function mergeParentAndSubSnapshot_(parentRaw, subRaw) {
  const p = parentRaw && typeof parentRaw === "object" ? { ...parentRaw } : {};
  const s = subRaw && typeof subRaw === "object" ? { ...subRaw } : {};
  const out = { ...p, ...s };
  const pickLonger = (key) => {
    const a = p[key];
    const b = s[key];
    return coerceFirestoreArray(b).length > coerceFirestoreArray(a).length ? b : a;
  };
  out.students = pickLonger("students");
  out.expenses = pickLonger("expenses");
  return out;
}

async function loadNanbanSnapshotLayers_(db, docId) {
  const parentSnap = await db.collection("businesses").doc(docId).get();
  const parentRaw = parentSnap.exists ? parentSnap.data() || {} : {};
  const ref = db.collection("businesses").doc(docId).collection("snapshot").doc("main");
  const subSnap = await ref.get();
  const subRaw = subSnap.exists ? subSnap.data() || {} : {};
  return mergeParentAndSubSnapshot_(parentRaw, subRaw);
}

async function getBusinessSnapshotDoc(businessName) {
  const name = String(businessName || "Nanban").trim() || "Nanban";
  const db = admin.firestore();
  let merged = await loadNanbanSnapshotLayers_(db, name);
  let out = normalizeSnapshotDocForRead(name, merged);
  const stuLen = Array.isArray(out.students) ? out.students.length : 0;
  const expLen = Array.isArray(out.expenses) ? out.expenses.length : 0;
  if (name === "Nanban" && stuLen === 0 && expLen === 0) {
    merged = await loadNanbanSnapshotLayers_(db, "nanban");
    out = normalizeSnapshotDocForRead("Nanban", merged);
  }
  return out;
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
