/**
 * Emergency diagnostic: raw Firestore vs merged snapshot API shape.
 * Run from repo root: node scripts/emergency-data-check.js
 * Requires Application Default Credentials (e.g. GOOGLE_APPLICATION_CREDENTIALS) or gcloud ADC.
 */
/* eslint-disable no-console */
const path = require("path");

const admin = require(path.join(__dirname, "..", "functions", "node_modules", "firebase-admin"));
const {
  getBusinessSnapshotDoc,
  coerceFirestoreArray,
  normalizeSnapshotDocForRead
} = require(path.join(__dirname, "..", "functions", "src", "services", "snapshotStore.js"));

const PROJECT_ID = process.env.GCLOUD_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || "nanban-driving-school-d7b20";
const RTDB_URL = `https://${PROJECT_ID}-default-rtdb.firebaseio.com`;

function describeField(name, v) {
  const t = v === null ? "null" : Array.isArray(v) ? "array" : typeof v;
  let len = "n/a";
  if (v === undefined) len = "missing";
  else if (Array.isArray(v)) len = String(v.length);
  else if (typeof v === "string") len = `string(${v.length} chars)`;
  else if (v && typeof v === "object") {
    const c = coerceFirestoreArray(v);
    len = `object→coercedLen=${c.length}`;
  }
  console.log(`    ${name}: type=${t} len/summary=${len}`);
}

function inspectRawDoc(label, data) {
  console.log(`\n--- ${label} (top-level keys: ${data ? Object.keys(data).sort().join(", ") : "N/A"}) ---`);
  if (!data || typeof data !== "object") {
    console.log("    (no data)");
    return;
  }
  describeField("students", data.students);
  describeField("Students", data.Students);
  describeField("studentList", data.studentList);
  describeField("expenses", data.expenses);
  describeField("Expenses", data.Expenses);
  describeField("expenseList", data.expenseList);
  if (data.payload && typeof data.payload === "object") {
    console.log("    payload keys:", Object.keys(data.payload).join(", "));
    describeField("payload.students", data.payload.students);
    describeField("payload.Students", data.payload.Students);
    describeField("payload.expenses", data.payload.expenses);
    describeField("payload.Expenses", data.payload.Expenses);
  }
}

async function scanOtherSnapshots(db) {
  console.log("\n=== SCAN collectionGroup('snapshot') id==main (coerced lengths) ===");
  const qs = await db.collectionGroup("snapshot").get();
  const rows = [];
  for (const doc of qs.docs) {
    if (doc.id !== "main") continue;
    const d = doc.data() || {};
    const n = coerceFirestoreArray(d.students).length;
    const m = coerceFirestoreArray(d.expenses).length;
    rows.push({ path: doc.ref.path, students: n, expenses: m });
  }
  rows.sort((a, b) => b.students - a.students);
  if (!rows.length) console.log("  (no snapshot docs named main)");
  else rows.slice(0, 15).forEach((r) => console.log(`  ${r.path}: students~${r.students} expenses~${r.expenses}`));
}

function rtdbArrayLen_(v) {
  if (Array.isArray(v)) return v.length;
  if (v && typeof v === "object") return Object.keys(v).length;
  return 0;
}

async function main() {
  if (!admin.apps.length) {
    try {
      admin.initializeApp({
        credential: admin.credential.applicationDefault(),
        projectId: PROJECT_ID,
        databaseURL: RTDB_URL
      });
    } catch (_) {
      admin.initializeApp({ projectId: PROJECT_ID, databaseURL: RTDB_URL });
    }
  }

  const db = admin.firestore();
  console.log("Project:", PROJECT_ID);
  console.log("Time:", new Date().toISOString());

  const parentRef = db.collection("businesses").doc("Nanban");
  const subRef = parentRef.collection("snapshot").doc("main");

  const parentSnap = await parentRef.get();
  const subSnap = await subRef.get();

  console.log("\n=== businesses/Nanban (parent) exists:", parentSnap.exists, "===");
  inspectRawDoc("parent", parentSnap.exists ? parentSnap.data() : null);

  console.log("\n=== businesses/Nanban/snapshot/main exists:", subSnap.exists, "===");
  inspectRawDoc("snapshot/main", subSnap.exists ? subSnap.data() : null);

  const mergedSpread = {
    ...(parentSnap.exists ? parentSnap.data() || {} : {}),
    ...(subSnap.exists ? subSnap.data() || {} : {})
  };
  console.log("\n=== naive {parent,...sub} spread (for comparison) ===");
  inspectRawDoc("mergedSpread", mergedSpread);

  const normMerged = normalizeSnapshotDocForRead("Nanban", mergedSpread);
  console.log("\n=== normalizeSnapshotDocForRead(mergedSpread) ===");
  console.log("    students array length:", normMerged.students.length);
  console.log("    expenses array length:", normMerged.expenses.length);

  const apiView = await getBusinessSnapshotDoc("Nanban");
  console.log("\n=== getBusinessSnapshotDoc('Nanban') [API path] ===");
  console.log("    students array length:", apiView.students.length);
  console.log("    expenses array length:", apiView.expenses.length);

  await scanOtherSnapshots(db);

  console.log("\n=== RTDB (legacy GAS paths — often still authoritative) ===");
  try {
    const rtdb = admin.database();
    for (const p of ["nanban/main", "nanban_driving_school", "esevai/main"]) {
      const snap = await rtdb.ref(p).once("value");
      const v = snap.val();
      if (!v) {
        console.log(`  ${p}: (null)`);
        continue;
      }
      console.log(
        `  ${p}: students~${rtdbArrayLen_(v.students)} expenses~${rtdbArrayLen_(v.expenses)} keys=${Object.keys(v).join(",")}`
      );
    }
  } catch (e) {
    console.log("  RTDB probe failed:", String(e && e.message ? e.message : e));
  }

  console.log("\n=== DONE ===\n");
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
