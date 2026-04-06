/**
 * EMERGENCY: Copy Nanban ERP snapshot from Firebase Realtime Database → Firestore
 * so Cloud Functions /v1/snapshot and the hosted UI (native API path) see data.
 *
 * RTDB paths (legacy GAS backend): nanban/main, nanban_driving_school
 * Target: businesses/Nanban/snapshot/main
 *
 * Run: node scripts/emergency-restore-rtdb-to-firestore.js
 */
/* eslint-disable no-console */
const path = require("path");

const admin = require(path.join(__dirname, "..", "functions", "node_modules", "firebase-admin"));

const PROJECT_ID = process.env.GCLOUD_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || "nanban-driving-school-d7b20";
const RTDB_URL = `https://${PROJECT_ID}-default-rtdb.firebaseio.com`;

function toArray_(v) {
  if (Array.isArray(v)) return v;
  if (v && typeof v === "object") return Object.values(v);
  return [];
}

/** Last write wins per id so newer RTDB branch overrides; keeps one row per id. */
function mergeStudentsById_(a, b) {
  const m = new Map();
  for (const x of [...toArray_(a), ...toArray_(b)]) {
    if (x && x.id != null) m.set(String(x.id), x);
  }
  return Array.from(m.values());
}

function mergeExpenses_(a, b) {
  const m = new Map();
  for (const x of [...toArray_(a), ...toArray_(b)]) {
    if (!x || typeof x !== "object") continue;
    const k = `${String(x.date || "")}|${Number(x.amt) || 0}|${String(x.desc || "").slice(0, 48)}`;
    if (!m.has(k)) m.set(k, x);
  }
  return Array.from(m.values());
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
      admin.initializeApp({
        projectId: PROJECT_ID,
        databaseURL: RTDB_URL
      });
    }
  }

  const rtdb = admin.database();
  const main = (await rtdb.ref("nanban/main").once("value")).val() || {};
  const vdb = (await rtdb.ref("nanban_driving_school").once("value")).val() || {};

  const students = mergeStudentsById_(main.students, vdb.students);
  const expenses = mergeExpenses_(main.expenses, vdb.expenses);
  const bundle = main.appSettingsBundle && typeof main.appSettingsBundle === "object" ? main.appSettingsBundle : {};
  const appSettings =
    bundle.appSettings && typeof bundle.appSettings === "object"
      ? bundle.appSettings
      : vdb.appSettingsBundle && typeof vdb.appSettingsBundle === "object" && vdb.appSettingsBundle.appSettings
        ? vdb.appSettingsBundle.appSettings
        : {};

  console.log("RTDB nanban/main students:", toArray_(main.students).length, "expenses:", toArray_(main.expenses).length);
  console.log("RTDB nanban_driving_school students:", toArray_(vdb.students).length, "expenses:", toArray_(vdb.expenses).length);
  console.log("Merged → Firestore students:", students.length, "expenses:", expenses.length);

  if (!students.length && !expenses.length) {
    console.error("Nothing to restore (RTDB arrays empty).");
    process.exit(2);
  }

  const db = admin.firestore();
  await db
    .collection("businesses")
    .doc("Nanban")
    .set(
      {
        businessId: "Nanban",
        source: "emergency_rtdb_restore",
        updated_at: admin.firestore.FieldValue.serverTimestamp()
      },
      { merge: true }
    );

  const ref = db.collection("businesses").doc("Nanban").collection("snapshot").doc("main");
  await ref.set(
    {
      students,
      expenses,
      appSettings,
      chitData: { groups: [], members: [], auctions: [], payments: [], bids: [], schedule: [] },
      updated_at: admin.firestore.FieldValue.serverTimestamp(),
      emergency_restored_from_rtdb: admin.firestore.FieldValue.serverTimestamp()
    },
    { merge: true }
  );

  console.log("OK: Firestore businesses/Nanban/snapshot/main written (merge).");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
