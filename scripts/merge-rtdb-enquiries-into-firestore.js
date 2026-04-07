/**
 * One-time / ops: copy enquiry rows from Firebase RTDB into Firestore snapshot
 * as `enquiries[]`. At read time, snapshotStore merges these into `students` with
 * type Enquiry (deduped by id).
 *
 * Probes: nanban_enquiries, nanban/main/enquiries, nanban_driving_school/enquiries
 *
 * Run (from repo root, with ADC / GOOGLE_APPLICATION_CREDENTIALS):
 *   node scripts/merge-rtdb-enquiries-into-firestore.js
 */
/* eslint-disable no-console */
const path = require("path");
const admin = require(path.join(__dirname, "..", "functions", "node_modules", "firebase-admin"));

const PROJECT_ID = process.env.GCLOUD_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || "nanban-driving-school-d7b20";
const RTDB_URL = `https://${PROJECT_ID}-default-rtdb.firebaseio.com`;

function toArray_(v) {
  if (Array.isArray(v)) return v.filter(Boolean);
  if (v && typeof v === "object") return Object.values(v).filter((x) => x && typeof x === "object");
  return [];
}

async function main() {
  if (!admin.apps.length) {
    try {
      admin.initializeApp({ credential: admin.credential.applicationDefault(), projectId: PROJECT_ID, databaseURL: RTDB_URL });
    } catch (_) {
      admin.initializeApp({ projectId: PROJECT_ID, databaseURL: RTDB_URL });
    }
  }

  const rtdb = admin.database();
  const paths = ["nanban_enquiries", "nanban/main/enquiries", "nanban_driving_school/enquiries"];
  let merged = [];
  for (const p of paths) {
    const snap = await rtdb.ref(p).once("value");
    const v = snap.val();
    const rows = toArray_(v);
    if (rows.length) {
      console.log("RTDB", p, "→", rows.length, "rows");
      merged = merged.concat(rows);
    }
  }

  if (!merged.length) {
    console.log("No enquiry rows found under:", paths.join(", "));
    return;
  }

  const db = admin.firestore();
  const ref = db.collection("businesses").doc("Nanban").collection("snapshot").doc("main");
  await ref.set(
    {
      enquiries: merged,
      enquiries_merged_from_rtdb_at: admin.firestore.FieldValue.serverTimestamp()
    },
    { merge: true }
  );
  console.log("OK: wrote enquiries[] to businesses/Nanban/snapshot/main (merge), count=", merged.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
