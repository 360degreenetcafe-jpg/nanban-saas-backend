/**
 * Writes merged+normalized Nanban snapshot to businesses/Nanban/snapshot/main
 * so direct Firestore clients and subdoc-only reads see full arrays.
 *
 * Run: node scripts/emergency-materialize-snapshot.js
 */
/* eslint-disable no-console */
const path = require("path");
const admin = require(path.join(__dirname, "..", "functions", "node_modules", "firebase-admin"));
const {
  getBusinessSnapshotDoc,
  setBusinessSnapshotDoc
} = require(path.join(__dirname, "..", "functions", "src", "services", "snapshotStore.js"));

const PROJECT_ID = process.env.GCLOUD_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || "nanban-driving-school-d7b20";

async function main() {
  if (!admin.apps.length) {
    try {
      admin.initializeApp({
        credential: admin.credential.applicationDefault(),
        projectId: PROJECT_ID
      });
    } catch (_) {
      admin.initializeApp({ projectId: PROJECT_ID });
    }
  }

  const data = await getBusinessSnapshotDoc("Nanban");
  const st = Array.isArray(data.students) ? data.students.length : 0;
  const ex = Array.isArray(data.expenses) ? data.expenses.length : 0;
  console.log("Materializing getBusinessSnapshotDoc('Nanban') → snapshot/main");
  console.log("  students:", st, "expenses:", ex);

  if (st === 0 && ex === 0) {
    console.log("Nothing to write (both arrays empty). Abort without Firestore write.");
    process.exit(2);
  }

  await setBusinessSnapshotDoc(
    "Nanban",
    {
      students: data.students,
      expenses: data.expenses,
      appSettings: data.appSettings && typeof data.appSettings === "object" ? data.appSettings : {},
      chitData:
        data.chitData && typeof data.chitData === "object"
          ? data.chitData
          : { groups: [], members: [], auctions: [], payments: [], bids: [], schedule: [] }
    },
    true
  );
  console.log("OK: snapshot/main updated.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
