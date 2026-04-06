#!/usr/bin/env node
/**
 * Seed Firestore meta doc for Nanban quiz bank (optional).
 * Requires: GOOGLE_APPLICATION_CREDENTIALS pointing at a service account JSON
 * with Firestore write access to the target project.
 *
 * Usage (from repo root): node scripts/seed-quiz-bank.js
 */
const path = require("path");
const admin = require(path.join(__dirname, "../functions/node_modules/firebase-admin"));

if (!admin.apps.length) {
  admin.initializeApp();
}

const rows = process.argv.includes("--empty") ? [] : [];

async function main() {
  const db = admin.firestore();
  await db
    .collection("businesses")
    .doc("Nanban")
    .collection("meta")
    .doc("quiz_bank")
    .set(
      {
        rows,
        updated_at: admin.firestore.FieldValue.serverTimestamp(),
        note: "Populate rows as sheet-shaped arrays: [Cat, Day, Question, Opt1, Opt2, Opt3, Answer, ImgUrl?, Explanation?]"
      },
      { merge: true }
    );
  console.log("quiz_bank doc written at businesses/Nanban/meta/quiz_bank");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
