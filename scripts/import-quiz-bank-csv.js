#!/usr/bin/env node
/**
 * Import quiz rows from a CSV (e.g. Google Sheets → File → Download → CSV) into
 * Firestore: businesses/Nanban/meta/quiz_bank { rows: [...] }
 *
 * CSV columns (header row optional):
 *   Cat, Day, Question, Opt1, Opt2, Opt3, Answer, ImgUrl, Explanation
 * Cat: 2W | 4W | General
 * Day: integer 1..N (quiz day index; morning job matches student day from join date)
 *
 * Requires: GOOGLE_APPLICATION_CREDENTIALS → service account with Firestore write.
 *
 * Usage:
 *   node scripts/import-quiz-bank-csv.js path/to/quiz.csv
 *   node scripts/import-quiz-bank-csv.js scripts/quiz_bank_rows.example.json
 *   node scripts/import-quiz-bank-csv.js path/to/quiz.csv --replace   (full doc replace)
 *
 * After import, deploy functions (or wait) — nanbanCronDailyMorning runs 07:00 Asia/Kolkata.
 */
const fs = require("fs");
const path = require("path");
const admin = require(path.join(__dirname, "../functions/node_modules/firebase-admin"));

function readDefaultProjectId() {
  try {
    const rc = JSON.parse(fs.readFileSync(path.join(__dirname, "../.firebaserc"), "utf8"));
    const p = rc.projects && rc.projects.default;
    if (p) return p;
  } catch (e) {}
  return process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || "";
}

function parseCsv(content) {
  const rows = [];
  let i = 0;
  let row = [];
  let cell = "";
  let inQuotes = false;
  while (i < content.length) {
    const c = content[i];
    if (inQuotes) {
      if (c === '"') {
        if (content[i + 1] === '"') {
          cell += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i++;
        continue;
      }
      cell += c;
      i++;
      continue;
    }
    if (c === '"') {
      inQuotes = true;
      i++;
      continue;
    }
    if (c === ",") {
      row.push(cell);
      cell = "";
      i++;
      continue;
    }
    if (c === "\r") {
      i++;
      continue;
    }
    if (c === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      i++;
      continue;
    }
    cell += c;
    i++;
  }
  row.push(cell);
  if (cell.length || row.length > 1) rows.push(row);
  return rows;
}

function toRowArrays(csvRows) {
  let start = 0;
  if (csvRows.length && String(csvRows[0][0] || "").toLowerCase().includes("cat")) {
    start = 1;
  }
  const out = [];
  for (let r = start; r < csvRows.length; r++) {
    const line = csvRows[r];
    if (!line || !line.length) continue;
    const cat = String(line[0] ?? "").trim();
    if (!cat) continue;
    const day = String(line[1] ?? "").trim();
    if (day === "" || Number.isNaN(parseInt(day, 10))) continue;
    const ques = String(line[2] ?? "").trim();
    if (!ques) continue;
    const row = [
      cat,
      day,
      ques,
      String(line[3] ?? ""),
      String(line[4] ?? ""),
      String(line[5] ?? ""),
      String(line[6] ?? ""),
      String(line[7] ?? ""),
      line[8] != null ? String(line[8]) : ""
    ];
    out.push(row);
  }
  return out;
}

async function main() {
  const file = process.argv[2];
  if (!file || file.startsWith("-")) {
    console.error("Usage: node scripts/import-quiz-bank-csv.js <file.csv> [--replace]");
    process.exit(1);
  }
  const replace = process.argv.includes("--replace");
  const abs = path.resolve(process.cwd(), file);
  const raw = fs.readFileSync(abs, "utf8");
  let rows;
  if (file.toLowerCase().endsWith(".json")) {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      console.error("JSON file must be an array of row arrays.");
      process.exit(1);
    }
    rows = parsed.filter((r) => Array.isArray(r) && r.length >= 7);
  } else {
    const csvRows = parseCsv(raw);
    rows = toRowArrays(csvRows);
  }
  if (!rows.length) {
    console.error("No valid data rows. CSV: Cat,Day,Question,Opt1,Opt2,Opt3,Answer,... JSON: array of arrays.");
    process.exit(1);
  }

  if (!admin.apps.length) {
    const projectId = readDefaultProjectId();
    if (!projectId) {
      console.error("Set project: add .firebaserc projects.default or GCLOUD_PROJECT.");
      process.exit(1);
    }
    admin.initializeApp({ projectId });
  }
  const db = admin.firestore();
  const ref = db.collection("businesses").doc("Nanban").collection("meta").doc("quiz_bank");
  const rowsForFirestore = rows.map((r) => JSON.stringify(r));
  const payload = {
    rows: rowsForFirestore,
    updated_at: admin.firestore.FieldValue.serverTimestamp(),
    note: "import-quiz-bank-csv.js; rows as JSON strings (Firestore-safe); Q indices 0..8"
  };
  if (replace) {
    await ref.set(payload);
  } else {
    await ref.set(payload, { merge: true });
  }
  console.log(`Wrote ${rows.length} rows to businesses/Nanban/meta/quiz_bank`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
