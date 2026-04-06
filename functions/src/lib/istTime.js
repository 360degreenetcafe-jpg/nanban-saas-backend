/** IST helpers (Asia/Kolkata) for scheduled jobs. */

function istParts(d = new Date()) {
  const fmt = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    day: "2-digit",
    month: "2-digit",
    year: "numeric"
  });
  const parts = fmt.formatToParts(d);
  const get = (t) => parts.find((p) => p.type === t)?.value || "";
  return { dd: get("day"), mm: get("month"), yyyy: get("year") };
}

function getISTDateString(d = new Date()) {
  const { dd, mm, yyyy } = istParts(d);
  return `${dd}/${mm}/${yyyy}`;
}

function getTomorrowYYYYMMDD(d = new Date()) {
  const x = new Date(d.getTime());
  x.setUTCDate(x.getUTCDate() + 1);
  const { dd, mm, yyyy } = istParts(x);
  return `${yyyy}-${mm}-${dd}`;
}

function parseStudentDateDDMMYYYY(s) {
  const raw = String(s || "").trim();
  if (!raw) return null;
  const m = raw.match(/^(\d{2})[/-](\d{2})[/-](\d{4})$/);
  if (!m) return null;
  const d = new Date(parseInt(m[3], 10), parseInt(m[2], 10) - 1, parseInt(m[1], 10));
  return Number.isNaN(d.getTime()) ? null : d;
}

function getDaysSinceStudentJoin(student) {
  const d = parseStudentDateDDMMYYYY(student?.dateJoined);
  if (!d) return 0;
  d.setHours(0, 0, 0, 0);
  const t = new Date();
  const istNow = new Date(
    t.toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
  );
  istNow.setHours(0, 0, 0, 0);
  const diff = Math.floor((istNow - d) / (86400000));
  return diff < 0 ? 0 : diff;
}

function getQuizDayByJoinDate(student) {
  return Math.max(1, getDaysSinceStudentJoin(student) + 1);
}

/** Normalize to YYYY-MM-DD for comparisons (accepts DD/MM/YYYY or ISO date prefix). */
function normalizeDateValue(input) {
  const s = String(input || "").trim();
  if (!s) return "";
  const m = s.match(/^(\d{2})[/-](\d{2})[/-](\d{4})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]}`;
  const m2 = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m2) return `${m2[1]}-${m2[2]}-${m2[3]}`;
  const isoT = s.match(/^(\d{4})-(\d{2})-(\d{2})T/);
  if (isoT) return `${isoT[1]}-${isoT[2]}-${isoT[3]}`;
  return s;
}

function getISTTimeHHMM(d = new Date()) {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  }).format(d);
}

module.exports = {
  getISTDateString,
  getTomorrowYYYYMMDD,
  parseStudentDateDDMMYYYY,
  getDaysSinceStudentJoin,
  getQuizDayByJoinDate,
  istParts,
  normalizeDateValue,
  getISTTimeHHMM
};
