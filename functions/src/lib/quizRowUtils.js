/** Column indices for quiz_bank rows (sheet-shaped arrays). */
const QUIZ_Q = { cat: 0, day: 1, ques: 2, o1: 3, o2: 4, o3: 5, ansText: 6, img: 7 };

function normQuizTxt(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function resolveQuizCorrectChoiceNo(row) {
  if (!Array.isArray(row) || row.length < 6) return 1;
  const ans = String(row[QUIZ_Q.ansText] ?? "").trim();
  const o1 = String(row[QUIZ_Q.o1] ?? "").trim();
  const o2 = String(row[QUIZ_Q.o2] ?? "").trim();
  const o3 = String(row[QUIZ_Q.o3] ?? "").trim();
  const n = parseInt(ans, 10);
  if (n >= 1 && n <= 3) return n;
  const na = normQuizTxt(ans);
  if (na && na === normQuizTxt(o1)) return 1;
  if (na && na === normQuizTxt(o2)) return 2;
  if (na && na === normQuizTxt(o3)) return 3;
  return 1;
}

module.exports = { QUIZ_Q, resolveQuizCorrectChoiceNo, normQuizTxt };
