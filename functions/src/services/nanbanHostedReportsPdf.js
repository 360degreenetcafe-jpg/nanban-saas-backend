/**
 * Hosted (Firebase) replacements for legacy GAS HTML→Drive PDF reports.
 * Uses pdfkit + snapshot data (same sources as index.html printFullAuditReport / gode.gs).
 * Tamil: optional Noto Sans Tamil VF at functions/assets/NotoSansTamil-VF.ttf (OFL — see OFL-NotoSansTamil.txt).
 */

const fs = require("fs");
const path = require("path");
const PDFDocument = require("pdfkit");
const { getBusinessSnapshotDoc } = require("./snapshotStore");
const { listNanbanFilingEntries } = require("./nanbanFilingLogStore");

const NB_TAMIL_TTF = path.join(__dirname, "../../assets/NotoSansTamil-VF.ttf");

function registerNanbanPdfBodyFont_(doc) {
  try {
    if (fs.existsSync(NB_TAMIL_TTF)) {
      doc.registerFont("_nbTamil", NB_TAMIL_TTF);
      return "_nbTamil";
    }
  } catch (_) {}
  return "Helvetica";
}

function businessDocIdForTenant_(tenantId) {
  const t = String(tenantId || "").trim();
  if (/^esevai$/i.test(t)) return "Nanban";
  if (!t || t === "nanban_main" || t === "nanban-main") return "Nanban";
  return t;
}

function parseFlexibleDate_(s) {
  const str = String(s || "").trim();
  if (!str) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const d = new Date(`${str}T12:00:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  const p = str.split("/");
  if (p.length === 3) {
    const dd = parseInt(p[0], 10);
    const mm = parseInt(p[1], 10) - 1;
    const yy = parseInt(p[2], 10);
    const d = new Date(yy, mm, dd, 12, 0, 0);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  return null;
}

function inIsoRange_(dateStr, fromIso, toIso) {
  const d = parseFlexibleDate_(dateStr);
  if (!d) return false;
  const f = new Date(`${String(fromIso).trim()}T00:00:00`);
  const t = new Date(`${String(toIso).trim()}T23:59:59`);
  return d >= f && d <= t;
}

function ymdInRange_(ymd, fromIso, toIso) {
  if (!ymd) return false;
  const d = new Date(String(ymd).trim());
  if (Number.isNaN(d.getTime())) return false;
  const f = new Date(`${String(fromIso).trim()}T00:00:00`);
  const t = new Date(`${String(toIso).trim()}T23:59:59`);
  return d >= f && d <= t;
}

function personFromNote_(text, fallback) {
  const tx = String(text || "").toLowerCase();
  if (tx.includes("நந்தகுமார்")) return "Nandha";
  if (tx.includes("ஆபீஸ்") || tx.includes("office")) return "Office";
  if (tx.includes("ரஞ்சித்")) return "Ranjith";
  return fallback || "Ranjith";
}

function money_(n) {
  return `Rs.${parseInt(n, 10) || 0}`;
}

function safeLine_(s, max) {
  const t = String(s ?? "").replace(/\s+/g, " ").trim();
  if (t.length <= max) return t;
  return `${t.slice(0, max - 1)}…`;
}

function collectCashbookRows_(snap, fromIso, toIso) {
  const students = Array.isArray(snap.students) ? snap.students : [];
  const expenses = Array.isArray(snap.expenses) ? snap.expenses : [];
  const rows = [];
  let sumIn = 0;
  let sumOut = 0;

  students
    .filter((s) => s && String(s.status || "").trim() !== "Deleted")
    .forEach((s) => {
      (Array.isArray(s.paymentHistory) ? s.paymentHistory : []).forEach((p) => {
        if (!inIsoRange_(p.date, fromIso, toIso)) return;
        const amt = parseInt(p.amount, 10) || 0;
        if (amt <= 0) return;
        const who = personFromNote_(p.note, "Ranjith");
        sumIn += amt;
        rows.push({
          date: String(p.date || ""),
          kind: "IN",
          account: who,
          cat: `Admission — ${s.service || "-"}`,
          desc: `${s.name || "-"} | ${p.note || "-"}`,
          amt
        });
      });
    });

  expenses.forEach((e) => {
    if (!e || !inIsoRange_(e.date, fromIso, toIso)) return;
    const amt = parseInt(e.amt, 10) || 0;
    if (amt <= 0) return;
    const cat = String(e.cat || "-");
    const isIncome =
      cat.includes("வரவு") || cat.includes("(In)") || cat.includes("Spot Collection");
    const isTransfer = cat.includes("பரிமாற்றம்");
    if (isIncome && !isTransfer) sumIn += amt;
    if (!isIncome && !isTransfer && !cat.includes("Spot Pending")) sumOut += amt;
    rows.push({
      date: String(e.date || ""),
      kind: isIncome ? "IN" : "OUT",
      account: String(e.spender || "-"),
      cat,
      desc: String(e.desc || "-"),
      amt
    });
  });

  rows.sort((a, b) => {
    const da = parseFlexibleDate_(a.date);
    const db = parseFlexibleDate_(b.date);
    return (da && db ? da - db : 0) || String(a.date).localeCompare(String(b.date));
  });

  return { rows, sumIn, sumOut };
}

function pdfToBuffer_(build) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ margin: 42, size: "A4", info: { Producer: "Nanban ERP" } });
    doc.__nbBodyFont = registerNanbanPdfBodyFont_(doc);
    const chunks = [];
    doc.on("data", (c) => chunks.push(c));
    doc.on("error", reject);
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    try {
      build(doc);
    } catch (e) {
      reject(e);
      return;
    }
    doc.end();
  });
}

function drawTableHeader_(doc, y, cols) {
  doc.fontSize(7).fillColor("#0f172a");
  let x = doc.page.margins.left;
  cols.forEach((c) => {
    doc.text(c.label, x, y, { width: c.w, continued: false });
    x += c.w;
  });
  return y + 12;
}

function drawTableRow_(doc, y, cols, vals, maxY) {
  if (y > maxY - 40) {
    doc.addPage();
    y = doc.page.margins.top;
  }
  const bodyFont = doc.__nbBodyFont || "Helvetica";
  doc.font(bodyFont).fontSize(6.5).fillColor("#334155");
  let x = doc.page.margins.left;
  let lineH = 0;
  cols.forEach((c, i) => {
    const t = safeLine_(vals[i] != null ? String(vals[i]) : "", Math.floor(c.w / 3));
    const h = doc.heightOfString(t, { width: c.w - 2 });
    doc.text(t, x + 1, y, { width: c.w - 2 });
    lineH = Math.max(lineH, h);
    x += c.w;
  });
  return y + lineH + 4;
}

async function generateHostedCashbookPdf({ tenantId, fromIso, toIso, loggedBy }) {
  const bid = businessDocIdForTenant_(tenantId);
  const snap = await getBusinessSnapshotDoc(bid);
  const { rows, sumIn, sumOut } = collectCashbookRows_(snap, fromIso, toIso);
  const inner =
    snap.appSettings && snap.appSettings.appSettings && typeof snap.appSettings.appSettings === "object"
      ? snap.appSettings.appSettings
      : {};
  const schoolName = String(inner.schoolName || "Driving School").trim();

  const cols = [
    { label: "#", w: 22 },
    { label: "Date", w: 52 },
    { label: "Type", w: 32 },
    { label: "Account", w: 58 },
    { label: "Category", w: 118 },
    { label: "Description", w: 168 },
    { label: "Amount", w: 58 }
  ];

  return pdfToBuffer_((doc) => {
    const BF = doc.__nbBodyFont || "Helvetica";
    doc.font(BF === "Helvetica" ? "Helvetica-Bold" : BF).fontSize(14).fillColor("#0f172a");
    doc.text(`${schoolName} — Cashbook`, { align: "left" });
    doc.moveDown(0.3);
    doc.font(BF).fontSize(9).fillColor("#64748b");
    doc.text(`Period: ${fromIso} to ${toIso}`, { continued: false });
    if (loggedBy) doc.text(`Generated by: ${loggedBy}`, { continued: false });
    doc.moveDown(0.6);
    doc.font(BF === "Helvetica" ? "Helvetica-Bold" : BF).fontSize(10).fillColor("#16a34a");
    doc.text(`Total IN: ${money_(sumIn)}   Total OUT: ${money_(sumOut)}   Net: ${money_(sumIn - sumOut)}`);
    doc.moveDown(0.8);

    let y = doc.y;
    const maxY = doc.page.height - doc.page.margins.bottom;
    y = drawTableHeader_(doc, y, cols);
    doc.moveTo(doc.page.margins.left, y).lineTo(doc.page.width - doc.page.margins.right, y).stroke("#e2e8f0");
    y += 4;

    rows.forEach((r, i) => {
      y = drawTableRow_(
        doc,
        y,
        cols,
        [i + 1, r.date, r.kind, r.account, r.cat, r.desc, money_(r.amt)],
        maxY
      );
    });
    if (!rows.length) {
      doc.fontSize(9).fillColor("#94a3b8").text("No cashbook lines in this range.", doc.page.margins.left, y);
    }
  });
}

async function generateHostedFullAuditPdf({ tenantId, fromIso, toIso, loggedBy }) {
  const bid = businessDocIdForTenant_(tenantId);
  const snap = await getBusinessSnapshotDoc(bid);
  const students = Array.isArray(snap.students) ? snap.students : [];
  const expenses = Array.isArray(snap.expenses) ? snap.expenses : [];
  const chit = snap.chitData && typeof snap.chitData === "object" ? snap.chitData : { auctions: [] };

  const admissions = [];
  const payRows = [];
  const expRows = [];
  const chitRows = [];
  const testRows = [];
  let totalIn = 0;
  let totalOut = 0;

  const inner =
    snap.appSettings && snap.appSettings.appSettings && typeof snap.appSettings.appSettings === "object"
      ? snap.appSettings.appSettings
      : {};
  const schoolName = String(inner.schoolName || "Driving School").trim();
  const mk = String(fromIso || "").length >= 7 ? String(fromIso).slice(0, 7) : "";
  const obm = inner.cashOpeningByMonth && typeof inner.cashOpeningByMonth === "object" ? inner.cashOpeningByMonth : {};
  const rowOpen = mk && obm[mk] ? obm[mk] : {};
  const ob = parseInt(rowOpen.ranjith, 10) || 0;
  const on = parseInt(rowOpen.nandha, 10) || 0;
  const oo = parseInt(rowOpen.office, 10) || 0;

  students
    .filter((s) => s && String(s.status || "").trim() !== "Deleted")
    .forEach((s) => {
      const typ = String(s.type || "")
        .trim()
        .toLowerCase();
      const isEnquiry = typ === "enquiry" || typ === "enquiries";
      if (!isEnquiry && inIsoRange_(s.dateJoined, fromIso, toIso)) {
        const adv = parseInt(s.advance, 10) || 0;
        const tot = parseInt(s.totalFee, 10) || 0;
        const bal = Math.max(0, tot - adv);
        admissions.push({
          date: s.dateJoined,
          name: s.name || "-",
          phone: s.phone || "-",
          service: s.service || "-",
          receiver: s.receiver || "-",
          total: tot,
          adv,
          bal
        });
      }
      (Array.isArray(s.paymentHistory) ? s.paymentHistory : []).forEach((p) => {
        if (!inIsoRange_(p.date, fromIso, toIso)) return;
        const amt = parseInt(p.amount, 10) || 0;
        if (amt <= 0) return;
        const who = personFromNote_(p.note, "Ranjith");
        totalIn += amt;
        payRows.push({
          date: p.date,
          who,
          name: s.name || "-",
          service: s.service || "-",
          note: p.note || "-",
          amt
        });
      });
      if (s.testDate && ymdInRange_(s.testDate, fromIso, toIso)) {
        const st = String(s.testStatus || "").trim();
        if (st) {
          const td = parseFlexibleDate_(String(s.testDate).trim());
          const dateStr =
            td != null
              ? `${String(td.getDate()).padStart(2, "0")}/${String(td.getMonth() + 1).padStart(2, "0")}/${td.getFullYear()}`
              : String(s.testDate);
          testRows.push({
            date: dateStr,
            name: s.name || "-",
            phone: s.phone || "-",
            service: s.service || "-",
            status: st
          });
        }
      }
    });

  expenses.forEach((e) => {
    if (!e || !inIsoRange_(e.date, fromIso, toIso)) return;
    const amt = parseInt(e.amt, 10) || 0;
    if (amt <= 0) return;
    const cat = String(e.cat || "-");
    const isIncome =
      cat.includes("வரவு") || cat.includes("(In)") || cat.includes("Spot Collection");
    const isTransfer = cat.includes("பரிமாற்றம்");
    if (isIncome && !isTransfer) totalIn += amt;
    if (!isIncome && !isTransfer && !cat.includes("Spot Pending")) totalOut += amt;
    expRows.push({
      date: e.date,
      who: e.spender || "-",
      cat,
      desc: e.desc || "-",
      amt,
      kind: isIncome ? "IN" : "OUT",
      isTransfer
    });
  });

  (Array.isArray(chit.auctions) ? chit.auctions : []).forEach((a) => {
    if (!a || !inIsoRange_(a.date, fromIso, toIso)) return;
    const comm = parseInt(a.commission, 10) || 0;
    const ex = parseInt(a.expenses, 10) || 0;
    const net = parseInt(a.netProfit, 10) || comm - ex;
    chitRows.push({
      date: a.date || "-",
      group: a.group || "-",
      month: a.month || "-",
      winner: a.winner || "-",
      perHead: parseInt(a.perHead, 10) || 0,
      comm,
      exp: ex,
      net
    });
  });

  const net = totalIn - totalOut;

  return pdfToBuffer_((doc) => {
    const BF = doc.__nbBodyFont || "Helvetica";
    doc.font(BF === "Helvetica" ? "Helvetica-Bold" : BF).fontSize(14).fillColor("#0f172a");
    doc.text(`${schoolName} — Full audit (A–Z)`, { align: "left" });
    doc.font(BF).fontSize(9).fillColor("#64748b");
    doc.moveDown(0.3);
    doc.text(`Period: ${fromIso} to ${toIso}`);
    if (loggedBy) doc.text(`Generated by: ${loggedBy}`);
    doc.moveDown(0.5);
    doc.font(BF === "Helvetica" ? "Helvetica-Bold" : BF).fontSize(10);
    doc.fillColor("#16a34a").text(`TOTAL IN: ${money_(totalIn)}`, { continued: true });
    doc.fillColor("#b91c1c").text(`   TOTAL OUT: ${money_(totalOut)}`, { continued: true });
    doc.fillColor("#1d4ed8").text(`   NET: ${money_(net)}`);
    doc.moveDown(0.3);
    doc.font(BF).fontSize(9).fillColor("#334155");
    doc.text(`Opening (month ${mk || "-"}): Ranjith ${money_(ob)} | Nandha ${money_(on)} | Office ${money_(oo)}`);
    doc.moveDown(0.8);

    const maxY = doc.page.height - doc.page.margins.bottom;
    let firstSection = true;
    const section = (title, headers, dataRows, rowFn) => {
      if (!firstSection) doc.addPage();
      firstSection = false;
      doc.font("Helvetica-Bold").fontSize(11).fillColor("#0f172a").text(title);
      doc.moveDown(0.4);
      let y = doc.y;
      y = drawTableHeader_(doc, y, headers);
      doc.moveTo(doc.page.margins.left, y).lineTo(doc.page.width - doc.page.margins.right, y).stroke("#e2e8f0");
      y += 6;
      dataRows.forEach((r, i) => {
        y = drawTableRow_(doc, y, headers, rowFn(r, i), maxY);
      });
      if (!dataRows.length) {
        doc.fontSize(8).fillColor("#94a3b8").text("No rows.", doc.page.margins.left, y);
      }
    };

    section(
      "1) Admissions",
      [
        { label: "#", w: 22 },
        { label: "Date", w: 52 },
        { label: "Name", w: 100 },
        { label: "Phone", w: 72 },
        { label: "Service", w: 80 },
        { label: "Recv", w: 58 },
        { label: "Total", w: 48 },
        { label: "Adv", w: 48 },
        { label: "Bal", w: 48 }
      ],
      admissions,
      (r, i) => [i + 1, r.date, r.name, r.phone, r.service, r.receiver, money_(r.total), money_(r.adv), money_(r.bal)]
    );

    section(
      "2) Collections (payments)",
      [
        { label: "#", w: 22 },
        { label: "Date", w: 52 },
        { label: "Acct", w: 52 },
        { label: "Student", w: 110 },
        { label: "Service", w: 72 },
        { label: "Note", w: 150 },
        { label: "Amt", w: 52 }
      ],
      payRows,
      (r, i) => [i + 1, r.date, r.who, r.name, r.service, r.note, money_(r.amt)]
    );

    section(
      "3) Income / expense lines",
      [
        { label: "#", w: 22 },
        { label: "Date", w: 52 },
        { label: "Type", w: 38 },
        { label: "Who", w: 58 },
        { label: "Category", w: 120 },
        { label: "Desc", w: 150 },
        { label: "Amt", w: 52 }
      ],
      expRows,
      (r, i) => [
        i + 1,
        r.date,
        r.isTransfer ? "XFER" : r.kind,
        r.who,
        r.cat,
        r.desc,
        money_(r.amt)
      ]
    );

    section(
      "4) Chit auctions",
      [
        { label: "#", w: 22 },
        { label: "Date", w: 52 },
        { label: "Group", w: 70 },
        { label: "M", w: 28 },
        { label: "Winner", w: 90 },
        { label: "PH", w: 42 },
        { label: "Comm", w: 48 },
        { label: "Exp", w: 42 },
        { label: "Net", w: 48 }
      ],
      chitRows,
      (r, i) => [
        i + 1,
        r.date,
        r.group,
        String(r.month),
        r.winner,
        money_(r.perHead),
        money_(r.comm),
        money_(r.exp),
        money_(r.net)
      ]
    );

    section(
      "5) Test results",
      [
        { label: "#", w: 22 },
        { label: "Date", w: 52 },
        { label: "Student", w: 120 },
        { label: "Phone", w: 72 },
        { label: "Service", w: 80 },
        { label: "Result", w: 180 }
      ],
      testRows,
      (r, i) => [i + 1, r.date, r.name, r.phone, r.service, r.status]
    );
  });
}

async function generateHostedFilingIndexPdf({ monthKey, tenantId }) {
  const mk = String(monthKey || "").trim();
  const listed = await listNanbanFilingEntries(tenantId, mk);
  const items = Array.isArray(listed.items) ? listed.items : [];

  return pdfToBuffer_((doc) => {
    const BF = doc.__nbBodyFont || "Helvetica";
    doc
      .font(BF === "Helvetica" ? "Helvetica-Bold" : BF)
      .fontSize(13)
      .fillColor("#0f172a")
      .text("Nanban filing index", { align: "left" });
    doc.moveDown(0.35);
    doc.font(BF).fontSize(9).fillColor("#64748b").text(`Month: ${mk || "(n/a)"}  •  Rows: ${items.length}`, {
      align: "left"
    });
    doc.moveDown(0.55);

    const cols = [
      { label: "#", w: 28 },
      { label: "Date", w: 52 },
      { label: "Time", w: 50 },
      { label: "Report", w: 128 },
      { label: "URL (trunc.)", w: 214 }
    ];
    let y = doc.y;
    const maxY = doc.page.height - doc.page.margins.bottom;
    y = drawTableHeader_(doc, y, cols);
    doc.moveTo(doc.page.margins.left, y).lineTo(doc.page.width - doc.page.margins.right, y).stroke("#e2e8f0");
    y += 4;

    items.forEach((it, i) => {
      y = drawTableRow_(
        doc,
        y,
        cols,
        [i + 1, it.date || "-", it.time || "-", it.type || "-", safeLine_(it.url || "", 90)],
        maxY
      );
    });
    if (!items.length) {
      doc
        .font(BF)
        .fontSize(9)
        .fillColor("#94a3b8")
        .text(
          "No filing rows for this month yet. Cashbook / Full audit / Monthly pack PDFs append signed URLs here after each download.",
          doc.page.margins.left,
          y,
          { width: doc.page.width - doc.page.margins.left - doc.page.margins.right }
        );
    }
  });
}

module.exports = {
  generateHostedCashbookPdf,
  generateHostedFullAuditPdf,
  generateHostedFilingIndexPdf
};
