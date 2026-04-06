const { info, warn } = require("../lib/logger");
const {
  getBusinessSnapshotDoc,
  setBusinessSnapshotDoc,
  getQuizBankRows,
  getRuntimeDoc
} = require("../services/snapshotStore");
const { enqueueWaOutboundSend } = require("../services/waOutboundQueue");
const { notifyAdminsText } = require("../services/adminNotify");
const {
  getISTDateString,
  getTomorrowYYYYMMDD,
  getQuizDayByJoinDate,
  parseStudentDateDDMMYYYY,
  normalizeDateValue,
  getISTTimeHHMM
} = require("../lib/istTime");

const TENANT_DEFAULT = "nanban_main";
const Q = { cat: 0, day: 1, ques: 2, o1: 3, o2: 4, o3: 5, ansText: 6, img: 7 };

function normPhone10(p) {
  let d = String(p || "").replace(/\D/g, "");
  if (d.length > 10) d = d.slice(-10);
  return d;
}

function waE164(phone) {
  const d = normPhone10(phone);
  return d.length === 10 ? `91${d}` : "";
}

function nanbanCfg(snap) {
  const a = snap?.appSettings;
  return a && typeof a === "object" ? a : {};
}

async function loadEsevaiSnapshot() {
  const raw = await getBusinessSnapshotDoc("ESevai");
  const data = raw && typeof raw === "object" ? { ...raw } : {};
  if (!data.balances) {
    data.balances = { Cash: 0, SBI: 0, "Federal 1": 0, "Federal 2": 0, Paytm: 0 };
  }
  for (const k of ["services", "customers", "agents", "ledgerEntries", "enquiries", "works", "transactions", "reminders"]) {
    if (!Array.isArray(data[k])) data[k] = [];
  }
  if (!data.settings) data.settings = {};
  return data;
}

async function persistEsevai(data) {
  await setBusinessSnapshotDoc("ESevai", data, true);
}

/**
 * Nanban: morning quiz, alumni, RTO tomorrow (≈7:00 IST).
 */
async function runNanbanDailyMorning(opts = {}) {
  const tenantId = String(opts.tenantId || TENANT_DEFAULT).trim() || TENANT_DEFAULT;
  const quizData = await getQuizBankRows();
  const snap = await getBusinessSnapshotDoc("Nanban");
  let students = Array.isArray(snap.students) ? snap.students.map((x) => ({ ...x })) : [];
  const cfg = nanbanCfg(snap);
  const tomorrowStr = getTomorrowYYYYMMDD();
  const today = new Date();
  const istToday = new Date(today.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
  istToday.setHours(0, 0, 0, 0);
  let delaySlot = 0;
  const bumpDelay = (n = 2) => {
    delaySlot += n;
    return delaySlot;
  };

  for (let i = 0; i < students.length; i++) {
    const s = students[i];
    try {
      if (s.status === "Deleted") continue;

      if (s.status !== "License_Completed" && quizData.length > 0) {
        const srv = String(s.service || "").toLowerCase();
        const quizIdx = getQuizDayByJoinDate(s);
        const targetCats = [];
        if (srv.includes("2 வீலர்") || srv.includes("2w")) targetCats.push("2W");
        else if (srv.includes("4 வீலர்") || srv.includes("4w")) targetCats.push("4W");
        else if (srv.includes("combo") || srv.includes("duo")) targetCats.push(quizIdx % 2 === 0 ? "4W" : "2W");
        if (targetCats.length === 0) targetCats.push("General");

        const todayQuestions = [];
        for (let ri = 0; ri < quizData.length; ri++) {
          const row = quizData[ri];
          if (!Array.isArray(row) || row.length < 6) continue;
          const rowCat = String(row[Q.cat] || "").trim();
          const rowDay = parseInt(row[Q.day], 10);
          if (rowDay === quizIdx && (targetCats.includes(rowCat) || rowCat === "General")) {
            todayQuestions.push({ row, index: ri + 1 });
          }
        }

        todayQuestions.sort((a, b) => {
          const catA = String(a.row[Q.cat] || "").trim();
          const catB = String(b.row[Q.cat] || "").trim();
          if (targetCats.includes(catA) && !targetCats.includes(catB)) return -1;
          if (!targetCats.includes(catA) && targetCats.includes(catB)) return 1;
          return 0;
        });

        const wa = waE164(s.phone);
        for (let q = 0; q < todayQuestions.length && q < 3; q++) {
          const qRow = todayQuestions[q].row;
          const titlePrefix =
            String(qRow[Q.cat] || "").trim() === "General" ? "🚦 பொது விழிப்புணர்வு" : "🎯 இன்றைய விசேஷ வினா";
          const optA = String(qRow[Q.o1] || "");
          const optB = String(qRow[Q.o2] || "");
          const optC = String(qRow[Q.o3] || "");
          const qText = String(qRow[Q.ques] || "");
          const msg =
            `${titlePrefix} - (நாள் ${quizIdx}) 🚗🚦\n\n${qText}\n\n1️⃣ ${optA}\n2️⃣ ${optB}\n3️⃣ ${optC}\n\n_சரியான விடையைத் தேர்ந்தெடுக்கவும்_ 👇`;
          if (wa) {
            await enqueueWaOutboundSend(
              {
                tenantId,
                to: wa,
                message: msg,
                messageType: "text",
                metadata: { kind: "daily_quiz", student_id: String(s.id), quiz_day: quizIdx }
              },
              { delaySeconds: bumpDelay(3) }
            );
          }
        }

        if (quizIdx > 1 && wa) {
          const prevIdx = quizIdx - 1;
          for (const row of quizData) {
            if (!Array.isArray(row)) continue;
            const cat = String(row[Q.cat] || "").trim();
            if (!(targetCats.includes(cat) || cat === "General")) continue;
            if (parseInt(row[Q.day], 10) !== prevIdx) continue;
            const correctAns = String(row[Q.ansText] || "").trim();
            const exp = row[8] ? String(row[8]) : "பாதுகாப்பாக ஓட்டுங்கள்!";
            const revealMsg = `✅ *நேற்றைய விடை (${cat}):*\n\n❓ ${row[Q.ques]}\n✔️ *விடை: ${correctAns}*\n💡 *விளக்கம்:* ${exp}`;
            await enqueueWaOutboundSend(
              {
                tenantId,
                to: wa,
                message: revealMsg,
                messageType: "text",
                metadata: { kind: "quiz_reveal", student_id: String(s.id) }
              },
              { delaySeconds: bumpDelay(1) }
            );
          }
        }

        s.quizDay = quizIdx + 1;
        students[i] = s;
      }

      if (s.status === "License_Completed" && s.dateJoined) {
        const joinDate = parseStudentDateDDMMYYYY(s.dateJoined);
        if (joinDate) {
          joinDate.setHours(0, 0, 0, 0);
          const diffDays = Math.round((istToday - joinDate) / (1000 * 60 * 60 * 24));
          if (diffDays === 365 || diffDays === 730 || diffDays === 1095) {
            const wa = waE164(s.phone);
            if (wa) {
              try {
                await enqueueWaOutboundSend(
                  {
                    tenantId,
                    to: wa,
                    message: "",
                    messageType: "template",
                    template: {
                      name: "alumni_anniversary",
                      languageCode: "ta",
                      bodyParams: [String(s.name || "நண்பரே")]
                    },
                    metadata: { kind: "alumni_anniversary", student_id: String(s.id) }
                  },
                  { delaySeconds: bumpDelay(2) }
                );
              } catch (e) {
                await enqueueWaOutboundSend(
                  {
                    tenantId,
                    to: wa,
                    message: `🎓 வாழ்த்துக்கள் ${s.name}! நண்பன் டிரைவிங் ஸ்கூலுடன் ஒரு வருடம் (மேலும்) நிறைவு.`,
                    messageType: "text",
                    metadata: { kind: "alumni_anniversary_fb", student_id: String(s.id) }
                  },
                  { delaySeconds: bumpDelay(1) }
                );
              }
            }
          }
        }
      }

      if (s.testStatus === "Pending" && s.testDate === tomorrowStr) {
        const wa = waE164(s.phone);
        const tpl = cfg.rtoTomorrowTemplate || "rto_test_tomorrow";
        if (wa) {
          try {
            await enqueueWaOutboundSend(
              {
                tenantId,
                to: wa,
                message: "",
                messageType: "template",
                template: { name: tpl, languageCode: "ta", bodyParams: [String(s.name || "-")] },
                metadata: { kind: "rto_tomorrow_morning", student_id: String(s.id) }
              },
              { delaySeconds: bumpDelay(2) }
            );
          } catch (e) {
            await enqueueWaOutboundSend(
              {
                tenantId,
                to: wa,
                message: `🌅 வணக்கம் ${s.name}! நாளை உங்களுக்கு RTO டெஸ்ட். காலை 8:00 முன் வரவும்.`,
                messageType: "text",
                metadata: { kind: "rto_tomorrow_morning_fb", student_id: String(s.id) }
              },
              { delaySeconds: bumpDelay(1) }
            );
          }
        }
      }
    } catch (e) {
      warn("NANBAN_MORNING_STUDENT_LOOP", { reason: String(e), studentId: s?.id });
    }
  }

  await setBusinessSnapshotDoc("Nanban", { students }, true);
  info("NANBAN_DAILY_MORNING_DONE", { tenantId, students: students.length });
  return { status: "success" };
}

/**
 * Admin summary + evening RTO test reminders (≈19:00 IST).
 */
async function runNanbanDailyEvening(opts = {}) {
  const tenantId = String(opts.tenantId || TENANT_DEFAULT).trim() || TENANT_DEFAULT;
  const today = getISTDateString();
  const snap = await getBusinessSnapshotDoc("Nanban");
  const students = Array.isArray(snap.students) ? snap.students : [];
  const expenses = Array.isArray(snap.expenses) ? snap.expenses : [];
  const settings = nanbanCfg(snap);
  const splits = settings.serviceSplits || {};

  let colToday = 0;
  let expToday = 0;
  let classesToday = 0;
  let countToday = 0;
  let count2W = 0;
  let count4W = 0;
  let countCombo = 0;
  let countTest = 0;
  let countTrainOnly = 0;
  let colTest = 0;
  let expTest = 0;
  let colRanjith = 0;
  let colNandha = 0;
  let totalPendingBalance = 0;

  for (const s of students) {
    if (s.status !== "License_Completed" && s.status !== "Hold") {
      if (s.status === "Deleted" || s.status === "Inactive" || s.type === "Enquiry" || String(s.id || "").includes("OLD")) {
        /* skip */
      } else {
        let adv = parseInt(s.advance, 10) || 0;
        let disc = parseInt(s.discount, 10) || 0;
        let tFee = parseInt(s.totalFee, 10) || 0;
        if (tFee === 0) {
          const sSplit = s.feeSplit || splits[s.service] || { llr: 0, train: 0, test: 0 };
          tFee = (parseInt(sSplit.llr, 10) || 0) + (parseInt(sSplit.train, 10) || 0) + (parseInt(sSplit.test, 10) || 0);
        }
        const bal = tFee - adv - disc;
        if (bal > 0 && bal < 1000000) totalPendingBalance += bal;
      }
    }

    const pays = Array.isArray(s.paymentHistory) ? s.paymentHistory : [];
    for (const p of pays) {
      if (p && p.date === today) {
        const amt = parseInt(p.amount, 10) || 0;
        colToday += amt;
        const note = String(p.note || "").toLowerCase();
        if (note.includes("ரஞ்சித்")) colRanjith += amt;
        else if (note.includes("நந்தகுமார்")) colNandha += amt;
        if ((s.service || "").toLowerCase().includes("test") || note.includes("test") || note.includes("டெஸ்ட்")) {
          colTest += amt;
        }
      }
    }

    const att = Array.isArray(s.attendanceHistory) ? s.attendanceHistory : [];
    for (const a of att) {
      if (typeof a === "string" && a.includes(today) && a.includes("✅")) classesToday++;
    }

    if (s.dateJoined === today) {
      countToday++;
      const srv = (s.service || "").toLowerCase();
      if (srv.includes("combo")) countCombo++;
      else if (srv.includes("2")) count2W++;
      else if (srv.includes("4")) count4W++;
      else if (s.type === "Test_Admission" || srv.includes("test")) countTest++;
      else if (s.type === "Training_Admission") countTrainOnly++;
    }
  }

  for (const e of expenses) {
    const isInc = String(e.cat || "").includes("வரவு") || String(e.cat || "").includes("(In)");
    if (!isInc && e.date === today && !String(e.cat || "").includes("Spot Pending")) {
      const amt = parseInt(e.amt, 10) || 0;
      expToday += amt;
      const desc = String(e.desc || "").toLowerCase();
      const cat = String(e.cat || "").toLowerCase();
      if (desc.includes("test") || desc.includes("டெஸ்ட்") || cat.includes("test") || cat.includes("டெஸ்ட்")) {
        expTest += amt;
      }
    }
  }

  const kmMeta = await getRuntimeDoc("Nanban", "nanban_km_today");
  let kmVal = "0";
  if (kmMeta && kmMeta.date_ist === today) kmVal = String(kmMeta.value || "0");

  const summaryMsg =
    `📊 *இன்றைய அறிக்கை (${today})*\n\n` +
    `📝 பதிவுகள்: ${countToday} (2W:${count2W}, 4W:${count4W}, Tst:${countTest}, Cmb:${countCombo})\n` +
    `🚗 பயிற்சி: ${classesToday} பேர் | ${kmVal} KM\n` +
    `💰 வசூல்: ₹${colToday} (R:${colRanjith}, N:${colNandha})\n` +
    `🔴 செலவு: ₹${expToday}\n` +
    `💵 கை இருப்பு: *₹${colToday - expToday}*\n` +
    `🎯 டெஸ்ட்: In:₹${colTest} | Out:₹${expTest}\n` +
    `📉 மொத்த நிலுவை: ₹${totalPendingBalance}\n\n` +
    `- நண்பன் ERP ஆட்டோமேஷன் 🤖`;

  await notifyAdminsText(tenantId, summaryMsg);

  const tomorrowStr = getTomorrowYYYYMMDD();
  let eveningDelay = 0;
  const testStudents = students.filter(
    (s) => s.status !== "Deleted" && s.testStatus === "Pending" && s.testDate === tomorrowStr && s.phone
  );
  for (let idx = 0; idx < testStudents.length; idx++) {
    const s = testStudents[idx];
    const wa = waE164(s.phone);
    if (!wa) continue;
    const testMsg =
      `🌙 *வணக்கம் ${s.name}!*\n\nநாளை உங்களுக்கு *RTO டெஸ்ட்* இருக்கிறது! 🚗\n\n⏰ *காலை 8:00 மணிக்கு* முன்பாக வந்துவிடுங்கள்.\n\n📋 *கார்டு, போட்டோ, LLR* கொண்டு வர மறக்காதீங்க! 💪`;
    eveningDelay += 2;
    await enqueueWaOutboundSend(
      {
        tenantId,
        to: wa,
        message: testMsg,
        messageType: "text",
        metadata: { kind: "rto_tomorrow_evening", student_id: String(s.id) }
      },
      { delaySeconds: eveningDelay }
    );
  }

  info("NANBAN_DAILY_EVENING_DONE", { tenantId });
  return { status: "success" };
}

function normalizeScheduleRows(chit) {
  const s = chit?.schedule;
  if (!Array.isArray(s)) return [];
  return s.map((r) => {
    if (Array.isArray(r)) {
      return { groupName: r[0], displayDate: r[1], rawDate: r[2] };
    }
    return {
      groupName: r.groupName || r.group || r.name,
      displayDate: r.displayDate || r.label,
      rawDate: r.rawDate || r.date || r.ymd
    };
  });
}

/**
 * Chit auction reminders (5 days and 1 day before) — ≈9:00 IST.
 */
async function runChitAutoReminder(opts = {}) {
  const tenantId = String(opts.tenantId || TENANT_DEFAULT).trim() || TENANT_DEFAULT;
  const snap = await getBusinessSnapshotDoc("Nanban");
  const chit =
    snap.chitData && typeof snap.chitData === "object"
      ? snap.chitData
      : { groups: [], members: [], auctions: [], payments: [], bids: [], schedule: [] };
  const schedule = normalizeScheduleRows(chit);
  const db = chit;
  const today = new Date();
  const ist = new Date(today.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
  ist.setHours(0, 0, 0, 0);
  let delay = 0;

  for (const row of schedule) {
    const groupName = row.groupName;
    const displayDate = row.displayDate;
    const rawDate = row.rawDate;
    if (!groupName || !rawDate) continue;
    const auctionDate = new Date(String(rawDate).trim() + "T00:00:00");
    if (Number.isNaN(auctionDate.getTime())) continue;
    const diffDays = Math.round((auctionDate - ist) / (1000 * 60 * 60 * 24));
    if (diffDays !== 5 && diffDays !== 1) continue;

    const members = (db.members || []).filter((m) => m.group === groupName);
    const pastAuctions = (db.auctions || []).filter((a) => a.group === groupName);
    const nextAucNo = pastAuctions.length + 1;
    const label = diffDays === 1 ? "நாளை" : "5 நாட்களில்";

    for (let idx = 0; idx < members.length; idx++) {
      const m = members[idx];
      if (!m.phone) continue;
      delay += 3;
      const wa = waE164(m.phone);
      if (!wa) continue;
      const msg = `வணக்கம் ${m.name}!\n\n*${groupName}* - ${nextAucNo}வது ஏலம் ${label} (${displayDate}) நடைபெறுகிறது.\n\n- நண்பன் சீட்டு`;
      await enqueueWaOutboundSend(
        {
          tenantId,
          to: wa,
          message: msg,
          messageType: "text",
          metadata: { kind: "chit_auction_reminder", group: String(groupName) }
        },
        { delaySeconds: delay }
      );
    }
  }

  info("CHIT_AUTO_REMINDER_DONE", { tenantId });
}

function parseLlrDate(str) {
  const raw = String(str || "").trim();
  if (!raw) return null;
  const iso = normalizeDateValue(raw);
  if (/^\d{4}-\d{2}-\d{2}$/.test(iso)) {
    const [y, mo, d] = iso.split("-").map((x) => parseInt(x, 10));
    return new Date(y, mo - 1, d);
  }
  return parseStudentDateDDMMYYYY(raw);
}

/**
 * E-Sevai: appointment reminders when appointment time passed (hourly).
 */
async function runESevaiAppointmentReminderCron(opts = {}) {
  const tenantId = String(opts.tenantId || TENANT_DEFAULT).trim() || TENANT_DEFAULT;
  const data = await loadEsevaiSnapshot();
  const works = data.works || [];
  const customers = data.customers || [];
  const todayYmd = normalizeDateValue(getISTDateString());
  const nowTime = getISTTimeHHMM();
  let sent = 0;

  for (let wi = 0; wi < works.length; wi++) {
    const w = works[wi];
    const status = String(w.status || w.Status || "pending").toLowerCase();
    if (status === "finished") continue;
    const apptYmd = normalizeDateValue(w.appointment_date || w.Appointment_Date || "");
    const apptTime = String(w.appointment_time || w.Appointment_Time || "").trim();
    if (!apptYmd || !apptTime) continue;
    if (apptYmd !== todayYmd) continue;
    const already = String(w.appointment_reminder_sent_at || w.Appointment_Reminder_Sent_At || "");
    if (already && normalizeDateValue(already) === todayYmd) continue;
    if (nowTime < apptTime) continue;

    const c = customers.find((x) => String(x.id || x.ID) === String(w.customer_id || w.Customer_ID));
    if (!c || !(c.phone || c.Phone)) continue;
    const wa = waE164(c.phone || c.Phone);
    if (!wa) continue;

    const msg =
      `📣 *நினைவூட்டல் - ${w.service_name || w.Service_Name || "E-Sevai Work"}*\n\n` +
      `👤 ${c.name || c.Name || "Customer"}\n` +
      `🗓️ தேதி: ${w.appointment_date || "-"}\n` +
      `⏰ நேரம்: ${apptTime || "-"}\n` +
      (w.pending_reason || w.Pending_Reason ? `⏳ நிலை: ${w.pending_reason || w.Pending_Reason}\n` : "") +
      `\nதயவுசெய்து நேரத்திற்கு வரவும். நன்றி 🙏`;

    await enqueueWaOutboundSend(
      { tenantId, to: wa, message: msg, messageType: "text", metadata: { kind: "esevai_appt", work_id: String(w.id) } },
      { delaySeconds: sent * 2 }
    );
    works[wi] = {
      ...w,
      appointment_reminder_sent_at: new Date().toISOString()
    };
    sent++;
  }

  if (sent > 0) await persistEsevai({ ...data, works });
  info("ESEVAI_APPT_REMINDER", { tenantId, sent });
  return { status: "success", sent };
}

/**
 * E-Sevai: delivery due reminders (every 2h in GAS → we run every 2h).
 */
async function runESevaiDeliveryReminderCron(opts = {}) {
  const tenantId = String(opts.tenantId || TENANT_DEFAULT).trim() || TENANT_DEFAULT;
  const data = await loadEsevaiSnapshot();
  const works = Array.isArray(data.works) ? data.works : [];
  const customers = Array.isArray(data.customers) ? data.customers : [];
  const today = normalizeDateValue(getISTDateString());
  let sent = 0;
  const nextWorks = works.map((w) => ({ ...w }));

  for (let wi = 0; wi < nextWorks.length; wi++) {
    const w = nextWorks[wi];
    const status = String(w.status || "").toLowerCase();
    if (status === "finished" || String(w.delivery_status || "").toLowerCase() === "delivered") continue;
    const td = normalizeDateValue(w.target_date || "");
    if (!td) continue;
    if (td > today) continue;
    if (w.delivery_notified_at && String(w.delivery_notified_at) === getISTDateString()) continue;
    const c = customers.find((x) => String(x.id) === String(w.customer_id));
    if (!c || !c.phone) continue;
    const wa = waE164(c.phone);
    if (!wa) continue;
    const note = `⏰ உங்கள் சேவை டெலிவரி தேதி நெருங்கியுள்ளது.\nService: ${w.service_name || "-"}\nTarget: ${w.target_date || "-"}\nNanban Ranjith E-Sevai Maiyam.`;
    await enqueueWaOutboundSend(
      { tenantId, to: wa, message: note, messageType: "text", metadata: { kind: "esevai_delivery", work_id: String(w.id) } },
      { delaySeconds: sent * 2 }
    );
    nextWorks[wi] = { ...w, delivery_notified_at: getISTDateString() };
    sent++;
  }

  if (sent > 0) await persistEsevai({ ...data, works: nextWorks });
  info("ESEVAI_DELIVERY_REMINDER", { tenantId, sent });
  return { status: "success", sent };
}

/**
 * E-Sevai: agent LLR 30-day follow-up (every 6h in GAS).
 */
async function runESevaiAgentLlrReminderCron(opts = {}) {
  const tenantId = String(opts.tenantId || TENANT_DEFAULT).trim() || TENANT_DEFAULT;
  const data = await loadEsevaiSnapshot();
  const works = Array.isArray(data.works) ? data.works : [];
  const agents = Array.isArray(data.agents) ? data.agents : [];
  const now = new Date();
  let sent = 0;
  const nextWorks = works.map((w) => ({ ...w }));

  for (let wi = 0; wi < nextWorks.length; wi++) {
    const w = nextWorks[wi];
    const llrDate = String(w.llr_date || "").trim();
    if (!llrDate) continue;
    if (String(w.llr_reminder_sent_at || "").trim()) continue;
    const aid = String(w.agent_id || "").trim();
    if (!aid) continue;
    const agent = agents.find((a) => String(a.id || "") === aid);
    if (!agent || !agent.phone) continue;
    const dt = parseLlrDate(llrDate);
    if (!dt || Number.isNaN(dt.getTime())) continue;
    const diffDays = Math.floor((now.getTime() - dt.getTime()) / 86400000);
    if (diffDays !== 30) continue;
    const wa = waE164(agent.phone);
    if (!wa) continue;
    const msg = `📣 Agent Reminder\n\nYour customer ${w.customer_name || w.customer_id || "-"}'s LLR has completed 30 days.\nLLR Date: ${llrDate}\nService: ${w.service_name || "LLR"}\nதயவு செய்து follow-up call செய்யவும்.\n\nNanban Pro E-Sevai`;
    await enqueueWaOutboundSend(
      { tenantId, to: wa, message: msg, messageType: "text", metadata: { kind: "esevai_agent_llr", work_id: String(w.id) } },
      { delaySeconds: sent * 2 }
    );
    nextWorks[wi] = { ...w, llr_reminder_sent_at: getISTDateString() };
    sent++;
  }

  if (sent > 0) await persistEsevai({ ...data, works: nextWorks });
  info("ESEVAI_AGENT_LLR", { tenantId, sent });
  return { status: "success", sent };
}

module.exports = {
  runNanbanDailyMorning,
  runNanbanDailyEvening,
  runChitAutoReminder,
  runESevaiAppointmentReminderCron,
  runESevaiDeliveryReminderCron,
  runESevaiAgentLlrReminderCron
};
