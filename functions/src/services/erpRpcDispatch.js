const admin = require("firebase-admin");
const { getISTDateString } = require("../lib/istTime");
const {
  getBusinessSnapshotDoc,
  setBusinessSnapshotDoc,
  getRuntimeDoc,
  setRuntimeDoc
} = require("./snapshotStore");
const { notifyAdminsText } = require("./adminNotify");
const { enqueueWaOutboundSend } = require("./waOutboundQueue");
const { inferJobKindFromStudent } = require("./waNativeJobProcessor");

const CAR_SYLLABUS = [
  "வாகனக் கட்டுப்பாடுகள் அறிமுகம் (Clutch, Brake, Accelerator).",
  "1st Gear-ல் வண்டியை நகர்த்துதல் மற்றும் நிறுத்துதல்.",
  "2nd Gear மாற்றம் மற்றும் ஸ்டீயரிங் கண்ட்ரோல்.",
  "3rd & 4th Gear-ல் வேகம் மற்றும் பிரேக்கிங்.",
  "நேர்க்கோட்டில் ரிவர்ஸ் எடுத்தல்.",
  "வளைவுகளில் கிளட்ச் மற்றும் பிரேக் பயன்பாடு.",
  "சிக்னல்கள் மற்றும் சாலை விதிகள்.",
  "மேடான பாதையில் (Uphill) வண்டியை நகர்த்துதல்.",
  "RTO தடம் 'H' (H-Track) பயிற்சி.",
  "வளைவுப் பாதை '8' (8-Track) பயிற்சி.",
  "பார்க்கிங் முறைகள் (Parallel & Perpendicular).",
  "சாலை விதிகள் மற்றும் டிராஃபிக் சைன்கள்.",
  "டிராஃபிக் உள்ள சாலைகளில் ஓட்டுதல்.",
  "நெடுஞ்சாலை (Highway) ஓட்டுதல்.",
  "இறுதி டெஸ்ட் ரிவிஷன் மற்றும் மாடல் டெஸ்ட்."
];

const TENANT_DEFAULT = "nanban_main";

function normPhone10(p) {
  let d = String(p || "").replace(/\D/g, "");
  if (d.length > 10) d = d.slice(-10);
  return d;
}

function waE164_(phone) {
  const d = normPhone10(phone);
  return d.length === 10 ? `91${d}` : "";
}

function nanbanTemplateCfg_(snap) {
  const a = snap?.appSettings;
  return a && typeof a === "object" ? a : {};
}

function popTenantFromArgs(args) {
  if (!Array.isArray(args) || !args.length) return { tenantId: TENANT_DEFAULT, args: [] };
  const last = args[args.length - 1];
  // Only treat trailing string as tenant when it looks like a tenant code (e.g. nanban_main), not a person's name.
  if (
    typeof last === "string" &&
    last.length > 0 &&
    last.length < 48 &&
    !last.includes(" ") &&
    !last.includes("/") &&
    !last.startsWith("{") &&
    last.includes("_")
  ) {
    return { tenantId: last, args: args.slice(0, -1) };
  }
  return { tenantId: TENANT_DEFAULT, args };
}

async function loadEsevaiModel(tenantId) {
  const raw = await getBusinessSnapshotDoc("ESevai");
  const data = raw && typeof raw === "object" ? { ...raw } : {};
  if (!data.balances) {
    data.balances = { Cash: 0, SBI: 0, "Federal 1": 0, "Federal 2": 0, Paytm: 0 };
  }
  for (const k of ["services", "customers", "agents", "ledgerEntries", "enquiries", "works", "transactions", "reminders"]) {
    if (!Array.isArray(data[k])) data[k] = [];
  }
  if (!data.settings) data.settings = {};
  data.tenant_id = tenantId;
  data.totalPending = (data.customers || []).reduce((s, c) => s + (parseFloat(c.balance) || 0), 0);
  data.subscription = data.subscription || {};
  return data;
}

async function persistEsevai(data) {
  await setBusinessSnapshotDoc("ESevai", data, true);
}

async function saveNanbanPartial(patch) {
  const snap = await getBusinessSnapshotDoc("Nanban");
  const next = Object.assign({}, snap, patch, {
    updated_at: admin.firestore.FieldValue.serverTimestamp()
  });
  await setBusinessSnapshotDoc("Nanban", next, true);
}

/**
 * GAS-compatible RPC: returns result object (not wrapped in ok/result).
 */
async function handleErpRpc(action, rawArgs) {
  const act = String(action || "").trim();
  const { tenantId, args } = popTenantFromArgs(Array.isArray(rawArgs) ? rawArgs : []);

  try {
    switch (act) {
      case "bridgePingAction":
        return { status: "success", pong: true, native: true };

      case "setupESevaiDeliveryReminderTrigger":
      case "setupESevaiAgentLlrReminderTrigger":
      case "setupAllDailyTriggers":
      case "setupDailyMorningTrigger":
      case "setupDailyEveningTrigger":
      case "setupChitReminderTrigger":
        return { status: "success", message: "native_scheduler_active", native: true };

      case "getDatabaseData": {
        const snap = await getBusinessSnapshotDoc("Nanban");
        return {
          status: "success",
          students: Array.isArray(snap.students) ? snap.students : [],
          expenses: Array.isArray(snap.expenses) ? snap.expenses : []
        };
      }

      case "getChitData": {
        const snap = await getBusinessSnapshotDoc("Nanban");
        const chit =
          snap.chitData && typeof snap.chitData === "object"
            ? snap.chitData
            : { groups: [], members: [], auctions: [], payments: [], bids: [], schedule: [] };
        return { status: "success", data: chit };
      }

      case "getAppSettings": {
        const snap = await getBusinessSnapshotDoc("Nanban");
        return snap.appSettings || {};
      }

      case "getAppUsers": {
        const db = admin.firestore();
        const qs = await db.collection("users").get();
        const users = [];
        qs.forEach((doc) => {
          const d = doc.data() || {};
          users.push({
            id: doc.id,
            name: String(d.name || "").trim(),
            pin: String(d.pin || "").trim(),
            role: String(d.role || "Staff").trim(),
            phone: String(d.phone || "").trim(),
            businesses: Array.isArray(d.businesses) ? d.businesses : []
          });
        });
        return users;
      }

      case "getESevaiInitialData": {
        return await loadEsevaiModel(tenantId);
      }

      case "saveAppSettings": {
        const key = args[0];
        const val = args[1];
        const snap = await getBusinessSnapshotDoc("Nanban");
        const appSettings = snap.appSettings && typeof snap.appSettings === "object" ? { ...snap.appSettings } : {};
        if (key === "appSettings") Object.assign(appSettings, val || {});
        else if (key === "serviceSplits") appSettings.serviceSplits = val || {};
        else if (key === "vehicleKm") appSettings.vehicleKm = val || {};
        else appSettings[key] = val;
        await saveNanbanPartial({ appSettings });
        return { status: "success" };
      }

      case "saveStudentData": {
        const s = args[0];
        if (!s || !s.id) return { status: "error", message: "Invalid student" };
        s.phone = normPhone10(s.phone);
        const snap = await getBusinessSnapshotDoc("Nanban");
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        students = students.filter((x) => String(x.id) !== String(s.id));
        students.unshift(s);
        await saveNanbanPartial({ students });
        return { status: "success" };
      }

      case "updateStudentData": {
        const s = args[0];
        if (!s || !s.id) return { status: "error", message: "Invalid student" };
        s.phone = normPhone10(s.phone);
        const snap = await getBusinessSnapshotDoc("Nanban");
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        let found = false;
        students = students.map((x) => {
          if (String(x.id) === String(s.id)) {
            found = true;
            return s;
          }
          return x;
        });
        if (!found) return { status: "error", message: "Not found" };
        await saveNanbanPartial({ students });
        return { status: "success" };
      }

      case "saveExpenseData": {
        const e = args[0];
        const snap = await getBusinessSnapshotDoc("Nanban");
        const expenses = Array.isArray(snap.expenses) ? [...snap.expenses] : [];
        expenses.push(e);
        await saveNanbanPartial({ expenses });
        return { status: "success" };
      }

      case "getKmTodayAction": {
        const today = getISTDateString();
        const rt = await getRuntimeDoc("Nanban", "nanban_km_today");
        const km = rt && rt.date_ist === today ? parseInt(rt.value, 10) || 0 : 0;
        return { status: "success", km };
      }

      case "getTrainerKmSessionAction": {
        const meta = await getRuntimeDoc("Nanban", "trainer_km_session");
        return { status: "success", data: meta.session || null };
      }

      case "startTrainerKmSessionAction": {
        const st = args[0];
        const trainerName = args[1] || "Trainer";
        const session = {
          start_km: st,
          trainer: trainerName,
          started_at: new Date().toISOString()
        };
        await setRuntimeDoc("Nanban", "trainer_km_session", { session });
        return { status: "success" };
      }

      case "clearTrainerKmSessionAction": {
        await setRuntimeDoc("Nanban", "trainer_km_session", { session: null });
        return { status: "success" };
      }

      case "saveChitMember": {
        const m = args[0] || {};
        m.phone = normPhone10(m.phone);
        if (String(m.phone || "").length !== 10) {
          return { status: "error", message: "Invalid phone (10-digit required)" };
        }
        const snap = await getBusinessSnapshotDoc("Nanban");
        const chit = snap.chitData && typeof snap.chitData === "object" ? { ...snap.chitData } : { groups: [], members: [], auctions: [], payments: [], bids: [], schedule: [] };
        if (!Array.isArray(chit.members)) chit.members = [];
        const id = Date.now();
        chit.members.push({
          id,
          name: m.name,
          phone: m.phone,
          group: m.group,
          joinedBy: m.joinedBy,
          date: getISTDateString()
        });
        await saveNanbanPartial({ chitData: chit });
        return { status: "success" };
      }

      case "editChitMemberData": {
        const m = args[0] || {};
        m.phone = normPhone10(m.phone);
        if (String(m.phone || "").length !== 10) {
          return { status: "error", message: "Invalid phone (10-digit required)" };
        }
        const snap = await getBusinessSnapshotDoc("Nanban");
        const chit = snap.chitData && typeof snap.chitData === "object" ? { ...snap.chitData } : {};
        if (!Array.isArray(chit.members)) chit.members = [];
        const idx = chit.members.findIndex((x) => String(x.id) === String(m.id));
        if (idx < 0) return { status: "error" };
        chit.members[idx] = Object.assign({}, chit.members[idx], m);
        await saveNanbanPartial({ chitData: chit });
        return { status: "success" };
      }

      case "deleteChitMemberData": {
        const id = args[0];
        const snap = await getBusinessSnapshotDoc("Nanban");
        const chit = snap.chitData && typeof snap.chitData === "object" ? { ...snap.chitData } : {};
        if (!Array.isArray(chit.members)) chit.members = [];
        chit.members = chit.members.filter((x) => String(x.id) !== String(id));
        await saveNanbanPartial({ chitData: chit });
        return { status: "success" };
      }

      case "saveESevaiCustomerAction": {
        const c = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        const id = "ESC" + Date.now();
        data.customers.unshift({
          id,
          name: c.name,
          phone: c.phone,
          balance: Number(c.oldBalance) || 0,
          type: c.type || "Direct",
          created_at: getISTDateString()
        });
        await persistEsevai(data);
        return { status: "success", id, tenant_id: tenantId };
      }

      case "saveESevaiAgentAction": {
        const a = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        const id = a.id || "ESAG" + Date.now();
        const idx = data.agents.findIndex((x) => String(x.id) === String(id));
        const payload = {
          id,
          name: a.name || "",
          phone: a.phone || "",
          area: a.area || "",
          active: a.active !== false,
          created_at: a.created_at || getISTDateString()
        };
        if (idx >= 0) data.agents[idx] = payload;
        else data.agents.unshift(payload);
        await persistEsevai(data);
        return { status: "success", id, tenant_id: tenantId };
      }

      case "closeESevaiDayAction": {
        const actuals = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        const today = getISTDateString();
        Object.keys(actuals || {}).forEach((acc) => {
          const actualAmt = Number(actuals[acc]) || 0;
          const liveAmt = Number(data.balances[acc]) || 0;
          const diff = actualAmt - liveAmt;
          if (diff !== 0) {
            data.ledgerEntries.unshift({
              date: today,
              type: diff > 0 ? "income" : "expense",
              category: "Settlement Adjustment",
              description: `Adjustment (Typo/Manual) for ${acc}`,
              amount: Math.abs(diff),
              account: acc
            });
          }
          data.balances[acc] = actualAmt;
        });
        data.day_closed_at = new Date().toISOString();
        data.day_closed_date = today;
        await persistEsevai(data);
        return { status: "success" };
      }

      case "updateESevaiWorkAction": {
        const id = args[0];
        const update = args[1] || {};
        const data = await loadEsevaiModel(tenantId);
        const idx = data.works.findIndex((w) => String(w.id) === String(id));
        if (idx < 0) return { status: "error", message: "Work ID not found" };
        data.works[idx] = Object.assign({}, data.works[idx], update);
        if (String(data.works[idx].status || "").toLowerCase() === "finished" && !data.works[idx].completed_at) {
          data.works[idx].completed_at = getISTDateString();
        }
        await persistEsevai(data);
        return { status: "success" };
      }

      case "saveESevaiSettingsAction": {
        const obj = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        data.settings = Object.assign({}, data.settings, obj);
        await persistEsevai(data);
        return { status: "success" };
      }

      case "saveESevaiOpeningBalanceAction": {
        const b = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        data.openingBalance = Object.assign({}, data.openingBalance || {}, b, { date: b.date || getISTDateString() });
        await persistEsevai(data);
        return { status: "success" };
      }

      case "saveESevaiServiceAction": {
        const payload = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        const id = payload.id || "ESSV" + Date.now();
        const ix = data.services.findIndex((s) => String(s.id) === String(id));
        const row = Object.assign({}, payload, { id });
        if (ix >= 0) data.services[ix] = row;
        else data.services.unshift(row);
        await persistEsevai(data);
        return { status: "success", id };
      }

      case "saveESevaiLedgerAction": {
        const l = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        data.ledgerEntries.unshift({
          date: l.date || getISTDateString(),
          type: l.type || "expense",
          category: l.category || "",
          description: l.description || "",
          amount: Number(l.amount) || 0,
          account: l.account || "Cash",
          customer_id: l.customer_id || ""
        });
        await persistEsevai(data);
        return { status: "success" };
      }

      case "saveESevaiEnquiryAction": {
        const enquiry = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        const enqId = "ESENQ" + Date.now();
        data.enquiries.unshift(
          Object.assign({}, enquiry, {
            id: enqId,
            created_at: getISTDateString()
          })
        );
        await persistEsevai(data);
        return { status: "success", id: enqId, tenant_id: tenantId };
      }

      case "saveESevaiTransactionAction": {
        const tx = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        const today = getISTDateString();
        const txId = "ESTX" + Date.now();
        const totalAmt = Number(tx.totalAmount) || 0;
        const recvAmt = Number(tx.receivedAmount) || 0;
        const balDiff = recvAmt - totalAmt;
        let customerType = "";

        data.transactions.unshift({
          id: txId,
          customer_id: tx.customerId,
          items: tx.items || [],
          gov_bank: tx.govBank || "SBI",
          payment_mode: tx.paymentMode || "Cash",
          sub_total: Number(tx.subTotal) || Number(tx.totalAmount) || 0,
          discount: Number(tx.discount) || 0,
          round_off: Number(tx.roundOff) || 0,
          total_amount: totalAmt,
          received_amount: recvAmt,
          balance_diff: balDiff,
          llr_date: String(tx.llrDate || "").trim(),
          llr_copy_url: String(tx.llrCopyUrl || "").trim(),
          other_expenses: Number(tx.otherExpenses) || 0,
          status: tx.status || "finished",
          date: today
        });

        for (let i = 0; i < data.customers.length; i++) {
          if (String(data.customers[i].id) === String(tx.customerId)) {
            customerType = String(data.customers[i].type || "");
            data.customers[i].balance = (Number(data.customers[i].balance) || 0) + balDiff;
            const phone = normPhone10(data.customers[i].phone);
            if (phone.length === 10) {
              const wa = `91${phone}`;
              const cname = String(data.customers[i].name || "Customer");
              const msg = `🙏 வணக்கம் ${cname}\n\n✅ E-Sevai Bill பதிவு செய்யப்பட்டது.\n🧾 Bill: ${txId}\n💰 Amount: ₹${totalAmt}\n💳 Received: ₹${recvAmt}\n📉 Balance Diff: ₹${balDiff}\n📅 தேதி: ${today}\n\nNanban Pro - Ranjith E-Sevai Maiyam`;
              try {
                await enqueueWaOutboundSend(
                  {
                    tenantId: TENANT_DEFAULT,
                    to: wa,
                    message: msg,
                    messageType: "text",
                    metadata: { kind: "esevai_bill", tx_id: txId }
                  },
                  { delaySeconds: 0 }
                );
              } catch (e) {}
            }
            break;
          }
        }

        await notifyAdminsText(
          tenantId,
          `🧾 E-Sevai POS Bill\nBill: ${txId}\nAmount: ₹${totalAmt}\nReceived: ₹${recvAmt}\nMode: ${tx.paymentMode || "Cash"}\nDate: ${today}`
        );

        if (tx.paymentMode !== "Pending" && recvAmt > 0) {
          data.balances[tx.paymentMode] = (Number(data.balances[tx.paymentMode]) || 0) + recvAmt;
        }
        const totalGovFee = (tx.items || []).reduce(
          (sum, item) => sum + (Number(item.gov_fee) || 0) * (item.qty || 1),
          0
        );
        if (totalGovFee > 0) {
          data.balances[tx.govBank] = (Number(data.balances[tx.govBank]) || 0) - totalGovFee;
          data.ledgerEntries.unshift({
            date: today,
            type: "expense",
            category: "Gov Fee",
            description: `Gov Fee for Bill #${txId}`,
            amount: totalGovFee,
            account: tx.govBank
          });
        }
        const totalSrvFee = (tx.items || []).reduce(
          (sum, item) => sum + (Number(item.srv_fee) || 0) * (item.qty || 1),
          0
        );
        const netIncome = totalSrvFee - (Number(tx.otherExpenses) || 0);
        if (netIncome !== 0) {
          const acc = tx.paymentMode === "Pending" ? "Cash" : tx.paymentMode;
          data.ledgerEntries.unshift({
            date: today,
            type: netIncome > 0 ? "income" : "expense",
            category: "Service Fee",
            description: `Service Fee for Bill #${txId}`,
            amount: Math.abs(netIncome),
            account: acc
          });
        }

        const items = tx.items || [];
        data.works = data.works.filter((w) => w.transaction_id !== txId);
        items.forEach((item) => {
          const govFee = Number(item.gov_fee) || 0;
          const isAutoFinished = customerType === "Agent" && govFee === 0;
          const workStatus = isAutoFinished ? "finished" : tx.status || "pending";
          data.works.unshift({
            id: "ESWK" + Date.now() + Math.floor(Math.random() * 100),
            transaction_id: txId,
            customer_id: tx.customerId,
            agent_id: tx.agentId || "",
            agent_name: tx.agentName || "",
            service_name: item.name,
            status: workStatus,
            service_type: govFee > 0 ? "regular" : "own",
            stages: item.stages || [],
            document_url: item.document_url || "",
            llr_date: String(item.llr_date || tx.llrDate || "").trim(),
            llr_copy_url: String(item.llr_copy_url || tx.llrCopyUrl || "").trim(),
            llr_reminder_sent_at: "",
            customer_type: customerType || "",
            target_date: item.target_date || "",
            delivery_status: item.delivery_status || "pending",
            delivery_notified_at: "",
            finished_date: workStatus === "finished" ? today : "",
            created_at: today
          });
        });

        await persistEsevai(data);
        return { status: "success", id: txId, tenant_id: tenantId };
      }

      case "processTrainerEntry": {
        const studentId = args[0];
        const type = String(args[1] || "");
        const att = parseInt(args[2], 10) || 0;
        const perf = String(args[3] || "");
        const amt = parseInt(args[4], 10) || 0;
        const trainer = String(args[5] || "Trainer");
        const today = getISTDateString();
        const snap = await getBusinessSnapshotDoc("Nanban");
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        const ix = students.findIndex((x) => String(x.id) === String(studentId));
        if (ix < 0) return { status: "error", message: "Student Not Found" };
        const s = { ...students[ix] };
        let admMsg = `✅ *டிரெய்னர் பதிவு (${trainer}):*\nமாணவர்: ${s.name}\n`;

        if (type === "absent") {
          if (!Array.isArray(s.attendanceHistory)) s.attendanceHistory = [];
          s.attendanceHistory.unshift(`❌ ${today} (Absent)`);
          admMsg += `ஸ்டேட்டஸ்: இன்று வரவில்லை (Absent)\n`;
        } else {
          if (s.status === "Hold" || s.status === "License_Completed") {
            s.status = "Training";
            admMsg += `⚠️ மாணவர் மீண்டும் பயிற்சிக்கு வந்துள்ளார் (Re-Activated)\n`;
          }
          if (att > 0) {
            s.classesAttended = (parseInt(s.classesAttended, 10) || 0) + att;
            if (!Array.isArray(s.attendanceHistory)) s.attendanceHistory = [];
            s.attendanceHistory.unshift(`✅ ${today} (Class ${s.classesAttended} - ${perf})`);
            admMsg += `வகுப்பு: ${att} கிளாஸ்\nசெயல்பாடு: ${perf}\n`;
          }
          if (amt > 0) {
            if (!Array.isArray(s.paymentHistory)) s.paymentHistory = [];
            const dup = s.paymentHistory.find(
              (p) =>
                p &&
                p.date === today &&
                parseInt(p.amount, 10) === amt &&
                String(p.note || "").includes("பயிற்சியாளர்") &&
                String(p.note || "").includes(trainer)
            );
            if (dup) return { status: "error", message: "Duplicate payment detected" };
            s.advance = (parseInt(s.advance, 10) || 0) + amt;
            s.paymentHistory.unshift({ date: today, amount: amt, note: `பயிற்சியாளர் ${trainer} கையில்` });
            admMsg += `பணம் வசூல்: ₹${amt}\n`;
          }
        }

        students[ix] = s;
        await saveNanbanPartial({ students });
        try {
          await notifyAdminsText(tenantId, admMsg);
        } catch (e) {}

        const wa = waE164_(s.phone);
        if (wa) {
          if (type === "absent") {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: `🚫 வணக்கம் ${s.name}, இன்று நீங்கள் பயிற்சிக்கு வரவில்லை என பதிவாகியுள்ளது.`,
                messageType: "text",
                metadata: { kind: "trainer_absent", student_id: String(studentId) }
              },
              { delaySeconds: 0 }
            );
          } else {
            if (att > 0) {
              const nextDay = (parseInt(s.classesAttended, 10) || 0) + 1;
              const syllabusText =
                nextDay <= 15
                  ? CAR_SYLLABUS[nextDay - 1]
                  : "அனைத்து பயிற்சிகளும் முடிந்தது! இனி டெஸ்டுக்குத் தயாராகலாம்.";
              const cfg = nanbanTemplateCfg_(snap);
              const tplName = cfg.dailyClassTemplate || "daily_class_alert";
              const bodyParams = [
                String(s.name || "-"),
                String(att || 1),
                String(perf || "-"),
                String(syllabusText || "-")
              ];
              try {
                await enqueueWaOutboundSend(
                  {
                    tenantId: TENANT_DEFAULT,
                    to: wa,
                    message: "",
                    messageType: "template",
                    template: { name: tplName, languageCode: "ta", bodyParams },
                    metadata: { kind: "daily_class", student_id: String(studentId) }
                  },
                  { delaySeconds: 0 }
                );
              } catch (e) {
                await enqueueWaOutboundSend(
                  {
                    tenantId: TENANT_DEFAULT,
                    to: wa,
                    message: `🚗 வணக்கம் ${s.name}, இன்று உங்கள் ${att} வகுப்பு முடிந்தது. செயல்பாடு: ${perf}\n\n📅 நாளைக்கான பயிற்சி: ${syllabusText}`,
                    messageType: "text",
                    metadata: { kind: "daily_class_fallback", student_id: String(studentId) }
                  },
                  { delaySeconds: 0 }
                );
              }
              if (s.classesAttended === 15 && !s.feedbackSent) {
                const feedbackMsg = `🙏 வணக்கம் ${s.name},\n\nநண்பன் டிரைவிங் ஸ்கூலில் உங்களுடைய 15 நாள் பயிற்சி இன்றுடன் நிறைவடைகிறது. எங்கள் பயிற்சி மற்றும் டிரெய்னரின் அணுகுமுறை உங்களுக்கு எப்படி இருந்தது?\n\nஉங்கள் மதிப்பெண்ணை (1 முதல் 5 வரை) Type செய்து ரிப்ளை செய்யவும். (உதா: 5)`;
                await enqueueWaOutboundSend(
                  {
                    tenantId: TENANT_DEFAULT,
                    to: wa,
                    message: feedbackMsg,
                    messageType: "text",
                    metadata: { kind: "feedback_request", student_id: String(studentId) }
                  },
                  { delaySeconds: 2 }
                );
                s.feedbackSent = true;
                students[ix] = s;
                await saveNanbanPartial({ students });
              }
            }
            if (amt > 0) {
              const bal =
                (parseInt(s.totalFee, 10) || 0) - (parseInt(s.advance, 10) || 0) - (parseInt(s.discount, 10) || 0);
              await enqueueWaOutboundSend(
                {
                  tenantId: TENANT_DEFAULT,
                  to: wa,
                  message: `💰 வணக்கம் ${s.name},\nஉங்களிடம் இருந்து ₹${amt} பெறப்பட்டது.\nபயிற்சியாளர்: ${trainer}\nமீதமுள்ள தொகை: ₹${bal}\n\nநன்றி! - நண்பன் டிரைவிங் ஸ்கூல்`,
                  messageType: "text",
                  metadata: { kind: "trainer_payment", student_id: String(studentId) }
                },
                { delaySeconds: 1 }
              );
              const receiptMsg =
                `💰 *கட்டண ரசீது (Receipt)*\n\n` +
                `மாணவர்: ${s.name}\n` +
                `தொகை: ₹${amt}\n` +
                `பெற்றவர்: ${trainer}\n` +
                `தேதி: ${today}\n` +
                `மீதம்: ₹${bal}\n\n` +
                `நண்பன் டிரைவிங் ஸ்கூல் - விபத்தில்லா தமிழ்நாடு! 🚦`;
              await enqueueWaOutboundSend(
                {
                  tenantId: TENANT_DEFAULT,
                  to: wa,
                  message: receiptMsg,
                  messageType: "text",
                  metadata: { kind: "fee_receipt_trainer", student_id: String(studentId) }
                },
                { delaySeconds: 2 }
              );
            }
          }
        }

        return { status: "success" };
      }

      case "markTestResultActionEx": {
        const studentId = args[0];
        const resultStr = String(args[1] || "");
        const trainerName = String(args[2] || "");
        const nextDate = args[3];
        const today = getISTDateString();
        const snap = await getBusinessSnapshotDoc("Nanban");
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        const ix = students.findIndex((x) => String(x.id) === String(studentId));
        if (ix < 0) return { status: "error" };
        const s = { ...students[ix] };
        s.testStatus = resultStr;
        if (!Array.isArray(s.adminRemarks)) s.adminRemarks = [];

        if (resultStr === "Pass") {
          s.status = "License_Completed";
          s.adminRemarks.unshift({ date: today, text: `🏆 RTO டெஸ்ட் பாஸ்! (Trainer: ${trainerName})` });
          students[ix] = s;
          await saveNanbanPartial({ students });
          const wa = waE164_(s.phone);
          if (wa) {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: `🎉 வெற்றி பெற்றீர்கள் ${s.name}! ஓட்டுநர் தேர்வில் இன்று சிறப்பாக செயல்பட்டதற்கு வாழ்த்துக்கள்! 🏆 உங்கள் லைசென்ஸ் கார்டு விரைவில் கைக்கு வரும்.`,
                messageType: "text",
                metadata: { kind: "rto_pass", student_id: String(studentId) }
              },
              { delaySeconds: 0 }
            );
          }
          await notifyAdminsText(tenantId, `🏆 *TEST PASS:* ${s.name} தேர்ச்சி பெற்றுவிட்டார்!`);
        } else {
          const resText = resultStr === "Fail" ? "ஃபெயில்" : "வரவில்லை (Absent)";
          const dtTxt = nextDate ? `அடுத்த டெஸ்ட்: ${nextDate}` : `தேதி முடிவாகவில்லை`;
          s.adminRemarks.unshift({ date: today, text: `❌ RTO டெஸ்ட் ${resText}. ${dtTxt} (Trainer: ${trainerName})` });
          if (nextDate) {
            s.testDate = nextDate;
            s.testStatus = "Pending";
          }
          students[ix] = s;
          await saveNanbanPartial({ students });
          await notifyAdminsText(tenantId, `❌ *TEST ${resultStr.toUpperCase()}:* ${s.name} டெஸ்டில் ${resText}. ${dtTxt}`);
        }
        return { status: "success" };
      }

      case "updateExpenseDataAction": {
        const expObj = args[0];
        const newAmt = args[1];
        const newDesc = args[2];
        const oldDate = String(expObj?.date || "").trim();
        const oldSpender = String(expObj?.spender || "").trim();
        const oldCat = String(expObj?.cat || "").trim();
        const oldAmt = parseInt(expObj?.amt, 10) || 0;
        const oldDesc = String(expObj?.desc || "").trim();
        const amt2 = parseInt(newAmt, 10) || 0;
        const desc2 = String(newDesc || "").trim();
        const snap = await getBusinessSnapshotDoc("Nanban");
        const expenses = Array.isArray(snap.expenses) ? [...snap.expenses] : [];
        let hit = -1;
        for (let i = 0; i < expenses.length; i++) {
          const row = expenses[i] || {};
          const d = String(row.date || "").trim();
          const sp = String(row.spender || "").trim();
          const c = String(row.cat || "").trim();
          const a = parseInt(row.amt, 10) || 0;
          const ds = String(row.desc || "").trim();
          if (d === oldDate && sp === oldSpender && c === oldCat && a === oldAmt && ds === oldDesc) {
            hit = i;
            break;
          }
        }
        if (hit < 0) return { status: "error", message: "Expense row not found" };
        const updated = { ...expenses[hit], amt: amt2, desc: desc2 };
        expenses[hit] = updated;
        await saveNanbanPartial({ expenses });
        await notifyAdminsText(
          tenantId,
          `📝 Expense Updated:\n${oldDate} | ${oldSpender}\nCat: ${oldCat}\nAmount: ₹${amt2}\nDesc: ${desc2}`
        );
        return { status: "success" };
      }

      case "sendBulkMessageAction": {
        const msgText = String(args[0] || "");
        const snap = await getBusinessSnapshotDoc("Nanban");
        const cfg = nanbanTemplateCfg_(snap);
        const tpl = cfg.bulkTemplate || "bulk_announcement";
        const students = Array.isArray(snap.students) ? snap.students : [];
        let count = 0;
        let delay = 0;
        for (const s of students) {
          if (s.status === "Deleted" || s.type === "Enquiry" || s.status === "License_Completed" || s.status === "Hold") {
            continue;
          }
          const wa = waE164_(s.phone);
          if (!wa) continue;
          try {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: "",
                messageType: "template",
                template: { name: tpl, languageCode: "ta", bodyParams: [String(msgText || "")] },
                metadata: { kind: "bulk_announcement", student_id: String(s.id || "") }
              },
              { delaySeconds: delay }
            );
          } catch (e) {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: `📢 *அறிவிப்பு:*\n\n${msgText}\n\n- நிர்வாகம், நண்பன் டிரைவிங் ஸ்கூல்`,
                messageType: "text",
                metadata: { kind: "bulk_announcement_fb", student_id: String(s.id || "") }
              },
              { delaySeconds: delay }
            );
          }
          delay += 2;
          count++;
        }
        return { status: "success", msg: `${count} பேருக்கு மெசேஜ் அனுப்பப்பட்டது!` };
      }

      case "sendWelcomeMessageAction": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc("Nanban");
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "Student Not Found" };
        const kind = inferJobKindFromStudent(s);
        await admin
          .firestore()
          .collection("tenants")
          .doc(tenantId)
          .collection("wa_native_jobs")
          .add({
            status: "pending",
            kind,
            student: s,
            source: "rpc_sendWelcomeMessageAction",
            created_at: admin.firestore.FieldValue.serverTimestamp()
          });
        return { status: "success" };
      }

      case "sendDigitalFeeReceiptAction": {
        const studentId = args[0];
        const amount = args[1];
        const receiver = args[2];
        const loggedBy = args[3];
        const amt = parseInt(amount, 10) || 0;
        if (amt <= 0) return { status: "error", message: "Invalid amount" };
        const snap = await getBusinessSnapshotDoc("Nanban");
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "Student not found" };
        const today = getISTDateString();
        const recv = String(receiver || loggedBy || "System");
        const bal =
          (parseInt(s.totalFee, 10) || 0) - (parseInt(s.advance, 10) || 0) - (parseInt(s.discount, 10) || 0);
        const msg =
          `💰 *கட்டண ரசீது (Receipt)*\n\n` +
          `மாணவர்: ${s.name}\n` +
          `தொகை: ₹${amt}\n` +
          `பெற்றவர்: ${recv}\n` +
          `தேதி: ${today}\n` +
          `மீதம்: ₹${bal}\n\n` +
          `நண்பன் டிரைவிங் ஸ்கூல் - விபத்தில்லா தமிழ்நாடு! 🚦`;
        const wa = waE164_(s.phone);
        if (wa) {
          await enqueueWaOutboundSend(
            {
              tenantId: TENANT_DEFAULT,
              to: wa,
              message: msg,
              messageType: "text",
              metadata: { kind: "digital_receipt", student_id: String(studentId) }
            },
            { delaySeconds: 0 }
          );
        }
        return { status: "success" };
      }

      case "runDailyAdminSummaryNowAction": {
        const { runNanbanDailyEvening } = require("../jobs/scheduledCrons");
        return await runNanbanDailyEvening({ tenantId });
      }

      case "processDayCloseHandover": {
        let trainer;
        let receiver;
        let runKm;
        let testResultsJson;
        let expAmt;
        let expDesc;
        if (args.length >= 9) {
          trainer = String(args[0] || "");
          receiver = String(args[1] || "");
          runKm = parseInt(args[2], 10) || 0;
          testResultsJson = args[3];
          expAmt = parseInt(args[6], 10) || 0;
          expDesc = String(args[7] || "");
        } else {
          trainer = String(args[0] || "");
          receiver = String(args[1] || "");
          expAmt = parseInt(args[2], 10) || 0;
          expDesc = String(args[3] || "");
          runKm = parseInt(args[4], 10) || 0;
          testResultsJson = args[5];
        }
        let testResults = [];
        try {
          if (testResultsJson) testResults = JSON.parse(String(testResultsJson));
        } catch (e) {}
        if (!Array.isArray(testResults)) testResults = [];
        const today = getISTDateString();
        await setRuntimeDoc("Nanban", "nanban_km_today", { date_ist: today, value: String(runKm) });
        await setRuntimeDoc("Nanban", "trainer_km_session", { session: null });

        const snap = await getBusinessSnapshotDoc("Nanban");
        const appSettings =
          snap.appSettings && typeof snap.appSettings === "object" ? { ...snap.appSettings } : {};
        const vk = appSettings.vehicleKm || { current: 0, lastService: 0, nextService: 5000 };
        const oldKm = parseInt(vk.current, 10) || 0;
        vk.current = oldKm + runKm;
        appSettings.vehicleKm = vk;
        let students = Array.isArray(snap.students) ? snap.students.map((x) => ({ ...x })) : [];
        let expenses = Array.isArray(snap.expenses) ? snap.expenses.map((x) => ({ ...x })) : [];
        let totalCollected = 0;
        let classesTaken = 0;
        let spotIncome = 0;

        for (let i = 0; i < students.length; i++) {
          let s = students[i];
          const resObj = testResults.find((r) => String(r.id) === String(s.id));
          if (resObj) {
            s.testStatus = resObj.result;
            if (!Array.isArray(s.adminRemarks)) s.adminRemarks = [];
            s.adminRemarks.unshift({
              date: today,
              text: `🏁 டெஸ்ட் முடிவு: ${resObj.result} (பதிவு செய்தவர்: ${trainer})`
            });
            if (String(resObj.result) === "Pass") {
              s.status = "License_Completed";
              s.adminRemarks.unshift({ date: today, text: `🏆 RTO டெஸ்ட் பாஸ்! (Trainer: ${trainer})` });
              const wa = waE164_(s.phone);
              if (wa) {
                await enqueueWaOutboundSend(
                  {
                    tenantId: TENANT_DEFAULT,
                    to: wa,
                    message: `🎉 வெற்றி பெற்றீர்கள் ${s.name}! ஓட்டுநர் தேர்வில் இன்று சிறப்பாக செயல்பட்டதற்கு வாழ்த்துக்கள்! 🏆`,
                    messageType: "text",
                    metadata: { kind: "day_close_pass", student_id: String(s.id) }
                  },
                  { delaySeconds: 0 }
                );
              }
              await notifyAdminsText(tenantId, `🏆 *TEST PASS:* ${s.name} தேர்ச்சி பெற்றுவிட்டார்!`);
            }
            students[i] = s;
          }
          s = students[i];
          const att = Array.isArray(s.attendanceHistory) ? s.attendanceHistory : [];
          for (const a of att) {
            if (typeof a === "string" && a.includes(today) && a.includes("✅")) classesTaken++;
          }
          let pays = Array.isArray(s.paymentHistory) ? [...s.paymentHistory] : [];
          let payDirty = false;
          for (let j = 0; j < pays.length; j++) {
            const p = pays[j];
            if (
              p &&
              p.date === today &&
              String(p.note || "").includes("பயிற்சியாளர்") &&
              String(p.note || "").includes(trainer)
            ) {
              totalCollected += parseInt(p.amount, 10) || 0;
              pays[j] = {
                ...p,
                note: `பயிற்சியாளர் வசூல் -> ${receiver}`
              };
              payDirty = true;
            }
          }
          if (payDirty) {
            students[i] = { ...s, paymentHistory: pays };
          }
        }

        for (let ei = 0; ei < expenses.length; ei++) {
          const row = expenses[ei];
          if (
            row &&
            row.date === today &&
            row.spender === trainer &&
            String(row.cat || "").includes("🟡 Spot Pending")
          ) {
            const amt = parseInt(row.amt, 10) || 0;
            spotIncome += amt;
            expenses[ei] = {
              ...row,
              spender: receiver,
              cat: `🟢 வரவு - Spot Collection (${receiver})`,
              desc: `Spot Collection Settled (${receiver}) - Trainer: ${trainer}`
            };
          }
        }

        if (expAmt > 0) {
          expenses.push({
            date: today,
            spender: trainer,
            cat: "🔴 செலவு - வண்டி செலவுகள்",
            amt: expAmt,
            desc: expDesc || ""
          });
        }

        await saveNanbanPartial({ students, expenses, appSettings });

        const expectedHandover = totalCollected + spotIncome - expAmt;
        let closeMsg = `🏁 *DAY CLOSE REPORT (${trainer})*\n\n`;
        closeMsg += `📅 தேதி: ${today}\n`;
        closeMsg += `🚗 பயிற்சி பெற்றவர்கள்: ${classesTaken} பேர்\n`;
        closeMsg += `🚗 ஓடியது: ${runKm} KM (Total: ${vk.current} KM)\n`;
        closeMsg += `💰 மாணவர் வசூல்: ₹${totalCollected}\n`;
        closeMsg += `💸 இதர வரவு (Spot): ₹${spotIncome}\n`;
        closeMsg += `🔴 செலவு: ₹${expAmt} (${expDesc || "-"})\n`;
        if (testResults.length > 0) {
          closeMsg += `--------------------\n🎯 *டெஸ்ட் முடிவுகள்:*`;
          for (const r of testResults) {
            const st = students.find((x) => String(x.id) === String(r.id));
            closeMsg += `\n- ${st ? st.name : "Unknown"}: ${r.result === "Pass" ? "✅ PASS" : "❌ FAIL"}`;
          }
        }
        closeMsg += `\n--------------------\n🤝 ஒப்படைத்த பணம்: *₹${expectedHandover}*\n(To: ${receiver})`;
        await notifyAdminsText(tenantId, closeMsg);

        const users = await handleErpRpc("getAppUsers", []);
        if (Array.isArray(users)) {
          const trainerUser = users.find((u) => u.name === trainer);
          if (trainerUser && trainerUser.phone) {
            const waT = waE164_(trainerUser.phone);
            if (waT) {
              const cfg = nanbanTemplateCfg_(await getBusinessSnapshotDoc("Nanban"));
              const tplName = cfg.dayCloseTemplate || "day_close_report";
              try {
                await enqueueWaOutboundSend(
                  {
                    tenantId: TENANT_DEFAULT,
                    to: waT,
                    message: "",
                    messageType: "template",
                    template: { name: tplName, languageCode: "ta", bodyParams: [closeMsg] },
                    metadata: { kind: "day_close_trainer" }
                  },
                  { delaySeconds: 0 }
                );
              } catch (e) {
                await enqueueWaOutboundSend(
                  {
                    tenantId: TENANT_DEFAULT,
                    to: waT,
                    message: closeMsg,
                    messageType: "text",
                    metadata: { kind: "day_close_trainer_fb" }
                  },
                  { delaySeconds: 0 }
                );
              }
            }
          }
        }

        if (Math.floor(vk.current / (vk.nextService || 5000)) > Math.floor(oldKm / (vk.nextService || 5000))) {
          await notifyAdminsText(
            tenantId,
            `⚠️ *வண்டி சர்வீஸ் அலர்ட்!* வண்டி ${vk.current} KM ஓடிவிட்டது. சர்வீஸ் செய்ய வேண்டிய நேரம் வந்துவிட்டது! 🛠️🚗`
          );
        }

        return { status: "success" };
      }

      default:
        return {
          status: "error",
          message: `Native RPC not implemented: ${act}. Use Firebase snapshot API or extend erpRpcDispatch.js.`,
          action: act
        };
    }
  } catch (e) {
    return { status: "error", message: String(e && e.message ? e.message : e) };
  }
}

module.exports = { handleErpRpc, popTenantFromArgs };
