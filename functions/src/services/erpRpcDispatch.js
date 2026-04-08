const crypto = require("crypto");
const admin = require("firebase-admin");
const { getISTDateString } = require("../lib/istTime");
const {
  getBusinessSnapshotDoc,
  setBusinessSnapshotDoc,
  getRuntimeDoc,
  setRuntimeDoc
} = require("./snapshotStore");

/**
 * Data separation: Nanban ERP lives under businesses/Nanban (+ students/expenses in snapshot).
 * E-Sevai POS lives under businesses/ESevai only. Admin WhatsApp for E-Sevai uses tenant id ESEVAI_ALERT_TENANT_ID (owner single line), not Nanban partner CSV.
 */
/** Firestore doc id for E-Sevai POS (not display name). Path: businesses/ESevai/snapshot/main + runtime/day_state */
const ESEVAI_BUSINESS_DOC_ID = "ESevai";
const { notifyAdminsText } = require("./adminNotify");
const { ESEVAI_ALERT_TENANT_ID } = require("./adminPhoneResolve");
const {
  enqueueWaOutboundSend,
  isUsableWaMediaUrl,
  normalizeMetaWhatsappMediaLink_
} = require("./waOutboundQueue");
const { inferJobKindFromStudent, notifyNanbanAfterStudentWrite_ } = require("./waNativeJobProcessor");
const {
  normalizeChit,
  buildMemberChitPassbook,
  fixHistoricalChitPayments
} = require("./nanbanChitFirestore");

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

/** Inner appSettings block (services, referrers, school profile, …) from snapshot.appSettings. */
function drivingSchoolInnerFromSnap_(snap) {
  const root = nanbanTemplateCfg_(snap);
  return root.appSettings && typeof root.appSettings === "object" ? root.appSettings : root;
}

function drivingSchoolReceiptTagline_(snap) {
  const inner = drivingSchoolInnerFromSnap_(snap);
  const name = String(inner.schoolName || "").trim();
  if (name) return `${name} — நன்றி! 🚦`;
  return `நண்பன் டிரைவிங் ஸ்கூல் - விபத்தில்லா தமிழ்நாடு! 🚦`;
}

/**
 * Staff list: SaaS tenants see only their tenant_id; nanban_main / ESevai include legacy users
 * (no tenant_id, Nanban business, or matching primary tenant).
 */
function includeUserInGetAppUsers_(d, requestTenantId) {
  const req = String(requestTenantId || "").trim();
  const rl = req.toLowerCase();
  const legacyMain = !req || rl === "nanban_main";
  const legacyEsevai = /^esevai$/i.test(req);

  const ut = String(d.tenant_id || d.tenantId || "").trim();
  const utl = ut.toLowerCase();
  const businesses = Array.isArray(d.businesses) ? d.businesses : [];
  const hasNanban = businesses.some((b) => String(b || "").trim().toLowerCase() === "nanban");
  const hasEsevai = businesses.some((b) => String(b || "").trim().toLowerCase() === "esevai");

  if (legacyMain || legacyEsevai) {
    if (!ut) return true;
    if (utl === "nanban_main") return true;
    if (hasNanban) return true;
    if (legacyEsevai && (utl === "esevai" || hasEsevai)) return true;
    return false;
  }

  return utl === rl;
}

/**
 * Driving-school ERP snapshot: businesses/{id}/snapshot/main.
 * nanban_main (and ESevai as RPC tenant) use legacy doc "Nanban"; SaaS schools use doc id === tenant_id.
 */
function nanbanBusinessDocIdForTenant(tenantId) {
  const t = String(tenantId || "").trim();
  if (/^esevai$/i.test(t)) return "Nanban";
  if (!t || t === "nanban_main") return "Nanban";
  return t;
}

async function assertSaasTrialAllowsWrites_(tenantId) {
  const t = String(tenantId || "").trim();
  if (!t || t === "nanban_main" || /^esevai$/i.test(t)) return;
  const doc = await admin.firestore().collection("platform_tenants").doc(t).get();
  if (!doc.exists) return;
  const te = (doc.data() || {}).trial_ends_at;
  if (te && typeof te.toMillis === "function" && te.toMillis() < Date.now()) {
    const e = new Error("trial_expired");
    e.code = "trial_expired";
    throw e;
  }
}

function normPhone10(p) {
  let d = String(p || "").replace(/\D/g, "");
  if (d.length > 10) d = d.slice(-10);
  return d;
}

function waE164_(phone) {
  const d = normPhone10(phone);
  return d.length === 10 ? `91${d}` : "";
}

const SCHOOL_REG_OTP_PEPPER = String(process.env.SCHOOL_REG_OTP_PEPPER || "nanban_school_reg_otp_v1");

function hashSchoolRegOtp_(code6) {
  return crypto.createHash("sha256").update(`${SCHOOL_REG_OTP_PEPPER}:${String(code6 || "").trim()}`).digest("hex");
}

function randomSixDigitOtp_() {
  return String(100000 + Math.floor(Math.random() * 900000));
}

function timingSafeEqualStr_(a, b) {
  const x = String(a || "");
  const y = String(b || "");
  if (x.length !== y.length) return false;
  try {
    return crypto.timingSafeEqual(Buffer.from(x, "utf8"), Buffer.from(y, "utf8"));
  } catch (_e) {
    return false;
  }
}

function ensureExpenseRowWithId_(raw) {
  const row = raw && typeof raw === "object" ? { ...raw } : {};
  if (!String(row.id || "").trim()) {
    row.id = `exp_${Date.now()}_${Math.floor(1000 + Math.random() * 9000)}`;
  }
  return row;
}

function nanbanTemplateCfg_(snap) {
  const a = snap?.appSettings;
  return a && typeof a === "object" ? a : {};
}

function decodeDataUrlForUpload_(dataUrl) {
  const s = String(dataUrl || "");
  const i = s.indexOf(",");
  if (i === -1) return null;
  const head = s.slice(0, i);
  const payload = s.slice(i + 1);
  let contentType = "application/octet-stream";
  const ctm = head.match(/^data:([^;]+)/i);
  if (ctm) contentType = String(ctm[1] || "").trim() || contentType;
  const isB64 = /;base64/i.test(head);
  try {
    const buf = isB64
      ? Buffer.from(payload.replace(/\s/g, ""), "base64")
      : Buffer.from(decodeURIComponent(payload), "utf8");
    return { buf, contentType };
  } catch (_e) {
    return null;
  }
}

async function uploadBufferToDefaultBucket_({ buf, contentType, rawName }) {
  const safe = String(rawName || "file")
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .slice(0, 180);
  if (!safe) throw new Error("invalid_name");
  const dest = `nanban_llr/${Date.now()}_${crypto.randomBytes(5).toString("hex")}_${safe}`;
  const token = crypto.randomUUID();

  const envBucket = String(process.env.NANBAN_STORAGE_BUCKET || process.env.FIREBASE_STORAGE_BUCKET || "").trim();
  const projectId = String(process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || "").trim();
  console.log(
    `NANBAN_STORAGE_CTX projectId=${projectId} env_bucket=${envBucket || "(unset)"} bytes=${buf && buf.length ? buf.length : 0}`
  );
  try {
    const defB = admin.storage().bucket();
    console.log(`NANBAN_STORAGE_DEFAULT_BUCKET name=${defB.name}`);
  } catch (probeErr) {
    console.error(`NANBAN_STORAGE_DEFAULT_BUCKET_PROBE ${String(probeErr && probeErr.message ? probeErr.message : probeErr)}`);
  }
  /** New projects often use .firebasestorage.app; older ones use .appspot.com. Wrong name → 404 / permission errors. */
  const bucketCandidates = envBucket
    ? [envBucket]
    : projectId
      ? [`${projectId}.appspot.com`, `${projectId}.firebasestorage.app`]
      : [];

  let lastErr;
  const trySave = async (bucket) => {
    const file = bucket.file(dest);
    await file.save(buf, {
      resumable: false,
      metadata: {
        contentType: contentType || "application/octet-stream",
        contentDisposition: `inline; filename="${safe.replace(/[^\x20-\x7E]/g, "_").slice(0, 120)}"`,
        metadata: {
          firebaseStorageDownloadTokens: token
        }
      }
    });
    const enc = encodeURIComponent(dest);
    const tokenUrl = `https://firebasestorage.googleapis.com/v0/b/${bucket.name}/o/${enc}?alt=media&token=${token}`;
    /** v2 signed URL opens reliably in browser tabs; token URLs often fail with popup / viewer. */
    let viewUrl = tokenUrl;
    try {
      const [signedUrl] = await file.getSignedUrl({
        version: "v2",
        action: "read",
        expires: Date.now() + 1000 * 60 * 60 * 24 * 365
      });
      viewUrl = signedUrl;
    } catch (signErr) {
      console.error(`NANBAN_LLR_SIGNED_URL_FALLBACK code=${signErr?.code || ""} msg=${String(signErr?.message || signErr)}`);
    }
    return { url: viewUrl, id: dest, bucket: bucket.name };
  };

  for (const name of bucketCandidates) {
    try {
      const bucket = admin.storage().bucket(name);
      const out = await trySave(bucket);
      console.log(`NANBAN_LLR_UPLOAD_OK bucket=${out.bucket} bytes=${buf.length} dest=${dest}`);
      return { url: out.url, id: out.id };
    } catch (err) {
      lastErr = err;
      console.error(
        `NANBAN_LLR_UPLOAD_TRY_FAILED bucket=${name} code=${err?.code || ""} msg=${String(err?.message || err)}`
      );
    }
  }

  try {
    const bucket = admin.storage().bucket();
    const out = await trySave(bucket);
    console.log(`NANBAN_LLR_UPLOAD_OK bucket=${out.bucket} bytes=${buf.length} dest=${dest} (admin default)`);
    return { url: out.url, id: out.id };
  } catch (err) {
    lastErr = err;
    console.error(
      `NANBAN_LLR_UPLOAD_TRY_FAILED bucket=(default) code=${err?.code || ""} msg=${String(err?.message || err)}`
    );
  }

  console.error(`NANBAN_LLR_UPLOAD_ALL_FAILED last=${String(lastErr?.message || lastErr)}`);
  throw lastErr || new Error("storage_upload_failed_all_buckets");
}

/** Trailing RPC arg tenant codes (includes ESevai without underscore). */
const TRAILING_TENANT_EXACT = new Set(["ESevai", "esevai"]);

function looksLikeRpcTenantToken_(last) {
  if (typeof last !== "string" || !last || last.length >= 48) return false;
  if (last.includes(" ") || last.includes("/") || last.startsWith("{")) return false;
  if (TRAILING_TENANT_EXACT.has(last)) return true;
  if (last.includes("_")) return true;
  if (/^ds-[a-z0-9-]+$/i.test(last)) return true;
  if (/^esevai$/i.test(last)) return true;
  return false;
}

function normalizePopTenantId_(raw) {
  const s = String(raw || "").trim();
  if (/^esevai$/i.test(s)) return "ESevai";
  return s;
}

function popTenantFromArgs(args) {
  if (!Array.isArray(args) || !args.length) return { tenantId: TENANT_DEFAULT, args: [] };
  const last = args[args.length - 1];
  if (typeof last === "string" && looksLikeRpcTenantToken_(last)) {
    return { tenantId: normalizePopTenantId_(last), args: args.slice(0, -1) };
  }
  return { tenantId: TENANT_DEFAULT, args };
}

function looksLikeJsonArrayString(s) {
  return String(s ?? "").trim().startsWith("[");
}

/**
 * Hosted UI (index.html confirmEndDay) sends: trainer, receiver, runKm, testResultsJson, 0, 0, expAmt, expDesc, receiptUrl?
 * Legacy Apps Script sent: trainer, receiver, expAmt, expDesc, runKm, testResultsJson
 * If the client omits receiptUrl (8 args), args.length >= 9 was false and runKm was mis-read as expAmt (₹1 vehicle expense).
 */
function parseProcessDayCloseArgs(rawArgs) {
  const a = Array.isArray(rawArgs) ? rawArgs : [];
  const pad4 = a[4];
  const pad5 = a[5];
  const hasHostedPadding =
    a.length >= 8 &&
    (pad4 === 0 || pad4 === "0") &&
    (pad5 === 0 || pad5 === "0");
  const hostedByJson = a.length >= 8 && looksLikeJsonArrayString(a[3]);
  if (hasHostedPadding || (a.length >= 9 && hostedByJson)) {
    return {
      trainer: String(a[0] || ""),
      receiver: String(a[1] || ""),
      runKm: parseInt(a[2], 10) || 0,
      testResultsJson: a[3],
      expAmt: parseInt(a[6], 10) || 0,
      expDesc: String(a[7] || "")
    };
  }
  return {
    trainer: String(a[0] || ""),
    receiver: String(a[1] || ""),
    expAmt: parseInt(a[2], 10) || 0,
    expDesc: String(a[3] || ""),
    runKm: parseInt(a[4], 10) || 0,
    testResultsJson: a[5]
  };
}

async function loadEsevaiModel(tenantId) {
  const raw = await getBusinessSnapshotDoc(ESEVAI_BUSINESS_DOC_ID);
  const data = raw && typeof raw === "object" ? { ...raw } : {};
  delete data.students;
  delete data.Students;
  delete data.expenses;
  delete data.Expenses;
  delete data.chitData;
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
  /** Nanban snapshot is never read here — E-Sevai uses businesses/ESevai only (runtime/day_state is written on opening save for ops/debug, not merged on read — avoids clobbering intraday balances). */
  return data;
}

async function persistEsevai(data) {
  await setBusinessSnapshotDoc(ESEVAI_BUSINESS_DOC_ID, data, true);
}

async function saveNanbanPartial(tenantId, patch) {
  await assertSaasTrialAllowsWrites_(tenantId);
  const bid = nanbanBusinessDocIdForTenant(tenantId);
  const snap = await getBusinessSnapshotDoc(bid);
  const next = Object.assign({}, snap, patch, {
    updated_at: admin.firestore.FieldValue.serverTimestamp()
  });
  await setBusinessSnapshotDoc(bid, next, true);
}

async function loadNanbanChit(tenantId) {
  const bid = nanbanBusinessDocIdForTenant(tenantId);
  const snap = await getBusinessSnapshotDoc(bid);
  return { snap, chit: normalizeChit(snap.chitData) };
}

async function persistChit(tenantId, chit) {
  await saveNanbanPartial(tenantId, { chitData: chit });
}

function upiLink_(amount, name, upiId) {
  if (!upiId || !String(upiId).trim()) return "";
  const pa = encodeURIComponent(String(upiId).trim());
  const pn = encodeURIComponent(String(name || "Student").slice(0, 40));
  return `upi://pay?pa=${pa}&pn=${pn}&am=${parseInt(amount, 10) || 0}&cu=INR`;
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

      case "sendSchoolRegistrationOtp": {
        const idToken = String(args[0] || "").trim();
        const adminPhoneRaw = String(args[1] || "").trim();
        if (!idToken) {
          return { status: "error", message: "Google sign-in required." };
        }
        const phone10 = normPhone10(adminPhoneRaw);
        if (phone10.length !== 10) {
          return { status: "error", message: "Enter a valid 10-digit mobile number." };
        }
        let decoded;
        try {
          decoded = await admin.auth().verifyIdToken(idToken);
        } catch (_verifyErr) {
          return { status: "error", message: "Invalid or expired session. Please sign in with Google again." };
        }
        const uid = String(decoded.uid || "").trim();
        if (!uid) {
          return { status: "error", message: "Invalid Google account." };
        }
        const db = admin.firestore();
        const metaRef = db.collection("registration_otp_meta").doc(uid);
        const now = Date.now();
        const metaSnap = await metaRef.get();
        const meta = metaSnap.exists ? metaSnap.data() || {} : {};
        const last = meta.last_sent_at && meta.last_sent_at.toMillis ? meta.last_sent_at.toMillis() : 0;
        if (last && now - last < 55 * 1000) {
          return { status: "error", message: "Please wait a minute before requesting another OTP." };
        }
        const windowStart = now - 60 * 60 * 1000;
        const sendsLastHour = typeof meta.sends_last_hour === "number" ? meta.sends_last_hour : 0;
        const windowMs = meta.rate_window_start && meta.rate_window_start.toMillis ? meta.rate_window_start.toMillis() : 0;
        let count = sendsLastHour;
        if (!windowMs || windowMs < windowStart) {
          count = 0;
        }
        if (count >= 5) {
          return { status: "error", message: "Too many OTP requests. Try again after one hour." };
        }
        const plain = randomSixDigitOtp_();
        const codeHash = hashSchoolRegOtp_(plain);
        const expiresAt = admin.firestore.Timestamp.fromMillis(now + 10 * 60 * 1000);
        await db.collection("registration_otps").doc(uid).set(
          {
            phone10,
            code_hash: codeHash,
            expires_at: expiresAt,
            attempts: 0,
            created_at: admin.firestore.FieldValue.serverTimestamp()
          },
          { merge: false }
        );
        await metaRef.set(
          {
            last_sent_at: admin.firestore.FieldValue.serverTimestamp(),
            rate_window_start:
              !windowMs || windowMs < windowStart
                ? admin.firestore.FieldValue.serverTimestamp()
                : meta.rate_window_start,
            sends_last_hour: count + 1
          },
          { merge: true }
        );
        const waTo = waE164_(phone10);
        if (!waTo) {
          return { status: "error", message: "Invalid phone for WhatsApp." };
        }
        const otpMsg =
          `🔐 *Wheels Nanban (wheelsnanban.in)*\n\nYour school registration OTP is: *${plain}*\nValid 10 minutes. Do not share this code.`;
        try {
          await enqueueWaOutboundSend(
            {
              tenantId: TENANT_DEFAULT,
              to: waTo,
              message: otpMsg,
              messageType: "text",
              metadata: { kind: "school_reg_otp" }
            },
            { delaySeconds: 0 }
          );
        } catch (waErr) {
          console.error(`SCHOOL_REG_OTP_WA_FAILED ${String(waErr && waErr.message ? waErr.message : waErr)}`);
          return {
            status: "error",
            message: "Could not send OTP on WhatsApp. Check the number has WhatsApp or try again later."
          };
        }
        return { status: "success", message: "OTP sent to WhatsApp.", expires_in_sec: 600 };
      }

      case "recordTenantSession": {
        const idToken = String(args[0] || "").trim();
        if (!idToken) return { status: "error", message: "token_required" };
        let dec;
        try {
          dec = await admin.auth().verifyIdToken(idToken);
        } catch (_e) {
          return { status: "error", message: "invalid_token" };
        }
        const db = admin.firestore();
        const udoc = await db.collection("users").doc(String(dec.uid || "")).get();
        if (!udoc.exists) return { status: "success", skipped: true };
        const tid = String(udoc.data()?.tenant_id || "").trim();
        if (!tid.startsWith("ds-")) return { status: "success", skipped: true };
        await db
          .collection("platform_tenants")
          .doc(tid)
          .set(
            {
              last_session_at: admin.firestore.FieldValue.serverTimestamp(),
              last_session_email: String(dec.email || "").toLowerCase().slice(0, 120),
              last_session_name: String(dec.name || "").slice(0, 80),
              last_session_via: "google"
            },
            { merge: true }
          );
        return { status: "success" };
      }

      case "recordSaaSStaffSession": {
        let payload;
        try {
          payload = JSON.parse(String(args[0] || "{}"));
        } catch (_e) {
          return { status: "error", message: "bad_json" };
        }
        const tid = String(payload.tenant_id || "").trim();
        if (!tid.startsWith("ds-")) return { status: "success", skipped: true };
        const db = admin.firestore();
        await db
          .collection("platform_tenants")
          .doc(tid)
          .set(
            {
              last_session_at: admin.firestore.FieldValue.serverTimestamp(),
              last_session_email: String(payload.email || "").toLowerCase().slice(0, 120),
              last_session_name: String(payload.name || "").slice(0, 80),
              last_session_via: "pin"
            },
            { merge: true }
          );
        return { status: "success" };
      }

      case "listSaaSDrivingSchoolActivity": {
        if (tenantId !== "nanban_main") {
          return { status: "error", message: "Only main platform tenant can view this list." };
        }
        const db = admin.firestore();
        let qs;
        try {
          qs = await db.collection("platform_tenants").where("is_saas_school", "==", true).limit(200).get();
        } catch (_e) {
          qs = await db.collection("platform_tenants").limit(300).get();
        }
        const schools = [];
        qs.forEach((doc) => {
          if (!String(doc.id || "").startsWith("ds-")) return;
          const d = doc.data() || {};
          let lastS = null;
          if (d.last_session_at && typeof d.last_session_at.toMillis === "function") {
            lastS = d.last_session_at.toMillis();
          }
          schools.push({
            tenant_id: doc.id,
            school_name: String(d.school_name || ""),
            owner_email: String(d.owner_email || ""),
            last_session_at: lastS,
            last_session_email: String(d.last_session_email || ""),
            last_session_name: String(d.last_session_name || ""),
            last_session_via: String(d.last_session_via || "")
          });
        });
        schools.sort((a, b) => (b.last_session_at || 0) - (a.last_session_at || 0));
        return { status: "success", schools };
      }

      case "registerNewDrivingSchool": {
        const idToken = String(args[0] || "").trim();
        const schoolName = String(args[1] || "").trim().slice(0, 120);
        const adminPhoneRaw = String(args[2] || "").trim();
        const otpPlain = String(args[3] || "").trim().replace(/\D/g, "");
        if (!idToken || !schoolName || schoolName.length < 2) {
          return { status: "error", message: "School name and Google sign-in are required." };
        }
        if (otpPlain.length !== 6) {
          return { status: "error", message: "Enter the 6-digit OTP sent to your WhatsApp." };
        }
        const phone10 = normPhone10(adminPhoneRaw);
        if (phone10.length !== 10) {
          return { status: "error", message: "Enter a valid 10-digit admin mobile number." };
        }
        let decoded;
        try {
          decoded = await admin.auth().verifyIdToken(idToken);
        } catch (_verifyErr) {
          return { status: "error", message: "Invalid or expired session. Please sign in with Google again." };
        }
        const uid = String(decoded.uid || "").trim();
        const db = admin.firestore();
        const otpRef = db.collection("registration_otps").doc(uid);
        const otpSnap = await otpRef.get();
        if (!otpSnap.exists) {
          return { status: "error", message: "No OTP found. Tap Send OTP first." };
        }
        const od = otpSnap.data() || {};
        if (String(od.phone10 || "") !== phone10) {
          return { status: "error", message: "Mobile number does not match OTP request." };
        }
        const exp = od.expires_at;
        if (exp && exp.toMillis && exp.toMillis() < Date.now()) {
          return { status: "error", message: "OTP expired. Request a new one." };
        }
        const attempts = parseInt(od.attempts, 10) || 0;
        if (attempts >= 8) {
          return { status: "error", message: "Too many wrong attempts. Request a new OTP." };
        }
        const expectedHash = String(od.code_hash || "");
        const gotHash = hashSchoolRegOtp_(otpPlain);
        if (!timingSafeEqualStr_(expectedHash, gotHash)) {
          await otpRef.set({ attempts: attempts + 1 }, { merge: true });
          return { status: "error", message: "Wrong OTP. Try again." };
        }
        const email = String(decoded.email || "").trim().toLowerCase();
        if (!email) {
          return { status: "error", message: "Google account must have an email address." };
        }
        if (!uid) {
          return { status: "error", message: "Invalid Google account." };
        }
        const userRef = db.collection("users").doc(uid);
        const existingU = await userRef.get();
        if (existingU.exists) {
          return { status: "error", message: "This Google account is already registered." };
        }
        const dupe = await db.collection("users").where("email", "==", email).limit(1).get();
        if (!dupe.empty) {
          return { status: "error", message: "This email is already registered." };
        }

        const newTenantId = `ds-${Date.now().toString(36)}-${crypto.randomBytes(4).toString("hex")}`;
        const trialEnd = new Date();
        trialEnd.setDate(trialEnd.getDate() + 2);
        const trialTs = admin.firestore.Timestamp.fromDate(trialEnd);
        const waPhone = `91${phone10}`;
        const displayName = String(decoded.name || schoolName || "Admin").trim().slice(0, 80);

        const emptyChit = { groups: [], members: [], auctions: [], payments: [], bids: [], schedule: [] };
        const initialSnapshot = {
          students: [],
          expenses: [],
          appSettings: {
            appSettings: {
              schoolName,
              adminPhone: phone10,
              trainerAlertPhone: waPhone
            }
          },
          chitData: emptyChit,
          updated_at: admin.firestore.FieldValue.serverTimestamp()
        };

        const batch = db.batch();
        batch.delete(otpRef);
        batch.set(
          db.collection("platform_tenants").doc(newTenantId),
          {
            school_name: schoolName,
            owner_email: email,
            admin_phones: [waPhone],
            trial_ends_at: trialTs,
            created_at: admin.firestore.FieldValue.serverTimestamp(),
            nanban_business_doc: newTenantId,
            plan: "trial",
            is_saas_school: true
          },
          { merge: false }
        );
        batch.set(
          userRef,
          {
            name: displayName,
            email,
            pin: "",
            role: "Admin",
            phone: phone10,
            tenant_id: newTenantId,
            businesses: ["Nanban"],
            trial_ends_at: trialTs,
            created_at: admin.firestore.FieldValue.serverTimestamp()
          },
          { merge: false }
        );
        await batch.commit();
        await setBusinessSnapshotDoc(newTenantId, initialSnapshot, true);

        return {
          status: "success",
          tenant_id: newTenantId,
          trial_ends_at: trialEnd.toISOString(),
          profile: {
            id: uid,
            name: displayName,
            email,
            role: "Admin",
            phone: phone10,
            tenant_id: newTenantId,
            businesses: ["Nanban"]
          }
        };
      }

      case "setupESevaiDeliveryReminderTrigger":
      case "setupESevaiAgentLlrReminderTrigger":
      case "setupAllDailyTriggers":
      case "setupDailyMorningTrigger":
      case "setupDailyEveningTrigger":
      case "setupChitReminderTrigger":
        return { status: "success", message: "native_scheduler_active", native: true };

      case "getDatabaseData": {
        // Firestore path: businesses/Nanban/snapshot/main — getBusinessSnapshotDoc coerces JSON strings / maps to arrays.
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        return {
          status: "success",
          students: Array.isArray(snap.students) ? snap.students : [],
          expenses: Array.isArray(snap.expenses) ? snap.expenses : [],
          appSettings: snap.appSettings && typeof snap.appSettings === "object" ? snap.appSettings : {}
        };
      }

      case "getChitData": {
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const chit =
          snap.chitData && typeof snap.chitData === "object"
            ? snap.chitData
            : { groups: [], members: [], auctions: [], payments: [], bids: [], schedule: [] };
        return { status: "success", data: chit };
      }

      case "getAppSettings": {
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        return snap.appSettings || {};
      }

      case "getAppUsers": {
        const db = admin.firestore();
        const qs = await db.collection("users").get();
        const users = [];
        qs.forEach((doc) => {
          const d = doc.data() || {};
          if (!includeUserInGetAppUsers_(d, tenantId)) return;
          let trialEnds = d.trial_ends_at;
          if (trialEnds && typeof trialEnds.toDate === "function") {
            trialEnds = trialEnds.toDate().toISOString();
          } else if (trialEnds) {
            trialEnds = String(trialEnds);
          } else {
            trialEnds = null;
          }
          users.push({
            id: doc.id,
            name: String(d.name || "").trim(),
            pin: String(d.pin || "").trim(),
            role: String(d.role || "Staff").trim(),
            phone: String(d.phone || "").trim(),
            email: String(d.email || d.Email || "").trim().toLowerCase(),
            tenant_id: String(d.tenant_id || d.tenantId || "").trim(),
            businesses: Array.isArray(d.businesses) ? d.businesses : [],
            trial_ends_at: trialEnds
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
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const appSettings = snap.appSettings && typeof snap.appSettings === "object" ? { ...snap.appSettings } : {};
        if (key === "appSettings") {
          const prevInner =
            appSettings.appSettings && typeof appSettings.appSettings === "object"
              ? { ...appSettings.appSettings }
              : {};
          const incoming = val && typeof val === "object" ? val : {};
          appSettings.appSettings = { ...prevInner, ...incoming };
        } else if (key === "serviceSplits") appSettings.serviceSplits = val || {};
        else if (key === "vehicleKm") appSettings.vehicleKm = val || {};
        else appSettings[key] = val;
        await saveNanbanPartial(tenantId, { appSettings });
        return { status: "success" };
      }

      case "saveStudentData": {
        const s = args[0];
        if (!s || !s.id) return { status: "error", message: "Invalid student" };
        s.phone = normPhone10(s.phone);
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        const prev = students.find((x) => String(x.id) === String(s.id)) || null;
        students = students.filter((x) => String(x.id) !== String(s.id));
        students.unshift(s);
        await saveNanbanPartial(tenantId, { students });
        try {
          await notifyNanbanAfterStudentWrite_(tenantId, prev, s);
        } catch (waErr) {
          console.error(
            `NANBAN_NOTIFY_WA_FAILED saveStudentData student=${s.id} ${String(waErr?.message || waErr)}`
          );
          await notifyAdminsText(
            tenantId,
            `⚠️ Admission WA notify failed (student ${s.id}): ${String(waErr?.message || waErr)}`
          );
        }
        const isNewStudent = !prev;
        if (isNewStudent) {
          const t = String(s.type || "").trim();
          try {
            if (t === "Enquiry") {
              await notifyAdminsText(
                tenantId,
                `📋 *புதிய விசாரணை*\nபெயர்: ${String(s.name || "-")}\nமொபைல்: ${String(s.phone || "-")}\nசர்வீஸ்: ${String(s.service || "-")}\nதேதி: ${String(s.dateJoined || "-")}`
              );
            } else if (t) {
              await notifyAdminsText(
                tenantId,
                `🎓 *புதிய அட்மிஷன்*\nபெயர்: ${String(s.name || "-")}\nமொபைல்: ${String(s.phone || "-")}\nவகை: ${t}\nசர்வீஸ்: ${String(s.service || "-")}\nமுன்பணம்: ₹${parseInt(s.advance, 10) || 0}\nதேதி: ${String(s.dateJoined || "-")}`
              );
            }
          } catch (_admErr) {
            /* non-fatal */
          }
        }
        return { status: "success" };
      }

      case "updateStudentData": {
        const s = args[0];
        if (!s || s.id == null) return { status: "error", message: "Invalid student" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        const ix = students.findIndex((x) => String(x.id) === String(s.id));
        if (ix < 0) return { status: "error", message: "Not found" };
        const prev = { ...students[ix] };
        const patch = typeof s === "object" && s ? { ...s } : {};
        const phoneNorm = normPhone10(patch.phone != null ? patch.phone : prev.phone);
        students[ix] = Object.assign({}, prev, patch, {
          id: prev.id,
          phone: phoneNorm || prev.phone
        });
        await saveNanbanPartial(tenantId, { students });
        try {
          await notifyNanbanAfterStudentWrite_(tenantId, prev, students[ix]);
        } catch (waErr) {
          console.error(
            `NANBAN_NOTIFY_WA_FAILED updateStudentData student=${s.id} ${String(waErr?.message || waErr)}`
          );
          await notifyAdminsText(
            tenantId,
            `⚠️ Student update WA notify failed (${s.id}): ${String(waErr?.message || waErr)}`
          );
        }
        return { status: "success" };
      }

      case "saveExpenseData": {
        const raw = args[0];
        if (!raw || typeof raw !== "object") return { status: "error", message: "Invalid expense" };
        const e = ensureExpenseRowWithId_(raw);
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const expenses = Array.isArray(snap.expenses) ? [...snap.expenses] : [];
        const dup = e.id && expenses.some((x) => x && String(x.id) === String(e.id));
        if (dup) return { status: "error", message: "Duplicate expense id" };
        expenses.push(e);
        await saveNanbanPartial(tenantId, { expenses });
        const catStr = String(e.cat || "");
        const isIncome =
          catStr.includes("வரவு") || /\bincome\b/i.test(catStr) || catStr.includes("(In)");
        const label = isIncome ? "வரவு (Income)" : "செலவு (Expense)";
        try {
          await notifyAdminsText(
            tenantId,
            `📒 *${label}*\nதொகை: ₹${Number(e.amt) || 0}\nபிரிவு: ${catStr || "-"}\nசெலவிட்டவர்: ${String(e.spender || "-")}\nவிவரம்: ${String(e.desc || "-").slice(0, 240)}\nதேதி: ${String(e.date || "-")}`
          );
        } catch (_expN) {
          /* non-fatal */
        }
        return { status: "success", expense: e };
      }

      case "getKmTodayAction": {
        const today = getISTDateString();
        const rt = await getRuntimeDoc("Nanban", "nanban_km_today");
        const km = rt && rt.date_ist === today ? parseInt(rt.value, 10) || 0 : 0;
        return { status: "success", km };
      }

      case "getTrainerKmSessionAction": {
        const today = getISTDateString();
        const meta = await getRuntimeDoc("Nanban", "trainer_km_session");
        const sess = meta.session && typeof meta.session === "object" ? meta.session : null;
        const empty = {
          status: "success",
          active: false,
          date: today,
          startKm: 0,
          startedBy: "",
          endKm: 0
        };
        if (!sess) return empty;
        const startKm =
          parseInt(sess.start_km, 10) || parseInt(sess.startKm, 10) || parseInt(sess.start, 10) || 0;
        let sessDate = String(sess.date_ist || "");
        if (!sessDate && sess.started_at) {
          try {
            sessDate = getISTDateString(new Date(sess.started_at));
          } catch (e) {
            sessDate = "";
          }
        }
        const active =
          sess.active !== false &&
          startKm > 0 &&
          String(sessDate || "") === String(today) &&
          sess.cleared !== true;
        if (!active) return empty;
        const endKm = parseInt(sess.end_km, 10) || parseInt(sess.endKm, 10) || 0;
        return {
          status: "success",
          active: true,
          date: sessDate || today,
          startKm,
          startedBy: String(sess.trainer || sess.startedBy || sess.started_by || ""),
          endKm
        };
      }

      case "startTrainerKmSessionAction": {
        const today = getISTDateString();
        const st = parseInt(args[0], 10) || 0;
        const trainerName = String(args[1] || "Trainer");
        if (st <= 0) return { status: "error", message: "Invalid Start KM" };
        const meta = await getRuntimeDoc("Nanban", "trainer_km_session");
        const prev = meta.session && typeof meta.session === "object" ? meta.session : null;
        const prevStart =
          parseInt(prev?.start_km, 10) || parseInt(prev?.startKm, 10) || parseInt(prev?.start, 10) || 0;
        const prevDate = String(prev?.date_ist || "");
        let prevActive = prev && prev.active !== false && prevStart > 0;
        if (prev && !prevDate && prev.started_at) {
          try {
            const d = getISTDateString(new Date(prev.started_at));
            prevActive = prevActive && d === today;
          } catch (e) {
            prevActive = false;
          }
        } else if (prevDate) {
          prevActive = prevActive && prevDate === today;
        } else if (prev && prevStart > 0) {
          prevActive = false;
        }
        if (prevActive && prevStart > 0) {
          return {
            status: "exists",
            date: today,
            active: true,
            startKm: prevStart,
            startedBy: String(prev.trainer || prev.startedBy || prev.started_by || trainerName),
            endKm: parseInt(prev.end_km, 10) || parseInt(prev.endKm, 10) || 0
          };
        }
        const session = {
          active: true,
          date_ist: today,
          start_km: st,
          trainer: trainerName,
          started_at: new Date().toISOString(),
          end_km: 0,
          cleared: false
        };
        await setRuntimeDoc("Nanban", "trainer_km_session", { session });
        return {
          status: "success",
          date: today,
          active: true,
          startKm: st,
          startedBy: trainerName,
          endKm: 0
        };
      }

      case "clearTrainerKmSessionAction": {
        await setRuntimeDoc("Nanban", "trainer_km_session", {
          session: {
            active: false,
            cleared: true,
            date_ist: getISTDateString(),
            start_km: 0,
            end_km: 0,
            trainer: "",
            started_at: ""
          }
        });
        return { status: "success" };
      }

      case "saveChitMember": {
        const m = args[0] || {};
        m.phone = normPhone10(m.phone);
        if (String(m.phone || "").length !== 10) {
          return { status: "error", message: "Invalid phone (10-digit required)" };
        }
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
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
        await saveNanbanPartial(tenantId, { chitData: chit });
        return { status: "success" };
      }

      case "editChitMemberData": {
        const m = args[0] || {};
        m.phone = normPhone10(m.phone);
        if (String(m.phone || "").length !== 10) {
          return { status: "error", message: "Invalid phone (10-digit required)" };
        }
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const chit = snap.chitData && typeof snap.chitData === "object" ? { ...snap.chitData } : {};
        if (!Array.isArray(chit.members)) chit.members = [];
        const idx = chit.members.findIndex((x) => String(x.id) === String(m.id));
        if (idx < 0) return { status: "error" };
        chit.members[idx] = Object.assign({}, chit.members[idx], m);
        await saveNanbanPartial(tenantId, { chitData: chit });
        return { status: "success" };
      }

      case "deleteChitMemberData": {
        const id = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const chit = snap.chitData && typeof snap.chitData === "object" ? { ...snap.chitData } : {};
        if (!Array.isArray(chit.members)) chit.members = [];
        chit.members = chit.members.filter((x) => String(x.id) !== String(id));
        await saveNanbanPartial(tenantId, { chitData: chit });
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
        const today = getISTDateString();
        const cash = Number(b.Cash) || 0;
        const sbi = Number(b.SBI) || 0;
        const fed1 = Number(b["Federal 1"]) || 0;
        const fed2 = Number(b["Federal 2"]) || 0;
        const paytm = Number(b.Paytm) || 0;
        /** Canonical shape (matches client firebaseReady path + GAS RTDB). UI reads balances + openingBalance.date. */
        data.openingBalance = {
          date: today,
          cash,
          sbi,
          federal1: fed1,
          federal2: fed2,
          paytm
        };
        data.balances = {
          Cash: cash,
          SBI: sbi,
          "Federal 1": fed1,
          "Federal 2": fed2,
          Paytm: paytm
        };
        if (!Array.isArray(data.ledgerEntries)) data.ledgerEntries = [];
        const desc = `Opening Balance for ${today}`;
        data.ledgerEntries = data.ledgerEntries.filter(
          (e) =>
            !(
              e &&
              String(e.category || "").toLowerCase() === "opening balance" &&
              String(e.date || "") === today
            )
        );
        Object.entries(data.balances).forEach(([acc, amt]) => {
          const a = Number(amt) || 0;
          if (a > 0) {
            data.ledgerEntries.unshift({
              date: today,
              type: "income",
              category: "Opening Balance",
              description: desc,
              amount: a,
              account: acc
            });
          }
        });
        await persistEsevai(data);
        await setRuntimeDoc(
          ESEVAI_BUSINESS_DOC_ID,
          "day_state",
          {
            opening_date: today,
            openingBalance: data.openingBalance,
            balances: data.balances,
            tenant_id: tenantId,
            business_doc_id: ESEVAI_BUSINESS_DOC_ID
          },
          true
        );
        console.log(
          `ESEVAI_OPENING_SAVED doc=${ESEVAI_BUSINESS_DOC_ID} date=${today} cash=${cash} tenant=${tenantId}`
        );
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
          ESEVAI_ALERT_TENANT_ID,
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
        let amt = parseInt(args[4], 10) || 0;
        if (type === "class") amt = 0;
        const trainer = String(args[5] || "Trainer");
        const today = getISTDateString();
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
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
        await saveNanbanPartial(tenantId, { students });
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
                await saveNanbanPartial(tenantId, { students });
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
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
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
          await saveNanbanPartial(tenantId, { students });
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
          await saveNanbanPartial(tenantId, { students });
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
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const expenses = Array.isArray(snap.expenses) ? [...snap.expenses] : [];
        let hit = -1;
        const targetId = String(expObj?.id || "").trim();
        if (targetId) {
          hit = expenses.findIndex((row) => row && String(row.id || "") === targetId);
        }
        if (hit < 0) {
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
        }
        if (hit < 0) return { status: "error", message: "Expense row not found" };
        const updated = { ...expenses[hit], amt: amt2, desc: desc2 };
        if (!String(updated.id || "").trim()) {
          updated.id = ensureExpenseRowWithId_(updated).id;
        }
        expenses[hit] = updated;
        await saveNanbanPartial(tenantId, { expenses });
        await notifyAdminsText(
          tenantId,
          `📝 Expense Updated:\n${oldDate} | ${oldSpender}\nCat: ${oldCat}\nAmount: ₹${amt2}\nDesc: ${desc2}`
        );
        return { status: "success" };
      }

      case "sendBulkMessageAction": {
        const msgText = String(args[0] || "");
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
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
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "Student Not Found" };
        const { processWaNativeJob } = require("./waNativeJobProcessor");
        try {
          await processWaNativeJob(tenantId, { student: s, kind: inferJobKindFromStudent(s) });
        } catch (e) {
          return { status: "error", message: String(e?.message || e) };
        }
        return { status: "success" };
      }

      case "sendDigitalFeeReceiptAction": {
        const studentId = args[0];
        const amount = args[1];
        const receiver = args[2];
        const loggedBy = args[3];
        const amt = parseInt(amount, 10) || 0;
        if (amt <= 0) return { status: "error", message: "Invalid amount" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "Student not found" };
        const today = getISTDateString();
        const recv = String(receiver || loggedBy || "System");
        const bal =
          (parseInt(s.totalFee, 10) || 0) - (parseInt(s.advance, 10) || 0) - (parseInt(s.discount, 10) || 0);
        const tagline = drivingSchoolReceiptTagline_(snap);
        const msg =
          `💰 *கட்டண ரசீது (Receipt)*\n\n` +
          `மாணவர்: ${s.name}\n` +
          `தொகை: ₹${amt}\n` +
          `பெற்றவர்: ${recv}\n` +
          `தேதி: ${today}\n` +
          `மீதம்: ₹${bal}\n\n` +
          tagline;
        const wa = waE164_(s.phone);
        if (wa) {
          await enqueueWaOutboundSend(
            {
              tenantId,
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
        const parsed = parseProcessDayCloseArgs(args);
        const trainer = parsed.trainer;
        const receiver = parsed.receiver;
        const runKm = parsed.runKm;
        const testResultsJson = parsed.testResultsJson;
        const expAmt = parsed.expAmt;
        const expDesc = parsed.expDesc;
        let testResults = [];
        try {
          if (testResultsJson) testResults = JSON.parse(String(testResultsJson));
        } catch (e) {}
        if (!Array.isArray(testResults)) testResults = [];
        const today = getISTDateString();
        await setRuntimeDoc("Nanban", "nanban_km_today", { date_ist: today, value: String(runKm) });
        await setRuntimeDoc("Nanban", "trainer_km_session", { session: null });

        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
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

        await saveNanbanPartial(tenantId, { students, expenses, appSettings });

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
              const cfg = nanbanTemplateCfg_(await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId)));
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

      case "getStudentPassportData": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (s) return { status: "success", data: s };
        return { status: "error", message: "Student Not Found" };
      }

      case "sendLLR30DayReminder": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: `மாணவர் கிடைக்கவில்லை! (ID: ${studentId})` };
        const wa = waE164_(s.phone);
        if (!wa) return { status: "error", message: "Phone missing" };
        const msg = `வணக்கம் ${s.name}! 🎉\nஉங்கள் LLR பதிவு செய்து இன்று 30 நாள் நிறைவடைந்துள்ளது.\n\nஇப்போது நீங்கள் RTO டெஸ்ட் தேதி பதிவு செய்ய தயாராக இருக்கிறீர்கள். 🚗\nதேதி fix செய்ய அலுவலகத்தைத் தொடர்பு கொள்ளவும் 👇`;
        await enqueueWaOutboundSend(
          { tenantId: TENANT_DEFAULT, to: wa, message: msg, messageType: "text", metadata: { kind: "llr_30d", student_id: String(studentId) } },
          { delaySeconds: 0 }
        );
        return { status: "success", message: "30 நாள் நினைவூட்டல் மெசேஜ் அனுப்பப்பட்டது! ✅" };
      }

      case "sendLLRExpireReminder": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: `மாணவர் கிடைக்கவில்லை! (ID: ${studentId})` };
        const wa = waE164_(s.phone);
        if (!wa) return { status: "error", message: "Phone missing" };
        const txt =
          "உங்கள் LLR இன்னும் சில நாட்களில் காலாவதியாக உள்ளது. தயவுசெய்து உடனே புதுப்பிக்கவும். - நண்பன் டிரைவிங் ஸ்கூல்";
        try {
          await enqueueWaOutboundSend(
            {
              tenantId: TENANT_DEFAULT,
              to: wa,
              message: "",
              messageType: "template",
              template: { name: "bulk_announcement", languageCode: "ta", bodyParams: [txt] },
              metadata: { kind: "llr_expire", student_id: String(studentId) }
            },
            { delaySeconds: 0 }
          );
        } catch (e) {
          await enqueueWaOutboundSend(
            {
              tenantId: TENANT_DEFAULT,
              to: wa,
              message: txt,
              messageType: "text",
              metadata: { kind: "llr_expire_fb", student_id: String(studentId) }
            },
            { delaySeconds: 0 }
          );
        }
        return { status: "success", message: "LLR காலாவதி நினைவூட்டல் அனுப்பப்பட்டது! ✅" };
      }

      case "markSyllabusAction": {
        const studentId = args[0];
        const itemKey = args[1];
        const isCompleted = !!args[2];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let students = Array.isArray(snap.students) ? snap.students.map((x) => ({ ...x })) : [];
        const ix = students.findIndex((x) => String(x.id) === String(studentId));
        if (ix < 0) return { status: "error", message: "Student not found" };
        const st = { ...students[ix] };
        if (!st.syllabus) st.syllabus = {};
        st.syllabus[itemKey] = isCompleted;
        st.syllabusLastUpdate = getISTDateString();
        students[ix] = st;
        await saveNanbanPartial(tenantId, { students });
        return { status: "success", syllabus: st.syllabus };
      }

      case "sendPaymentReminderAction": {
        const studentId = args[0];
        const adminName = String(args[1] || "");
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", msg: "Student not found" };
        const bal =
          (parseInt(s.totalFee, 10) || 0) - (parseInt(s.advance, 10) || 0) - (parseInt(s.discount, 10) || 0);
        if (bal <= 0) return { status: "error", msg: "இவருக்கு பேலன்ஸ் ஏதும் இல்லை." };
        const cfg = nanbanTemplateCfg_(snap);
        const wa = waE164_(s.phone);
        if (wa) {
          try {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: "",
                messageType: "template",
                template: {
                  name: cfg.paymentReminderTemplate || "payment_reminder_nds",
                  languageCode: "ta",
                  bodyParams: [String(s.name || "நண்பரே"), String(bal)]
                },
                metadata: { kind: "payment_reminder", student_id: String(studentId) }
              },
              { delaySeconds: 0 }
            );
          } catch (e) {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: `🔔 *கட்டண நினைவூட்டல் (Reminder)*\n\nவணக்கம் ${s.name},\nஉங்கள் ஓட்டுநர் பயிற்சி கட்டணத்தில் ₹${bal} நிலுவையில் உள்ளது.\n\nநன்றி! 🙏\n- நண்பன் டிரைவிங் ஸ்கூல்`,
                messageType: "text",
                metadata: { kind: "payment_reminder_fb", student_id: String(studentId) }
              },
              { delaySeconds: 0 }
            );
          }
          const upiId = cfg.businessUpi || "";
          const link = upiLink_(bal, s.name, upiId);
          if (link) {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: `💳 *சுலபமாக பணம் செலுத்த (Online UPI):*\n\n👉 ${link}\n\n🆔 *${upiId}*`,
                messageType: "text",
                metadata: { kind: "payment_upi", student_id: String(studentId) }
              },
              { delaySeconds: 2 }
            );
          }
        }
        let students = Array.isArray(snap.students) ? snap.students.map((x) => ({ ...x })) : [];
        const ix = students.findIndex((x) => String(x.id) === String(studentId));
        if (ix >= 0) {
          const st = { ...students[ix] };
          if (!Array.isArray(st.adminRemarks)) st.adminRemarks = [];
          st.adminRemarks.unshift({ date: getISTDateString(), text: `🔔 Payment Reminder அனுப்பப்பட்டது (${adminName})` });
          students[ix] = st;
          await saveNanbanPartial(tenantId, { students });
        }
        return { status: "success", msg: "மெசேஜ் அனுப்பப்பட்டது!" };
      }

      case "bulkSendRemindersAction": {
        const studentIds = args[0];
        const type = args[1];
        if (!Array.isArray(studentIds) || !studentIds.length) return { status: "error", msg: "No students selected." };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const list = snap.students || [];
        let sentCount = 0;
        let failCount = 0;
        let delay = 0;
        for (const id of studentIds) {
          const s = list.find((x) => String(x.id) === String(id));
          if (!s || !s.phone) {
            failCount++;
            continue;
          }
          const wa = waE164_(s.phone);
          if (!wa) {
            failCount++;
            continue;
          }
          delay += 2;
          try {
            if (type === "today_30") {
              await enqueueWaOutboundSend(
                {
                  tenantId: TENANT_DEFAULT,
                  to: wa,
                  message: "",
                  messageType: "template",
                  template: { name: "llr_30_days_reminder", languageCode: "ta", bodyParams: [String(s.name || "-")] },
                  metadata: { kind: "bulk_llr30", student_id: String(id) }
                },
                { delaySeconds: delay }
              );
            } else {
              const txt =
                "உங்கள் LLR இன்னும் சில நாட்களில் காலாவதியாக உள்ளது. தயவுசெய்து புதுப்பிக்கவும்.";
              await enqueueWaOutboundSend(
                {
                  tenantId: TENANT_DEFAULT,
                  to: wa,
                  message: "",
                  messageType: "template",
                  template: { name: "bulk_announcement", languageCode: "ta", bodyParams: [txt] },
                  metadata: { kind: "bulk_llr_exp", student_id: String(id) }
                },
                { delaySeconds: delay }
              );
            }
            sentCount++;
          } catch (e) {
            failCount++;
          }
        }
        return { status: "success", msg: `${sentCount} மெசேஜ்கள் அனுப்பப்பட்டன. (தோல்வி: ${failCount})` };
      }

      case "syncOldStudentsData": {
        return { status: "success", msg: "பழைய டேட்டா சின்க் செய்யப்பட்டது! (Firebase native)" };
      }

      case "processReTestUpdate": {
        const studentId = args[0];
        const testFee = parseInt(args[1], 10) || 0;
        const advancePaid = parseInt(args[2], 10) || 0;
        const newDate = args[3];
        const adminName = String(args[4] || "");
        const today = getISTDateString();
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let students = Array.isArray(snap.students) ? snap.students.map((x) => ({ ...x })) : [];
        const ix = students.findIndex((x) => String(x.id) === String(studentId));
        if (ix < 0) return { status: "error" };
        const s = { ...students[ix] };
        s.totalFee = (parseInt(s.totalFee, 10) || 0) + testFee;
        if (advancePaid > 0) {
          if (!Array.isArray(s.paymentHistory)) s.paymentHistory = [];
          const dup = s.paymentHistory.find(
            (p) =>
              p &&
              p.date === today &&
              parseInt(p.amount, 10) === advancePaid &&
              String(p.note || "").includes("Re-Test") &&
              String(p.note || "").includes(adminName)
          );
          if (dup) return { status: "error", message: "Duplicate payment detected" };
          s.advance = (parseInt(s.advance, 10) || 0) + advancePaid;
          s.paymentHistory.unshift({ date: today, amount: advancePaid, note: `Re-Test கட்டணம் (${adminName})` });
        }
        s.testDate = newDate;
        s.testStatus = "Pending";
        s.status = "Ready_for_Test";
        if (!Array.isArray(s.adminRemarks)) s.adminRemarks = [];
        s.adminRemarks.unshift({ date: today, text: `🔄 Re-Test பதிவு: ₹${testFee}. தேதி: ${newDate}` });
        students[ix] = s;
        await saveNanbanPartial(tenantId, { students });
        await notifyAdminsText(tenantId, `🔄 *Re-Test பதிவு:*\nமாணவர்: ${s.name}\nகட்டணம்: ₹${testFee}\nதேதி: ${newDate}`);
        const wa = waE164_(s.phone);
        if (wa) {
          const cfg = nanbanTemplateCfg_(snap);
          const dateLabel = String(newDate || "").split("-").length === 3 ? `${newDate.split("-")[2]}/${newDate.split("-")[1]}/${newDate.split("-")[0]}` : String(newDate || "-");
          try {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: "",
                messageType: "template",
                template: {
                  name: cfg.rtoTemplate || "rto_test_reminder",
                  languageCode: "ta",
                  bodyParams: [String(s.name || "-"), String(dateLabel || "-"), String(cfg.inspectorTime || "-")]
                },
                metadata: { kind: "retest_rto", student_id: String(studentId) }
              },
              { delaySeconds: 0 }
            );
          } catch (e) {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: `🎯 RTO டெஸ்ட் நினைவூட்டல்\n\nவணக்கம் ${s.name},\nடெஸ்ட் தேதி: ${newDate}\n- நண்பன் டிரைவிங் ஸ்கூல்`,
                messageType: "text",
                metadata: { kind: "retest_rto_fb", student_id: String(studentId) }
              },
              { delaySeconds: 0 }
            );
          }
        }
        return { status: "success" };
      }

      case "processFundTransfer": {
        const fromPerson = args[0];
        const toPerson = args[1];
        const amt = parseInt(args[2], 10) || 0;
        const desc = String(args[3] || "");
        const loggedBy = String(args[4] || "");
        const d = getISTDateString();
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const expenses = Array.isArray(snap.expenses) ? [...snap.expenses] : [];
        expenses.unshift({
          date: d,
          spender: fromPerson,
          cat: "🔄 பணப் பரிமாற்றம் (Out)",
          amt,
          desc: `To ${toPerson}: ${desc} (By ${loggedBy})`
        });
        expenses.unshift({
          date: d,
          spender: toPerson,
          cat: "🔄 பணப் பரிமாற்றம் (In)",
          amt,
          desc: `From ${fromPerson}: ${desc} (By ${loggedBy})`
        });
        await saveNanbanPartial(tenantId, { expenses });
        await notifyAdminsText(
          tenantId,
          `🔄 *பணப் பரிமாற்றம் (Transfer)*\n\nகொடுத்தவர்: ${fromPerson}\nபெற்றவர்: ${toPerson}\nதொகை: ₹${amt}\nவிவரம்: ${desc}\nபதிவு: ${loggedBy}`
        );
        return { status: "success" };
      }

      case "saveChitGroup": {
        const gObj = args[0] || {};
        const { chit } = await loadNanbanChit(tenantId);
        const idToUse = gObj.id || String(Date.now());
        const row = {
          id: idToUse,
          name: gObj.name,
          total: gObj.total,
          months: gObj.months,
          members: gObj.members,
          status: gObj.status || "Active",
          ranjithQuota: parseInt(gObj.rQuota, 10) || 0,
          nandhaQuota: parseInt(gObj.nQuota, 10) || 0,
          companyQuota: parseInt(gObj.cQuota, 10) || 0
        };
        const groups = Array.isArray(chit.groups) ? [...chit.groups] : [];
        const gi = groups.findIndex((g) => String(g.id) === String(idToUse));
        if (gi >= 0) groups[gi] = row;
        else groups.push(row);
        chit.groups = groups;
        await persistChit(tenantId, chit);
        return { status: "success" };
      }

      case "saveChitAuction": {
        const auctionObj = args[0] || {};
        const isOldHistory = !!args[1];
        const { snap, chit } = await loadNanbanChit(tenantId);
        const d = getISTDateString();
        const id = String(Date.now());
        const expensesAmt = parseInt(auctionObj.expenses, 10) || 0;
        const commission = parseInt(auctionObj.commission, 10) || 0;
        const netProfit = commission - expensesAmt;
        const auctionStatus = isOldHistory ? "Settled" : "Active";
        const row = {
          id,
          group: auctionObj.group,
          month: auctionObj.monthNo,
          date: d,
          winner: auctionObj.winner,
          interestRate: auctionObj.interestRate || "0",
          discount: auctionObj.discount,
          commission: auctionObj.commission,
          perHead: auctionObj.perHead,
          status: auctionStatus,
          bidders: auctionObj.bidders || "",
          expenses: String(expensesAmt),
          netProfit: String(netProfit)
        };
        chit.auctions.unshift(row);
        if (isOldHistory) {
          const members = chit.members.filter((m) => m.group === auctionObj.group);
          const perHead = parseInt(auctionObj.perHead, 10) || 0;
          for (const m of members) {
            chit.payments.unshift({
              id: String(Date.now() + Math.floor(Math.random() * 1000)),
              auctionId: id,
              memberName: m.name,
              phone: m.phone || "",
              amount: perHead,
              receiver: "Historical Entry",
              date: d
            });
          }
          await persistChit(tenantId, chit);
        } else {
          let expensesList = Array.isArray(snap.expenses) ? [...snap.expenses] : [];
          if (commission > 0) {
            expensesList.unshift({
              date: d,
              spender: "Office",
              cat: "🟢 வரவு - சீட்டு கமிஷன்",
              amt: commission,
              desc: `${auctionObj.group} (Month ${auctionObj.monthNo})`
            });
          }
          if (expensesAmt > 0) {
            expensesList.unshift({
              date: d,
              spender: "Office",
              cat: "🔴 செலவு - சீட்டு செலவு",
              amt: expensesAmt,
              desc: `${auctionObj.group} (Month ${auctionObj.monthNo})`
            });
          }
          chit.bids = [];
          await saveNanbanPartial(tenantId, { chitData: chit, expenses: expensesList });
          const cfg = nanbanTemplateCfg_(snap);
          const members = chit.members.filter((m) => m.group === auctionObj.group);
          const winMsg = `📢 *சீட்டு ஏல முடிவு - Month ${auctionObj.monthNo}*\n\nகுழு: ${auctionObj.group}\n🏆 ஏலம் எடுத்தவர்: *${auctionObj.winner || "தகவல் இல்லை"}*\nதள்ளுபடி: ₹${auctionObj.discount}\n\nஇந்த மாதம் ஒவ்வொருவரும் கட்ட வேண்டிய தொகை: *₹${auctionObj.perHead}*\n\nதயவுசெய்து தொகையைச் செலுத்தவும். 🙏`;
          let delay = 0;
          for (const m of members) {
            const wa = waE164_(m.phone);
            if (!wa) continue;
            delay += 2;
            try {
              await enqueueWaOutboundSend(
                {
                  tenantId: TENANT_DEFAULT,
                  to: wa,
                  message: "",
                  messageType: "template",
                  template: {
                    name: cfg.chitAuctionTemplate || "chit_auction_alert",
                    languageCode: "ta",
                    bodyParams: [String(auctionObj.group || "-"), String(d || "-")]
                  },
                  metadata: { kind: "chit_auction", auction_id: id }
                },
                { delaySeconds: delay }
              );
            } catch (e) {
              await enqueueWaOutboundSend(
                {
                  tenantId: TENANT_DEFAULT,
                  to: wa,
                  message: winMsg,
                  messageType: "text",
                  metadata: { kind: "chit_auction_fb", auction_id: id }
                },
                { delaySeconds: delay }
              );
            }
          }
        }
        return { status: "success", auctionId: id };
      }

      case "settleAuctionWinner": {
        const auctionId = args[0];
        const { chit } = await loadNanbanChit(tenantId);
        const auctions = chit.auctions.map((a) => ({ ...a }));
        const ix = auctions.findIndex((a) => String(a.id) === String(auctionId));
        if (ix < 0) return { status: "error", message: "Auction not found" };
        if (String(auctions[ix].status) === "Settled") return { status: "error", message: "Already settled" };
        auctions[ix] = { ...auctions[ix], status: "Settled" };
        chit.auctions = auctions;
        await persistChit(tenantId, chit);
        await notifyAdminsText(tenantId, `🤝 *சீட்டு பட்டுவாடா நிறைவு*\n\nஏல எண்: ${auctionId}\nநிலை: Settled`);
        return { status: "success" };
      }

      case "deleteChitAuction": {
        const auctionId = args[0];
        const { chit } = await loadNanbanChit(tenantId);
        const before = chit.auctions.length;
        chit.auctions = chit.auctions.filter((a) => String(a.id) !== String(auctionId));
        if (chit.auctions.length === before) return { status: "error", message: "Auction not found" };
        await persistChit(tenantId, chit);
        return { status: "success" };
      }

      case "saveChitPayment": {
        const payObj = args[0] || {};
        payObj.phone = normPhone10(payObj.phone);
        if (payObj.phone && String(payObj.phone).length !== 10) {
          return { status: "error", message: "Invalid phone (10-digit required)" };
        }
        const { chit } = await loadNanbanChit(tenantId);
        const id = String(Date.now());
        const d = getISTDateString();
        chit.payments.unshift({
          id,
          auctionId: payObj.auctionId,
          memberName: payObj.memberName,
          amount: payObj.amount,
          receiver: payObj.receiver,
          date: d,
          phone: payObj.phone || ""
        });
        await persistChit(tenantId, chit);
        const wa = waE164_(payObj.phone);
        if (wa) {
          const msg = `💰 வணக்கம் ${payObj.memberName},\nஉங்களின் இந்த மாத சீட்டுத் தொகை ₹${payObj.amount} பெறப்பட்டது. (வசூலர்: ${payObj.receiver})\nநன்றி! 🙏\n- நண்பன் சீட்டு நிறுவனம்`;
          await enqueueWaOutboundSend(
            {
              tenantId: TENANT_DEFAULT,
              to: wa,
              message: msg,
              messageType: "text",
              metadata: { kind: "chit_payment", payment_id: id }
            },
            { delaySeconds: 0 }
          );
        }
        await notifyAdminsText(
          tenantId,
          `💰 *சீட்டு வசூல்!*\n\nமெம்பர்: ${payObj.memberName}\nதொகை: ₹${payObj.amount}\nபெற்றவர்: ${payObj.receiver}`
        );
        return { status: "success" };
      }

      case "sendChitBulkAlert": {
        const phonesArray = args[0] || [];
        const msgTemplate = String(args[1] || "");
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const cfg = nanbanTemplateCfg_(snap);
        let successCount = 0;
        let delay = 0;
        for (let i = 0; i < phonesArray.length; i++) {
          const item = phonesArray[i];
          const phone = typeof item === "object" && item ? item.phone : item;
          const name = typeof item === "object" && item ? item.name : "";
          const group = typeof item === "object" && item ? item.group : "";
          if (!phone) continue;
          delay += 2;
          const wa = waE164_(phone);
          if (!wa) continue;
          try {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: "",
                messageType: "template",
                template: {
                  name: cfg.chitDueTemplate || "chit_due_reminder",
                  languageCode: "ta",
                  bodyParams: [String(name || "நண்பரே"), String(group || "சீட்டு குழு")]
                },
                metadata: { kind: "chit_bulk_due" }
              },
              { delaySeconds: delay }
            );
          } catch (e) {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: msgTemplate,
                messageType: "text",
                metadata: { kind: "chit_bulk_due_fb" }
              },
              { delaySeconds: delay }
            );
          }
          successCount++;
        }
        return { status: "success", msg: `${successCount} பேருக்கு மெசேஜ் அனுப்பப்பட்டது!` };
      }

      case "triggerLiveChitBidding": {
        const groupName = args[0];
        const { chit } = await loadNanbanChit(tenantId);
        const members = chit.members.filter((m) => m.group === groupName);
        const bidMsg = `📢 *சீட்டு ஏலம் ஆரம்பம்!*\n\nகுழு: ${groupName}\n\nஉங்களின் ஏலத் தொகையை (எ.கா: 15000) வாட்ஸ்அப்பில் ரிப்ளை செய்யவும். ஏலம் அரை மணி நேரத்தில் முடிவடையும்.`;
        let count = 0;
        let delay = 0;
        for (const m of members) {
          const wa = waE164_(m.phone);
          if (!wa) continue;
          delay += 2;
          await enqueueWaOutboundSend(
            { tenantId: TENANT_DEFAULT, to: wa, message: bidMsg, messageType: "text", metadata: { kind: "chit_live_bid", group: String(groupName) } },
            { delaySeconds: delay }
          );
          count++;
        }
        return { status: "success", msg: `${count} பேருக்கு ஏல அறிவிப்பு சென்றது!` };
      }

      case "sendChitAdvanceAlert": {
        const groupName = args[0];
        const dateText = String(args[1] || "");
        const note = String(args[2] || "");
        const rawDate = String(args[3] || "");
        const { chit } = await loadNanbanChit(tenantId);
        const d = getISTDateString();
        if (rawDate) {
          const sched = Array.isArray(chit.schedule) ? [...chit.schedule] : [];
          const si = sched.findIndex((r) => (r.groupName || r[0]) === groupName);
          const row =
            typeof sched[0] === "object" && !Array.isArray(sched[0])
              ? { groupName, displayDate: dateText, rawDate, savedOn: d }
              : [groupName, dateText, rawDate, d];
          if (si >= 0) sched[si] = row;
          else sched.push(row);
          chit.schedule = sched;
          await persistChit(tenantId, chit);
        }
        const pastAuctions = chit.auctions.filter((a) => a.group === groupName);
        const nextAucNo = pastAuctions.length + 1;
        const aucNoStr = `${nextAucNo}வது`;
        const noteStr = note ? `\n\n📌 குறிப்பு: ${note}` : "";
        const members = chit.members.filter((m) => m.group === groupName);
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const cfg = nanbanTemplateCfg_(snap);
        let count = 0;
        let delay = 0;
        for (const m of members) {
          const wa = waE164_(m.phone);
          if (!wa) continue;
          delay += 2;
          const personalMsg = `வணக்கம் ${m.name}!\n\n*${groupName}* - ${aucNoStr} ஏலம் ${dateText} நடைபெறுகிறது.${noteStr}\n\n- நண்பன் சீட்டு`;
          try {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: "",
                messageType: "template",
                template: {
                  name: "chit_auction_alert",
                  languageCode: "ta",
                  bodyParams: [String(m.name || "-"), String(groupName || "-"), String(dateText || "-")]
                },
                metadata: { kind: "chit_prealert", group: String(groupName) }
              },
              { delaySeconds: delay }
            );
          } catch (e) {
            await enqueueWaOutboundSend(
              {
                tenantId: TENANT_DEFAULT,
                to: wa,
                message: personalMsg,
                messageType: "text",
                metadata: { kind: "chit_prealert_fb", group: String(groupName) }
              },
              { delaySeconds: delay }
            );
          }
          count++;
        }
        return { status: "success", msg: `${count} பேருக்கு அறிவிப்பு சென்றது!` };
      }

      case "getMemberChitPassbook": {
        const name = args[0];
        const { chit } = await loadNanbanChit(tenantId);
        return buildMemberChitPassbook(name, chit);
      }

      case "fixAllHistoricalChitPayments": {
        const { chit } = await loadNanbanChit(tenantId);
        const { chit: fixed, fixes } = fixHistoricalChitPayments(chit);
        await persistChit(tenantId, fixed);
        return { status: "success", message: `${fixes} payments were fixed and marked as Paid! ✅` };
      }

      case "saveCashOpeningAction": {
        const monthKey = String(args[0] || "").trim();
        const ranjithAmt = args[1];
        const nandhaAmt = args[2];
        const officeAmt = args[3];
        const loggedBy = String(args[4] || "");
        if (!monthKey) return { status: "error", message: "Month required" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const appSettings = snap.appSettings && typeof snap.appSettings === "object" ? { ...snap.appSettings } : {};
        if (!appSettings.appSettings) appSettings.appSettings = {};
        if (!appSettings.appSettings.cashOpeningByMonth) appSettings.appSettings.cashOpeningByMonth = {};
        appSettings.appSettings.cashOpeningByMonth[monthKey] = {
          ranjith: parseInt(ranjithAmt, 10) || 0,
          nandha: parseInt(nandhaAmt, 10) || 0,
          office: parseInt(officeAmt, 10) || 0,
          by: loggedBy
        };
        await saveNanbanPartial(tenantId, { appSettings });
        return { status: "success" };
      }

      case "getAuditLogAction":
      case "getFilingEntriesAction":
        return { status: "success", items: [], message: "Native mode: audit/filing logs are not migrated from Sheets. Use Firestore console if needed." };

      case "generateFilingIndexPdfAction":
      case "generateFullAuditPdfAction":
      case "generateMonthlyPdfPackAction":
      case "generateMonthlyCashbookPdfAction":
        return {
          status: "error",
          message:
            "PDF reports are not generated on Firebase (legacy GAS used Google Drive). Export data from Firestore or reintroduce a PDF Cloud Function later."
        };

      case "testWaTemplatePreview": {
        const { sanitizeTemplateParamText } = require("../lib/sanitizeTemplateParam");
        const { getTenantWaTemplateRegistry } = require("./waTemplateConfig");
        const {
          normalizeServiceKey,
          buildEnquiryWelcomTwoParams,
          buildEnquiryStyleTemplateParams
        } = require("./dynamicPricingEngine");

        const kind = String(args[0] || "").trim();
        let raw = String(args[1] || "").replace(/\D/g, "");
        if (raw.length === 10) raw = `91${raw}`;
        if (raw.length < 12) return { status: "error", message: "Enter valid 10-digit mobile" };

        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const cfg = nanbanTemplateCfg_(snap);
        const name = "சோதனை மாணவர்";
        const dateStr = getISTDateString();
        const tid = tenantId || TENANT_DEFAULT;

        const sendTpl = async (tplName, lang, bodyParams, fallbackText) => {
          const nm = String(tplName || "").trim();
          const bp = (bodyParams || []).map((p) => sanitizeTemplateParamText(p));
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: raw,
              message: String(fallbackText || ""),
              messageType: nm ? "template_with_text_fallback" : "text",
              template: nm
                ? { name: nm, languageCode: lang || "ta", bodyParams: bp, tryFirst: true }
                : null,
              metadata: { kind: "admin_template_test", test_kind: kind }
            },
            { delaySeconds: 0 }
          );
        };

        if (kind === "welcome") {
          const tpl = cfg.welcomeTemplate || "welcome_admission";
          const line = "2 வீலர் லைசென்ஸ் அட்மிஷன்";
          const fb =
            `வாழ்த்துக்கள் ${name}! 🎉\nஅட்மிஷன் பதிவு (சோதனை). தேதி: ${dateStr}\nமொத்தம் ₹15000 · முன்பணம் ₹5000 · மீதம் ₹10000`;
          await sendTpl(tpl, "ta", [name, line, dateStr, "15000", "5000", "10000"], fb);
        } else if (kind === "enquiry") {
          const reg = await getTenantWaTemplateRegistry(tid);
          const slots = reg.enquiry.bodyParamCount;
          const selected = [normalizeServiceKey("2 வீலர்")];
          let bodyParams;
          if (slots === 4) bodyParams = buildEnquiryStyleTemplateParams(selected, name);
          else if (slots === 2) bodyParams = buildEnquiryWelcomTwoParams(selected, name);
          else bodyParams = [name];
          const tplName = String(reg.enquiry.name || cfg.enquiryTemplate || "enquiry_welcom").trim();
          const fb = `வணக்கம் ${name}! 🙏 விசாரணை பதிவு (சோதனை).`;
          await sendTpl(tplName, reg.enquiry.language || "ta", bodyParams, fb);
        } else if (kind === "fee_2w") {
          const tpl = cfg.welcomeTemplate || "welcome_admission";
          const line = "2 வீலர் லைசென்ஸ் அட்மிஷன்";
          const fb = `சோதனை: ${line} · மீதம் ₹8000`;
          await sendTpl(tpl, "ta", [name, line, dateStr, "12000", "4000", "8000"], fb);
        } else if (kind === "fee_4w") {
          const tpl = cfg.welcomeTemplate || "welcome_admission";
          const line = "4 வீலர் லைசென்ஸ் அட்மிஷன்";
          const fb = `சோதனை: ${line} · மீதம் ₹12000`;
          await sendTpl(tpl, "ta", [name, line, dateStr, "20000", "8000", "12000"], fb);
        } else if (kind === "llr_reminder") {
          const tpl = cfg.rtoReminderTemplate || "rto_test_reminder";
          const ins = String(cfg.inspectorTime || "காலை 10:30");
          const fb = `வணக்கம் ${name}! RTO டெஸ்ட் நினைவூட்டல் (சோதனை). ${dateStr} ${ins}`;
          await sendTpl(tpl, "ta", [name, dateStr, ins], fb);
        } else if (kind === "quiz_sample") {
          const { getQuizBankRows } = require("./snapshotStore");
          const { QUIZ_Q: QQ, resolveQuizCorrectChoiceNo } = require("../lib/quizRowUtils");
          const rows = await getQuizBankRows();
          let qRow = null;
          for (const row of rows) {
            if (Array.isArray(row) && parseInt(row[QQ.day], 10) === 1) {
              qRow = row;
              break;
            }
          }
          if (!qRow && rows.length) qRow = rows[0];
          const titlePrefix = "🧪 TEST QUIZ";
          const optA = String((qRow && qRow[QQ.o1]) || "விடை A");
          const optB = String((qRow && qRow[QQ.o2]) || "விடை B");
          const optC = String((qRow && qRow[QQ.o3]) || "விடை C");
          const qText = String((qRow && qRow[QQ.ques]) || "சோதனை வினா");
          const correctNo = qRow ? resolveQuizCorrectChoiceNo(qRow) : 2;
          const tplQuiz = String(cfg.quizTemplate || "daily_quiz_btn").trim();
          const fb =
            `${titlePrefix}\n\n${qText}\n\n1️⃣ ${optA}\n2️⃣ ${optB}\n3️⃣ ${optC}\n\n(டெம்ப்ளேட் இல்லை எனில் உரை மட்டும் அனுப்பப்பட்டது.)`;
          /** daily_quiz_btn: {{1}} day, {{2}} name, {{3}}–{{6}} question + three options */
          await sendTpl(tplQuiz, "ta", ["1", name, qText, optA, optB, optC], fb);

          const phone10Quiz = normPhone10(raw);
          const snapQ = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
          let stu = Array.isArray(snapQ.students) ? snapQ.students.map((x) => ({ ...x })) : [];
          const stuIdx = stu.findIndex((s) => normPhone10(s.phone) === phone10Quiz);
          if (stuIdx >= 0) {
            const st = { ...stu[stuIdx] };
            if (!Array.isArray(st.quizPendingQueue)) st.quizPendingQueue = [];
            st.quizPendingQueue.push({ quizDay: 1, correctNo });
            if (st.quizPendingQueue.length > 15) st.quizPendingQueue = st.quizPendingQueue.slice(-15);
            stu[stuIdx] = st;
            await saveNanbanPartial(tenantId, { students: stu });
          }
        } else if (kind === "chit_alert") {
          const tpl = cfg.chitDueTemplate || "chit_due_reminder";
          const fb = `சோதனை: சீட்டு நினைவூட்டல் — ${name}`;
          await sendTpl(tpl, "ta", [name, "1 லட்சம் சீட்டு குழு"], fb);
        } else if (kind === "day_close") {
          const tpl = cfg.dayCloseTemplate || "day_close_report";
          const sampleReport =
            `🏁 *DAY CLOSE (சோதனை)*\n📅 ${dateStr}\n🚗 KM: 25\n💰 வசூல்: ₹5000\n🔴 செலவு: ₹200\n🤝 ஒப்படைப்பு: ₹4800`;
          const headerImg = normalizeMetaWhatsappMediaLink_(String(cfg.quizHeaderImageUrl || "").trim());
          const tPayload = {
            name: tpl,
            languageCode: "ta",
            bodyParams: [sanitizeTemplateParamText(sampleReport)],
            tryFirst: true
          };
          if (isUsableWaMediaUrl(headerImg)) tPayload.headerImageLink = headerImg;
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: raw,
              message: sampleReport,
              messageType: "template_with_text_fallback",
              template: tPayload,
              metadata: { kind: "admin_template_test", test_kind: kind }
            },
            { delaySeconds: 0 }
          );
        } else {
          return { status: "error", message: `Unknown test kind: ${kind}` };
        }

        return { status: "success", message: `Queued WhatsApp test: ${kind}` };
      }

      case "uploadFileToDrive":
      case "processFileUpload": {
        const dataUrl = args[0];
        const name = String(args[1] || "document.bin");
        const dec = decodeDataUrlForUpload_(dataUrl);
        if (!dec || !dec.buf || !dec.buf.length) {
          console.error("[NANBAN_RPC_UPLOAD] Invalid or empty file data (decode failed)");
          return { status: "error", message: "Invalid or empty file data" };
        }
        if (dec.buf.length > 12 * 1024 * 1024) {
          console.error(`[NANBAN_RPC_UPLOAD] Too large: ${dec.buf.length} bytes`);
          return { status: "error", message: "File too large (max 12 MB)" };
        }
        try {
          const { url, id } = await uploadBufferToDefaultBucket_({
            buf: dec.buf,
            contentType: dec.contentType,
            rawName: name
          });
          return { status: "success", url, id };
        } catch (upErr) {
          const msg = String(upErr?.message || upErr);
          console.error(`[NANBAN_RPC_UPLOAD_EXCEPTION] ${msg}`);
          return {
            status: "error",
            message: `Storage upload failed: ${msg}. Check Cloud Function service account has Storage Object Admin (or Creator+Viewer) on the default bucket, or set NANBAN_STORAGE_BUCKET.`
          };
        }
      }

      default:
        return {
          status: "error",
          message: `Native RPC not implemented: ${act}. Use Firebase snapshot API or extend erpRpcDispatch.js.`,
          action: act
        };
    }
  } catch (e) {
    if (e && e.code === "trial_expired") {
      return {
        status: "error",
        code: "trial_expired",
        message: "Your free trial has ended. Please subscribe to continue editing data."
      };
    }
    const em = String(e && e.message ? e.message : e);
    console.error(`NANBAN_ERP_RPC_TOP_CATCH action=${act} ${em}`);
    return { status: "error", message: em };
  }
}

module.exports = { handleErpRpc, popTenantFromArgs };
