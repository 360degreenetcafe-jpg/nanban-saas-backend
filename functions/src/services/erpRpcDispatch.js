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
const { notifyAdminsText, notifyTrainersText } = require("./adminNotify");
const {
  ESEVAI_ALERT_TENANT_ID,
  finalizeInnerAppSettingsAdmin_,
  stableSerializeAdminConfigsInner,
  migrateInnerToAdminConfigsArray,
  getPrimaryPhone10ForAdminOtp
} = require("./adminPhoneResolve");
const {
  enqueueWaOutboundSend,
  isUsableWaMediaUrl,
  normalizeMetaWhatsappMediaLink_
} = require("./waOutboundQueue");
const { sanitizeTemplateParamText } = require("../lib/sanitizeTemplateParam");
const { loadTenantMessagingBrand, tamilReasonFromWaError } = require("./tenantMessagingBrand");
const { studentPassportUrl_, enqueuePdfOrText_ } = require("./waPdfOutbound");
const { resolveLlrDocumentUrlForWhatsApp_, uploadPdfGetSignedUrl } = require("./waPdfStorage");
const { buildPassportSummaryPdf } = require("./nanbanPdfService");
const { inferJobKindFromStudent, notifyNanbanAfterStudentWrite_ } = require("./waNativeJobProcessor");
const {
  normalizeChit,
  buildMemberChitPassbook,
  fixHistoricalChitPayments
} = require("./nanbanChitFirestore");
const { getPendingRtoChecklistLines } = require("../lib/rtoChecklistDef");
const {
  getTenantWaTemplateRegistry,
  buildFeeSummaryTemplateObject,
  buildLlr30dWaTemplate,
  buildPaymentReminderWaTemplate
} = require("./waTemplateConfig");
const {
  syncStudentToGoogleContacts,
  createGoogleOAuthConsentUrl,
  contactSyncOutcomeForRpc,
  syncNanbanTeamProfileToGoogleContacts
} = require("./contactSyncService");
const {
  generateHostedCashbookPdf,
  generateHostedFullAuditPdf,
  generateHostedFilingIndexPdf
} = require("./nanbanHostedReportsPdf");
const { appendNanbanFilingEntryQuiet, listNanbanFilingEntries } = require("./nanbanFilingLogStore");
const { normalizeConfig_: normalizeStudentWaConfig_ } = require("../lib/studentWaSmartReplies");
const {
  extractLlrFieldsFromBuffer_,
  extractLlrFieldsFromPdfBuffer_,
  isPdfMagicBytes_
} = require("../lib/llrVisionExtract");

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

/**
 * Plain-text WhatsApp body for Daily Class Report (used as automatic fallback if Meta rejects the template).
 * Structure: Name, Date, Vehicle/service, cumulative class #, sessions today, activity, KMs, next session.
 *
 * Keep this aligned with `bodyParams` sent to Meta (same facts, same order of ideas).
 * Meta template {{2}} is cumulative classes completed (1–15), not “sessions logged today” (often 1).
 */
function formatDailyClassReportPlainText_({
  name,
  dateIst,
  vehicleLabel,
  sessionsLoggedToday,
  totalClassesCompleted,
  activity,
  nextSyllabus,
  trainer,
  runKm
}) {
  const nm = String(name || "-").trim() || "-";
  const dt = String(dateIst || "-").trim() || "-";
  const veh = String(vehicleLabel || "").trim() || "—";
  const tot =
    totalClassesCompleted !== undefined && totalClassesCompleted !== null
      ? String(totalClassesCompleted).trim()
      : "—";
  const sess =
    sessionsLoggedToday !== undefined && sessionsLoggedToday !== null
      ? String(sessionsLoggedToday).trim()
      : "—";
  const act = String(activity || "—").trim() || "—";
  const next = String(nextSyllabus || "—").trim() || "—";
  const tr = String(trainer || "").trim();
  const kmLine =
    runKm !== undefined && runKm !== null && String(runKm).trim() !== "" ? String(runKm).trim() : "—";
  return (
    `📋 *Daily class report*\n` +
    `Name: ${nm}\n` +
    `Date: ${dt}\n` +
    `Vehicle / service: ${veh}\n` +
    `Class # (total completed): ${tot}\n` +
    `Session(s) logged today: ${sess}\n` +
    `Activity: ${act}\n` +
    `KMs: ${kmLine}\n` +
    `Remarks / next session:\n${next}` +
    (tr ? `\n\nTrainer: ${tr}` : "")
  );
}

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

/** Settings → PVR Processing Fee for receipt breakdown when student row has no amount. */
function schoolPvrFeeDefaultFromSnap_(snap) {
  const inner = drivingSchoolInnerFromSnap_(snap);
  const n = parseInt(inner.pvrFee, 10);
  return Number.isFinite(n) && n >= 0 ? n : 500;
}

/** Extra fee-split line for admin WA (₹1–₹5 “noise” vs vehicle slips). */
function feeSplitAdminDetail_(s) {
  const fs = s && s.feeSplit && typeof s.feeSplit === "object" ? s.feeSplit : {};
  const a = parseInt(fs.llr, 10) || 0;
  const b = parseInt(fs.train, 10) || 0;
  const c = parseInt(fs.test, 10) || 0;
  if (!a && !b && !c) return "";
  return `\n💳 LLR / Train / Test: ₹${a} · ₹${b} · ₹${c}\n_₹1–₹5 gaps vs slips: treat as rounding in reports; Firestore split is source of truth._`;
}

function summaryStudentWatchForAdminNotify_(o) {
  if (!o || typeof o !== "object") return {};
  const fs = o.feeSplit && typeof o.feeSplit === "object" ? o.feeSplit : {};
  return {
    name: String(o.name || ""),
    phone: String(o.phone || ""),
    status: String(o.status || ""),
    testStatus: String(o.testStatus || ""),
    testDate: String(o.testDate || ""),
    totalFee: o.totalFee,
    advance: o.advance,
    discount: o.discount,
    feeSplit: { llr: fs.llr, train: fs.train, test: fs.test },
    type: String(o.type || ""),
    service: String(o.service || ""),
    llrStatus: String(o.llrStatus || ""),
    classesAttended: o.classesAttended,
    dateJoined: String(o.dateJoined || ""),
    pvrStatus: String(o.pvrStatus || ""),
    hasBadge: !!o.hasBadge,
    pvrMode: String(o.pvrMode || "")
  };
}

function stableStudentWatchJson_(o) {
  try {
    return JSON.stringify(summaryStudentWatchForAdminNotify_(o));
  } catch (_e) {
    return "";
  }
}

async function queueRtoPassCertificateWa_(tenantId, student, snap) {
  const wa = waE164_(student.phone);
  if (!wa) return;
  const inner = drivingSchoolInnerFromSnap_(snap);
  const schoolName = String(inner.schoolName || "").trim() || "Nanban Driving School";
  const today = getISTDateString();
  const { buildRtoCertificatePdf } = require("./nanbanPdfService");
  const { enqueuePdfOrText_ } = require("./waPdfOutbound");
  const buf = await buildRtoCertificatePdf({
    studentName: student.name,
    testDate: today,
    schoolName
  });
  const caption = `Congratulations! You passed the RTO test.`;
  const tamilFb = `🎉 வெற்றி பெற்றீர்கள் ${student.name}! ஓட்டுநர் தேர்வில் இன்று சிறப்பாக செயல்பட்டதற்கு வாழ்த்துக்கள்! 🏆 உங்கள் லைசென்ஸ் கார்டு விரைவில் கைக்கு வரும்.`;
  await enqueuePdfOrText_({
    tenantId: String(tenantId || "").trim() || TENANT_DEFAULT,
    to: wa,
    pdfBuffer: buf,
    filename: `RTO_Certificate_${String(student.id)}.pdf`,
    caption,
    textFallback: tamilFb,
    metadata: { kind: "rto_pass_cert", student_id: String(student.id) }
  });
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

function normalizeTeamTenantId_(raw) {
  return String(raw || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
}

function teamTenantMatches_(docTenant, requestTenantId) {
  const a = normalizeTeamTenantId_(docTenant);
  const b = normalizeTeamTenantId_(requestTenantId);
  return !!a && !!b && a === b;
}

function isInactiveTeamMember_(d) {
  const x = d && typeof d === "object" ? d : {};
  return x.inactive_team_member === true;
}

/** One row for hosted UI login / getAppUsers (shape kept in sync with getAppUsers case). */
function hostedUserProfileFromFirestoreDoc_(docId, d) {
  const data = d || {};
  let trialEnds = data.trial_ends_at;
  if (trialEnds && typeof trialEnds.toDate === "function") {
    trialEnds = trialEnds.toDate().toISOString();
  } else if (trialEnds) {
    trialEnds = String(trialEnds);
  } else {
    trialEnds = null;
  }
  let createdAt = null;
  const ca = data.created_at || data.createdAt;
  if (ca && typeof ca.toDate === "function") createdAt = ca.toDate().toISOString();
  return {
    id: String(docId || "").trim(),
    name: String(data.name || "").trim(),
    pin: String(data.pin || "").trim(),
    role: String(data.role || "Staff").trim(),
    phone: String(data.phone || "").trim(),
    email: String(data.email || data.Email || "").trim().toLowerCase(),
    tenant_id: String(data.tenant_id || data.tenantId || "").trim(),
    businesses: Array.isArray(data.businesses) ? data.businesses : [],
    trial_ends_at: trialEnds,
    invited_by: String(data.invitedBy || data.invited_by || "").trim(),
    created_at: createdAt,
    auth_link_pending: data.auth_link_pending === true,
    inactive_team_member: data.inactive_team_member === true
  };
}

/** Merge one row into snapshot `owners` / `admins` arrays by email (idempotent migration helper). */
function upsertSnapshotIdentityRow_(arr, { email, uid, role }) {
  const e = String(email || "")
    .trim()
    .toLowerCase();
  const u = String(uid || "").trim();
  if (!e) return Array.isArray(arr) ? arr : [];
  const base = Array.isArray(arr) ? [...arr] : [];
  const ix = base.findIndex((x) => x && String(x.email || "").trim().toLowerCase() === e);
  const row = { email: e, uid: u, role: String(role || "Owner").trim() };
  if (ix >= 0) base[ix] = Object.assign({}, base[ix], row);
  else base.push(row);
  return base;
}

function isSchoolAdminOrOwnerRole_(roleRaw) {
  const r = String(roleRaw || "")
    .trim()
    .toLowerCase();
  return r === "admin" || r === "owner";
}

/** Bootstrap / registration — use their own Firebase checks; no session gate. */
const RPC_AUTH_EXEMPT_ACTIONS = new Set([
  "bridgePingAction",
  "sendSchoolRegistrationOtp",
  "registerNewDrivingSchool",
  "recordTenantSession",
  "recordSaaSStaffSession",
  "linkInvitedUserToAuthUidAction"
]);

/** Valid Firebase session only — no users/{uid}.tenant_id vs RPC tenant match (needed when UI still defaults to nanban_main). */
const RPC_SESSION_ONLY_ACTIONS = new Set([
  "resolveHostedGoogleProfileAction",
  "resolveHostedGoogleLoginUserAction"
]);

async function assertFirebaseSessionOnly_(idToken) {
  const tok = String(idToken || "").trim();
  if (!tok) return { ok: false, message: "firebase_auth_required" };
  try {
    await admin.auth().verifyIdToken(tok);
    return { ok: true };
  } catch (_e) {
    return { ok: false, message: "invalid_token" };
  }
}

/**
 * Settings, team admin, OTP-gated deletes, E‑Sevai admin, chit control, triggers — require
 * Firebase ID token and users/{uid}.role ∈ { Owner, Admin } plus tenant_id match.
 * Day-to-day student/expense rows (saveExpenseData, updateStudentData, admissions) use
 * tenant membership only (Staff/Trainer) so existing workflows keep working.
 */
const RPC_OWNER_ADMIN_ACTIONS = new Set([
  "saveAppSettings",
  "syncWaTemplatesFromMasterAction",
  "syncOldStudentsData",
  "addTeamMemberAction",
  "updateTeamMemberPinAction",
  "deactivateTeamMemberAction",
  "reactivateTeamMemberAction",
  "getStudentWaAssistantConfigAction",
  "updateStudentWaAssistantConfigAction",
  "sendOtpToExistingAdmin",
  "verifyAdminPhoneEditOtp",
  "linkFormalOwnerToMainTenantAction",
  "requestProfileDeleteOtp",
  "deleteStudentWithOtp",
  "generateGoogleOAuthUrl",
  "testContactSyncAction",
  "processFundTransfer",
  "saveCashOpeningAction",
  /** Nanban hosted reports: financial exports + filing URLs (Staff/Trainer blocked). */
  "generateFilingIndexPdfAction",
  "generateFullAuditPdfAction",
  "generateMonthlyCashbookPdfAction",
  "generateMonthlyPdfPackAction",
  "getFilingEntriesAction",
  "fixAllHistoricalChitPayments",
  "closeESevaiDayAction",
  "saveESevaiCustomerAction",
  "saveESevaiAgentAction",
  "updateESevaiWorkAction",
  "saveESevaiSettingsAction",
  "saveESevaiOpeningBalanceAction",
  "saveESevaiServiceAction",
  "saveESevaiLedgerAction",
  "saveESevaiEnquiryAction",
  "saveESevaiTransactionAction",
  "notifyESevaiCustomerBillWaAction",
  "updateESevaiTransactionAction",
  "processDayCloseHandover",
  "runDailyAdminSummaryNowAction",
  "setupESevaiDeliveryReminderTrigger",
  "setupESevaiAgentLlrReminderTrigger",
  "setupAllDailyTriggers",
  "setupDailyMorningTrigger",
  "setupDailyEveningTrigger",
  "setupChitReminderTrigger",
  "uploadFileToDrive",
  "processReTestUpdate",
  "saveChitGroup",
  "saveChitMember",
  "editChitMemberData",
  "deleteChitMemberData",
  "saveChitPayment",
  "saveChitAuction",
  "settleAuctionWinner",
  "deleteChitAuction",
  "triggerLiveChitBidding",
  "sendChitBulkAlert",
  "sendChitAdvanceAlert",
  "saveRtoServiceAction",
  "saveVehicleAction",
  "saveFleetLogAction",
  "saveFleetFuelLogAction",
  "saveFleetServiceLogAction",
  "updateSaaSTenantBillingAction",
  "recordSaaSPaymentAction"
]);

async function assertFirebaseUserOwnerOrAdminForTenant_(idToken, tenantId) {
  const base = await assertFirebaseUserOwnsTenant_(idToken, tenantId);
  if (!base.ok) return base;
  const udoc = await admin.firestore().collection("users").doc(base.uid).get();
  const role = String(udoc.data()?.role || "")
    .trim()
    .toLowerCase();
  if (!isSchoolAdminOrOwnerRole_(role)) {
    return { ok: false, message: "owner_or_admin_required" };
  }
  return { ok: true, uid: base.uid };
}

/**
 * @param {string} act
 * @param {string} tenantId
 * @param {string} idToken
 * @returns {Promise<{ ok: boolean, message?: string }>}
 */
async function assertRpcAuthGate_(act, tenantId, idToken) {
  if (RPC_AUTH_EXEMPT_ACTIONS.has(act)) {
    return { ok: true };
  }
  const tok = String(idToken || "").trim();
  if (!tok) {
    return { ok: false, message: "firebase_auth_required" };
  }
  if (RPC_SESSION_ONLY_ACTIONS.has(act)) {
    return assertFirebaseSessionOnly_(tok);
  }
  if (RPC_OWNER_ADMIN_ACTIONS.has(act)) {
    return assertFirebaseUserOwnerOrAdminForTenant_(tok, tenantId);
  }
  return assertFirebaseUserOwnsTenant_(tok, tenantId);
}

/**
 * Driving-school ERP snapshot: businesses/{id}/snapshot/main.
 * nanban_main → legacy doc "Nanban"; white-label SaaS uses doc id === tenant_id.
 * NOTE: `expenses[]` for the driving school must never be written with tenant `ESevai` — use `assertSchoolLedgerExpenseTenant_` in saveExpenseData (E-Sevai money uses businesses/ESevai via loadEsevaiModel).
 */
function nanbanBusinessDocIdForTenant(tenantId) {
  const t = String(tenantId || "").trim();
  if (/^esevai$/i.test(t)) return "Nanban";
  // normalizeTenantCode_("nanban_main") → nanban-main; same legacy Nanban snapshot doc
  if (!t || t === "nanban_main" || t === "nanban-main") return "Nanban";
  return t;
}

/** Reject school cashbook RPC if client mistakenly sends E-Sevai tenant (those rows belong in E-Sevai POS / ledger, not Nanban expenses[]). */
function assertSchoolLedgerExpenseTenant_(tenantId) {
  const t = String(tenantId || "").trim();
  if (/^esevai$/i.test(t) || t === "ESevai") {
    return {
      status: "error",
      message:
        "Driving-school income/expense lines must use tenant nanban_main. E-Sevai uses POS, Cashbook, and businesses/ESevai (separate ledger)."
    };
  }
  return null;
}

/** Matches client nanbanIsEnquiryStudent_ / form values (Enquiry, enquiry, status Enquiry). */
function adminNotifyEventForNewStudent_(s) {
  if (!s || typeof s !== "object") return "";
  const typ = String(s.type ?? s.Type ?? "").trim();
  const typL = typ.toLowerCase();
  if (typL === "enquiry" || typL === "enquiries" || typL.includes("enquiry")) return "enquiry";
  const st = String(s.status ?? "").trim().toLowerCase();
  if (st === "enquiry") return "enquiry";
  if (typ) return "admission";
  return "";
}

function formatStudentBadgeLineForAdminNotify_(s) {
  if (!s || typeof s !== "object" || !s.hasBadge) return "";
  const mode = String(s.pvrMode || "customer").toLowerCase();
  const modeLabel = mode === "school" ? "School applies (fee)" : "Customer provides";
  const amt = parseInt(s.pvrFeeAmount, 10) || 0;
  return `\n🛡️ Badge/Heavy: Yes · PVR: ${modeLabel} · PVR ₹${amt}`;
}

async function assertSaasTrialAllowsWrites_(tenantId) {
  const t = String(tenantId || "").trim();
  if (!t || t === "nanban_main" || /^esevai$/i.test(t)) return;
  const doc = await admin.firestore().collection("platform_tenants").doc(t).get();
  if (!doc.exists) return;
  const d = doc.data() || {};
  if (String(d.saas_subscription_status || "").trim().toLowerCase() === "active") return;
  const te = d.trial_ends_at;
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

/** Reference monthly INR (excl GST) from marketing tiers — UI hint only. */
function saasPlanMonthlyReferenceInr_(planRaw, cycleRaw) {
  const p = String(planRaw || "").toLowerCase();
  const c = String(cycleRaw || "").toLowerCase();
  if (p === "starter") return c === "annual" ? Math.round(5999 / 12) : 649;
  if (p === "growth") return c === "annual" ? Math.round(11999 / 12) : 1199;
  if (p === "platform") return 2999;
  return 0;
}

function deriveSaasSubscriptionStatusForList_(d, nowMs) {
  const explicit = String(d.saas_subscription_status || "").trim().toLowerCase();
  if (
    explicit === "trial" ||
    explicit === "active" ||
    explicit === "past_due" ||
    explicit === "paused"
  ) {
    return explicit;
  }
  const te = d.trial_ends_at;
  if (te && typeof te.toMillis === "function" && te.toMillis() >= nowMs) return "trial";
  return "past_due";
}

/** Google Contacts sync after student write; never throws. */
async function runGoogleContactSyncAfterStudentWrite_(tenantId, student, snap) {
  try {
    return await syncStudentToGoogleContacts({ tenantId, student, snap });
  } catch (e) {
    console.error(
      `GOOGLE_CONTACT_SYNC_UNHANDLED tenant=${tenantId} student=${String(student?.id)} ${String(e?.message || e)}`
    );
    return { status: "failed", reason: String(e?.message || e) };
  }
}

/** SaaS: users/{uid}.tenant_id must match the RPC tenant (driving school owner session). */
async function assertFirebaseUserOwnsTenant_(idToken, tenantId) {
  const tid = String(tenantId || "").trim();
  if (!tid) return { ok: false, message: "tenant_required" };
  if (!idToken) return { ok: false, message: "token_required" };
  let dec;
  try {
    dec = await admin.auth().verifyIdToken(idToken);
  } catch (_e) {
    return { ok: false, message: "invalid_token" };
  }
  const uid = String(dec.uid || "");
  const udoc = await admin.firestore().collection("users").doc(uid).get();
  const d0 = udoc.data() || {};
  if (isInactiveTeamMember_(d0)) {
    return { ok: false, message: "account_deactivated" };
  }
  const ut = String(d0.tenant_id || d0.tenantId || "").trim();
  // Client sends normalizeTenantCode_ slugs (hyphens); Firestore may use underscores — must not fail strict !==.
  if (!ut || !teamTenantMatches_(ut, tid)) {
    return { ok: false, message: "tenant_mismatch" };
  }
  return { ok: true, uid };
}

/** Only Admin may link personal Google Contacts (matches Settings UI; Staff/Trainer blocked). */
async function assertFirebaseUserIsAdminForGoogleContacts_(idToken, tenantId) {
  const base = await assertFirebaseUserOwnsTenant_(idToken, tenantId);
  if (!base.ok) return base;
  const udoc = await admin.firestore().collection("users").doc(base.uid).get();
  const role = String(udoc.data()?.role || "").trim().toLowerCase();
  if (role !== "admin" && role !== "owner") {
    return { ok: false, message: "google_contacts_admin_only" };
  }
  return { ok: true, uid: base.uid };
}

function waE164_(phone) {
  const d = normPhone10(phone);
  return d.length === 10 ? `91${d}` : "";
}

/**
 * Thank-you WhatsApp to referrer when a new admission is saved with a referral selected.
 * Non-fatal: logs on failure; does not throw.
 */
async function maybeEnqueueReferrerThankYouWa_(tenantId, snapBeforeWrite, student) {
  if (!student || typeof student !== "object") return;
  if (adminNotifyEventForNewStudent_(student) !== "admission") return;
  const refLabel = String(student.referral || "").trim();
  if (!refLabel || refLabel === "-" || /^none$/i.test(refLabel)) return;
  const inner =
    snapBeforeWrite &&
    snapBeforeWrite.appSettings &&
    typeof snapBeforeWrite.appSettings === "object" &&
    snapBeforeWrite.appSettings.appSettings &&
    typeof snapBeforeWrite.appSettings.appSettings === "object"
      ? snapBeforeWrite.appSettings.appSettings
      : {};
  const referrers = Array.isArray(inner.referrers) ? inner.referrers : [];
  let phone = "";
  let refName = refLabel;
  for (const r of referrers) {
    if (r && typeof r === "object") {
      const nm = String(r.name || "").trim();
      if (nm === refLabel) {
        phone = r.phone;
        refName = nm || refLabel;
        break;
      }
    } else if (String(r || "").trim() === refLabel) {
      refName = refLabel;
      break;
    }
  }
  const to = waE164_(phone);
  if (!to) return;
  const stuName = String(student.name || "-").trim() || "the student";
  let brand;
  try {
    brand = await loadTenantMessagingBrand(tenantId);
  } catch (_b) {
    brand = { schoolName: "Driving School" };
  }
  const sch = String(brand.schoolName || "Driving School").trim();
  const taMsg = `வணக்கம் ${refName}! ${sch}-க்கு ${stuName}-ஐ அறிமுகப்படுத்தியதற்கு மிக்க நன்றி. அவர்களுக்கான அட்மிஷன் வெற்றிகரமாக முடிந்தது.`;
  const enMsg = `Hello ${refName}! Thank you for referring ${stuName} to ${sch}. Their admission was completed successfully.`;
  try {
    await enqueueWaOutboundSend(
      {
        tenantId: String(tenantId || "").trim() || "nanban_main",
        to,
        message: `${taMsg}\n\n${enMsg}`,
        messageType: "text",
        metadata: {
          kind: "referrer_thank_you",
          student_id: String(student.id || ""),
          referral: refLabel
        }
      },
      { delaySeconds: 0 }
    );
  } catch (e) {
    console.error(`REFERRER_THANKYOU_WA_FAILED student=${student.id} ${String(e?.message || e)}`);
  }
}

const SCHOOL_REG_OTP_PEPPER = String(process.env.SCHOOL_REG_OTP_PEPPER || "nanban_school_reg_otp_v1");
const ADMIN_PHONE_EDIT_OTP_PEPPER = String(
  process.env.ADMIN_PHONE_EDIT_OTP_PEPPER || "nanban_admin_phone_edit_otp_v1"
);
const PROFILE_DELETE_OTP_PEPPER = String(
  process.env.PROFILE_DELETE_OTP_PEPPER || "nanban_profile_delete_otp_v1"
);

function hashSchoolRegOtp_(code6) {
  return crypto.createHash("sha256").update(`${SCHOOL_REG_OTP_PEPPER}:${String(code6 || "").trim()}`).digest("hex");
}

function hashAdminPhoneEditOtp_(code6) {
  return crypto
    .createHash("sha256")
    .update(`${ADMIN_PHONE_EDIT_OTP_PEPPER}:${String(code6 || "").trim()}`)
    .digest("hex");
}

function hashProfileDeleteOtp_(code6) {
  return crypto
    .createHash("sha256")
    .update(`${PROFILE_DELETE_OTP_PEPPER}:${String(code6 || "").trim()}`)
    .digest("hex");
}

function profileDeleteOtpDocId_(tenantId, studentId) {
  const base = adminPhoneEditFirestoreDocId_(tenantId);
  const sid = String(studentId).replace(/[/\\]/g, "_").slice(0, 100);
  return `${base}_pd_${sid}`;
}

function adminPhoneEditFirestoreDocId_(tenantId) {
  return String(tenantId || "nanban_main")
    .trim()
    .replace(/[/\\]/g, "_")
    .slice(0, 120);
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

/** Meta `daily_class_alert` (and same-shape templates) use exactly four body variables: {{1}}–{{4}}. */
const DAILY_CLASS_WA_BODY_PARAM_COUNT = 4;

function nanbanTemplateCfg_(snap) {
  const a = snap?.appSettings;
  if (!a || typeof a !== "object") {
    return { dailyClassBodyParamCount: DAILY_CLASS_WA_BODY_PARAM_COUNT };
  }
  const base = { ...a };
  base.dailyClassBodyParamCount = DAILY_CLASS_WA_BODY_PARAM_COUNT;
  if (base.appSettings && typeof base.appSettings === "object") {
    base.appSettings = {
      ...base.appSettings,
      dailyClassBodyParamCount: DAILY_CLASS_WA_BODY_PARAM_COUNT
    };
  }
  return base;
}

/**
 * Template name/language: inner `appSettings.appSettings` (where the hosted UI saves) wins over outer `appSettings`.
 */
function resolveDailyClassWaSending_(snap) {
  const cfg = nanbanTemplateCfg_(snap);
  const inner = cfg.appSettings && typeof cfg.appSettings === "object" ? cfg.appSettings : {};
  const tplName =
    String(inner.dailyClassTemplate || cfg.dailyClassTemplate || "daily_class_alert").trim() ||
    "daily_class_alert";
  const tplLang =
    String(inner.dailyClassTemplateLanguage || cfg.dailyClassTemplateLanguage || "ta").trim() || "ta";
  return {
    tplName,
    tplLang,
    bodyParamCount: DAILY_CLASS_WA_BODY_PARAM_COUNT
  };
}

/**
 * Always length 4 — Meta body {{1}} name, {{2}} cumulative class # (1–15), {{3}} activity, {{4}} next/syllabus.
 * {{2}} must be total classes completed after this save (not “sessions today”, which is often 1 every day).
 */
function buildDailyClassWaBodyParamsFour_({ studentName, classNumberCompletedTotal, activity, nextSyllabus }) {
  const cell = (v) => {
    if (v === undefined || v === null) return "-";
    const s = String(v).trim();
    return s || "-";
  };
  return [
    cell(studentName),
    cell(classNumberCompletedTotal),
    cell(activity),
    cell(nextSyllabus)
  ];
}

/** When data URL says application/octet-stream but bytes are a known raster (Storage / mobile quirks). */
function sniffImageMimeFromBuffer_(buf) {
  if (!buf || buf.length < 4) return "";
  const b0 = buf[0];
  const b1 = buf[1];
  const b2 = buf[2];
  const b3 = buf[3];
  if (b0 === 0xff && b1 === 0xd8) return "image/jpeg";
  if (b0 === 0x89 && b1 === 0x50 && b2 === 0x4e && b3 === 0x47) return "image/png";
  if (b0 === 0x47 && b1 === 0x49 && b2 === 0x46) return "image/gif";
  if (b0 === 0x52 && b1 === 0x49 && b2 === 0x46 && b3 === 0x46 && buf.length >= 12) {
    const t = String.fromCharCode(buf[8] || 0, buf[9] || 0, buf[10] || 0, buf[11] || 0);
    if (t === "WEBP") return "image/webp";
  }
  return "";
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
  // Plain numbers are RPC flags/amounts, not tenant slugs
  if (/^\d+$/.test(last)) return false;
  // Hosted UI appends normalizeTenantCode_ slugs: nanban-main, myschool, city-school-name, …
  if (/^[a-z0-9]+(-[a-z0-9]+)*$/i.test(last) && last.length >= 2) return true;
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

/** E-Sevai POS: canonical work_status on transaction + work rows (Vyapar-style tracking). */
function esevaiNormalizeIncomingWorkStatus_(tx) {
  const w = String((tx && (tx.work_status || tx.workStatus)) || "").toLowerCase();
  if (w === "processing" || w === "action_required" || w === "done") return w;
  if (w === "pending") return "pending";
  const pm = String((tx && (tx.paymentMode || tx.payment_mode)) || "").trim();
  if (pm === "Pending") return "pending";
  return "done";
}

function esevaiWorkRowStatusFromNormalized_(nw, customerType, govFee) {
  const ct = String(customerType || "").toLowerCase();
  if ((ct === "agent" || ct === "broker") && (Number(govFee) || 0) === 0) return "finished";
  if (nw === "done") return "finished";
  if (nw === "processing") return "processing";
  if (nw === "action_required") return "action_required";
  if (nw === "pending") return "pending";
  return "finished";
}

function esevaiItemsServiceLabel_(items) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return "சேவை";
  const joined = list
    .map((it) => String((it && it.name) || "").trim())
    .filter(Boolean)
    .join(" · ");
  return joined || "சேவை";
}

/** Payload for buildEsevaiInvoicePdf from Firestore E-Sevai snapshot rows. */
function esevaiInvoiceOptsFromModel_(data, tx, cust) {
  const s = (data && data.settings) || {};
  const total = Number(tx.total_amount) || 0;
  const recv = Number(tx.received_amount) || 0;
  const bal = Math.max(0, total - recv);
  return {
    businessName: s.business_name || "Ranjith E-Sevai Maiyam",
    businessAddress: s.business_address || "",
    businessPhone: s.business_phone || "",
    gstin: s.business_gstin || "",
    invoiceNo: String(tx.id || ""),
    invoiceDate: String(tx.date || ""),
    customerName: cust ? String(cust.name || "") : "",
    customerPhone: cust ? String(cust.phone || "") : "",
    items: Array.isArray(tx.items) ? tx.items : [],
    totalAmount: total,
    receivedAmount: recv,
    balanceDue: bal,
    upiVpa: String(s.upi_vpa || "").trim(),
    termsText: String(s.terms_and_conditions || s.terms_text || "").trim()
  };
}

function buildEsevaiCustomerBillTamilBody_(opts) {
  const name = String((opts && opts.customerName) || "வாடிக்கையாளர்").trim();
  const svc = String((opts && opts.serviceLabel) || "சேவை").trim();
  const amt = Number((opts && opts.amount) || 0) || 0;
  const isPending = !!(opts && opts.isPending);
  if (isPending) {
    return `வணக்கம் ${name}! ரஞ்சித் ஈ-சேவை மையத்தில் உங்கள் ${svc} சேவைக்கான நிலுவைத் தொகை (₹${amt}) பதிவு செய்யப்பட்டது. தயவுசெய்து விரைவில் செலுத்தவும்.`;
  }
  return `வணக்கம் ${name}! ரஞ்சித் ஈ-சேவை மையத்தில் உங்கள் ${svc} சேவைக்கான கட்டணம் (₹${amt}) பெறப்பட்டது. நன்றி!`;
}

/**
 * Customer-facing Tamil bill alert on Meta WA queue (E-Sevai tenant).
 * Non-fatal: logs on failure.
 */
async function enqueueEsevaiBillCustomerWa_(data, txRecord, txId) {
  try {
    const cust = (data.customers || []).find((c) => String(c.id) === String(txRecord.customer_id));
    if (!cust) return;
    const phone = normPhone10(cust.phone);
    if (phone.length !== 10) return;
    const wa = `91${phone}`;
    const cname = String(cust.name || "Customer");
    const totalAmt = Number(txRecord.total_amount) || 0;
    const svcLabel = esevaiItemsServiceLabel_(txRecord.items);
    const payMode = String(txRecord.payment_mode || "").trim();
    const st = String(txRecord.status || "").toLowerCase();
    const isPending = payMode === "Pending" || st === "pending";
    const msg = buildEsevaiCustomerBillTamilBody_({
      customerName: cname,
      serviceLabel: svcLabel,
      amount: totalAmt,
      isPending
    });
    const settings = (data && data.settings) || {};
    const attachPdf = settings.wa_bill_attach_pdf !== false;
    if (attachPdf) {
      try {
        const { buildEsevaiInvoicePdf } = require("./nanbanPdfService");
        const { enqueuePdfOrText_ } = require("./waPdfOutbound");
        const pdfBuf = await buildEsevaiInvoicePdf(esevaiInvoiceOptsFromModel_(data, txRecord, cust));
        await enqueuePdfOrText_({
          tenantId: ESEVAI_ALERT_TENANT_ID,
          to: wa,
          pdfBuffer: pdfBuf,
          filename: `Esevai_Invoice_${String(txId || "").replace(/[^\w.-]/g, "_")}.pdf`,
          caption: msg,
          textFallback: msg,
          metadata: { kind: "esevai_bill", tx_id: String(txId || "") },
          delaySeconds: 0
        });
        return;
      } catch (pdfErr) {
        console.error(
          `ESEVAI_BILL_PDF_WA_FALLBACK_TEXT tx=${txId} ${String(pdfErr && pdfErr.message ? pdfErr.message : pdfErr)}`
        );
      }
    }
    await enqueueWaOutboundSend(
      {
        tenantId: ESEVAI_ALERT_TENANT_ID,
        to: wa,
        message: msg,
        messageType: "text",
        metadata: { kind: "esevai_bill", tx_id: String(txId || "") }
      },
      { delaySeconds: 0 }
    );
  } catch (e) {
    console.error(`ESEVAI_CUSTOMER_BILL_WA_FAIL tx=${txId} ${String(e && e.message ? e.message : e)}`);
  }
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
 * @param {string} action
 * @param {unknown[]} rawArgs
 * @param {{ idToken?: string, internal?: boolean }} [authOpts]
 */
async function handleErpRpc(action, rawArgs, authOpts) {
  const act = String(action || "").trim();
  const { tenantId, args } = popTenantFromArgs(Array.isArray(rawArgs) ? rawArgs : []);
  const _auth = authOpts && typeof authOpts === "object" ? authOpts : {};

  try {
    if (!_auth.internal) {
      const gate = await assertRpcAuthGate_(act, tenantId, _auth.idToken || "");
      if (!gate.ok) {
        return { status: "error", message: gate.message || "forbidden" };
      }
    }

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
          const wem = String(waErr && waErr.message ? waErr.message : waErr);
          console.error(`SCHOOL_REG_OTP_WA_FAILED ${wem}`);
          try {
            await notifyAdminsText(
              TENANT_DEFAULT,
              `⚠️ School registration OTP could not be queued to WhatsApp (••••${phone10.slice(-4)}). ${wem.slice(0, 160)}`,
              "admission"
            );
          } catch (_n) {
            console.error("SCHOOL_REG_OTP_NOTIFY_FAIL", _n && _n.message ? _n.message : _n);
          }
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
        const nowMs = Date.now();
        qs.forEach((doc) => {
          if (!String(doc.id || "").startsWith("ds-")) return;
          const d = doc.data() || {};
          let lastS = null;
          if (d.last_session_at && typeof d.last_session_at.toMillis === "function") {
            lastS = d.last_session_at.toMillis();
          }
          let nextDueMs = null;
          const np = d.saas_next_payment_due_at;
          if (np && typeof np.toMillis === "function") nextDueMs = np.toMillis();
          const planEff = String(d.saas_plan || d.plan || "trial").trim().toLowerCase();
          const cycleEff = String(d.saas_billing_cycle || "").trim().toLowerCase();
          schools.push({
            tenant_id: doc.id,
            school_name: String(d.school_name || ""),
            owner_email: String(d.owner_email || ""),
            last_session_at: lastS,
            last_session_email: String(d.last_session_email || ""),
            last_session_name: String(d.last_session_name || ""),
            last_session_via: String(d.last_session_via || ""),
            saas_plan: planEff,
            saas_billing_cycle: cycleEff,
            saas_subscription_status: deriveSaasSubscriptionStatusForList_(d, nowMs),
            ref_monthly_inr_ex_gst: saasPlanMonthlyReferenceInr_(planEff, cycleEff),
            next_payment_due_ms: nextDueMs,
            trial_ends_ms:
              d.trial_ends_at && typeof d.trial_ends_at.toMillis === "function"
                ? d.trial_ends_at.toMillis()
                : null,
            last_payment_inr:
              Array.isArray(d.saas_payments) && d.saas_payments.length > 0
                ? parseInt(d.saas_payments[0].amount_inr, 10) || 0
                : null,
            last_payment_note:
              Array.isArray(d.saas_payments) && d.saas_payments[0]
                ? String(d.saas_payments[0].note || "").slice(0, 80)
                : "",
            saas_billing_note: String(d.saas_billing_note || "").slice(0, 500)
          });
        });
        schools.sort((a, b) => (b.last_session_at || 0) - (a.last_session_at || 0));
        return { status: "success", schools };
      }

      case "updateSaaSTenantBillingAction": {
        if (tenantId !== "nanban_main") {
          return { status: "error", message: "Only main platform tenant can update billing." };
        }
        let billingPayload;
        try {
          billingPayload = typeof args[0] === "string" ? JSON.parse(args[0]) : args[0];
        } catch (_e) {
          return { status: "error", message: "bad_json" };
        }
        const billingTargetId = String(billingPayload.target_tenant_id || "").trim();
        if (!billingTargetId.startsWith("ds-")) {
          return { status: "error", message: "invalid_target_tenant" };
        }
        const planIn = String(billingPayload.saas_plan || "").trim().toLowerCase();
        const allowedPlan = new Set(["trial", "starter", "growth", "platform"]);
        if (!allowedPlan.has(planIn)) return { status: "error", message: "invalid_plan" };
        const cycleIn = String(billingPayload.saas_billing_cycle || "").trim().toLowerCase();
        if (cycleIn && cycleIn !== "monthly" && cycleIn !== "annual") {
          return { status: "error", message: "invalid_cycle" };
        }
        const statIn = String(billingPayload.saas_subscription_status || "").trim().toLowerCase();
        const allowedStat = new Set(["trial", "active", "past_due", "paused"]);
        if (!allowedStat.has(statIn)) return { status: "error", message: "invalid_status" };

        /** @type {Record<string, unknown>} */
        const billingPatch = {
          saas_plan: planIn,
          plan: planIn,
          saas_billing_cycle: cycleIn || "",
          saas_subscription_status: statIn,
          saas_billing_note: String(billingPayload.saas_billing_note || "").slice(0, 500),
          billing_updated_at: admin.firestore.FieldValue.serverTimestamp()
        };
        if (billingPayload.clear_next_payment_due === true) {
          billingPatch.saas_next_payment_due_at = admin.firestore.FieldValue.delete();
        } else {
          const isoDueBilling = String(billingPayload.saas_next_payment_due_iso || "").trim();
          if (isoDueBilling) {
            const dtBill = new Date(isoDueBilling);
            if (!Number.isNaN(dtBill.getTime())) {
              billingPatch.saas_next_payment_due_at = admin.firestore.Timestamp.fromDate(dtBill);
            }
          }
        }
        await admin.firestore().collection("platform_tenants").doc(billingTargetId).set(billingPatch, {
          merge: true
        });
        return { status: "success" };
      }

      case "recordSaaSPaymentAction": {
        if (tenantId !== "nanban_main") {
          return { status: "error", message: "Only main platform tenant can record payments." };
        }
        let payPayload;
        try {
          payPayload = typeof args[0] === "string" ? JSON.parse(args[0]) : args[0];
        } catch (_e) {
          return { status: "error", message: "bad_json" };
        }
        const payTargetId = String(payPayload.target_tenant_id || "").trim();
        if (!payTargetId.startsWith("ds-")) return { status: "error", message: "invalid_target_tenant" };
        const payAmt = parseInt(payPayload.amount_inr, 10) || 0;
        if (payAmt <= 0 || payAmt > 99999999) return { status: "error", message: "invalid_amount" };
        let payActorEmail = "";
        try {
          const payDec = await admin.auth().verifyIdToken(String(_auth.idToken || ""));
          payActorEmail = String(payDec.email || "").toLowerCase().slice(0, 120);
        } catch (_e2) {
          return { status: "error", message: "invalid_token" };
        }
        const payDb = admin.firestore();
        const payRef = payDb.collection("platform_tenants").doc(payTargetId);
        const paySnap = await payRef.get();
        if (!paySnap.exists) return { status: "error", message: "tenant_not_found" };
        const payPrev = paySnap.data() || {};
        const payList = Array.isArray(payPrev.saas_payments) ? [...payPrev.saas_payments] : [];
        payList.unshift({
          amount_inr: payAmt,
          note: String(payPayload.note || "").slice(0, 240),
          recorded_at: admin.firestore.FieldValue.serverTimestamp(),
          recorded_by: payActorEmail
        });
        while (payList.length > 80) payList.pop();
        await payRef.set(
          {
            saas_payments: payList,
            last_payment_at: admin.firestore.FieldValue.serverTimestamp(),
            billing_updated_at: admin.firestore.FieldValue.serverTimestamp()
          },
          { merge: true }
        );
        return { status: "success", payments_count: payList.length };
      }

      case "sendOtpToExistingAdmin": {
        const bid = nanbanBusinessDocIdForTenant(tenantId);
        const snap = await getBusinessSnapshotDoc(bid);
        const inner = drivingSchoolInnerFromSnap_(snap);
        const phone10 = getPrimaryPhone10ForAdminOtp(inner);
        if (!phone10) {
          return {
            status: "error",
            message: "No saved admin WhatsApp number found. Save a number first, then you can protect changes with OTP."
          };
        }
        const db = admin.firestore();
        const docId = adminPhoneEditFirestoreDocId_(tenantId);
        const otpRef = db.collection("admin_phone_edit_otps").doc(docId);
        const existing = await otpRef.get();
        if (existing.exists) {
          const c = (existing.data() || {}).created_at;
          if (c && typeof c.toMillis === "function" && Date.now() - c.toMillis() < 55 * 1000) {
            return { status: "error", message: "Please wait about a minute before requesting another OTP." };
          }
        }
        const plain = randomSixDigitOtp_();
        const codeHash = hashAdminPhoneEditOtp_(plain);
        const now = Date.now();
        await otpRef.set(
          {
            code_hash: codeHash,
            expires_at: admin.firestore.Timestamp.fromMillis(now + 5 * 60 * 1000),
            attempts: 0,
            target_phone10: phone10,
            created_at: admin.firestore.FieldValue.serverTimestamp()
          },
          { merge: false }
        );
        const waTo = waE164_(phone10);
        if (!waTo) {
          await otpRef.delete().catch(() => {});
          return { status: "error", message: "Invalid phone for WhatsApp." };
        }
        const otpMsg =
          `🔐 *Nanban ERP — Admin number change*\n\n` +
          `Someone requested to edit admin/partner alert numbers.\n` +
          `OTP: *${plain}*\n` +
          `Valid 5 minutes. If this wasn’t you, ignore this message.`;
        try {
          await enqueueWaOutboundSend(
            {
              tenantId,
              to: waTo,
              message: otpMsg,
              messageType: "text",
              metadata: { kind: "admin_phone_edit_otp", tenant_id: docId }
            },
            { delaySeconds: 0 }
          );
        } catch (waErr) {
          console.error(`ADMIN_PHONE_OTP_WA_FAILED ${String(waErr && waErr.message ? waErr.message : waErr)}`);
          await otpRef.delete().catch(() => {});
          return {
            status: "error",
            message: "Could not send OTP on WhatsApp. Try again later."
          };
        }
        const masked = phone10.length >= 4 ? `••••${phone10.slice(-4)}` : "••••";
        return { status: "success", message: "OTP sent to WhatsApp.", expires_in_sec: 300, masked_phone: masked };
      }

      case "verifyAdminPhoneEditOtp": {
        const otpPlain = String(args[0] || "")
          .trim()
          .replace(/\D/g, "");
        if (otpPlain.length !== 6) {
          return { status: "error", message: "Enter the 6-digit OTP." };
        }
        const db = admin.firestore();
        const docId = adminPhoneEditFirestoreDocId_(tenantId);
        const otpRef = db.collection("admin_phone_edit_otps").doc(docId);
        const otpSnap = await otpRef.get();
        if (!otpSnap.exists) {
          return { status: "error", message: "No active OTP. Tap Edit to request one." };
        }
        const od = otpSnap.data() || {};
        const exp = od.expires_at;
        if (exp && typeof exp.toMillis === "function" && exp.toMillis() < Date.now()) {
          await otpRef.delete().catch(() => {});
          return { status: "error", message: "OTP expired. Request a new one." };
        }
        const attempts = parseInt(od.attempts, 10) || 0;
        if (attempts >= 8) {
          await otpRef.delete().catch(() => {});
          return { status: "error", message: "Too many wrong attempts. Request a new OTP." };
        }
        const expectedHash = String(od.code_hash || "");
        const gotHash = hashAdminPhoneEditOtp_(otpPlain);
        if (!timingSafeEqualStr_(expectedHash, gotHash)) {
          await otpRef.set({ attempts: attempts + 1 }, { merge: true });
          return { status: "error", message: "Wrong OTP. Try again." };
        }
        await otpRef.delete().catch(() => {});
        const unlockRef = db.collection("admin_phone_edit_unlock").doc(docId);
        await unlockRef.set({
          expires_at: admin.firestore.Timestamp.fromMillis(Date.now() + 10 * 60 * 1000),
          created_at: admin.firestore.FieldValue.serverTimestamp()
        });
        return { status: "success", message: "Verified. You can edit the number now." };
      }

      case "requestProfileDeleteOtp": {
        const studentId = args[0];
        if (studentId == null || String(studentId).trim() === "") {
          return { status: "error", message: "மாணவர் தேர்வு செல்லாது." };
        }
        const bid = nanbanBusinessDocIdForTenant(tenantId);
        const snap = await getBusinessSnapshotDoc(bid);
        const inner = drivingSchoolInnerFromSnap_(snap);
        const phone10 = getPrimaryPhone10ForAdminOtp(inner);
        if (!phone10) {
          return {
            status: "error",
            message:
              "முதன்மை அட்மின் WhatsApp எண் சேமிக்கப்படவில்லை. அமைப்புகளில் அட்மின் எண்ணைச் சேமித்த பிறகு OTP அனுப்பவும்."
          };
        }
        const stu = (Array.isArray(snap.students) ? snap.students : []).find(
          (x) => x && String(x.id).trim() === String(studentId).trim()
        );
        if (!stu) return { status: "error", message: "மாணவர் கிடைக்கவில்லை." };
        if (String(stu.status || "").toLowerCase() === "deleted") {
          return { status: "error", message: "இந்த கணக்கு ஏற்கனவே நீக்கப்பட்டது." };
        }
        const db = admin.firestore();
        const otpDocId = profileDeleteOtpDocId_(tenantId, studentId);
        const otpRef = db.collection("profile_delete_otps").doc(otpDocId);
        const existing = await otpRef.get();
        if (existing.exists) {
          const c = (existing.data() || {}).created_at;
          if (c && typeof c.toMillis === "function" && Date.now() - c.toMillis() < 55 * 1000) {
            return { status: "error", message: "சுமார் ஒரு நிமிடம் கழித்து மீண்டும் OTP கேட்கவும்." };
          }
        }
        const plain = randomSixDigitOtp_();
        const codeHash = hashProfileDeleteOtp_(plain);
        const now = Date.now();
        await otpRef.set(
          {
            code_hash: codeHash,
            expires_at: admin.firestore.Timestamp.fromMillis(now + 5 * 60 * 1000),
            attempts: 0,
            student_id: String(studentId),
            target_phone10: phone10,
            created_at: admin.firestore.FieldValue.serverTimestamp()
          },
          { merge: false }
        );
        const waTo = waE164_(phone10);
        if (!waTo) {
          await otpRef.delete().catch(() => {});
          return { status: "error", message: "WhatsApp-க்கு செல்லாத எண்." };
        }
        const stName = String(stu.name || "-").trim().slice(0, 80);
        const otpMsg =
          `🔐 *Nanban ERP — புரொபைல் நீக்கம்*\n\n` +
          `மாணவர்: *${stName}*\nID: \`${String(studentId)}\`\n\n` +
          `OTP: *${plain}*\n` +
          `5 நிமிட செல்லுபடி. நீங்கள் கேட்கவில்லையெனில் இந்த மெசேஜை புறக்கணிக்கவும்.`;
        try {
          await enqueueWaOutboundSend(
            {
              tenantId,
              to: waTo,
              message: otpMsg,
              messageType: "text",
              metadata: { kind: "profile_delete_otp", student_id: String(studentId), tenant_id: otpDocId }
            },
            { delaySeconds: 0 }
          );
        } catch (waErr) {
          console.error(`PROFILE_DELETE_OTP_WA_FAILED ${String(waErr && waErr.message ? waErr.message : waErr)}`);
          await otpRef.delete().catch(() => {});
          return {
            status: "error",
            message: "WhatsApp OTP அனுப்ப முடியவில்லை. பிறகு முயற்சிக்கவும்."
          };
        }
        const masked = phone10.length >= 4 ? `••••${phone10.slice(-4)}` : "••••";
        return { status: "success", message: "OTP முதன்மை அட்மின் WhatsApp-க்கு அனுப்பப்பட்டது.", expires_in_sec: 300, masked_phone: masked };
      }

      case "deleteStudentWithOtp": {
        const studentId = args[0];
        const otpPlain = String(args[1] || "")
          .trim()
          .replace(/\D/g, "");
        if (studentId == null || String(studentId).trim() === "" || otpPlain.length !== 6) {
          return { status: "error", message: "6 இலக்க OTP மற்றும் மாணவர் ID தேவை." };
        }
        const db = admin.firestore();
        const otpDocId = profileDeleteOtpDocId_(tenantId, studentId);
        const otpRef = db.collection("profile_delete_otps").doc(otpDocId);
        const otpSnap = await otpRef.get();
        if (!otpSnap.exists) {
          return { status: "error", message: "OTP இல்லை. முதலில் «OTP அனுப்பு» அழுத்தவும்." };
        }
        const od = otpSnap.data() || {};
        if (String(od.student_id || "") !== String(studentId)) {
          await otpRef.delete().catch(() => {});
          return { status: "error", message: "OTP பொருந்தவில்லை. மீண்டும் OTP கேட்கவும்." };
        }
        const exp = od.expires_at;
        if (exp && typeof exp.toMillis === "function" && exp.toMillis() < Date.now()) {
          await otpRef.delete().catch(() => {});
          return { status: "error", message: "OTP காலாவதி. புதிய OTP கேட்கவும்." };
        }
        const attempts = parseInt(od.attempts, 10) || 0;
        if (attempts >= 8) {
          await otpRef.delete().catch(() => {});
          return { status: "error", message: "தவறான முயற்சிகள் அதிகம். புதிய OTP கேட்கவும்." };
        }
        const expectedHash = String(od.code_hash || "");
        const gotHash = hashProfileDeleteOtp_(otpPlain);
        if (!timingSafeEqualStr_(expectedHash, gotHash)) {
          await otpRef.set({ attempts: attempts + 1 }, { merge: true });
          return { status: "error", message: "OTP தவறு. மீண்டும் முயற்சிக்கவும்." };
        }
        await otpRef.delete().catch(() => {});

        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        const wantId = String(studentId).trim();
        const ix = students.findIndex((x) => x && x.id != null && String(x.id).trim() === wantId);
        if (ix < 0) return { status: "error", message: "மாணவர் கிடைக்கவில்லை." };
        const prev = { ...students[ix] };
        if (String(prev.status || "").toLowerCase() === "deleted") {
          return { status: "error", message: "ஏற்கனவே நீக்கப்பட்ட கணக்கு." };
        }
        const remarks = Array.isArray(prev.adminRemarks) ? [...prev.adminRemarks] : [];
        remarks.unshift({
          date: getISTDateString(),
          text: "🗑️ கணக்கு நீக்கப்பட்டது (OTP உறுதிப்பாடு)"
        });
        students[ix] = Object.assign({}, prev, {
          status: "Deleted",
          adminRemarks: remarks
        });
        await saveNanbanPartial(tenantId, { students });
        try {
          await notifyNanbanAfterStudentWrite_(tenantId, prev, students[ix]);
        } catch (waErr) {
          console.error(
            `NANBAN_NOTIFY_WA_FAILED deleteStudentWithOtp student=${studentId} ${String(waErr?.message || waErr)}`
          );
        }
        try {
          const n = students[ix];
          await notifyAdminsText(
            tenantId,
            `🗑️ *Profile deleted (OTP)*\nName: ${String(n.name || "-")}\nID: ${String(n.id)}\nService: ${String(n.service || "-")}`
          );
        } catch (_e) {
          /* non-fatal */
        }
        return { status: "success", message: "கணக்கு நீக்கப்பட்டது." };
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
          const ed = existingU.data() || {};
          const docEmail = String(ed.email || "")
            .trim()
            .toLowerCase();
          if (docEmail && docEmail !== email) {
            return {
              status: "error",
              message:
                "This Google account is linked to a different email on file. Sign out and use the correct Google account, or contact support."
            };
          }
          const phoneInDoc = normPhone10(ed.phone || "");
          if (phoneInDoc !== phone10) {
            return {
              status: "error",
              message:
                "Mobile number does not match this account’s registered WhatsApp number. Use the same number or correct it in registration."
            };
          }
          await otpRef.delete();
          const tid = String(ed.tenant_id || ed.tenantId || "").trim();
          let trialEndIso = null;
          const te = ed.trial_ends_at;
          if (te && typeof te.toDate === "function") trialEndIso = te.toDate().toISOString();
          else if (te) trialEndIso = String(te);
          return {
            status: "success",
            resumed_existing_registration: true,
            tenant_id: tid,
            trial_ends_at: trialEndIso,
            profile: {
              id: uid,
              name: String(ed.name || "").trim(),
              email: docEmail || email,
              role: String(ed.role || "Admin").trim(),
              phone: phoneInDoc,
              tenant_id: tid,
              businesses: Array.isArray(ed.businesses) ? ed.businesses : ["Nanban"]
            }
          };
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
              trainerAlertPhone: waPhone,
              adminConfigs: [
                {
                  id: "adm_owner",
                  phone: waPhone,
                  name: displayName,
                  alerts: {
                    admission: true,
                    enquiry: true,
                    income: true,
                    expense: true,
                    dayClose: true,
                    chit: true
                  }
                }
              ]
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
            saas_plan: "trial",
            saas_subscription_status: "trial",
            saas_billing_cycle: "",
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

        try {
          await notifyAdminsText(
            TENANT_DEFAULT,
            `🏫 New driving school registered\nName: ${schoolName}\nTenant: ${newTenantId}\nOwner: ${email}\nAdmin WA: ••••${phone10.slice(-4)}`,
            "admission"
          );
        } catch (_n) {
          console.error("NEW_DRIVING_SCHOOL_NOTIFY_FAIL", _n && _n.message ? _n.message : _n);
        }

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
          appSettings: snap.appSettings && typeof snap.appSettings === "object" ? snap.appSettings : {},
          fleetVehicles: Array.isArray(snap.fleetVehicles) ? snap.fleetVehicles : [],
          fleetLogs: Array.isArray(snap.fleetLogs) ? snap.fleetLogs : [],
          fleetFuelLogs: Array.isArray(snap.fleetFuelLogs) ? snap.fleetFuelLogs : [],
          fleetServiceLogs: Array.isArray(snap.fleetServiceLogs) ? snap.fleetServiceLogs : [],
          rtoServices: Array.isArray(snap.rtoServices) ? snap.rtoServices : []
        };
      }

      case "saveRtoServiceAction": {
        const raw = args[0];
        if (!raw || typeof raw !== "object") return { status: "error", message: "Invalid RTO service row" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let rows = Array.isArray(snap.rtoServices) ? [...snap.rtoServices] : [];
        const id =
          raw.id != null && String(raw.id).trim() !== "" ? String(raw.id).trim() : String(Date.now());
        const allowed = new Set([
          "application_pending",
          "appointment_booked",
          "rto_visit_pending",
          "completed"
        ]);
        const st = String(raw.status || "").trim();
        const status = allowed.has(st) ? st : "application_pending";
        const dateStr = String(raw.date || "").trim() || getISTDateString();
        const row = {
          id,
          date: dateStr,
          customerName: String(raw.customerName || "").trim(),
          phone: normPhone10(raw.phone),
          dlNumber: String(raw.dlNumber || "").trim().slice(0, 40),
          serviceType: String(raw.serviceType || "dlRenewal").trim(),
          totalFee: Math.max(0, parseInt(raw.totalFee, 10) || 0),
          advancePaid: Math.max(0, parseInt(raw.advancePaid, 10) || 0),
          status,
          appointmentDate: String(raw.appointmentDate || "").trim(),
          remarks: String(raw.remarks || "").trim().slice(0, 4000)
        };
        const ix = rows.findIndex((x) => x && String(x.id) === id);
        if (ix >= 0) rows[ix] = Object.assign({}, rows[ix], row);
        else rows.unshift(row);
        await saveNanbanPartial(tenantId, { rtoServices: rows });
        return { status: "success", rtoService: row };
      }

      case "saveVehicleAction": {
        const raw = args[0];
        if (!raw || typeof raw !== "object") return { status: "error", message: "Invalid vehicle" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let vehicles = Array.isArray(snap.fleetVehicles) ? [...snap.fleetVehicles] : [];
        const id =
          raw.id != null && String(raw.id).trim() !== "" ? String(raw.id).trim() : String(Date.now());
        const name = String(raw.name || "").trim() || "Vehicle";
        const regNumber = String(raw.regNumber || "").trim();
        const currentKm = Math.max(0, parseInt(raw.currentKm, 10) || 0);
        const nextServiceKm = Math.max(0, parseInt(raw.nextServiceKm, 10) || 0);
        const model = String(raw.model || "").trim();
        const insuranceExpiry = String(raw.insuranceExpiry || "").trim();
        const fcExpiry = String(raw.fcExpiry || "").trim();
        const permitExpiry = String(raw.permitExpiry || "").trim();
        const lastServiceDate = String(raw.lastServiceDate || "").trim();
        const row = {
          id,
          name,
          regNumber,
          currentKm,
          nextServiceKm,
          model,
          insuranceExpiry,
          fcExpiry,
          permitExpiry,
          lastServiceDate
        };
        const ix = vehicles.findIndex((x) => x && String(x.id) === id);
        if (ix >= 0) vehicles[ix] = Object.assign({}, vehicles[ix], row);
        else vehicles.unshift(row);
        await saveNanbanPartial(tenantId, { fleetVehicles: vehicles });
        return { status: "success", vehicle: row };
      }

      case "saveFleetLogAction": {
        const raw = args[0];
        if (!raw || typeof raw !== "object") return { status: "error", message: "Invalid log" };
        const vehicleId = String(raw.vehicleId || "").trim();
        if (!vehicleId) return { status: "error", message: "vehicleId required" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let vehicles = Array.isArray(snap.fleetVehicles) ? [...snap.fleetVehicles] : [];
        let logs = Array.isArray(snap.fleetLogs) ? [...snap.fleetLogs] : [];
        const vix = vehicles.findIndex((x) => x && String(x.id) === vehicleId);
        if (vix < 0) return { status: "error", message: "Vehicle not found" };
        const startKm = Math.max(0, parseInt(raw.startKm, 10) || 0);
        const endKm = Math.max(0, parseInt(raw.endKm, 10) || 0);
        if (endKm < startKm) return { status: "error", message: "endKm must be >= startKm" };
        const logId =
          raw.id != null && String(raw.id).trim() !== "" ? String(raw.id).trim() : String(Date.now());
        const dateStr = String(raw.date || "").trim() || getISTDateString();
        const log = {
          id: logId,
          date: dateStr,
          vehicleId,
          instructorName: String(raw.instructorName || "").trim(),
          startKm,
          endKm,
          fuelAmountRs: Math.max(0, parseInt(raw.fuelAmountRs, 10) || 0),
          remarks: String(raw.remarks || "").trim()
        };
        logs.unshift(log);
        vehicles[vix] = Object.assign({}, vehicles[vix], { currentKm: endKm });
        await saveNanbanPartial(tenantId, { fleetVehicles: vehicles, fleetLogs: logs });
        return { status: "success", log, vehicle: vehicles[vix] };
      }

      case "saveFleetFuelLogAction": {
        const raw = args[0];
        if (!raw || typeof raw !== "object") return { status: "error", message: "Invalid fuel log" };
        const vehicleId = String(raw.vehicleId || "").trim();
        if (!vehicleId) return { status: "error", message: "vehicleId required" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let vehicles = Array.isArray(snap.fleetVehicles) ? [...snap.fleetVehicles] : [];
        let fuelLogs = Array.isArray(snap.fleetFuelLogs) ? [...snap.fleetFuelLogs] : [];
        const vix = vehicles.findIndex((x) => x && String(x.id) === vehicleId);
        if (vix < 0) return { status: "error", message: "Vehicle not found" };
        const logId =
          raw.id != null && String(raw.id).trim() !== "" ? String(raw.id).trim() : String(Date.now());
        const dateStr = String(raw.date || "").trim() || getISTDateString();
        const km = Math.max(0, parseInt(raw.km, 10) || 0);
        const amountRs = Math.max(0, Number(raw.amountRs) || parseInt(raw.amountRs, 10) || 0);
        const liters = Math.max(0, Number(raw.liters) || parseFloat(String(raw.liters).replace(",", ".")) || 0);
        const log = {
          id: logId,
          date: dateStr,
          vehicleId,
          km,
          amountRs,
          liters
        };
        fuelLogs.unshift(log);
        const curV = parseInt(vehicles[vix].currentKm, 10) || 0;
        if (km > curV) {
          vehicles[vix] = Object.assign({}, vehicles[vix], { currentKm: km });
        }
        await saveNanbanPartial(tenantId, { fleetVehicles: vehicles, fleetFuelLogs: fuelLogs });
        return { status: "success", log, vehicle: vehicles[vix] };
      }

      case "saveFleetServiceLogAction": {
        const raw = args[0];
        if (!raw || typeof raw !== "object") return { status: "error", message: "Invalid service log" };
        const vehicleId = String(raw.vehicleId || "").trim();
        if (!vehicleId) return { status: "error", message: "vehicleId required" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let vehicles = Array.isArray(snap.fleetVehicles) ? [...snap.fleetVehicles] : [];
        let serviceLogs = Array.isArray(snap.fleetServiceLogs) ? [...snap.fleetServiceLogs] : [];
        const vix = vehicles.findIndex((x) => x && String(x.id) === vehicleId);
        if (vix < 0) return { status: "error", message: "Vehicle not found" };
        const logId =
          raw.id != null && String(raw.id).trim() !== "" ? String(raw.id).trim() : String(Date.now());
        const dateStr = String(raw.date || "").trim() || getISTDateString();
        const km = Math.max(0, parseInt(raw.km, 10) || 0);
        const amountRs = Math.max(0, Number(raw.amountRs) || parseInt(raw.amountRs, 10) || 0);
        const description = String(raw.description || "").trim();
        const log = {
          id: logId,
          date: dateStr,
          vehicleId,
          km,
          description,
          amountRs
        };
        serviceLogs.unshift(log);
        const curV = parseInt(vehicles[vix].currentKm, 10) || 0;
        const vPatch = { lastServiceDate: dateStr };
        if (km > curV) vPatch.currentKm = km;
        vehicles[vix] = Object.assign({}, vehicles[vix], vPatch);
        await saveNanbanPartial(tenantId, { fleetVehicles: vehicles, fleetServiceLogs: serviceLogs });
        return { status: "success", log, vehicle: vehicles[vix] };
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

      case "resolveHostedGoogleProfileAction": {
        const idTok = String(args[0] || "").trim();
        if (!idTok) return { status: "error", message: "token_required" };
        let dec;
        try {
          dec = await admin.auth().verifyIdToken(idTok);
        } catch (_e) {
          return { status: "error", message: "invalid_token" };
        }
        const uid = String(dec.uid || "").trim();
        const authEmail = String(dec.email || "")
          .trim()
          .toLowerCase();
        if (!uid) return { status: "error", message: "invalid_token" };
        const db = admin.firestore();
        const usnap = await db.collection("users").doc(uid).get();
        if (!usnap.exists) {
          return { status: "error", message: "no_hosted_profile" };
        }
        const d = usnap.data() || {};
        if (isInactiveTeamMember_(d)) {
          return { status: "error", message: "account_deactivated" };
        }
        let trialEnds = d.trial_ends_at;
        let trialEndsOut = null;
        if (trialEnds && typeof trialEnds.toDate === "function") {
          trialEndsOut = trialEnds.toDate().toISOString();
        } else if (trialEnds) {
          trialEndsOut = String(trialEnds);
        }
        return {
          status: "success",
          profile: {
            id: uid,
            name: String(d.name || "").trim(),
            pin: String(d.pin || "").trim(),
            role: String(d.role || "Staff").trim(),
            phone: String(d.phone || "").trim(),
            email: String(d.email || authEmail || "")
              .trim()
              .toLowerCase(),
            tenant_id: String(d.tenant_id || d.tenantId || "").trim(),
            businesses: Array.isArray(d.businesses) ? d.businesses : [],
            trial_ends_at: trialEndsOut,
            auth_link_pending: d.auth_link_pending === true,
            inactive_team_member: d.inactive_team_member === true
          }
        };
      }

      case "getAppUsers": {
        const db = admin.firestore();
        const qs = await db.collection("users").get();
        const users = [];
        qs.forEach((doc) => {
          const d = doc.data() || {};
          if (!includeUserInGetAppUsers_(d, tenantId)) return;
          users.push(hostedUserProfileFromFirestoreDoc_(doc.id, d));
        });
        return users;
      }

      case "resolveHostedGoogleLoginUserAction": {
        const idTok = String(args[0] || "").trim();
        if (!idTok) return { status: "error", message: "token_required" };
        let dec;
        try {
          dec = await admin.auth().verifyIdToken(idTok);
        } catch (_e) {
          return { status: "error", message: "invalid_token" };
        }
        const uid = String(dec.uid || "").trim();
        const email = String(dec.email || "")
          .trim()
          .toLowerCase();
        if (!uid || !email) return { status: "error", message: "email_required" };
        const db = admin.firestore();
        const uidSnap = await db.collection("users").doc(uid).get();
        if (uidSnap.exists) {
          const d0 = uidSnap.data() || {};
          if (isInactiveTeamMember_(d0)) {
            return { status: "error", message: "account_deactivated" };
          }
          return { status: "success", profile: hostedUserProfileFromFirestoreDoc_(uidSnap.id, d0) };
        }
        const q = await db.collection("users").where("email", "==", email).limit(40).get();
        const candidates = [];
        q.forEach((doc) => {
          const d = doc.data() || {};
          if (isInactiveTeamMember_(d)) return;
          candidates.push({ doc, d });
        });
        let chosen = candidates.find((c) => c.d.auth_link_pending === true) || null;
        if (!chosen && candidates.length) {
          chosen = candidates[0];
        }
        if (!chosen) {
          if (!q.empty && candidates.length === 0) {
            return { status: "error", message: "account_deactivated" };
          }
          return { status: "error", message: "no_profile" };
        }
        return { status: "success", profile: hostedUserProfileFromFirestoreDoc_(chosen.doc.id, chosen.d) };
      }

      case "syncLoggedInUserContactToGoogleAction": {
        const idTok = String(args[0] || "").trim();
        const gate = await assertFirebaseUserOwnsTenant_(idTok, tenantId);
        if (!gate.ok) return { status: "error", message: gate.message || "forbidden" };
        const syncOut = await syncNanbanTeamProfileToGoogleContacts(gate.uid);
        return { status: "success", sync: syncOut };
      }

      case "deactivateTeamMemberAction": {
        let payloadDe;
        try {
          payloadDe = typeof args[0] === "string" ? JSON.parse(args[0]) : args[0];
        } catch (_e) {
          return { status: "error", message: "Invalid JSON payload" };
        }
        const tidDe = String(tenantId || "").trim();
        const targetEmail = String(payloadDe.target_email || "")
          .trim()
          .toLowerCase()
          .slice(0, 120);
        if (!targetEmail.includes("@")) {
          return { status: "error", message: "target_email required" };
        }
        const dbDe = admin.firestore();
        const qDe = await dbDe.collection("users").where("email", "==", targetEmail).limit(25).get();
        let targetRef = null;
        let targetData = null;
        qDe.forEach((doc) => {
          if (targetRef) return;
          const d = doc.data() || {};
          const ut = String(d.tenant_id || d.tenantId || "").trim();
          if (!teamTenantMatches_(ut, tidDe)) return;
          targetRef = doc.ref;
          targetData = d;
        });
        if (!targetRef || !targetData) {
          return { status: "error", message: "user_not_found_in_tenant" };
        }
        const rLow = String(targetData.role || "")
          .trim()
          .toLowerCase();
        if (rLow === "admin" || rLow === "owner") {
          return { status: "error", message: "cannot_deactivate_admin_owner" };
        }
        await targetRef.set(
          {
            inactive_team_member: true,
            inactive_team_member_at: admin.firestore.FieldValue.serverTimestamp()
          },
          { merge: true }
        );
        return { status: "success" };
      }

      case "reactivateTeamMemberAction": {
        let payloadRe;
        try {
          payloadRe = typeof args[0] === "string" ? JSON.parse(args[0]) : args[0];
        } catch (_e2) {
          return { status: "error", message: "Invalid JSON payload" };
        }
        const tidRe = String(tenantId || "").trim();
        const targetEmailRe = String(payloadRe.target_email || "")
          .trim()
          .toLowerCase()
          .slice(0, 120);
        if (!targetEmailRe.includes("@")) {
          return { status: "error", message: "target_email required" };
        }
        const dbRe = admin.firestore();
        const qRe = await dbRe.collection("users").where("email", "==", targetEmailRe).limit(25).get();
        let targetRefRe = null;
        qRe.forEach((doc) => {
          if (targetRefRe) return;
          const d = doc.data() || {};
          const ut = String(d.tenant_id || d.tenantId || "").trim();
          if (!teamTenantMatches_(ut, tidRe)) return;
          targetRefRe = doc.ref;
        });
        if (!targetRefRe) {
          return { status: "error", message: "user_not_found_in_tenant" };
        }
        await targetRefRe.set(
          {
            inactive_team_member: false,
            inactive_team_member_at: admin.firestore.FieldValue.serverTimestamp(),
            inactive_team_member_restored_at: admin.firestore.FieldValue.serverTimestamp()
          },
          { merge: true }
        );
        return { status: "success" };
      }

      case "addTeamMemberAction": {
        let payload;
        try {
          payload = typeof args[0] === "string" ? JSON.parse(args[0]) : args[0];
        } catch (_e) {
          return { status: "error", message: "Invalid JSON payload" };
        }
        if (!payload || typeof payload !== "object") {
          return { status: "error", message: "Invalid payload" };
        }
        const tidReq = String(tenantId || "").trim();
        const name = String(payload.name || "").trim().slice(0, 80);
        const email = String(payload.email || "")
          .trim()
          .toLowerCase()
          .slice(0, 120);
        const roleRaw = String(payload.role || "Staff").trim();
        const role =
          roleRaw.charAt(0).toUpperCase() + (roleRaw.slice(1).toLowerCase() || "");
        const phone = normPhone10(payload.phone || "");
        const memberPinRaw = String(payload.member_pin || "").trim();
        const idToken = String(payload.idToken || "").trim();
        const adminName = String(payload.adminName || "").trim();
        const adminPin = String(payload.adminPin || "").trim();
        const allowedRoles = new Set(["Admin", "Owner", "Partner", "Staff", "Trainer"]);
        if (!name || !email || !email.includes("@") || !allowedRoles.has(role)) {
          return { status: "error", message: "Valid name, email, and role (Admin/Owner/Partner/Staff/Trainer) required" };
        }
        const db = admin.firestore();
        let invitedByLabel = "";
        let businessesOut = ["Nanban"];

        if (idToken) {
          let dec;
          try {
            dec = await admin.auth().verifyIdToken(idToken);
          } catch (_e2) {
            return { status: "error", message: "Invalid or expired Google session" };
          }
          const uid = String(dec.uid || "").trim();
          if (!uid) return { status: "error", message: "Invalid token" };
          const udoc = await db.collection("users").doc(uid).get();
          if (!udoc.exists) {
            return { status: "error", message: "Your user profile was not found. Finish Google link first." };
          }
          const ad = udoc.data() || {};
          const ut = String(ad.tenant_id || ad.tenantId || "").trim();
          if (!isSchoolAdminOrOwnerRole_(ad.role)) {
            return { status: "error", message: "Only Admin can add team members" };
          }
          if (!teamTenantMatches_(ut, tidReq)) {
            return { status: "error", message: "Tenant mismatch for admin account" };
          }
          invitedByLabel = String(dec.email || uid).slice(0, 160);
          if (Array.isArray(ad.businesses) && ad.businesses.length) {
            businessesOut = ad.businesses.map((x) => String(x || "").trim()).filter(Boolean);
          }
          if (role !== "Admin") {
            businessesOut = ["Nanban"];
          }
        } else if (adminName && adminPin.length === 4) {
          const qs = await db.collection("users").get();
          let found = false;
          qs.forEach((doc) => {
            if (found) return;
            const d = doc.data() || {};
            if (!includeUserInGetAppUsers_(d, tidReq)) return;
            if (String(d.name || "").trim() !== adminName) return;
            if (String(d.pin || "").trim() !== adminPin) return;
            if (!isSchoolAdminOrOwnerRole_(d.role)) return;
            found = true;
            if (Array.isArray(d.businesses) && d.businesses.length) {
              businessesOut = d.businesses.map((x) => String(x || "").trim()).filter(Boolean);
            }
            if (role !== "Admin") {
              businessesOut = ["Nanban"];
            }
            invitedByLabel = adminName;
          });
          if (!found) {
            return { status: "error", message: "Admin name and 4-digit PIN do not match this school" };
          }
        } else {
          return {
            status: "error",
            message: "Sign in with Google (recommended) or enter Admin name + PIN used at login"
          };
        }

        const dupeQ = await db.collection("users").where("email", "==", email).limit(20).get();
        let dupeSameTenant = false;
        dupeQ.forEach((doc) => {
          const d = doc.data() || {};
          const ut = String(d.tenant_id || d.tenantId || "").trim();
          if (teamTenantMatches_(ut, tidReq)) dupeSameTenant = true;
        });
        if (dupeSameTenant) {
          return { status: "error", message: "This email is already registered for this school" };
        }

        let pinOut = "";
        if (memberPinRaw) {
          if (!/^\d{4}$/.test(memberPinRaw)) {
            return { status: "error", message: "member_pin must be exactly 4 digits" };
          }
          pinOut = memberPinRaw;
        }
        const needsMemberPin = role === "Partner" || role === "Staff" || role === "Trainer";
        if (needsMemberPin && !pinOut) {
          return {
            status: "error",
            message:
              "Set member_pin (4 digits) so this user can sign in with name + PIN, or they must use Google with the invited email."
          };
        }

        const newRef = db.collection("users").doc();
        await newRef.set({
          name,
          email,
          role,
          tenant_id: tidReq,
          phone: phone || "",
          pin: pinOut,
          businesses: businessesOut,
          invitedBy: invitedByLabel,
          invited_at: admin.firestore.FieldValue.serverTimestamp(),
          created_at: admin.firestore.FieldValue.serverTimestamp(),
          auth_link_pending: true
        });

        return { status: "success", member_id: newRef.id, email, role };
      }

      case "updateTeamMemberPinAction": {
        let payloadPin;
        try {
          payloadPin = typeof args[0] === "string" ? JSON.parse(args[0]) : args[0];
        } catch (_e) {
          return { status: "error", message: "Invalid JSON payload" };
        }
        const tidPin = String(tenantId || "").trim();
        const targetEmail = String(payloadPin.target_email || "")
          .trim()
          .toLowerCase()
          .slice(0, 120);
        const newPin = String(payloadPin.new_pin || "").trim();
        if (!targetEmail.includes("@")) {
          return { status: "error", message: "target_email required" };
        }
        if (!/^\d{4}$/.test(newPin)) {
          return { status: "error", message: "new_pin must be exactly 4 digits" };
        }
        const dbPin = admin.firestore();
        const qPin = await dbPin.collection("users").where("email", "==", targetEmail).limit(25).get();
        let targetRef = null;
        qPin.forEach((doc) => {
          if (targetRef) return;
          const d = doc.data() || {};
          const ut = String(d.tenant_id || d.tenantId || "").trim();
          if (!teamTenantMatches_(ut, tidPin)) return;
          if (isInactiveTeamMember_(d)) return;
          targetRef = doc.ref;
        });
        if (!targetRef) {
          return { status: "error", message: "user_not_found_in_tenant_or_inactive" };
        }
        await targetRef.set(
          {
            pin: newPin,
            pin_updated_at: admin.firestore.FieldValue.serverTimestamp()
          },
          { merge: true }
        );
        return { status: "success" };
      }

      case "getStudentWaAssistantConfigAction": {
        const tidWa = String(tenantId || "").trim();
        if (!tidWa) return { status: "error", message: "tenant_required" };
        const snapWa = await admin.firestore().collection("platform_tenants").doc(tidWa).get();
        const rawWa = snapWa.exists ? snapWa.data()?.student_wa_config : null;
        return {
          status: "success",
          config: normalizeStudentWaConfig_(rawWa || {})
        };
      }

      case "updateStudentWaAssistantConfigAction": {
        let cfgPayload;
        try {
          cfgPayload = typeof args[0] === "string" ? JSON.parse(args[0]) : args[0];
        } catch (_e) {
          return { status: "error", message: "Invalid JSON payload" };
        }
        const tidWaUp = String(tenantId || "").trim();
        if (!tidWaUp) return { status: "error", message: "tenant_required" };
        const nextCfg = normalizeStudentWaConfig_(cfgPayload || {});
        await admin.firestore().collection("platform_tenants").doc(tidWaUp).set(
          {
            student_wa_config: nextCfg,
            student_wa_config_updated_at: admin.firestore.FieldValue.serverTimestamp()
          },
          { merge: true }
        );
        return { status: "success", config: nextCfg };
      }

      case "linkInvitedUserToAuthUidAction": {
        const idToken = String(args[0] || "").trim();
        if (!idToken) return { status: "error", message: "token_required" };
        let dec;
        try {
          dec = await admin.auth().verifyIdToken(idToken);
        } catch (_e) {
          return { status: "error", message: "invalid_token" };
        }
        const uid = String(dec.uid || "").trim();
        const email = String(dec.email || "")
          .trim()
          .toLowerCase();
        if (!uid || !email) return { status: "error", message: "email_required" };
        const tidReq = String(tenantId || "").trim();
        const db = admin.firestore();
        const uidRef = db.collection("users").doc(uid);
        const uidSnap = await uidRef.get();
        if (uidSnap.exists) {
          const d = uidSnap.data() || {};
          const ut = String(d.tenant_id || d.tenantId || "").trim();
          if (ut && teamTenantMatches_(ut, tidReq)) {
            await uidRef.set(
              { auth_link_pending: false, updated_at: admin.firestore.FieldValue.serverTimestamp() },
              { merge: true }
            );
            return { status: "success", linked: false, already: true };
          }
          if (ut && !teamTenantMatches_(ut, tidReq)) {
            return { status: "error", message: "This Google account is already linked to another tenant" };
          }
        }
        const q = await db.collection("users").where("email", "==", email).get();
        let placeholder = null;
        q.forEach((doc) => {
          if (doc.id === uid) return;
          const d = doc.data() || {};
          if (isInactiveTeamMember_(d)) return;
          if (!includeUserInGetAppUsers_(d, tidReq)) return;
          const ut = String(d.tenant_id || d.tenantId || "").trim();
          if (!teamTenantMatches_(ut, tidReq)) return;
          placeholder = doc;
        });
        if (!placeholder) {
          return { status: "success", linked: false, no_placeholder: true };
        }
        const pdata = placeholder.data() || {};
        const merged = {
          ...pdata,
          email,
          auth_link_pending: false,
          linked_at: admin.firestore.FieldValue.serverTimestamp(),
          updated_at: admin.firestore.FieldValue.serverTimestamp()
        };
        const batch = db.batch();
        batch.set(uidRef, merged, { merge: true });
        if (placeholder.id !== uid) {
          batch.delete(placeholder.ref);
        }
        await batch.commit();
        return { status: "success", linked: true };
      }

      /**
       * One-time migration: bind a formal Google email (Auth UID) to legacy `nanban_main` data.
       * Requires env `NANBAN_OWNER_LINK_SECRET` (set via Firebase Functions secrets / console).
       * Overwrites existing `users/{uid}` (any prior tenant_id) — Owner, nanban_main, businesses Nanban+ESevai.
       * Args: [linkSecret, targetEmail, displayNameOptional?, snapshotBusinessDocIdOptional?, tenantToken?]
       * Trailing tenant should be `nanban_main`. Snapshot defaults to `Nanban` (not Firebase project id).
       */
      case "linkFormalOwnerToMainTenantAction": {
        const linkSecret = String(args[0] || "").trim();
        const targetEmail = String(args[1] || "")
          .trim()
          .toLowerCase();
        const displayNameOpt = String(args[2] || "").trim().slice(0, 80);
        const snapshotBizIdArg = String(args[3] || "").trim();

        const expected = String(process.env.NANBAN_OWNER_LINK_SECRET || "").trim();
        if (!expected) {
          return { status: "error", message: "migration_secret_not_configured" };
        }
        if (!timingSafeEqualStr_(expected, linkSecret)) {
          return { status: "error", message: "invalid_migration_secret" };
        }
        if (!targetEmail || !targetEmail.includes("@")) {
          return { status: "error", message: "valid_target_email_required" };
        }

        const tidMain = TENANT_DEFAULT;
        const reqTid = String(tenantId || "").trim();
        if (reqTid && reqTid !== tidMain) {
          return { status: "error", message: "use_trailing_tenant_nanban_main" };
        }

        let userRecord;
        try {
          userRecord = await admin.auth().getUserByEmail(targetEmail);
        } catch (e) {
          const code = String(e.code || "");
          if (code === "auth/user-not-found") {
            return {
              status: "error",
              message: "auth_user_not_found_sign_in_once_with_google_so_auth_creates_uid"
            };
          }
          return { status: "error", message: String(e.message || e) };
        }
        const uid = String(userRecord.uid || "").trim();
        if (!uid) {
          return { status: "error", message: "invalid_uid" };
        }

        const db = admin.firestore();
        const uidRef = db.collection("users").doc(uid);

        const q = await db.collection("users").where("email", "==", targetEmail).get();
        const dupes = [];
        q.forEach((doc) => {
          if (doc.id === uid) return;
          dupes.push(doc);
        });
        const placeholder = dupes.length ? dupes[0] : null;

        const displayName =
          displayNameOpt ||
          String(userRecord.displayName || "")
            .trim()
            .slice(0, 80) ||
          targetEmail.split("@")[0];
        const phone10 = normPhone10(userRecord.phoneNumber || "");

        const baseData = {
          name: displayName,
          email: targetEmail,
          role: "Owner",
          tenant_id: tidMain,
          phone: phone10,
          pin: "",
          businesses: ["Nanban", "ESevai"],
          auth_link_pending: false,
          linked_formal_owner_at: admin.firestore.FieldValue.serverTimestamp(),
          updated_at: admin.firestore.FieldValue.serverTimestamp()
        };

        if (dupes.length) {
          const pdata = (placeholder && placeholder.data()) || {};
          const merged = {
            ...pdata,
            ...baseData,
            phone: phone10 || String(pdata.phone || "").trim() || "",
            invitedBy: pdata.invitedBy || pdata.invited_by || ""
          };
          const batch = db.batch();
          batch.set(uidRef, merged, { merge: true });
          for (const d of dupes) {
            batch.delete(d.ref);
          }
          await batch.commit();
        } else {
          await uidRef.set(baseData, { merge: true });
        }

        const bid = snapshotBizIdArg || nanbanBusinessDocIdForTenant(tidMain);
        const snap = await getBusinessSnapshotDoc(bid);
        const owners = upsertSnapshotIdentityRow_(snap.owners, {
          email: targetEmail,
          uid,
          role: "Owner"
        });
        const admins = upsertSnapshotIdentityRow_(snap.admins, {
          email: targetEmail,
          uid,
          role: "Owner"
        });
        await setBusinessSnapshotDoc(
          bid,
          Object.assign({}, snap, {
            owners,
            admins,
            updated_at: admin.firestore.FieldValue.serverTimestamp()
          }),
          true
        );

        return {
          status: "success",
          uid,
          email: targetEmail,
          tenant_id: tidMain,
          snapshot_business_doc: bid,
          message: "owner_linked_to_nanban_main"
        };
      }

      case "getESevaiInitialData": {
        return await loadEsevaiModel(tenantId);
      }

      case "saveAppSettings": {
        const key = args[0];
        const val = args[1];
        let profileChangeNotify = null;
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const appSettings = snap.appSettings && typeof snap.appSettings === "object" ? { ...snap.appSettings } : {};
        if (key === "appSettings") {
          const prevInner =
            appSettings.appSettings && typeof appSettings.appSettings === "object"
              ? { ...appSettings.appSettings }
              : {};
          const incoming = val && typeof val === "object" ? val : {};
          const hadAdmins = migrateInnerToAdminConfigsArray(prevInner).length > 0;
          const finalizedPreview = finalizeInnerAppSettingsAdmin_(prevInner, incoming);
          const prevSer = stableSerializeAdminConfigsInner(prevInner);
          const nextSer = stableSerializeAdminConfigsInner(finalizedPreview);
          if (hadAdmins && prevSer !== nextSer) {
            const db = admin.firestore();
            const unlockId = adminPhoneEditFirestoreDocId_(tenantId);
            const unlockRef = db.collection("admin_phone_edit_unlock").doc(unlockId);
            const uSnap = await unlockRef.get();
            if (!uSnap.exists) {
              return {
                status: "error",
                message: "Verify OTP on the primary admin WhatsApp before changing admin alert recipients."
              };
            }
            const ue = (uSnap.data() || {}).expires_at;
            if (ue && typeof ue.toMillis === "function" && ue.toMillis() < Date.now()) {
              await unlockRef.delete().catch(() => {});
              return {
                status: "error",
                message: "Verification expired. Tap Edit and complete OTP again."
              };
            }
            await unlockRef.delete().catch(() => {});
          }
          appSettings.appSettings = finalizeInnerAppSettingsAdmin_(prevInner, incoming);
          const PROFILE_KEYS = ["schoolName", "schoolAddress", "contactPhone", "receiptFooter", "pvrFee"];
          const pickProf = (o) => {
            const x = {};
            if (!o || typeof o !== "object") return x;
            for (const k of PROFILE_KEYS) x[k] = o[k];
            return x;
          };
          const prevP = pickProf(prevInner);
          const nextP = pickProf(appSettings.appSettings);
          if (PROFILE_KEYS.some((k) => String(prevP[k] ?? "") !== String(nextP[k] ?? ""))) {
            profileChangeNotify = nextP;
          }
        } else if (key === "serviceSplits") appSettings.serviceSplits = val || {};
        else if (key === "vehicleKm") appSettings.vehicleKm = val || {};
        else appSettings[key] = val;
        await saveNanbanPartial(tenantId, { appSettings });
        if (profileChangeNotify) {
          try {
            await notifyAdminsText(
              tenantId,
              `🏫 *Driving school profile updated*\nSchool: ${String(profileChangeNotify.schoolName || "-").slice(0, 100)}\nPVR fee (₹): ${String(profileChangeNotify.pvrFee ?? "-")}\nContact: ${String(profileChangeNotify.contactPhone || "-")}`
            );
          } catch (_e) {
            /* non-fatal */
          }
        }
        try {
          const { patchPlatformTenantWaTemplatesFromMaster, shouldAutoHealWaTemplatesForTenant_ } = require("./waTemplateConfig");
          if (shouldAutoHealWaTemplatesForTenant_(tenantId)) {
            patchPlatformTenantWaTemplatesFromMaster(tenantId).catch(() => {});
          }
        } catch (_waHeal) {
          /* non-fatal */
        }
        return { status: "success" };
      }

      case "syncWaTemplatesFromMasterAction": {
        try {
          const { patchPlatformTenantWaTemplatesFromMaster } = require("./waTemplateConfig");
          const r = await patchPlatformTenantWaTemplatesFromMaster(tenantId);
          return { status: "success", synced: true, tenantId: r.tenantId };
        } catch (e) {
          return { status: "error", message: String(e && e.message ? e.message : e) };
        }
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
        const csSync = await runGoogleContactSyncAfterStudentWrite_(tenantId, s, snap);
        const contactSync = contactSyncOutcomeForRpc(csSync);
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
        if (!prev) {
          try {
            await maybeEnqueueReferrerThankYouWa_(tenantId, snap, s);
          } catch (_refWa) {
            /* non-fatal */
          }
        }
        const isNewStudent = !prev;
        if (isNewStudent) {
          const ev = adminNotifyEventForNewStudent_(s);
          try {
            if (ev === "enquiry") {
              const pkg = String(s.enquiryPackageMode || "").trim();
              const days =
                s.enquiryTrainingDays != null && s.enquiryTrainingDays !== ""
                  ? String(s.enquiryTrainingDays)
                  : "";
              const pkgLine =
                pkg || days
                  ? `\nபேக்கேஜ்: ${pkg || "-"}${days ? ` · நாட்கள்: ${days}` : ""}`
                  : "";
              const fee = s.feeSplit && typeof s.feeSplit === "object" ? s.feeSplit : {};
              const feeHint =
                fee.llr || fee.train || fee.test
                  ? `\nகணிப்பு கட்டணம்: LLR ₹${parseInt(fee.llr, 10) || 0} · Train ₹${parseInt(fee.train, 10) || 0} · Test ₹${parseInt(fee.test, 10) || 0}`
                  : "";
              await notifyAdminsText(
                tenantId,
                `📋 *புதிய விசாரணை*\nபெயர்: ${String(s.name || "-")}\nமொபைல்: ${String(s.phone || "-")}\nசர்வீஸ்: ${String(s.service || "-")}${pkgLine}${feeHint}${feeSplitAdminDetail_(s)}${formatStudentBadgeLineForAdminNotify_(s)}\nதேதி: ${String(s.dateJoined || "-")}`
              );
            } else if (ev === "admission") {
              const t = String(s.type || s.Type || "").trim() || "Admission";
              await notifyAdminsText(
                tenantId,
                `🎓 *புதிய அட்மிஷன்*\nபெயர்: ${String(s.name || "-")}\nமொபைல்: ${String(s.phone || "-")}\nவகை: ${t}\nசர்வீஸ்: ${String(s.service || "-")}\nமுன்பணம்: ₹${parseInt(s.advance, 10) || 0}${feeSplitAdminDetail_(s)}${formatStudentBadgeLineForAdminNotify_(s)}\nதேதி: ${String(s.dateJoined || "-")}`
              );
              const typL = String(s.type || s.Type || "")
                .trim()
                .toLowerCase();
              const pm = String(s.packageMode || "")
                .trim()
                .toLowerCase();
              const wantsTraining =
                pm === "training" ||
                pm === "combo" ||
                typL === "training_admission";
              if (wantsTraining) {
                const days =
                  s.trainingDays != null && String(s.trainingDays).trim() !== ""
                    ? String(s.trainingDays)
                    : "";
                const pkgLine = pm ? `பேக்கேஜ்: *${pm}*${days ? ` · நாட்கள்: ${days}` : ""}` : "";
                try {
                  await notifyTrainersText(
                    tenantId,
                    `🚗 *புதிய பயிற்சி அட்மிஷன் — assign பண்ணணும்*\nபெயர்: ${String(s.name || "-")}\nமொபைல்: ${String(s.phone || "-")}\nசர்வீஸ்: ${String(s.service || "-")}\nவகை: ${t}${pkgLine ? `\n${pkgLine}` : ""}\nமுன்பணம்: ₹${parseInt(s.advance, 10) || 0}\nதேதி: ${String(s.dateJoined || "-")}\n\n👉 Dashboard → ட்ரெயினர் tab-ல புதிய மாணவரை இன்றைய ஸ்லாட் assign பண்ணுங்க.`
                  );
                } catch (_trWa) {
                  /* non-fatal */
                }
              }
            }
          } catch (_admErr) {
            /* non-fatal */
          }
        }
        return { status: "success", contactSync };
      }

      case "updateStudentData": {
        const s = args[0];
        if (!s || s.id == null) return { status: "error", message: "Invalid student" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        const wantId = String(s.id).trim();
        const ix = students.findIndex(
          (x) => x && x.id != null && String(x.id).trim() === wantId
        );
        if (ix < 0) return { status: "error", message: "Not found" };
        const prev = { ...students[ix] };
        const patch = typeof s === "object" && s ? { ...s } : {};
        const prevStatusNorm = String(prev.status || "").toLowerCase();
        const patchStatusNorm =
          patch.status != null && String(patch.status).trim() !== ""
            ? String(patch.status).toLowerCase()
            : null;
        if (patchStatusNorm === "deleted" && prevStatusNorm !== "deleted") {
          return {
            status: "error",
            message:
              "புரொபைல் நீக்க: முதன்மை அட்மின் WhatsApp OTP தேவை. (நீக்கு → OTP அனுப்பு → உறுதிப்படுத்தி நீக்கு)",
            msg: "profile_delete_requires_otp"
          };
        }
        const phoneNorm = normPhone10(patch.phone != null ? patch.phone : prev.phone);
        students[ix] = Object.assign({}, prev, patch, {
          id: prev.id,
          phone: phoneNorm || prev.phone
        });
        await saveNanbanPartial(tenantId, { students });
        const csSyncUp = await runGoogleContactSyncAfterStudentWrite_(tenantId, students[ix], snap);
        const contactSync = contactSyncOutcomeForRpc(csSyncUp);
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
        if (stableStudentWatchJson_(prev) !== stableStudentWatchJson_(students[ix])) {
          const n = students[ix];
          const fs = n.feeSplit && typeof n.feeSplit === "object" ? n.feeSplit : {};
          const fsLine =
            parseInt(fs.llr, 10) || parseInt(fs.train, 10) || parseInt(fs.test, 10)
              ? `\nFee split: LLR ₹${parseInt(fs.llr, 10) || 0} · Train ₹${parseInt(fs.train, 10) || 0} · Test ₹${parseInt(fs.test, 10) || 0}`
              : "";
          const pvrLine =
            n.hasBadge && String(n.pvrMode || "").toLowerCase() === "school"
              ? `\nPVR board: ${String(n.pvrStatus || "applied")}`
              : "";
          try {
            await notifyAdminsText(
              tenantId,
              `✏️ *Student updated*\nName: ${String(n.name || "-")}\nID: ${String(n.id)}\nStatus: ${String(n.status || "-")} | Test: ${String(n.testStatus || "-")}\nService: ${String(n.service || "-")}${fsLine}${pvrLine}`
            );
          } catch (_e) {
            /* non-fatal */
          }
        }
        return { status: "success", contactSync };
      }

      case "updateStudentAndExpenseAtomicAction": {
        const payload = args[0];
        if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
          return { status: "error", message: "invalid_payload" };
        }
        const s = payload.student;
        const rawExps = Array.isArray(payload.expenses) ? payload.expenses : [];
        if (!s || s.id == null) return { status: "error", message: "Invalid student" };
        const tenantErr = rawExps.length ? assertSchoolLedgerExpenseTenant_(tenantId) : null;
        if (tenantErr) return tenantErr;

        const bid = nanbanBusinessDocIdForTenant(tenantId);
        const snap = await getBusinessSnapshotDoc(bid);
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        let expenses = Array.isArray(snap.expenses) ? [...snap.expenses] : [];
        const wantId = String(s.id).trim();
        const ix = students.findIndex((x) => x && x.id != null && String(x.id).trim() === wantId);
        if (ix < 0) return { status: "error", message: "Not found" };
        const prev = { ...students[ix] };
        const patch = typeof s === "object" && s ? { ...s } : {};
        const prevStatusNorm = String(prev.status || "").toLowerCase();
        const patchStatusNorm =
          patch.status != null && String(patch.status).trim() !== ""
            ? String(patch.status).toLowerCase()
            : null;
        if (patchStatusNorm === "deleted" && prevStatusNorm !== "deleted") {
          return {
            status: "error",
            message:
              "புரொபைல் நீக்க: முதன்மை அட்மின் WhatsApp OTP தேவை. (நீக்கு → OTP அனுப்பு → உறுதிப்படுத்தி நீக்கு)",
            msg: "profile_delete_requires_otp"
          };
        }
        const phoneNorm = normPhone10(patch.phone != null ? patch.phone : prev.phone);
        students[ix] = Object.assign({}, prev, patch, {
          id: prev.id,
          phone: phoneNorm || prev.phone
        });
        for (const raw of rawExps) {
          if (!raw || typeof raw !== "object") continue;
          const e = ensureExpenseRowWithId_(raw);
          const dup = e.id && expenses.some((x) => x && String(x.id) === String(e.id));
          if (dup) continue;
          expenses.push(e);
        }
        await saveNanbanPartial(tenantId, { students, expenses });
        const csSyncUp = await runGoogleContactSyncAfterStudentWrite_(tenantId, students[ix], snap);
        const contactSync = contactSyncOutcomeForRpc(csSyncUp);
        try {
          await notifyNanbanAfterStudentWrite_(tenantId, prev, students[ix]);
        } catch (waErr) {
          console.error(
            `NANBAN_NOTIFY_WA_FAILED updateStudentAndExpenseAtomicAction student=${s.id} ${String(waErr?.message || waErr)}`
          );
          await notifyAdminsText(
            tenantId,
            `⚠️ Student update WA notify failed (${s.id}): ${String(waErr?.message || waErr)}`
          );
        }
        if (stableStudentWatchJson_(prev) !== stableStudentWatchJson_(students[ix])) {
          const n = students[ix];
          const fs = n.feeSplit && typeof n.feeSplit === "object" ? n.feeSplit : {};
          const fsLine =
            parseInt(fs.llr, 10) || parseInt(fs.train, 10) || parseInt(fs.test, 10)
              ? `\nFee split: LLR ₹${parseInt(fs.llr, 10) || 0} · Train ₹${parseInt(fs.train, 10) || 0} · Test ₹${parseInt(fs.test, 10) || 0}`
              : "";
          const pvrLine =
            n.hasBadge && String(n.pvrMode || "").toLowerCase() === "school"
              ? `\nPVR board: ${String(n.pvrStatus || "applied")}`
              : "";
          try {
            await notifyAdminsText(
              tenantId,
              `✏️ *Student updated*\nName: ${String(n.name || "-")}\nID: ${String(n.id)}\nStatus: ${String(n.status || "-")} | Test: ${String(n.testStatus || "-")}\nService: ${String(n.service || "-")}${fsLine}${pvrLine}`
            );
          } catch (_e) {
            /* non-fatal */
          }
        }
        for (const raw of rawExps) {
          if (!raw || typeof raw !== "object") continue;
          const e = ensureExpenseRowWithId_(raw);
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
        }
        return { status: "success", contactSync };
      }

      case "markFollowUpCalledAction": {
        const studentId = args[0];
        const noteLine = String(args[1] || "").trim().slice(0, 1200);
        if (studentId == null || String(studentId).trim() === "")
          return { status: "error", message: "Student ID required" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        const wantId = String(studentId).trim();
        const ix = students.findIndex((x) => x && x.id != null && String(x.id).trim() === wantId);
        if (ix < 0) return { status: "error", message: "Not found" };
        const prev = { ...students[ix] };
        const nowMs = Date.now();
        const nextMs = nowMs + 7 * 24 * 60 * 60 * 1000;
        const dayStr = getISTDateString();
        const append = noteLine ? `${dayStr}: ${noteLine}` : `${dayStr}: (call logged)`;
        const oldNotes = String(prev.callNotes || "").trim();
        students[ix] = Object.assign({}, prev, {
          lastContactedDate: nowMs,
          nextFollowUpDate: nextMs,
          callNotes: oldNotes ? `${oldNotes}\n${append}` : append
        });
        await saveNanbanPartial(tenantId, { students });
        return { status: "success", student: students[ix] };
      }

      case "saveExpenseData": {
        const tenantErr = assertSchoolLedgerExpenseTenant_(tenantId);
        if (tenantErr) return tenantErr;
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
          /* Omit eventType so adminConfigs alert toggles do not drop partner/owner (broad delivery). */
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

      /** Persist end odometer on the active daily session (Firestore runtime `trainer_km_session` only). */
      case "updateTrainerKmSessionEndAction": {
        const endKm = parseInt(args[0], 10) || 0;
        const today = getISTDateString();
        const meta = await getRuntimeDoc("Nanban", "trainer_km_session");
        const sess = meta.session && typeof meta.session === "object" ? meta.session : null;
        if (!sess || sess.active === false) {
          return { status: "error", message: "no_active_km_session" };
        }
        let sessDate = String(sess.date_ist || "");
        if (!sessDate && sess.started_at) {
          try {
            sessDate = getISTDateString(new Date(sess.started_at));
          } catch (e) {
            sessDate = "";
          }
        }
        if (sessDate && sessDate !== today) {
          return { status: "error", message: "session_wrong_day" };
        }
        const startKm =
          parseInt(sess.start_km, 10) || parseInt(sess.startKm, 10) || parseInt(sess.start, 10) || 0;
        if (startKm <= 0) return { status: "error", message: "no_start_km" };
        if (endKm <= startKm) {
          return { status: "error", message: "end_km_must_exceed_start" };
        }
        const next = {
          ...sess,
          end_km: endKm,
          endKm,
          end_updated_at: new Date().toISOString()
        };
        await setRuntimeDoc("Nanban", "trainer_km_session", { session: next });
        return {
          status: "success",
          startKm,
          endKm,
          runKm: endKm - startKm
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
        const addr = String(c.address || "").trim();
        const custRow = {
          id,
          name: c.name,
          phone: c.phone,
          address: addr,
          balance: Number(c.oldBalance) || 0,
          type: c.type || "Direct",
          created_at: getISTDateString()
        };
        data.customers.unshift(custRow);
        const typ = String(c.type || "").toLowerCase();
        if (typ.includes("agent")) {
          if (!Array.isArray(data.agents)) data.agents = [];
          const agIdx = data.agents.findIndex((x) => String(x.id) === String(id));
          const agPayload = {
            id,
            name: c.name || "",
            phone: c.phone || "",
            area: addr,
            address: addr,
            active: true,
            created_at: custRow.created_at
          };
          if (agIdx >= 0) data.agents[agIdx] = Object.assign({}, data.agents[agIdx], agPayload);
          else data.agents.unshift(agPayload);
        }
        await persistEsevai(data);
        return { status: "success", id, tenant_id: tenantId };
      }

      case "saveESevaiAgentAction": {
        const a = args[0] || {};
        const data = await loadEsevaiModel(tenantId);
        const id = a.id || "ESAG" + Date.now();
        const idx = data.agents.findIndex((x) => String(x.id) === String(id));
        const addr = String(a.address || a.area || "").trim();
        const payload = {
          id,
          name: a.name || "",
          phone: a.phone || "",
          area: addr,
          address: addr,
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

      case "generateESevaiInvoicePdfAction": {
        const p = args[0] && typeof args[0] === "object" ? args[0] : {};
        const txId = String(p.transactionId || p.id || "").trim();
        if (!txId) return { status: "error", message: "Missing transaction id" };
        const data = await loadEsevaiModel(tenantId);
        const tx = (data.transactions || []).find((t) => String(t.id) === txId);
        if (!tx) return { status: "error", message: "Transaction not found" };
        const cust = (data.customers || []).find((c) => String(c.id) === String(tx.customer_id));
        const { buildEsevaiInvoicePdf } = require("./nanbanPdfService");
        const buf = await buildEsevaiInvoicePdf(esevaiInvoiceOptsFromModel_(data, tx, cust));
        return {
          status: "success",
          pdf_base64: buf.toString("base64"),
          filename: `Esevai_Invoice_${txId.replace(/[^\w.-]/g, "_")}.pdf`
        };
      }

      case "generateESevaiLedgerPdfAction": {
        const p = args[0] && typeof args[0] === "object" ? args[0] : {};
        const customerId = String(p.customerId || "").trim();
        if (!customerId) return { status: "error", message: "Missing customer id" };
        const data = await loadEsevaiModel(tenantId);
        const cust = (data.customers || []).find((c) => String(c.id) === customerId);
        if (!cust) return { status: "error", message: "Customer not found" };
        const txs = (data.transactions || []).filter((t) => String(t.customer_id) === customerId);
        txs.sort((a, b) => String(b.date || "").localeCompare(String(a.date || "")));
        let totalBilled = 0;
        let totalPaid = 0;
        const ledgerRows = txs.map((t) => {
          const total = Number(t.total_amount) || 0;
          const recv = Number(t.received_amount) || 0;
          totalBilled += total;
          totalPaid += recv;
          const items = Array.isArray(t.items) ? t.items : [];
          const svc = items.map((it) => String(it.name || "").trim()).filter(Boolean).join(" · ") || "—";
          const st =
            String(t.payment_mode || "") === "Pending" || String(t.status || "").toLowerCase() === "pending"
              ? "Pending"
              : "Paid";
          return { date: String(t.date || "-"), service: svc, amount: total, status: st };
        });
        const s = data.settings || {};
        const { buildEsevaiCustomerLedgerPdf } = require("./nanbanPdfService");
        const buf = await buildEsevaiCustomerLedgerPdf({
          businessName: s.business_name || "Ranjith E-Sevai Maiyam",
          businessAddress: s.business_address || "",
          businessPhone: s.business_phone || "",
          customerName: cust.name,
          customerPhone: cust.phone,
          totalBilled,
          totalPaid,
          totalPending: Number(cust.balance) || 0,
          transactions: ledgerRows
        });
        return {
          status: "success",
          pdf_base64: buf.toString("base64"),
          filename: `Esevai_Ledger_${String(customerId).replace(/[^\w.-]/g, "_")}.pdf`
        };
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
        const todayL = l.date || getISTDateString();
        data.ledgerEntries.unshift({
          date: todayL,
          type: l.type || "expense",
          category: l.category || "",
          description: l.description || "",
          amount: Number(l.amount) || 0,
          account: l.account || "Cash",
          customer_id: l.customer_id || ""
        });
        await persistEsevai(data);
        try {
          const typ = String(l.type || "expense");
          await notifyAdminsText(
            ESEVAI_ALERT_TENANT_ID,
            `📒 *E-Sevai Cashbook*\n${typ.toUpperCase()}: ₹${Number(l.amount) || 0}\nCat: ${String(l.category || "-")}\nAcct: ${String(l.account || "Cash")}\n${String(l.description || "").slice(0, 200)}\nDate: ${todayL}`
          );
        } catch (_esLed) {
          /* non-fatal */
        }
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
        const nwStatus = esevaiNormalizeIncomingWorkStatus_(tx);

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
          work_status: nwStatus,
          date: today
        });

        for (let i = 0; i < data.customers.length; i++) {
          if (String(data.customers[i].id) === String(tx.customerId)) {
            customerType = String(data.customers[i].type || "");
            data.customers[i].balance = (Number(data.customers[i].balance) || 0) + balDiff;
            break;
          }
        }

        const txRowNew = data.transactions[0];
        if (txRowNew && String(txRowNew.id) === String(txId)) {
          try {
            await enqueueEsevaiBillCustomerWa_(data, txRowNew, txId);
          } catch (_waErr) {}
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
          const workStatus = esevaiWorkRowStatusFromNormalized_(nwStatus, customerType, govFee);
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

      case "notifyESevaiCustomerBillWaAction": {
        const p = args[0] || {};
        const txId = String(p.transactionId || p.id || "").trim();
        if (!txId) return { status: "error", message: "Missing transaction id" };
        const data = await loadEsevaiModel(tenantId);
        const txRec = (data.transactions || []).find((t) => String(t.id) === txId);
        if (!txRec) return { status: "error", message: "Transaction not found" };
        try {
          await enqueueEsevaiBillCustomerWa_(data, txRec, txId);
        } catch (_e) {}
        return { status: "success" };
      }

      case "updateESevaiTransactionAction": {
        const patch = args[0] || {};
        const txId = String(patch.transactionId || patch.id || "").trim();
        if (!txId) return { status: "error", message: "Missing transaction id" };
        const data = await loadEsevaiModel(tenantId);
        const tIx = data.transactions.findIndex((t) => String(t.id) === txId);
        if (tIx < 0) return { status: "error", message: "Transaction not found" };
        const old = { ...data.transactions[tIx] };
        const today = String(old.date || "").trim() || getISTDateString();

        const oldTotal = Number(old.total_amount) || 0;
        const oldRecv = Number(old.received_amount) || 0;
        const oldMode = String(old.payment_mode || "Cash");
        const oldGovBank = String(old.gov_bank || "SBI");
        const oldBalDiff =
          Number(old.balance_diff) === Number(old.balance_diff)
            ? Number(old.balance_diff)
            : oldRecv - oldTotal;

        const items = Array.isArray(patch.items) && patch.items.length ? patch.items : old.items || [];
        const newTotal =
          patch.totalAmount !== undefined && patch.totalAmount !== null
            ? Number(patch.totalAmount) || 0
            : oldTotal;
        const newRecv =
          patch.receivedAmount !== undefined && patch.receivedAmount !== null
            ? Number(patch.receivedAmount) || 0
            : oldRecv;
        const newMode = String(patch.paymentMode || oldMode);
        const newGovBank = String(patch.govBank || oldGovBank);
        let newStatus = "finished";
        if (newMode === "Pending") newStatus = "pending";
        else if (String(patch.status || "").toLowerCase() === "pending") newStatus = "pending";
        else newStatus = "finished";

        const newBalDiff = newRecv - newTotal;

        const custIx = data.customers.findIndex((c) => String(c.id) === String(old.customer_id));
        let customerType = "";
        if (custIx >= 0) {
          customerType = String(data.customers[custIx].type || "");
          data.customers[custIx].balance =
            (Number(data.customers[custIx].balance) || 0) - oldBalDiff + newBalDiff;
        }

        if (oldMode !== "Pending" && oldRecv > 0) {
          data.balances[oldMode] = (Number(data.balances[oldMode]) || 0) - oldRecv;
        }
        if (newMode !== "Pending" && newRecv > 0) {
          data.balances[newMode] = (Number(data.balances[newMode]) || 0) + newRecv;
        }

        const oldGovTotal = (old.items || []).reduce(
          (sum, item) => sum + (Number(item.gov_fee) || 0) * (Number(item.qty) || 1),
          0
        );
        const newGovTotal = (items || []).reduce(
          (sum, item) => sum + (Number(item.gov_fee) || 0) * (Number(item.qty) || 1),
          0
        );
        if (oldGovTotal > 0) {
          data.balances[oldGovBank] = (Number(data.balances[oldGovBank]) || 0) + oldGovTotal;
        }
        if (newGovTotal > 0) {
          data.balances[newGovBank] = (Number(data.balances[newGovBank]) || 0) - newGovTotal;
        }

        data.ledgerEntries = (data.ledgerEntries || []).filter((e) => {
          const d = String((e && e.description) || "");
          return !d.includes(`Bill #${txId}`);
        });

        if (newGovTotal > 0) {
          data.ledgerEntries.unshift({
            date: today,
            type: "expense",
            category: "Gov Fee",
            description: `Gov Fee for Bill #${txId}`,
            amount: newGovTotal,
            account: newGovBank
          });
        }
        const totalSrvFee = (items || []).reduce(
          (sum, item) => sum + (Number(item.srv_fee) || 0) * (Number(item.qty) || 1),
          0
        );
        const otherExpenses =
          patch.otherExpenses !== undefined && patch.otherExpenses !== null
            ? Number(patch.otherExpenses) || 0
            : Number(old.other_expenses) || 0;
        const netIncome = totalSrvFee - otherExpenses;
        if (netIncome !== 0) {
          const acc = newMode === "Pending" ? "Cash" : newMode;
          data.ledgerEntries.unshift({
            date: today,
            type: netIncome > 0 ? "income" : "expense",
            category: "Service Fee",
            description: `Service Fee for Bill #${txId}`,
            amount: Math.abs(netIncome),
            account: acc
          });
        }

        const mergedWs = esevaiNormalizeIncomingWorkStatus_(
          Object.assign({}, old, patch, { payment_mode: newMode, paymentMode: newMode })
        );

        data.works = (data.works || []).filter((w) => String(w.transaction_id) !== String(txId));
        items.forEach((item) => {
          const govFee = Number(item.gov_fee) || 0;
          const workStatus = esevaiWorkRowStatusFromNormalized_(mergedWs, customerType, govFee);
          data.works.unshift({
            id: "ESWK" + Date.now() + Math.floor(Math.random() * 100),
            transaction_id: txId,
            customer_id: old.customer_id,
            agent_id: old.agent_id || "",
            agent_name: old.agent_name || "",
            service_name: item.name,
            status: workStatus,
            service_type: govFee > 0 ? "regular" : "own",
            stages: item.stages || [],
            document_url: item.document_url || "",
            llr_date: String(item.llr_date || old.llr_date || "").trim(),
            llr_copy_url: String(item.llr_copy_url || old.llr_copy_url || "").trim(),
            llr_reminder_sent_at: "",
            customer_type: customerType || "",
            target_date: item.target_date || "",
            delivery_status: item.delivery_status || "pending",
            delivery_notified_at: "",
            finished_date: workStatus === "finished" ? today : "",
            created_at: today
          });
        });

        const merged = Object.assign({}, old, {
          items,
          gov_bank: newGovBank,
          payment_mode: newMode,
          sub_total:
            patch.subTotal !== undefined && patch.subTotal !== null
              ? Number(patch.subTotal) || 0
              : Number(old.sub_total) || newTotal,
          discount:
            patch.discount !== undefined && patch.discount !== null
              ? Number(patch.discount) || 0
              : Number(old.discount) || 0,
          round_off:
            patch.roundOff !== undefined && patch.roundOff !== null
              ? Number(patch.roundOff) || 0
              : Number(old.round_off) || 0,
          total_amount: newTotal,
          received_amount: newRecv,
          balance_diff: newBalDiff,
          status: newStatus,
          work_status: mergedWs,
          other_expenses: otherExpenses
        });
        data.transactions[tIx] = merged;

        await persistEsevai(data);
        return { status: "success", id: txId, tenant_id: tenantId };
      }

      case "logAttendance": {
        const o = args[0];
        if (!o || typeof o !== "object" || Array.isArray(o)) {
          return { status: "error", message: "logAttendance requires a single object payload" };
        }
        const na = [
          o.studentId ?? o.student_id,
          String(o.type || "class"),
          parseInt(o.att, 10) || 0,
          String(o.perf || ""),
          parseInt(o.amt, 10) || 0,
          String(o.trainer || "Trainer")
        ];
        return await handleErpRpc("processTrainerEntry", [...na, tenantId], Object.assign({}, _auth, { internal: true }));
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

        if (att > 0) {
          const meta = await getRuntimeDoc("Nanban", "trainer_km_session");
          const sess = meta.session && typeof meta.session === "object" ? meta.session : null;
          let sessDate = sess ? String(sess.date_ist || "") : "";
          if (!sessDate && sess && sess.started_at) {
            try {
              sessDate = getISTDateString(new Date(sess.started_at));
            } catch (_e) {
              sessDate = "";
            }
          }
          const startKm =
            parseInt(sess?.start_km, 10) ||
            parseInt(sess?.startKm, 10) ||
            parseInt(sess?.start, 10) ||
            0;
          const sessionOk =
            sess &&
            sess.active !== false &&
            sess.cleared !== true &&
            startKm > 0 &&
            String(sessDate || "") === String(today);
          if (!sessionOk) {
            return { status: "error", message: "start_km_required_before_attendance" };
          }
        }

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
              const totalAfter = parseInt(s.classesAttended, 10) || 0;
              const nextDay = totalAfter + 1;
              const syllabusText =
                nextDay <= 15
                  ? CAR_SYLLABUS[nextDay - 1]
                  : "அனைத்து பயிற்சிகளும் முடிந்தது! இனி டெஸ்டுக்குத் தயாராகலாம்.";
              const dc = resolveDailyClassWaSending_(snap);
              const tplName = dc.tplName;
              const tplLang = dc.tplLang;
              const bodyParams = buildDailyClassWaBodyParamsFour_({
                studentName: s.name,
                classNumberCompletedTotal: totalAfter,
                activity: perf,
                nextSyllabus: syllabusText
              });
              const plainReport = formatDailyClassReportPlainText_({
                name: s.name,
                dateIst: today,
                vehicleLabel: String(s.service || "").trim(),
                sessionsLoggedToday: att,
                totalClassesCompleted: totalAfter,
                activity: perf,
                nextSyllabus: syllabusText,
                trainer,
                runKm: null
              });
              await enqueueWaOutboundSend(
                {
                  tenantId: TENANT_DEFAULT,
                  to: wa,
                  message: plainReport,
                  messageType: "template",
                  template: {
                    name: tplName,
                    languageCode: tplLang,
                    bodyParams,
                    disallowTextFallback: false
                  },
                  metadata: {
                    kind: "daily_class",
                    student_id: String(studentId),
                    report_date_ist: today,
                    vehicle_label: String(s.service || "").trim() || "—",
                    trainer_name: trainer,
                    classes_total_completed: String(totalAfter),
                    classes_sessions_today: String(att)
                  }
                },
                { delaySeconds: 0 }
              );
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
              const tagline = drivingSchoolReceiptTagline_(snap);
              const { buildFeeReceiptPdf, feeReceiptBreakdownNote } = require("./nanbanPdfService");
              const breakdownNote = feeReceiptBreakdownNote(s, {
                defaultSchoolPvrFee: schoolPvrFeeDefaultFromSnap_(snap)
              });
              const receiptMsg =
                `💰 *கட்டண ரசீது (Receipt)*\n\n` +
                `மாணவர்: ${s.name}\n` +
                `தொகை: ₹${amt}\n` +
                `பெற்றவர்: ${trainer}\n` +
                `தேதி: ${today}\n` +
                `மீதம்: ₹${bal}\n\n` +
                (breakdownNote ? `${breakdownNote}\n\n` : "") +
                tagline;
              const { enqueuePdfOrText_ } = require("./waPdfOutbound");
              const pdfBuf = await buildFeeReceiptPdf({
                studentName: s.name,
                amount: amt,
                receiver: trainer,
                dateStr: today,
                balance: bal,
                tagline,
                breakdownNote
              });
              await enqueuePdfOrText_({
                tenantId: TENANT_DEFAULT,
                to: wa,
                pdfBuffer: pdfBuf,
                filename: `fee_receipt_${studentId}.pdf`,
                caption: receiptMsg,
                textFallback: receiptMsg,
                metadata: { kind: "fee_receipt_trainer", student_id: String(studentId) },
                delaySeconds: 2
              });
            }
          }
        }

        return { status: "success", student: s };
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
          try {
            await queueRtoPassCertificateWa_(tenantId, s, snap);
          } catch (_certErr) {
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
          await notifyAdminsText(
            tenantId,
            `❌ *TEST ${resultStr.toUpperCase()}:* ${s.name} டெஸ்டில் ${resText}. ${dtTxt}`
          );
        }
        return { status: "success" };
      }

      case "updateExpenseDataAction": {
        const tenantErrUp = assertSchoolLedgerExpenseTenant_(tenantId);
        if (tenantErrUp) return tenantErrUp;
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
        try {
          await notifyAdminsText(
            tenantId,
            `📝 Expense Updated:\n${oldDate} | ${oldSpender}\nCat: ${oldCat}\nAmount: ₹${amt2}\nDesc: ${desc2}`
          );
        } catch (_upN) {
          /* non-fatal */
        }
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

      case "updateStudentPvrStatusAction": {
        const studentId = args[0];
        const rawNext = String(args[1] || "")
          .trim()
          .toLowerCase()
          .replace(/\s+/g, "_");
        const allowed = { applied: "applied", pending_station: "pending_station", received: "received" };
        const next = allowed[rawNext];
        if (!next) return { status: "error", message: "Invalid PVR status" };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        let students = Array.isArray(snap.students) ? [...snap.students] : [];
        const ix = students.findIndex((x) => String(x.id) === String(studentId));
        if (ix < 0) return { status: "error", message: "Student not found" };
        const prevRow = { ...students[ix] };
        if (!prevRow.hasBadge || String(prevRow.pvrMode || "").toLowerCase() !== "school") {
          return { status: "error", message: "Not a school-PVR badge student" };
        }
        const prevSt = String(prevRow.pvrStatus || "applied")
          .trim()
          .toLowerCase()
          .replace(/\s+/g, "_");
        const prevNorm = allowed[prevSt] || "applied";
        const s = { ...prevRow, pvrStatus: next };
        students[ix] = s;
        await saveNanbanPartial(tenantId, { students });
        if (next === "received" && prevNorm !== "received") {
          const wa = waE164_(s.phone);
          const studentMsg = `Great news ${String(s.name || "there")}! Your PVR (Police Verification) letter has been received. We will proceed with your Heavy License process.`;
          if (wa) {
            try {
              await enqueueWaOutboundSend(
                {
                  tenantId: TENANT_DEFAULT,
                  to: wa,
                  message: studentMsg,
                  messageType: "text",
                  metadata: { kind: "pvr_received_student", student_id: String(s.id) }
                },
                { delaySeconds: 0 }
              );
            } catch (_waE) {
              /* non-fatal */
            }
          }
          try {
            await notifyAdminsText(
              tenantId,
              `✅ *PVR received (Heavy track):* ${String(s.name || "-")} — ready for Heavy License steps.\nMobile: ${String(s.phone || "-")}`
            );
          } catch (_a) {
            /* non-fatal */
          }
        }
        return { status: "success", pvrStatus: next };
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
        const { buildFeeReceiptPdf, feeReceiptBreakdownNote } = require("./nanbanPdfService");
        const breakdownNote = feeReceiptBreakdownNote(s, {
          defaultSchoolPvrFee: schoolPvrFeeDefaultFromSnap_(snap)
        });
        const msg =
          `💰 *கட்டண ரசீது (Receipt)*\n\n` +
          `மாணவர்: ${s.name}\n` +
          `தொகை: ₹${amt}\n` +
          `பெற்றவர்: ${recv}\n` +
          `தேதி: ${today}\n` +
          `மீதம்: ₹${bal}\n\n` +
          (breakdownNote ? `${breakdownNote}\n\n` : "") +
          tagline;
        const wa = waE164_(s.phone);
        if (wa) {
          const { enqueuePdfOrText_ } = require("./waPdfOutbound");
          const pdfBuf = await buildFeeReceiptPdf({
            studentName: s.name,
            amount: amt,
            receiver: recv,
            dateStr: today,
            balance: bal,
            tagline,
            breakdownNote
          });
          await enqueuePdfOrText_({
            tenantId,
            to: wa,
            pdfBuffer: pdfBuf,
            filename: `fee_receipt_${studentId}.pdf`,
            caption: msg,
            textFallback: msg,
            metadata: { kind: "digital_receipt", student_id: String(studentId) }
          });
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
              try {
                await queueRtoPassCertificateWa_(tenantId, s, snap);
              } catch (_certErr) {
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

        const users = await handleErpRpc("getAppUsers", [tenantId], Object.assign({}, _auth, { internal: true }));
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
        const tid = String(tenantId || "").trim() || TENANT_DEFAULT;
        let brand;
        try {
          brand = await loadTenantMessagingBrand(tid);
        } catch (_e) {
          brand = { schoolName: "" };
        }
        const reg = await getTenantWaTemplateRegistry(tid);
        const line = sanitizeTemplateParamText(
          `வணக்கம் ${String(s.name || "").trim()}! 🎉 ${brand.schoolName ? `${brand.schoolName} — ` : ""}LLR பதிவு 30 நாள் நிறைவு. RTO டெஸ்ட் தேதிக்கு ${brand.contactLine || "அலுவலகத்தை"} தொடர்பு கொள்ளவும்.`.slice(
            0,
            1020
          )
        );
        try {
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: wa,
              message: "",
              messageType: "template",
              template: buildLlr30dWaTemplate(reg, line, { studentName: s.name }),
              metadata: { kind: "llr_30d", student_id: String(studentId), template_route: reg.llr_30d?.mode || "meta_static" }
            },
            { delaySeconds: 0 }
          );
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        return { status: "success", message: "30 நாள் நினைவூட்டல் வரிசையில் சேர்க்கப்பட்டது." };
      }

      case "sendLLRExpireReminder": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: `மாணவர் கிடைக்கவில்லை! (ID: ${studentId})` };
        const wa = waE164_(s.phone);
        if (!wa) return { status: "error", message: "Phone missing" };
        const tid = String(tenantId || "").trim() || TENANT_DEFAULT;
        let brand;
        try {
          brand = await loadTenantMessagingBrand(tid);
        } catch (_e) {
          brand = { schoolName: "டிரைவிங் ஸ்கூல்" };
        }
        const txt = sanitizeTemplateParamText(
          `உங்கள் LLR விரைவில் காலாவதி — உடனே புதுப்பிக்கவும். ${brand.schoolName}`.slice(0, 1020)
        );
        try {
          const reg = await getTenantWaTemplateRegistry(tid);
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: wa,
              message: "",
              messageType: "template",
              template: buildFeeSummaryTemplateObject(reg, txt, { studentName: s.name }),
              metadata: { kind: "llr_expire", student_id: String(studentId) }
            },
            { delaySeconds: 0 }
          );
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        return { status: "success", message: "LLR காலாவதி நினைவூட்டல் வரிசையில் சேர்க்கப்பட்டது." };
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
        if (!s) return { status: "error", message: "Student not found", msg: "Student not found" };
        const bal =
          (parseInt(s.totalFee, 10) || 0) - (parseInt(s.advance, 10) || 0) - (parseInt(s.discount, 10) || 0);
        if (bal <= 0) return { status: "error", message: "இவருக்கு பேலன்ஸ் ஏதும் இல்லை.", msg: "இவருக்கு பேலன்ஸ் ஏதும் இல்லை." };
        const cfg = nanbanTemplateCfg_(snap);
        const wa = waE164_(s.phone);
        const tid = String(tenantId || "").trim() || TENANT_DEFAULT;
        let brand;
        try {
          brand = await loadTenantMessagingBrand(tid);
        } catch (_e) {
          brand = { schoolName: "எங்கள் ஸ்கூல்" };
        }
        if (wa) {
          try {
            const regPay = await getTenantWaTemplateRegistry(tid);
            const stName = String(s.name || "நண்பரே").trim() || "நண்பரே";
            const payFb =
              `🔔 கட்டண நினைவூட்டல்\n` +
              `${stName} — மீதம்: ₹${bal}\n` +
              `${brand.schoolName}`;
            await enqueueWaOutboundSend(
              {
                tenantId: tid,
                to: wa,
                message: payFb,
                messageType: "template",
                template: buildPaymentReminderWaTemplate(regPay, cfg, s, bal),
                metadata: { kind: "payment_reminder", student_id: String(studentId) }
              },
              { delaySeconds: 0 }
            );
          } catch (e) {
            return { status: "error", message: tamilReasonFromWaError(e), msg: tamilReasonFromWaError(e) };
          }
          const upiId = cfg.businessUpi || "";
          const link = upiLink_(bal, s.name, upiId);
          if (link) {
            try {
              const reg = await getTenantWaTemplateRegistry(tid);
              const upiLine = sanitizeTemplateParamText(
                `💳 UPI செலுத்த: ${link}\n${brand.schoolName}\nUPI: ${upiId}`.slice(0, 1020)
              );
              await enqueueWaOutboundSend(
                {
                  tenantId: tid,
                  to: wa,
                  message: "",
                  messageType: "template",
                  template: buildFeeSummaryTemplateObject(reg, upiLine, { studentName: s.name }),
                  metadata: { kind: "payment_upi", student_id: String(studentId) }
                },
                { delaySeconds: 2 }
              );
            } catch (e2) {
              return { status: "error", message: tamilReasonFromWaError(e2), msg: tamilReasonFromWaError(e2) };
            }
          }
        }
        let students = Array.isArray(snap.students) ? snap.students.map((x) => ({ ...x })) : [];
        const ix = students.findIndex((x) => String(x.id) === String(studentId));
        let outStudent = null;
        if (ix >= 0) {
          const st = { ...students[ix] };
          if (!Array.isArray(st.adminRemarks)) st.adminRemarks = [];
          st.adminRemarks.unshift({ date: getISTDateString(), text: `🔔 Payment Reminder அனுப்பப்பட்டது (${adminName})` });
          const nowMs = Date.now();
          const nextMs = nowMs + 7 * 24 * 60 * 60 * 1000;
          const dayStr = getISTDateString();
          const append = `${dayStr}: WA — Payment reminder queued (${adminName || "office"})`;
          const oldNotes = String(st.callNotes || "").trim();
          st.lastContactedDate = nowMs;
          st.nextFollowUpDate = nextMs;
          st.callNotes = oldNotes ? `${oldNotes}\n${append}` : append;
          students[ix] = st;
          outStudent = st;
          await saveNanbanPartial(tenantId, { students });
        }
        return {
          status: "success",
          message: "கட்டண நினைவூட்டல் வரிசையில் சேர்க்கப்பட்டது.",
          msg: "கட்டண நினைவூட்டல் வரிசையில் சேர்க்கப்பட்டது.",
          student: outStudent,
          crmAuto: true
        };
      }

      case "sendRtoTestAlertAction": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const studentsArr = Array.isArray(snap.students) ? snap.students : [];
        const s = studentsArr.find((x) => x && String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "Student not found" };
        const td = String(s.testDate || "").trim();
        if (!td) {
          return {
            status: "error",
            message: "RTO டெஸ்ட் தேதி பதிவு இல்லை. ப்ரொபைலில் டெஸ்ட் தேதியைச் சேமிக்கவும்."
          };
        }
        const wa = waE164_(s.phone);
        if (!wa) return { status: "error", message: "மொபைல் எண் செல்லுபடியாகவில்லை." };
        const parts = td.split("-");
        const dateStr = parts.length === 3 ? `${parts[2]}/${parts[1]}/${parts[0]}` : td;
        const nm = String(s.name || "அன்புள்ளவர்").trim() || "அன்புள்ளவர்";
        const tid = String(tenantId || "").trim() || TENANT_DEFAULT;
        let brand;
        try {
          brand = await loadTenantMessagingBrand(tid);
        } catch (_e) {
          brand = { schoolName: "" };
        }
        try {
          const reg = await getTenantWaTemplateRegistry(tid);
          const detail = sanitizeTemplateParamText(
            `வணக்கம் ${nm}, ${brand.schoolName ? `${brand.schoolName} — ` : ""}டிரைவிங் டெஸ்ட் ${dateStr} அன்று. LLR ஆவணத்துடன் வரவும்.`.slice(
              0,
              1020
            )
          );
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: wa,
              message: "",
              messageType: "template",
              template: buildFeeSummaryTemplateObject(reg, detail, { studentName: s.name }),
              metadata: { kind: "rto_test_alert", student_id: String(studentId) }
            },
            { delaySeconds: 0 }
          );
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        let students = studentsArr.map((x) => ({ ...x }));
        const ix = students.findIndex((x) => x && String(x.id) === String(studentId));
        if (ix >= 0) {
          const st = { ...students[ix] };
          if (!Array.isArray(st.adminRemarks)) st.adminRemarks = [];
          st.adminRemarks.unshift({
            date: getISTDateString(),
            text: "🚦 RTO test alert sent (WhatsApp)"
          });
          students[ix] = st;
          await saveNanbanPartial(tenantId, { students });
        }
        return { status: "success", message: "RTO அலர்ட் வரிசையில் சேர்க்கப்பட்டது." };
      }

      case "sendRtoChecklistReminderAction": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const studentsArr = Array.isArray(snap.students) ? snap.students : [];
        const s = studentsArr.find((x) => x && String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "Student not found" };
        const wa = waE164_(s.phone);
        if (!wa) return { status: "error", message: "மொபைல் எண் செல்லுபடியாகவில்லை." };
        const pending = getPendingRtoChecklistLines(s, snap);
        const nm = String(s.name || "அன்புள்ளவர்").trim() || "அன்புள்ளவர்";
        const tid = String(tenantId || "").trim() || TENANT_DEFAULT;
        let brand;
        try {
          brand = await loadTenantMessagingBrand(tid);
        } catch (_e) {
          brand = { schoolName: "" };
        }
        let msg;
        if (!pending.length) {
          msg = `வணக்கம் ${nm}, ✅ ஆவண செக்லிஸ்ட் முழுமை — ${brand.schoolName || "எங்கள் ஸ்கூல்"}`;
        } else {
          msg =
            `வணக்கம் ${nm}, 📋 பேண்டிங் (${pending.length}): ` +
            pending.map((t, i) => `${i + 1}.${t}`).join(" | ") +
            ` — ${brand.schoolName || "எங்கள் ஸ்கூல்"}`;
        }
        const line = sanitizeTemplateParamText(msg.slice(0, 1020));
        try {
          const reg = await getTenantWaTemplateRegistry(tid);
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: wa,
              message: "",
              messageType: "template",
              template: buildFeeSummaryTemplateObject(reg, line, { studentName: s.name }),
              metadata: { kind: "rto_checklist_reminder", student_id: String(studentId), pending_count: pending.length }
            },
            { delaySeconds: 0 }
          );
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        let students = studentsArr.map((x) => ({ ...x }));
        const ix = students.findIndex((x) => x && String(x.id) === String(studentId));
        if (ix >= 0) {
          const st = { ...students[ix] };
          if (!Array.isArray(st.adminRemarks)) st.adminRemarks = [];
          st.adminRemarks.unshift({
            date: getISTDateString(),
            text: `📋 RTO checklist WhatsApp (${pending.length} pending)`
          });
          students[ix] = st;
          await saveNanbanPartial(tenantId, { students });
        }
        return {
          status: "success",
          message: pending.length
            ? `செக்லிஸ்ட் நினைவூட்டல் வரிசையில் (${pending.length} பேண்டிங்).`
            : "உறுதிப்படுத்தும் செய்தி வரிசையில் சேர்க்கப்பட்டது."
        };
      }

      case "sendStudentLlrDocumentAction": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "மாணவர் இல்லை" };
        const raw = String(s.llrDocUrl || s.llrDocId || "").trim();
        if (!raw) return { status: "error", message: "LLR கோப்பு ஏற்றப்படவில்லை." };
        let url = "";
        try {
          url = await resolveLlrDocumentUrlForWhatsApp_(
            String(s.llrDocUrl || "").trim(),
            String(s.llrDocId || "").trim()
          );
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        if (!url) {
          url = normalizeMetaWhatsappMediaLink_(raw);
        }
        if (!isUsableWaMediaUrl(url)) {
          return { status: "error", message: "LLR இணைப்பு செல்லாது (HTTPS / Storage பாதை சரிபார்க்கவும்)." };
        }
        const wa = waE164_(s.phone);
        if (!wa) return { status: "error", message: "மொபைல் எண் இல்லை." };
        const tid = String(tenantId || "").trim() || TENANT_DEFAULT;
        let brand;
        try {
          brand = await loadTenantMessagingBrand(tid);
        } catch (_e) {
          brand = { schoolName: "எங்கள் ஸ்கூல்" };
        }
        const cap = `${brand.schoolName} — LLR நகல்`;
        /** Same slot as fee_summary / bulk_announcement — works outside 24h session (template). */
        let reg;
        try {
          reg = await getTenantWaTemplateRegistry(tid);
        } catch (_r) {
          reg = await getTenantWaTemplateRegistry(TENANT_DEFAULT);
        }
        const line = sanitizeTemplateParamText(`${cap}\n📎 ${url}`.slice(0, 1020));
        try {
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: wa,
              message: "",
              messageType: "template",
              template: buildFeeSummaryTemplateObject(reg, line, { studentName: s.name }),
              metadata: {
                kind: "llr_uploaded_doc",
                student_id: String(studentId),
                delivery: "fee_summary_template"
              }
            },
            { delaySeconds: 0 }
          );
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        return { status: "success", message: "LLR நகல் (டெம்ப்ளேட்) வரிசையில் சேர்க்கப்பட்டது." };
      }

      case "sendStudentPassportPdfAction": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "மாணவர் இல்லை" };
        const wa = waE164_(s.phone);
        if (!wa) return { status: "error", message: "மொபைல் எண் இல்லை." };
        const tid = String(tenantId || "").trim() || TENANT_DEFAULT;
        let brand;
        try {
          brand = await loadTenantMessagingBrand(tid);
        } catch (_e) {
          brand = { schoolName: "எங்கள் ஸ்கூல்" };
        }
        const passportLink = studentPassportUrl_(s.id);
        const bal =
          (parseInt(s.totalFee, 10) || 0) - (parseInt(s.advance, 10) || 0) - (parseInt(s.discount, 10) || 0);
        let pdfBuf;
        try {
          pdfBuf = await buildPassportSummaryPdf({
            studentName: String(s.name || "").trim(),
            service: s.service,
            dateJoined: s.dateJoined,
            llrStatus: s.llrStatus,
            classesAttended: s.classesAttended,
            totalFee: s.totalFee,
            advance: s.advance,
            balance: bal,
            passportUrl: passportLink,
            schoolBrand: brand.schoolName
          });
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        let signedUrl = "";
        try {
          const up = await uploadPdfGetSignedUrl(tid, `Passport_${String(studentId)}.pdf`, pdfBuf);
          signedUrl = String(up?.url || "").trim();
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        if (!isUsableWaMediaUrl(signedUrl)) {
          return { status: "error", message: "PDF இணைப்பு உருவாக்க முடியவில்லை." };
        }
        let regP;
        try {
          regP = await getTenantWaTemplateRegistry(tid);
        } catch (_r) {
          regP = await getTenantWaTemplateRegistry(TENANT_DEFAULT);
        }
        const passLine = sanitizeTemplateParamText(
          `${brand.schoolName} — Digital Passport PDF\n📎 ${signedUrl}`.slice(0, 1020)
        );
        try {
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: wa,
              message: "",
              messageType: "template",
              template: buildFeeSummaryTemplateObject(regP, passLine, { studentName: s.name }),
              metadata: {
                kind: "passport_pdf_manual",
                student_id: String(studentId),
                delivery: "fee_summary_template"
              }
            },
            { delaySeconds: 0 }
          );
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        return { status: "success", message: "பாஸ்போர்ட் PDF (டெம்ப்ளேட்) வரிசையில் சேர்க்கப்பட்டது." };
      }

      case "sendEnquiryBrochureAction": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "மாணவர் இல்லை" };
        const wa = waE164_(s.phone);
        if (!wa) return { status: "error", message: "மொபைல் எண் இல்லை." };
        const tid = String(tenantId || "").trim() || TENANT_DEFAULT;
        const brand = await loadTenantMessagingBrand(tid);
        const line = sanitizeTemplateParamText(
          `வணக்கம் ${String(s.name || "").trim()}! ${brand.schoolName} — விவரக்குறிப்பு / பயிற்சி விவரங்களுக்கு ${brand.contactLine || "எங்களை அழைக்கவும்"}. 🚗`.slice(
            0,
            1020
          )
        );
        try {
          const reg = await getTenantWaTemplateRegistry(tid);
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: wa,
              message: "",
              messageType: "template",
              template: buildFeeSummaryTemplateObject(reg, line, { studentName: s.name }),
              metadata: { kind: "enquiry_brochure", student_id: String(studentId) }
            },
            { delaySeconds: 0 }
          );
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        return { status: "success", message: "விவரக்குறிப்பு செய்தி வரிசையில் சேர்க்கப்பட்டது." };
      }

      case "sendEnquiryFollowUpAction": {
        const studentId = args[0];
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const s = (snap.students || []).find((x) => String(x.id) === String(studentId));
        if (!s) return { status: "error", message: "மாணவர் இல்லை" };
        const wa = waE164_(s.phone);
        if (!wa) return { status: "error", message: "மொபைல் எண் இல்லை." };
        const tid = String(tenantId || "").trim() || TENANT_DEFAULT;
        const brand = await loadTenantMessagingBrand(tid);
        const line = sanitizeTemplateParamText(
          `வணக்கம் ${String(s.name || "").trim()}! ${brand.schoolName} — உங்கள் விசாரணைக்கு தொடர்ந்து உதவ தயார். ${brand.contactLine || ""}`.slice(
            0,
            1020
          )
        );
        try {
          const reg = await getTenantWaTemplateRegistry(tid);
          await enqueueWaOutboundSend(
            {
              tenantId: tid,
              to: wa,
              message: "",
              messageType: "template",
              template: buildFeeSummaryTemplateObject(reg, line, { studentName: s.name }),
              metadata: { kind: "enquiry_followup", student_id: String(studentId) }
            },
            { delaySeconds: 0 }
          );
        } catch (e) {
          return { status: "error", message: tamilReasonFromWaError(e) };
        }
        return { status: "success", message: "பின்தொடர் நினைவூட்டல் வரிசையில் சேர்க்கப்பட்டது." };
      }

      case "bulkSendRemindersAction": {
        const studentIds = args[0];
        const type = args[1];
        if (!Array.isArray(studentIds) || !studentIds.length) return { status: "error", msg: "No students selected." };
        const snap = await getBusinessSnapshotDoc(nanbanBusinessDocIdForTenant(tenantId));
        const list = snap.students || [];
        const tidB = String(tenantId || "").trim() || TENANT_DEFAULT;
        const regB = await getTenantWaTemplateRegistry(tidB);
        let brandB = { schoolName: "" };
        try {
          brandB = await loadTenantMessagingBrand(tidB);
        } catch (_e) {
          /* ignore */
        }
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
              const line30 = sanitizeTemplateParamText(
                `வணக்கம் ${String(s.name || "").trim()}! ${brandB.schoolName ? `${brandB.schoolName} — ` : ""}LLR 30 நாள் நிறைவு — RTO தேதிக்கு தொடர்பு கொள்ளவும்.`.slice(
                  0,
                  1020
                )
              );
              await enqueueWaOutboundSend(
                {
                  tenantId: tidB,
                  to: wa,
                  message: "",
                  messageType: "template",
                  template: buildLlr30dWaTemplate(regB, line30, { studentName: s.name }),
                  metadata: { kind: "bulk_llr30", student_id: String(id) }
                },
                { delaySeconds: delay }
              );
            } else {
              const txt = sanitizeTemplateParamText(
                `உங்கள் LLR விரைவில் காலாவதி — புதுப்பிக்கவும். ${brandB.schoolName || ""}`.slice(0, 1020)
              );
              await enqueueWaOutboundSend(
                {
                  tenantId: tidB,
                  to: wa,
                  message: "",
                  messageType: "template",
                  template: buildFeeSummaryTemplateObject(regB, txt, { studentName: s.name }),
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
        await notifyAdminsText(
          tenantId,
          `🔄 *Re-Test பதிவு:*\nமாணவர்: ${s.name}\nகட்டணம்: ₹${testFee}\nதேதி: ${newDate}`
        );
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
        await notifyAdminsText(
          tenantId,
          `🤝 *சீட்டு பட்டுவாடா நிறைவு*\n\nஏல எண்: ${auctionId}\nநிலை: Settled`,
          "chit"
        );
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
          `💰 *சீட்டு வசூல்!*\n\nமெம்பர்: ${payObj.memberName}\nதொகை: ₹${payObj.amount}\nபெற்றவர்: ${payObj.receiver}`,
          "chit"
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
        return { status: "success", items: [], message: "Native mode: audit logs are not migrated from Sheets." };

      case "getFilingEntriesAction": {
        const mkF = String(args[0] || "").trim();
        if (!mkF || mkF.length < 7) return { status: "success", items: [] };
        return listNanbanFilingEntries(tenantId, mkF);
      }

      case "generateFilingIndexPdfAction": {
        const mk = String(args[0] || "").trim();
        if (!mk || mk.length < 7) return { status: "error", message: "Month (YYYY-MM) required" };
        try {
          const prior = await listNanbanFilingEntries(tenantId, mk);
          const priorCount = (prior.items || []).length;
          const buf = await generateHostedFilingIndexPdf({ monthKey: mk, tenantId });
          const out = await uploadPdfGetSignedUrl(
            tenantId,
            `Filing_Index_${mk.replace(/[^\w.-]/g, "_")}.pdf`,
            buf
          );
          await appendNanbanFilingEntryQuiet(tenantId, {
            monthKey: mk,
            reportType: "FilingIndex",
            url: out.url,
            metaObj: { count: priorCount },
            actor: String(args[1] || "").trim()
          });
          return { status: "success", url: out.url, id: out.objectPath };
        } catch (e) {
          return { status: "error", message: String(e && e.message ? e.message : e) };
        }
      }

      case "generateMonthlyCashbookPdfAction": {
        const fromC = String(args[0] || "").trim();
        const toC = String(args[1] || "").trim();
        const byC = String(args[2] || "").trim();
        if (!fromC || !toC) return { status: "error", message: "Date range required" };
        try {
          const buf = await generateHostedCashbookPdf({
            tenantId,
            fromIso: fromC,
            toIso: toC,
            loggedBy: byC
          });
          const fn = `Cashbook_${fromC}_${toC}.pdf`.replace(/[^\w.-]/g, "_");
          const out = await uploadPdfGetSignedUrl(tenantId, fn, buf);
          const mkC = fromC.length >= 7 ? fromC.slice(0, 7) : "";
          if (mkC) {
            await appendNanbanFilingEntryQuiet(tenantId, {
              monthKey: mkC,
              reportType: "Cashbook",
              url: out.url,
              metaObj: { from: fromC, to: toC },
              actor: byC
            });
          }
          return { status: "success", url: out.url, id: out.objectPath };
        } catch (e) {
          return { status: "error", message: String(e && e.message ? e.message : e) };
        }
      }

      case "generateFullAuditPdfAction": {
        const fromF = String(args[0] || "").trim();
        const toF = String(args[1] || "").trim();
        const byF = String(args[2] || "").trim();
        if (!fromF || !toF) return { status: "error", message: "Date range required" };
        try {
          const buf = await generateHostedFullAuditPdf({
            tenantId,
            fromIso: fromF,
            toIso: toF,
            loggedBy: byF
          });
          const fn = `FullAudit_${fromF}_${toF}.pdf`.replace(/[^\w.-]/g, "_");
          const out = await uploadPdfGetSignedUrl(tenantId, fn, buf);
          const mkF = fromF.length >= 7 ? fromF.slice(0, 7) : "";
          if (mkF) {
            await appendNanbanFilingEntryQuiet(tenantId, {
              monthKey: mkF,
              reportType: "FullAudit",
              url: out.url,
              metaObj: { from: fromF, to: toF },
              actor: byF
            });
          }
          return { status: "success", url: out.url, id: out.objectPath };
        } catch (e) {
          return { status: "error", message: String(e && e.message ? e.message : e) };
        }
      }

      case "generateMonthlyPdfPackAction": {
        const fromP = String(args[0] || "").trim();
        const toP = String(args[1] || "").trim();
        const byP = String(args[2] || "").trim();
        if (!fromP || !toP) return { status: "error", message: "Date range required" };
        try {
          const fullBuf = await generateHostedFullAuditPdf({
            tenantId,
            fromIso: fromP,
            toIso: toP,
            loggedBy: byP
          });
          const cashBuf = await generateHostedCashbookPdf({
            tenantId,
            fromIso: fromP,
            toIso: toP,
            loggedBy: byP
          });
          const fullUp = await uploadPdfGetSignedUrl(
            tenantId,
            `Pack_Full_${fromP}_${toP}.pdf`.replace(/[^\w.-]/g, "_"),
            fullBuf
          );
          const cashUp = await uploadPdfGetSignedUrl(
            tenantId,
            `Pack_Cash_${fromP}_${toP}.pdf`.replace(/[^\w.-]/g, "_"),
            cashBuf
          );
          const mkP = fromP.length >= 7 ? fromP.slice(0, 7) : "";
          if (mkP) {
            await appendNanbanFilingEntryQuiet(tenantId, {
              monthKey: mkP,
              reportType: "Pack_Full",
              url: fullUp.url,
              metaObj: { from: fromP, to: toP },
              actor: byP
            });
            await appendNanbanFilingEntryQuiet(tenantId, {
              monthKey: mkP,
              reportType: "Pack_Cash",
              url: cashUp.url,
              metaObj: { from: fromP, to: toP },
              actor: byP
            });
          }
          return {
            status: "success",
            full: { status: "success", url: fullUp.url, id: fullUp.objectPath },
            cashbook: { status: "success", url: cashUp.url, id: cashUp.objectPath }
          };
        } catch (e) {
          return { status: "error", message: String(e && e.message ? e.message : e) };
        }
      }

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
          const { getTopicKeyFromQuizRow } = require("../lib/quizTopicMap");
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
            st.quizPendingQueue.push({
              quizDay: 1,
              correctNo,
              topicKey: qRow ? getTopicKeyFromQuizRow(qRow) : "general"
            });
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

      case "extractLlrFromImageAction": {
        const dataUrl = args[0];
        const dec = decodeDataUrlForUpload_(dataUrl);
        if (!dec || !dec.buf || !dec.buf.length) {
          return { status: "error", message: "Invalid or empty image" };
        }
        if (dec.buf.length > 9 * 1024 * 1024) {
          return { status: "error", message: "Image too large (max 9 MB)" };
        }
        const ctRaw = String(dec.contentType || "").toLowerCase();
        const isPdf = ctRaw === "application/pdf" || isPdfMagicBytes_(dec.buf);
        const sniffed = sniffImageMimeFromBuffer_(dec.buf);
        let mediaCt = ctRaw;
        if (!isPdf && !mediaCt.startsWith("image/") && sniffed) mediaCt = sniffed;
        if (isPdf) {
          try {
            const parsed = await extractLlrFieldsFromPdfBuffer_(dec.buf);
            if (!parsed.llrNumber && !parsed.dobYmd) {
              return {
                status: "error",
                message:
                  "PDF-ல் LLR எண்/DOB கண்டறிய முடியவில்லை. ஸ்கேன் PDF எனில் முதல் பக்கத்தை JPG screenshot ஆக எடுத்து அனுப்பவும்."
              };
            }
            return {
              status: "success",
              llrNumber: parsed.llrNumber || "",
              dobYmd: parsed.dobYmd || "",
              holderName: parsed.holderName || "",
              bloodGroup: parsed.bloodGroup || "",
              rawTextSnippet: parsed.rawTextSnippet || ""
            };
          } catch (e) {
            const code = e && e.code;
            if (code === "PDF_SCANNED_NO_TEXT" || String(e && e.message).includes("pdf_no_extractable")) {
              return {
                status: "error",
                message:
                  "இந்த PDF ஸ்கேன் படம் — உள்ளே உரை இல்லை. மொபைலில் முதல் பக்கம் screenshot (JPG) எடுத்து upload செய்யவும்."
              };
            }
            const msg = String(e && e.message ? e.message : e);
            return {
              status: "error",
              message: `PDF: ${msg}`
            };
          }
        }
        if (!mediaCt.startsWith("image/")) {
          return {
            status: "error",
            message: `JPG / PNG / WebP / PDF மட்டும் (கோப்பு: ${ctRaw || "?"})`
          };
        }
        try {
          const parsed = await extractLlrFieldsFromBuffer_(dec.buf);
          return {
            status: "success",
            llrNumber: parsed.llrNumber || "",
            dobYmd: parsed.dobYmd || "",
            holderName: parsed.holderName || "",
            bloodGroup: parsed.bloodGroup || "",
            rawTextSnippet: parsed.rawTextSnippet || ""
          };
        } catch (e) {
          const msg = String(e && e.message ? e.message : e);
          return {
            status: "error",
            message: `Vision OCR: ${msg}. GCP-ல் Cloud Vision API enable செய்யவும்.`
          };
        }
      }

      case "generateGoogleOAuthUrl": {
        const idToken = String(args[0] || "").trim();
        const authz = await assertFirebaseUserIsAdminForGoogleContacts_(idToken, tenantId);
        if (!authz.ok) {
          return { status: "error", message: authz.message };
        }
        const out = await createGoogleOAuthConsentUrl({
          tenantId,
          firebaseUid: authz.uid
        });
        if (out.status !== "success") {
          return { status: "error", message: out.message || "oauth_url_failed" };
        }
        return { status: "success", authUrl: out.authUrl };
      }

      case "testContactSyncAction": {
        const idToken = String(args[0] || "").trim();
        const studentId = args[1];
        const authz = await assertFirebaseUserIsAdminForGoogleContacts_(idToken, tenantId);
        if (!authz.ok) {
          return { status: "error", message: authz.message };
        }
        const bid = nanbanBusinessDocIdForTenant(tenantId);
        const snap = await getBusinessSnapshotDoc(bid);
        const students = Array.isArray(snap.students) ? snap.students : [];
        const student = students.find((x) => x && String(x.id) === String(studentId));
        if (!student) {
          return { status: "error", message: "student_not_found" };
        }
        const syncRes = await runGoogleContactSyncAfterStudentWrite_(tenantId, student, snap);
        return {
          status: "success",
          contactSync: contactSyncOutcomeForRpc(syncRes),
          detail: syncRes
        };
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
