const admin = require("firebase-admin");
const { sanitizeTemplateParamText } = require("../lib/sanitizeTemplateParam");
const { enqueueWaOutboundSend } = require("./waOutboundQueue");
const { getTenantWaTemplateRegistry } = require("./waTemplateConfig");
const {
  normalizeServiceKey,
  buildEnquiryWelcomTwoParams,
  buildEnquiryStyleTemplateParams
} = require("./dynamicPricingEngine");

const GAS_TEMPLATE_DEFAULT_NAMES = {
  welcome_admission: "welcome_admission",
  rto: "rto_test_reminder"
};

function digitsOnly(phone) {
  return String(phone || "").replace(/\D/g, "");
}

/**
 * WhatsApp Cloud API expects country code; Nanban uses India 91.
 */
function normalizePhoneForWa(raw) {
  let d = digitsOnly(raw);
  if (d.length === 10 && /^[6-9]/.test(d)) d = `91${d}`;
  return d;
}

function inferJobKindFromStudent(student) {
  const t = String(student?.type || "").trim();
  if (t === "Enquiry") return "enquiry_welcome";
  if (t === "Test_Admission") return "rto_test_reminder";
  return "welcome_admission";
}

async function loadWelcomeAndRtoSpecs(tenantId) {
  const tid = String(tenantId || "").trim() || "nanban_main";
  const db = admin.firestore();
  const snap = await db.collection("platform_tenants").doc(tid).get();
  const data = snap.exists ? snap.data() || {} : {};
  const wt = data.wa_templates && typeof data.wa_templates === "object" ? data.wa_templates : {};
  const wWelcome = wt.welcome_admission || {};
  const wRto = wt.rto_reminder || wt.rto || {};
  return {
    welcome: {
      name: String(wWelcome.name || data.wa_welcome_template_name || GAS_TEMPLATE_DEFAULT_NAMES.welcome_admission).trim(),
      language: String(wWelcome.language || data.wa_welcome_template_language || "ta").trim() || "ta"
    },
    rto: {
      name: String(wRto.name || data.wa_rto_template_name || GAS_TEMPLATE_DEFAULT_NAMES.rto).trim(),
      language: String(wRto.language || data.wa_rto_template_language || "ta").trim() || "ta"
    }
  };
}

function admissionServiceLineTa(serviceRaw) {
  const keys = [normalizeServiceKey(serviceRaw)];
  const pair = buildEnquiryWelcomTwoParams(keys, "x");
  const line = String(pair[1] || "ஓட்டுநர் பயிற்சி").trim();
  return `${line} அட்மிஷன்`;
}

function feeBalance(student) {
  const total = parseInt(student?.totalFee, 10) || 0;
  const adv = parseInt(student?.advance, 10) || 0;
  const disc = parseInt(student?.discount, 10) || 0;
  return Math.max(0, total - adv - disc);
}

function fallbackEnquiryText(name) {
  const n = String(name || "நண்பர்").trim() || "நண்பர்";
  return (
    `வணக்கம் ${n}! 🙏\n\n` +
    `நண்பன் டிரைவிங் ஸ்கூலைத் தொடர்பு கொண்டதற்கு நன்றி. உங்கள் விசாரணை பதிவு செய்யப்பட்டுள்ளது.\n\n` +
    `எங்கள் பயிற்சியாளர் விரைவில் தொடர்புகொள்வார்.`
  );
}

function fallbackWelcomeAdmissionText(student) {
  const n = String(student?.name || "நண்பர்").trim();
  const bal = feeBalance(student);
  return (
    `வாழ்த்துக்கள் ${n}! 🎉\n\n` +
    `நண்பன் டிரைவிங் ஸ்கூலில் உங்கள் அட்மிஷன் பதிவாகியுள்ளது.\n\n` +
    `📅 தேதி: ${String(student?.dateJoined || "-")}\n` +
    `💰 மொத்தம்: ₹${String(student?.totalFee || 0)}\n` +
    `✅ முன்பணம்: ₹${String(student?.advance || 0)}\n` +
    `⚠️ மீதம்: ₹${bal}\n\n` +
    `நன்றி! 🙏`
  );
}

function fallbackRtoText(student) {
  const n = String(student?.name || "நண்பர்").trim();
  return (
    `வணக்கம் ${n}! 🚗\n\n` +
    `உங்கள் RTO ஓட்டுநர் தேர்வு நெருங்குகிறது. அசல் LLR, ஆதார் கண்டிப்பாக எடுத்து வரவும்.\n\n` +
    `அனைத்து வெற்றிகளும்! 🏆`
  );
}

/**
 * Process a single WA native job document (Firestore trigger or direct call).
 * @param {string} tenantId
 * @param {object} job — { kind, student }
 * @param {FirebaseFirestore.DocumentReference} [jobRef]
 */
async function processWaNativeJob(tenantId, job, jobRef) {
  const tid = String(tenantId || "").trim() || "nanban_main";
  const kind = String(job?.kind || "").trim() || inferJobKindFromStudent(job?.student || {});
  const student = job?.student && typeof job.student === "object" ? job.student : {};
  const to = normalizePhoneForWa(student.phone);

  if (!to || to.length < 10) {
    const err = new Error("WA_NATIVE_JOB_MISSING_PHONE");
    err.code = "MISSING_PHONE";
    throw err;
  }

  const displayName = String(student.name || "நண்பர்").trim() || "நண்பர்";
  const specs = await loadWelcomeAndRtoSpecs(tid);

  if (kind === "enquiry_welcome") {
    const reg = await getTenantWaTemplateRegistry(tid);
    const slots = reg.enquiry.bodyParamCount;
    const selected = [normalizeServiceKey(student.service)];
    let bodyParams;
    if (slots === 4) {
      bodyParams = buildEnquiryStyleTemplateParams(selected, displayName).map(sanitizeTemplateParamText);
    } else if (slots === 2) {
      bodyParams = buildEnquiryWelcomTwoParams(selected, displayName).map(sanitizeTemplateParamText);
    } else {
      bodyParams = [sanitizeTemplateParamText(displayName)];
    }
    const tplName = String(reg.enquiry.name || "").trim();
    const message = fallbackEnquiryText(displayName);
    await enqueueWaOutboundSend(
      {
        tenantId: tid,
        to,
        message,
        messageType: "template_with_text_fallback",
        template: tplName
          ? {
              name: tplName,
              languageCode: reg.enquiry.language,
              bodyParams,
              tryFirst: true
            }
          : null,
        metadata: {
          source: "wa_native_job",
          kind: "enquiry_welcome",
          student_id: String(student.id || "")
        }
      },
      { delaySeconds: 0 }
    );
    return;
  }

  if (kind === "rto_test_reminder") {
    const message = fallbackRtoText(student);
    await enqueueWaOutboundSend(
      {
        tenantId: tid,
        to,
        message,
        messageType: "template_with_text_fallback",
        template: specs.rto.name
          ? {
              name: specs.rto.name,
              languageCode: specs.rto.language,
              bodyParams: [sanitizeTemplateParamText(displayName)],
              tryFirst: true
            }
          : null,
        metadata: { source: "wa_native_job", kind: "rto_test_reminder", student_id: String(student.id || "") }
      },
      { delaySeconds: 0 }
    );
    return;
  }

  if (kind === "welcome_admission") {
    const serviceLine = admissionServiceLineTa(student.service);
    const bal = feeBalance(student);
    const bodyParams = [
      sanitizeTemplateParamText(displayName),
      sanitizeTemplateParamText(serviceLine),
      sanitizeTemplateParamText(String(student.dateJoined || "-")),
      sanitizeTemplateParamText(String(parseInt(student.totalFee, 10) || 0)),
      sanitizeTemplateParamText(String(parseInt(student.advance, 10) || 0)),
      sanitizeTemplateParamText(String(bal))
    ];
    const message = fallbackWelcomeAdmissionText(student);
    await enqueueWaOutboundSend(
      {
        tenantId: tid,
        to,
        message,
        messageType: "template_with_text_fallback",
        template: specs.welcome.name
          ? {
              name: specs.welcome.name,
              languageCode: specs.welcome.language,
              bodyParams,
              tryFirst: true
            }
          : null,
        metadata: { source: "wa_native_job", kind: "welcome_admission", student_id: String(student.id || "") }
      },
      { delaySeconds: 0 }
    );
    return;
  }

  const err = new Error(`WA_NATIVE_JOB_UNKNOWN_KIND:${kind}`);
  err.code = "UNKNOWN_KIND";
  throw err;
}

module.exports = {
  processWaNativeJob,
  inferJobKindFromStudent,
  normalizePhoneForWa,
  digitsOnly
};
