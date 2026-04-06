// ==============================================================================
// 🚀 NANBAN DRIVING SCHOOL ERP - BACKEND CODE (WORLD CLASS EDITION V16)
// 📅 Date: March 2026
// 📝 Description: Driving School + Chit Fund + Digital Passport + Auto-Alerts
// ⚠️ STATUS: FULLY EXPANDED (UNMINIFIED), PROPERLY FORMATTED
// ==============================================================================

// ------------------------------------------------------------------------------
// 1. GLOBAL CONSTANTS & CONFIGURATIONS
// ------------------------------------------------------------------------------

const WA_TOKEN = "EAAUDXkCpcxEBQ8PP6ypyKwBvPElhwaAiultcZCYhK53P9ZAVmABJKLmmwZA1Gn4hb5LW5oRiRHV6fqT5IaLBbQvTADYJSUQE7c6MZCoOwGhSlBbyGyeEkRl2m1qobi6MclFWAEvRZCZBFyyQJZBfja3YSZCC1BCrmQZCu3ezP25CGYqiMEddDEZCYlITZBukmEBpgZDZD"; 
const WA_PHONE_ID = "978781185326220";
const FIREBASE_RTDB_URL = "https://nanban-driving-school-d7b20-default-rtdb.firebaseio.com";
// Optional: set Database secret in Script Properties key FIREBASE_DB_SECRET
const FIREBASE_DB_SECRET = "";
const IMPORT_BACKUP_KEY = "nanban_import_2026";

// 🎯 அட்மின் போன் எண்கள் (Day Close, Negative Feedback Alerts)
const ADMINS = [
    "919942391870", // ரஞ்சித் அண்ணா
    "919092036666"  // நந்தகுமார் அண்ணா
];

// --- RANJITH E-SEVAI SHEET NAMES ---
const ESEVAI_SERVICES_SHEET = "ES_Services";
const ESEVAI_CUSTOMERS_SHEET = "ES_Customers";
const ESEVAI_TRANSACTIONS_SHEET = "ES_Transactions";
const ESEVAI_LEDGER_SHEET = "ES_Ledger";
const ESEVAI_ENQUIRIES_SHEET = "ES_Enquiries";
const ESEVAI_WORKS_SHEET = "ES_Works";
const ESEVAI_BALANCES_SHEET = "ES_Opening_Balances";
const ESEVAI_SETTINGS_SHEET = "ES_Settings";
const DEFAULT_TENANT_ID = "nanban_main";

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

// ------------------------------------------------------------------------------
// 2. UTILITY FUNCTIONS (அடிப்படை வசதிகள்)
// ------------------------------------------------------------------------------

let __vdbCache = null;
let __vdbDirty = false;
let __vdbBatchDepth = 0;
let __vdbDirtySheets = {};

function loadVirtualDbStore_() {
    if (__vdbCache) return __vdbCache;
    __vdbCache = { sheets: fbGet_("virtual_db/sheets", {}) || {} };
    if (!__vdbCache.sheets || typeof __vdbCache.sheets !== "object") __vdbCache.sheets = {};
    return __vdbCache;
}

function saveVirtualDbStore_() {
    if (!__vdbCache) return;
    let dirtyNames = Object.keys(__vdbDirtySheets || {});
    if (!dirtyNames.length) {
        __vdbDirty = false;
        return;
    }
    dirtyNames.forEach(function(name) {
        let s = (__vdbCache.sheets && __vdbCache.sheets[name]) ? __vdbCache.sheets[name] : { rows: [] };
        fbPut_("virtual_db/sheets/" + name, { rows: toMatrix_(s.rows || []) });
    });
    __vdbDirtySheets = {};
    __vdbDirty = false;
}

function markVirtualDbDirty_(sheetName) {
    __vdbDirty = true;
    if (sheetName) __vdbDirtySheets[sheetName] = true;
}

function beginVdbBatch_() {
    __vdbBatchDepth++;
}

function endVdbBatch_() {
    __vdbBatchDepth = Math.max(0, __vdbBatchDepth - 1);
    if (__vdbBatchDepth === 0 && __vdbDirty) saveVirtualDbStore_();
}

function flushVirtualDb_() {
    if (__vdbDirty) saveVirtualDbStore_();
}

function withVdbBatch_(fn) {
    beginVdbBatch_();
    try {
        return fn();
    } finally {
        endVdbBatch_();
    }
}

function toMatrix_(rows) {
    return (rows || []).map(function(r) { return Array.isArray(r) ? r.slice() : []; });
}

function maxCols_(rows) {
    let m = 0;
    (rows || []).forEach(function(r) { if (Array.isArray(r) && r.length > m) m = r.length; });
    return m;
}

function VirtualRange_(sheet, row, col, numRows, numCols) {
    this.sheet = sheet;
    this.row = Math.max(1, row || 1);
    this.col = Math.max(1, col || 1);
    this.numRows = Math.max(1, numRows || 1);
    this.numCols = Math.max(1, numCols || 1);
}

VirtualRange_.prototype.getValues = function() {
    let rows = this.sheet._getRows();
    let out = [];
    for (let r = 0; r < this.numRows; r++) {
        let rowArr = [];
        for (let c = 0; c < this.numCols; c++) {
            let rr = this.row - 1 + r, cc = this.col - 1 + c;
            rowArr.push((rows[rr] && rows[rr][cc] !== undefined) ? rows[rr][cc] : "");
        }
        out.push(rowArr);
    }
    return out;
};

VirtualRange_.prototype.getDisplayValues = function() {
    return this.getValues().map(function(r) { return r.map(function(v) { return String(v === undefined || v === null ? "" : v); }); });
};

VirtualRange_.prototype.setValue = function(v) {
    return this.setValues([[v]]);
};

VirtualRange_.prototype.setValues = function(vals) {
    let rows = this.sheet._getRows();
    for (let r = 0; r < this.numRows; r++) {
        let rr = this.row - 1 + r;
        while (rows.length <= rr) rows.push([]);
        for (let c = 0; c < this.numCols; c++) {
            let cc = this.col - 1 + c;
            rows[rr][cc] = (vals[r] && vals[r][c] !== undefined) ? vals[r][c] : "";
        }
    }
    this.sheet._setRows(rows);
    return this;
};

VirtualRange_.prototype.setFontWeight = function() { return this; };
VirtualRange_.prototype.setBackground = function() { return this; };

function VirtualSheet_(name) {
    this.name = name;
}

VirtualSheet_.prototype._getRows = function() {
    let store = loadVirtualDbStore_();
    let s = store.sheets[this.name];
    if (!s) {
        store.sheets[this.name] = { rows: [] };
        s = store.sheets[this.name];
    }
    if (!Array.isArray(s.rows)) s.rows = [];
    return toMatrix_(s.rows);
};

VirtualSheet_.prototype._setRows = function(rows) {
    let store = loadVirtualDbStore_();
    store.sheets[this.name] = { rows: toMatrix_(rows) };
    markVirtualDbDirty_(this.name);
    if (__vdbBatchDepth === 0) saveVirtualDbStore_();
};

VirtualSheet_.prototype.getDataRange = function() {
    let rows = this._getRows();
    let nr = Math.max(1, rows.length);
    let nc = Math.max(1, maxCols_(rows));
    return new VirtualRange_(this, 1, 1, nr, nc);
};

VirtualSheet_.prototype.getRange = function(row, col, numRows, numCols) {
    return new VirtualRange_(this, row, col, numRows || 1, numCols || 1);
};

VirtualSheet_.prototype.getLastRow = function() {
    return this._getRows().length;
};

VirtualSheet_.prototype.appendRow = function(arr) {
    let rows = this._getRows();
    rows.push(Array.isArray(arr) ? arr.slice() : []);
    this._setRows(rows);
    return this;
};

VirtualSheet_.prototype.deleteRow = function(r) {
    let rows = this._getRows();
    let idx = Math.max(0, (r || 1) - 1);
    if (idx < rows.length) rows.splice(idx, 1);
    this._setRows(rows);
    return this;
};

VirtualSheet_.prototype.clear = function() {
    this._setRows([]);
    return this;
};

function VirtualDb_() {}

VirtualDb_.prototype.getSheetByName = function(name) {
    let store = loadVirtualDbStore_();
    if (!store.sheets[name]) return null;
    return new VirtualSheet_(name);
};

VirtualDb_.prototype.insertSheet = function(name) {
    let store = loadVirtualDbStore_();
    if (!store.sheets[name]) {
        store.sheets[name] = { rows: [] };
        saveVirtualDbStore_();
    }
    return new VirtualSheet_(name);
};

VirtualDb_.prototype.getSheets = function() {
    let store = loadVirtualDbStore_();
    return Object.keys(store.sheets || {}).map(function(k) { return new VirtualSheet_(k); });
};

function getDB() {
    return new VirtualDb_();
}

function getCleanToken() { 
    return WA_TOKEN.replace(/\s+/g, ''); 
}

function uiAlert_(msg) {
    try { console.log(String(msg || "")); } catch (e) {}
}

function uiToast_(msg, title) {
    try { console.log((title ? (title + ": ") : "") + String(msg || "")); } catch (e) {}
}

function flushNoop_() {
    flushVirtualDb_();
    return true;
}

function useFirebaseRtdb_() {
    return !!FIREBASE_RTDB_URL;
}

function getFirebaseDbSecret_() {
    try {
        let p = PropertiesService.getScriptProperties().getProperty("FIREBASE_DB_SECRET");
        return p || FIREBASE_DB_SECRET || "";
    } catch (e) {
        return FIREBASE_DB_SECRET || "";
    }
}

function getImportBackupKey_() {
    try {
        let p = PropertiesService.getScriptProperties().getProperty("IMPORT_BACKUP_KEY");
        return String(p || IMPORT_BACKUP_KEY || "").trim();
    } catch (e) {
        return String(IMPORT_BACKUP_KEY || "").trim();
    }
}

function getWebBridgeKey_() {
    try {
        let sp = PropertiesService.getScriptProperties();
        let p = sp.getProperty("WEB_BRIDGE_KEY");
        if (!p) p = sp.getProperty("WEB_BRIDGE_SECRET");
        return String(p || "").trim();
    } catch (e) {
        return "";
    }
}

function jsonOut_(obj) {
    return ContentService
        .createTextOutput(JSON.stringify(obj || {}))
        .setMimeType(ContentService.MimeType.JSON);
}

function isCallableBridgeFunction_(fnName) {
    let f = String(fnName || "").trim();
    if (!f) return false;
    if (!/^[A-Za-z0-9_]+$/.test(f)) return false;
    if (f.slice(-1) === "_") return false;
    if (/^(zz|test|runDiagnostic|fix|migrate|restore|setup)/i.test(f)) return false;

    // Deny critical internals and webhook/runtime entry points.
    let deny = {
        "doGet": 1,
        "doPost": 1,
        "AUTHORIZE_SCRIPT": 1,
        "getCleanToken": 1,
        "getFirebaseDbSecret_": 1,
        "getImportBackupKey_": 1,
        "getWebBridgeKey_": 1,
        "fbRequest_": 1,
        "fbPut_": 1,
        "fbGet_": 1,
        "importBackupPayloadAction": 1,
        "restoreFromDriveXlsxToFirebase": 1,
        "restoreFromNamedXlsxBackup": 1,
        "migrateDataToFirebase": 1,
        "migrateLegacyAllDataToFirebase": 1,
        "AUTHORIZE_SCRIPT": 1
    };
    if (deny[f]) return false;
    return (typeof this[f] === "function");
}

function secureCompare_(a, b) {
    let x = String(a || "");
    let y = String(b || "");
    let maxLen = Math.max(x.length, y.length);
    let diff = x.length ^ y.length;
    for (let i = 0; i < maxLen; i++) {
        let cx = i < x.length ? x.charCodeAt(i) : 0;
        let cy = i < y.length ? y.charCodeAt(i) : 0;
        diff |= (cx ^ cy);
    }
    return diff === 0;
}

function sanitizeInboundArg_(v, depth) {
    depth = depth || 0;
    if (depth > 7) return null;
    if (v === null || v === undefined) return v;
    let t = typeof v;
    if (t === "string") {
        // Keep printable text; strip null/control characters.
        return v.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "").slice(0, 20000);
    }
    if (t === "number" || t === "boolean") return v;
    if (Array.isArray(v)) return v.map(function(item) { return sanitizeInboundArg_(item, depth + 1); });
    if (t === "object") {
        let out = {};
        Object.keys(v).forEach(function(k) {
            let kk = String(k || "").replace(/[^A-Za-z0-9_\-]/g, "_").slice(0, 80);
            if (!kk) return;
            out[kk] = sanitizeInboundArg_(v[k], depth + 1);
        });
        return out;
    }
    return String(v);
}

function assertBridgeRateLimit_(reqKey, fnName) {
    try {
        let cache = CacheService.getScriptCache();
        let bucket = Utilities.formatDate(new Date(), "UTC", "yyyyMMddHHmm");
        let rk = String(reqKey || "").slice(0, 12);
        let key = "br_rl_" + rk + "_" + String(fnName || "").slice(0, 24) + "_" + bucket;
        let cur = parseInt(cache.get(key) || "0", 10);
        let next = (isNaN(cur) ? 0 : cur) + 1;
        cache.put(key, String(next), 70);
        if (next > 120) throw new Error("Rate limit exceeded. Retry in 1 minute.");
    } catch (e) {
        if (String(e || "").indexOf("Rate limit exceeded") !== -1) throw e;
    }
}

function handleApiBridgeCall_(json) {
    try {
        let reqKey = String((json && json.key) || "").trim();
        let serverKey = getWebBridgeKey_();
        if (!serverKey || !reqKey || !secureCompare_(reqKey, serverKey)) {
            return jsonOut_({ status: "error", message: "Unauthorized bridge request" });
        }

        let fnName = String((json && json.fn) || "").trim();
        let args = (json && Array.isArray(json.args)) ? json.args : [];
        if (JSON.stringify(args).length > 250000) {
            return jsonOut_({ status: "error", message: "Payload too large" });
        }
        assertBridgeRateLimit_(reqKey, fnName);
        args = args.map(function(a) { return sanitizeInboundArg_(a, 0); });

        if (!isCallableBridgeFunction_(fnName)) {
            return jsonOut_({ status: "error", message: "Function not allowed: " + fnName });
        }

        let result = this[fnName].apply(this, args);
        return jsonOut_({ status: "success", result: result });
    } catch (e) {
        return jsonOut_({ status: "error", message: String(e && e.message ? e.message : e) });
    }
}

function fbUrl_(path) {
    let cleanPath = String(path || "").replace(/^\/+|\/+$/g, "");
    let base = FIREBASE_RTDB_URL.replace(/\/+$/g, "");
    let secret = getFirebaseDbSecret_();
    let qs = secret ? ("?auth=" + encodeURIComponent(secret)) : "";
    return base + "/" + cleanPath + ".json" + qs;
}

function fbRequest_(method, path, payload) {
    if (!useFirebaseRtdb_()) return null;
    let opts = {
        method: method,
        muteHttpExceptions: true,
        contentType: "application/json"
    };
    if (payload !== undefined) opts.payload = JSON.stringify(payload);
    let res = UrlFetchApp.fetch(fbUrl_(path), opts);
    let code = res.getResponseCode();
    if (code < 200 || code >= 300) {
        let body = res.getContentText();
        let hint = "";
        if (code === 401 || code === 403) {
            hint = " (Set Script Property FIREBASE_DB_SECRET or allow server access in RTDB rules.)";
        }
        throw new Error("Firebase RTDB error " + code + ": " + body + hint);
    }
    let txt = res.getContentText();
    if (!txt) return null;
    try { return JSON.parse(txt); } catch (e) { return txt; }
}

function fbGet_(path, fallback) {
    try {
        let v = fbRequest_("get", path);
        return (v === null || v === undefined) ? fallback : v;
    } catch (e) {
        return fallback;
    }
}

function fbPut_(path, payload) {
    return fbRequest_("put", path, payload);
}

function normalizeTenantId_(tenantId) {
    let t = String(tenantId || "").trim().toLowerCase();
    if (!t) return "";
    t = t.replace(/[^a-z0-9_-]/g, "-").replace(/-+/g, "-").replace(/^[-_]+|[-_]+$/g, "");
    if (!t) return "";
    if (t.length > 64) t = t.slice(0, 64);
    return t;
}

function getDefaultTenantId_() {
    try {
        let p = PropertiesService.getScriptProperties().getProperty("DEFAULT_TENANT_ID");
        let t = normalizeTenantId_(p || "");
        if (t) return t;
    } catch (e) {}
    return DEFAULT_TENANT_ID;
}

function resolveTenantId_(tenantId) {
    let t = normalizeTenantId_(tenantId || "");
    if (t) return t;
    try {
        let p = PropertiesService.getScriptProperties().getProperty("ACTIVE_TENANT_ID");
        t = normalizeTenantId_(p || "");
        if (t) return t;
    } catch (e) {}
    return getDefaultTenantId_();
}

function tenantPath_(tenantId, relPath) {
    let tid = resolveTenantId_(tenantId);
    let rel = String(relPath || "").replace(/^\/+|\/+$/g, "");
    return "tenants/" + tid + (rel ? ("/" + rel) : "");
}

function tenantFbGet_(tenantId, relPath, fallback) {
    return fbGet_(tenantPath_(tenantId, relPath), fallback);
}

function tenantFbPut_(tenantId, relPath, payload) {
    return fbPut_(tenantPath_(tenantId, relPath), payload);
}

function ensurePlatformPlans_() {
    try {
        let plans = fbGet_("platform/plans", null);
        if (plans && typeof plans === "object" && Object.keys(plans).length) return;
        fbPut_("platform/plans", {
            trial: {
                id: "trial",
                name: "Free Trial",
                duration_days: 14,
                limits: { users: 5, messages_per_day: 100, branches: 1 },
                features: { esevai: true, nanban: true, analytics: false, api_access: false }
            },
            basic: {
                id: "basic",
                name: "Basic Plan",
                monthly_price_inr: 1499,
                limits: { users: 15, messages_per_day: 1000, branches: 2 },
                features: { esevai: true, nanban: true, analytics: true, api_access: false }
            },
            premium: {
                id: "premium",
                name: "Premium Plan",
                monthly_price_inr: 3999,
                limits: { users: 100, messages_per_day: 10000, branches: 20 },
                features: { esevai: true, nanban: true, analytics: true, api_access: true, white_label: true }
            }
        });
    } catch (e) {}
}

function ensureTenantBootstrap_(tenantId) {
    let tid = resolveTenantId_(tenantId);
    try {
        ensurePlatformPlans_();
        let meta = tenantFbGet_(tid, "meta", null);
        if (!meta || typeof meta !== "object" || !meta.created_at) {
            tenantFbPut_(tid, "meta", {
                id: tid,
                code: tid,
                name: tid,
                status: "active",
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            });
        }
        let sub = tenantFbGet_(tid, "subscription", null);
        if (!sub || typeof sub !== "object" || !sub.plan_id) {
            let now = new Date();
            let trialEnd = new Date(now.getTime() + (14 * 86400000));
            tenantFbPut_(tid, "subscription", {
                plan_id: "trial",
                status: "trialing",
                trial_ends_at: trialEnd.toISOString(),
                current_period_ends_at: trialEnd.toISOString(),
                grace_until: "",
                seat_limit: 5,
                feature_overrides: {},
                updated_at: new Date().toISOString()
            });
        }
    } catch (e) {}
    return tid;
}

function getTenantSubscription_(tenantId) {
    let tid = ensureTenantBootstrap_(tenantId);
    return tenantFbGet_(tid, "subscription", {}) || {};
}

function isTenantSubscriptionAllowed_(subObj) {
    let sub = subObj || {};
    let status = String(sub.status || "expired").toLowerCase();
    if (status === "active" || status === "trialing") {
        let endIso = String(sub.current_period_ends_at || sub.trial_ends_at || "").trim();
        if (!endIso) return { allowed: true, reason: "no_end_date" };
        let endTs = new Date(endIso).getTime();
        if (!isFinite(endTs)) return { allowed: true, reason: "invalid_end_treated_as_allowed" };
        if (Date.now() <= endTs) return { allowed: true, reason: "within_period" };
        let graceIso = String(sub.grace_until || "").trim();
        if (graceIso) {
            let gTs = new Date(graceIso).getTime();
            if (isFinite(gTs) && Date.now() <= gTs) return { allowed: true, reason: "within_grace" };
        }
        return { allowed: false, reason: "expired_period" };
    }
    return { allowed: false, reason: "subscription_status_" + status };
}

function buildTenantAccessContext_(tenantId, actorName, actorRole) {
    let tid = ensureTenantBootstrap_(tenantId);
    let meta = tenantFbGet_(tid, "meta", {}) || {};
    let tenantStatus = String(meta.status || "active").toLowerCase();
    if (tenantStatus !== "active") {
        return { allowed: false, tenant_id: tid, reason: "tenant_" + tenantStatus };
    }
    let sub = getTenantSubscription_(tid);
    let s = isTenantSubscriptionAllowed_(sub);
    if (!s.allowed) {
        return { allowed: false, tenant_id: tid, reason: s.reason, subscription: sub };
    }
    return {
        allowed: true,
        tenant_id: tid,
        actor_name: String(actorName || ""),
        actor_role: String(actorRole || ""),
        subscription: sub
    };
}

function getNanbanSnapshot_() {
    let tid = ensureTenantBootstrap_();
    let v = tenantFbGet_(tid, "nanban_driving_school", null);
    if (v) return v;
    // Legacy fallback for existing single-tenant data.
    if (tid === getDefaultTenantId_()) return fbGet_("nanban_driving_school", null);
    return null;
}

function saveNanbanSnapshot_(obj) {
    if (!useFirebaseRtdb_()) throw new Error("Firebase RTDB URL not configured");
    let tid = ensureTenantBootstrap_();
    obj.updatedAt = new Date().toISOString();
    tenantFbPut_(tid, "nanban_driving_school", obj);
    if (tid === getDefaultTenantId_()) {
        // Legacy mirror for freeze-period compatibility.
        fbPut_("nanban_driving_school", obj);
    }
}

function getESevaiSnapshot_(tenantId) {
    let tid = ensureTenantBootstrap_(tenantId);
    let v = tenantFbGet_(tid, "esevai", null);
    if (v) return v;
    if (tid === getDefaultTenantId_()) return fbGet_("esevai", null);
    return null;
}

function saveESevaiSnapshot_(obj, tenantId) {
    if (!useFirebaseRtdb_()) throw new Error("Firebase RTDB URL not configured");
    let tid = ensureTenantBootstrap_(tenantId);
    obj.updatedAt = new Date().toISOString();
    tenantFbPut_(tid, "esevai", obj);
    if (tid === getDefaultTenantId_()) fbPut_("esevai", obj);
}

function getAdminPinConfig_(tenantId) {
    let tid = ensureTenantBootstrap_(tenantId);
    let cfg = tenantFbGet_(tid, "AdminConfig/PIN", null);
    if (!cfg && tid === getDefaultTenantId_()) cfg = fbGet_("AdminConfig/PIN", {}) || {};
    cfg = cfg || {};
    return {
        adminPin: String(cfg.adminPin || cfg.admin || cfg.Admin || "").trim(),
        partnerPin: String(cfg.partnerPin || cfg.partner || cfg.Partner || "").trim(),
        defaultPin: String(cfg.defaultPin || cfg.pin || "").trim(),
        byRole: cfg.byRole && typeof cfg.byRole === "object" ? cfg.byRole : {},
        byUser: cfg.byUser && typeof cfg.byUser === "object" ? cfg.byUser : {}
    };
}

function getTenantAccessSummaryAction(tenantId, actorName, actorRole) {
    try {
        return buildTenantAccessContext_(tenantId, actorName, actorRole);
    } catch (e) {
        return { allowed: false, reason: e.toString(), tenant_id: resolveTenantId_(tenantId) };
    }
}

function tenantDataMigrationAction(targetTenantId, dryRun, includeLegacyUsers) {
    try {
        let tid = ensureTenantBootstrap_(targetTenantId);
        let out = { status: "success", tenant_id: tid, dry_run: !!dryRun, copied: {} };
        let map = [
            ["esevai", "esevai"],
            ["nanban_driving_school", "nanban_driving_school"],
            ["AdminConfig", "AdminConfig"],
            ["virtual_db", "virtual_db"]
        ];
        if (!!includeLegacyUsers) map.push(["users", "users"]);
        map.forEach(function(pair) {
            let src = fbGet_(pair[0], null);
            if (src === null || src === undefined) {
                out.copied[pair[0]] = "missing";
                return;
            }
            if (!dryRun) tenantFbPut_(tid, pair[1], src);
            out.copied[pair[0]] = "ok";
        });
        if (!dryRun) {
            let meta = tenantFbGet_(tid, "meta", {}) || {};
            meta.last_migrated_at = new Date().toISOString();
            meta.updated_at = new Date().toISOString();
            tenantFbPut_(tid, "meta", meta);
        }
        return out;
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function getVdbSheetRows_(sheetName) {
    let vdb = fbGet_("virtual_db/sheets/" + encodeURIComponent(sheetName), null);
    if (vdb && Array.isArray(vdb.rows)) return vdb.rows;
    // RTDB path keys are not URL encoded in fbGet helper; retry plain
    vdb = fbGet_("virtual_db/sheets/" + sheetName, null);
    if (vdb && Array.isArray(vdb.rows)) return vdb.rows;
    return [];
}

function rowsToObjectsByHeader_(rows) {
    if (!Array.isArray(rows) || rows.length < 2) return [];
    let header = rows[0] || [];
    let out = [];
    for (let i = 1; i < rows.length; i++) {
        let r = rows[i] || [];
        let o = {};
        for (let c = 0; c < header.length; c++) {
            let k = String(header[c] || "").trim();
            if (!k) continue;
            o[k] = r[c];
        }
        out.push(o);
    }
    return out;
}

function parseLegacyStudentsFromVdb_() {
    let rows = getVdbSheetRows_("Students");
    let out = [];
    for (let i = 1; i < rows.length; i++) {
        let blob = rows[i] && rows[i][6];
        if (!blob) continue;
        try { out.push(normalizeStudentObject(JSON.parse(blob))); } catch (e) {}
    }
    return out;
}

function parseLegacyExpensesFromVdb_() {
    let rows = getVdbSheetRows_("Expenses");
    let out = [];
    for (let i = 1; i < rows.length; i++) {
        let r = rows[i] || [];
        if (!r[0]) continue;
        out.push({
            date: r[0], spender: r[1], cat: r[2], amt: parseInt(r[3]) || 0, desc: r[4] || "", receiptUrl: r[5] || ""
        });
    }
    return out;
}

function parseLegacyESevaiFromVdb_() {
    function read(name) { return rowsToObjectsByHeader_(getVdbSheetRows_(name)); }
    return {
        services: read(ESEVAI_SERVICES_SHEET).map(function(s) {
            return {
                id: s.ID || s.Id || s.id || ("ESS" + new Date().getTime()),
                name: s.Name || s.name || "",
                category: s.Category || s.category || "General",
                type: s.Type || s.type || "regular",
                gov_fee: Number(s.Gov_Fee || s.gov_fee || 0) || 0,
                direct_charge: Number(s.Direct_Charge || s.direct_charge || 0) || 0,
                agent_charge: Number(s.Agent_Charge || s.agent_charge || 0) || 0,
                required_documents: s.Required_Documents || s.required_documents || "",
                icon: s.Icon || s.icon || ""
            };
        }),
        customers: read(ESEVAI_CUSTOMERS_SHEET).map(function(c) {
            return {
                id: c.ID || c.id || ("ESC" + new Date().getTime()),
                name: c.Name || c.name || "",
                phone: c.Phone || c.phone || "",
                balance: Number(c.Balance || c.balance || 0) || 0,
                type: c.Type || c.type || "Direct",
                created_at: c.Created_At || c.created_at || getISTDate()
            };
        }),
        enquiries: read(ESEVAI_ENQUIRIES_SHEET).map(function(e) {
            return {
                id: e.ID || e.id || ("ESENQ" + new Date().getTime()),
                customer_id: e.Customer_ID || e.customer_id || "",
                service_name: e.Service_Name || e.service_name || "",
                quoted_amount: Number(e.Quoted_Amount || e.quoted_amount || 0) || 0,
                advance: Number(e.Advance_Received || e.advance || 0) || 0,
                status: e.Status || e.status || "pending",
                notes: e.Notes || e.notes || "",
                created_at: e.Created_At || e.created_at || getISTDate()
            };
        }),
        works: read(ESEVAI_WORKS_SHEET).map(function(w) {
            let stages = w.Stages_Json || w.Stages_JSON || w.stages || "[]";
            try { if (typeof stages === "string") stages = JSON.parse(stages); } catch (e) { stages = []; }
            return {
                id: w.ID || w.id || ("ESWK" + new Date().getTime()),
                enquiry_id: w.Enquiry_ID || w.Transaction_ID || w.enquiry_id || "",
                customer_id: w.Customer_ID || w.customer_id || "",
                service_name: w.Service_Name || w.service_name || "",
                status: w.Status || w.status || "pending",
                stages: Array.isArray(stages) ? stages : [],
                document_url: w.Document_Url || w.document_url || "",
                completed_at: w.Completed_At || w.completed_at || "",
                created_at: w.Created_At || w.created_at || getISTDate()
            };
        }),
        transactions: read(ESEVAI_TRANSACTIONS_SHEET).map(function(t) {
            return {
                id: t.ID || t.id || ("ESTX" + new Date().getTime()),
                customer_id: t.Customer_ID || t.customer_id || "",
                total_amount: Number(t.Total_Amount || t.total_amount || 0) || 0,
                received_amount: Number(t.Received_Amount || t.received_amount || 0) || 0,
                balance_diff: Number(t.Balance_Diff || t.balance_diff || 0) || 0,
                payment_mode: t.Payment_Mode || t.payment_mode || "Cash",
                gov_bank: t.Gov_Bank || t.gov_bank || "SBI",
                status: t.Status || t.status || "finished",
                date: t.Created_At || t.date || getISTDate()
            };
        })
    };
}

function tryRecoverNanbanDataIfEmpty_() {
    let snap = getNanbanSnapshot_() || {};
    let students = Array.isArray(snap.students) ? snap.students : [];
    let expenses = Array.isArray(snap.expenses) ? snap.expenses : [];
    if (students.length > 0) return snap;

    let old = fbGet_("nanban/main", {}) || {};
    let vdbStudents = parseLegacyStudentsFromVdb_();
    let vdbExpenses = parseLegacyExpensesFromVdb_();
    students = mergeByKey_(students, old.students || [], "id");
    students = mergeByKey_(students, vdbStudents || [], "id");
    expenses = (expenses || []).concat(old.expenses || [], vdbExpenses || []);

    if (students.length > 0 || expenses.length > 0) {
        snap.students = students.map(function(s) { return normalizeStudentObject(s); });
        snap.expenses = expenses;
        if (!snap.appSettingsBundle && old.appSettingsBundle) snap.appSettingsBundle = old.appSettingsBundle;
        saveNanbanSnapshot_(snap);
    }
    return snap;
}

function tryRecoverESevaiDataIfEmpty_(tenantId) {
    tenantId = resolveTenantId_(tenantId);
    let snap = getESevaiSnapshot_(tenantId) || {};
    let total = (snap.services || []).length + (snap.customers || []).length + (snap.transactions || []).length;
    if (total > 0) return snap;

    let old = fbGet_("esevai/main", {}) || {};
    let vdb = parseLegacyESevaiFromVdb_();
    snap.services = mergeByKey_(snap.services || [], old.services || [], "id");
    snap.services = mergeByKey_(snap.services || [], vdb.services || [], "id");
    snap.customers = mergeByKey_(snap.customers || [], old.customers || [], "id");
    snap.customers = mergeByKey_(snap.customers || [], vdb.customers || [], "id");
    snap.enquiries = mergeByKey_(snap.enquiries || [], old.enquiries || [], "id");
    snap.enquiries = mergeByKey_(snap.enquiries || [], vdb.enquiries || [], "id");
    snap.works = mergeByKey_(snap.works || [], old.works || [], "id");
    snap.works = mergeByKey_(snap.works || [], vdb.works || [], "id");
    snap.transactions = mergeByKey_(snap.transactions || [], old.transactions || [], "id");
    snap.transactions = mergeByKey_(snap.transactions || [], vdb.transactions || [], "id");
    snap.ledgerEntries = (snap.ledgerEntries || []).concat(old.ledgerEntries || []);
    snap.settings = Object.assign({}, old.settings || {}, snap.settings || {});
    if (!snap.balances) snap.balances = old.balances || { Cash: 0, SBI: 0, "Federal 1": 0, "Federal 2": 0, Paytm: 0 };
    if (!snap.openingBalance) snap.openingBalance = old.openingBalance || null;

    if ((snap.services || []).length || (snap.customers || []).length) saveESevaiSnapshot_(snap, tenantId);
    return snap;
}

function mergeExpensesUnique_(baseArr, addArr) {
    let out = Array.isArray(baseArr) ? baseArr.slice() : [];
    let seen = {};
    out.forEach(function(e) {
        let k = [e.date, e.spender, e.cat, parseInt(e.amt) || 0, e.desc].join("|");
        seen[k] = true;
    });
    (addArr || []).forEach(function(e) {
        let k = [e.date, e.spender, e.cat, parseInt(e.amt) || 0, e.desc].join("|");
        if (seen[k]) return;
        seen[k] = true;
        out.push(e);
    });
    return out;
}

function importBackupPayloadAction(payload) {
    try {
        if (!useFirebaseRtdb_()) return { status: "error", message: "Firebase not configured" };
        payload = payload || {};
        let nan = payload.nanban || {};
        let es = payload.esevai || {};

        return withVdbBatch_(function() {
            let nSnap = tryRecoverNanbanDataIfEmpty_() || {};
            if (!Array.isArray(nSnap.students)) nSnap.students = [];
            if (!Array.isArray(nSnap.expenses)) nSnap.expenses = [];
            if (Array.isArray(nan.students)) nSnap.students = mergeByKey_(nSnap.students, nan.students.map(normalizeStudentObject), "id");
            if (Array.isArray(nan.expenses)) nSnap.expenses = mergeExpensesUnique_(nSnap.expenses, nan.expenses);
            saveNanbanSnapshot_(nSnap);

            let eSnap = tryRecoverESevaiDataIfEmpty_() || {};
            if (!Array.isArray(eSnap.services)) eSnap.services = [];
            if (!Array.isArray(eSnap.customers)) eSnap.customers = [];
            if (!Array.isArray(eSnap.enquiries)) eSnap.enquiries = [];
            if (!Array.isArray(eSnap.works)) eSnap.works = [];
            if (!Array.isArray(eSnap.transactions)) eSnap.transactions = [];
            if (!Array.isArray(eSnap.ledgerEntries)) eSnap.ledgerEntries = [];
            if (Array.isArray(es.services)) eSnap.services = mergeByKey_(eSnap.services, es.services, "id");
            if (Array.isArray(es.customers)) eSnap.customers = mergeByKey_(eSnap.customers, es.customers, "id");
            if (Array.isArray(es.enquiries)) eSnap.enquiries = mergeByKey_(eSnap.enquiries, es.enquiries, "id");
            if (Array.isArray(es.works)) eSnap.works = mergeByKey_(eSnap.works, es.works, "id");
            if (Array.isArray(es.transactions)) eSnap.transactions = mergeByKey_(eSnap.transactions, es.transactions, "id");
            if (Array.isArray(es.ledgerEntries)) eSnap.ledgerEntries = mergeExpensesUnique_(eSnap.ledgerEntries, es.ledgerEntries);
            saveESevaiSnapshot_(eSnap);

            return {
                status: "success",
                nanban: { students: nSnap.students.length, expenses: nSnap.expenses.length },
                esevai: {
                    services: eSnap.services.length, customers: eSnap.customers.length,
                    enquiries: eSnap.enquiries.length, works: eSnap.works.length, transactions: eSnap.transactions.length
                }
            };
        });
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function colRefToIndex_(cellRef) {
    let m = String(cellRef || "").match(/[A-Za-z]+/);
    if (!m) return 1;
    let letters = m[0].toUpperCase();
    let n = 0;
    for (let i = 0; i < letters.length; i++) n = n * 26 + (letters.charCodeAt(i) - 64);
    return n;
}

function parseXlsxTextNode_(node) {
    if (!node) return "";
    let out = "";
    let children = node.getChildren();
    if (!children || !children.length) return String(node.getText() || "");
    for (let i = 0; i < children.length; i++) out += parseXlsxTextNode_(children[i]);
    let t = String(node.getText() || "");
    if (t) out += t;
    return out;
}

function parseXlsxBlob_(blob) {
    let zipped = Utilities.unzip(blob);
    let map = {};
    zipped.forEach(function(b) { map[b.getName()] = b.getDataAsString("UTF-8"); });

    let wbXml = map["xl/workbook.xml"];
    let relXml = map["xl/_rels/workbook.xml.rels"];
    if (!wbXml || !relXml) throw new Error("Invalid XLSX structure");

    let wbDoc = XmlService.parse(wbXml);
    let relDoc = XmlService.parse(relXml);
    let wbRoot = wbDoc.getRootElement();
    let relRoot = relDoc.getRootElement();
    let nsMain = wbRoot.getNamespace();
    let nsRel = XmlService.getNamespace("http://schemas.openxmlformats.org/officeDocument/2006/relationships");
    let nsPkg = relRoot.getNamespace();

    let shared = [];
    if (map["xl/sharedStrings.xml"]) {
        let ssDoc = XmlService.parse(map["xl/sharedStrings.xml"]);
        let ssRoot = ssDoc.getRootElement();
        let si = ssRoot.getChildren("si", ssRoot.getNamespace());
        si.forEach(function(n) { shared.push(parseXlsxTextNode_(n)); });
    }

    let relById = {};
    relRoot.getChildren("Relationship", nsPkg).forEach(function(r) {
        relById[String(r.getAttribute("Id").getValue())] = String(r.getAttribute("Target").getValue());
    });

    let sheetsEl = wbRoot.getChild("sheets", nsMain);
    let out = {};
    if (!sheetsEl) return out;
    let sheetEls = sheetsEl.getChildren("sheet", nsMain);
    sheetEls.forEach(function(s) {
        let sName = String(s.getAttribute("name").getValue());
        let rid = String(s.getAttribute("id", nsRel).getValue());
        let target = relById[rid] || "";
        if (!target) return;
        let path = target.indexOf("xl/") === 0 ? target : ("xl/" + target.replace(/^\/+/, ""));
        let wsXml = map[path];
        if (!wsXml) return;
        let wsDoc = XmlService.parse(wsXml);
        let wsRoot = wsDoc.getRootElement();
        let ns = wsRoot.getNamespace();
        let sheetData = wsRoot.getChild("sheetData", ns);
        if (!sheetData) { out[sName] = []; return; }

        let rows = [];
        sheetData.getChildren("row", ns).forEach(function(rEl) {
            let rowIdx = parseInt(rEl.getAttribute("r") ? rEl.getAttribute("r").getValue() : (rows.length + 1));
            if (!rows[rowIdx - 1]) rows[rowIdx - 1] = [];
            let cells = rEl.getChildren("c", ns);
            cells.forEach(function(cEl) {
                let ref = cEl.getAttribute("r") ? cEl.getAttribute("r").getValue() : "";
                let col = Math.max(1, colRefToIndex_(ref));
                let tAttr = cEl.getAttribute("t");
                let t = tAttr ? tAttr.getValue() : "";
                let v = "";
                let vEl = cEl.getChild("v", ns);
                if (vEl) v = String(vEl.getText() || "");
                else {
                    let isEl = cEl.getChild("is", ns);
                    v = isEl ? parseXlsxTextNode_(isEl) : "";
                }
                if (t === "s") {
                    let idx = parseInt(v);
                    v = (!isNaN(idx) && idx >= 0 && idx < shared.length) ? shared[idx] : "";
                } else if (t === "b") {
                    v = (String(v) === "1");
                }
                rows[rowIdx - 1][col - 1] = v;
            });
        });
        out[sName] = rows.map(function(r) { return Array.isArray(r) ? r : []; });
    });
    return out;
}

function parseXlsxFromDrive_(fileId) {
    let file = DriveApp.getFileById(fileId);
    return parseXlsxBlob_(file.getBlob());
}

function parseGoogleSheetFileAsXlsx_(fileId) {
    let token = ScriptApp.getOAuthToken();
    let exportUrl = "https://www.googleapis.com/drive/v3/files/" + encodeURIComponent(fileId) + "/export?mimeType=application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    let res = UrlFetchApp.fetch(exportUrl, {
        method: "get",
        headers: { Authorization: "Bearer " + token },
        muteHttpExceptions: true
    });
    if (res.getResponseCode() < 200 || res.getResponseCode() >= 300) {
        throw new Error("Google Sheet export failed: " + res.getResponseCode() + " " + res.getContentText());
    }
    return parseXlsxBlob_(res.getBlob());
}

function restoreFromDriveXlsxToFirebase(fileId) {
    try {
        if (!fileId) return { status: "error", message: "fileId required" };
        let file = DriveApp.getFileById(fileId);
        let mime = String(file.getMimeType() || "");
        let sheets = (mime === MimeType.GOOGLE_SHEETS)
            ? parseGoogleSheetFileAsXlsx_(fileId)
            : parseXlsxFromDrive_(fileId);
        let students = [];
        let expenses = [];
        let es = { services: [], customers: [], enquiries: [], works: [], transactions: [], ledgerEntries: [] };

        let sRows = sheets["Students"] || [];
        for (let i = 1; i < sRows.length; i++) {
            let blob = sRows[i][6];
            if (!blob) continue;
            try { students.push(normalizeStudentObject(JSON.parse(String(blob)))); } catch (e) {}
        }
        let eRows = sheets["Expenses"] || [];
        for (let j = 1; j < eRows.length; j++) {
            if (!eRows[j][0]) continue;
            expenses.push({
                date: String(eRows[j][0] || ""), spender: String(eRows[j][1] || ""), cat: String(eRows[j][2] || ""),
                amt: parseInt(eRows[j][3]) || 0, desc: String(eRows[j][4] || ""), receiptUrl: String(eRows[j][5] || "")
            });
        }

        function idxMap(headerRow) {
            let m = {};
            (headerRow || []).forEach(function(h, i) { m[String(h || "").trim()] = i; });
            return m;
        }
        function rowsToObjects(name) {
            let rr = sheets[name] || [];
            if (rr.length < 2) return [];
            let mapH = idxMap(rr[0]);
            let arr = [];
            for (let i = 1; i < rr.length; i++) {
                let r = rr[i] || [];
                let o = {};
                Object.keys(mapH).forEach(function(k) { o[k] = r[mapH[k]]; });
                arr.push(o);
            }
            return arr;
        }

        rowsToObjects("ES_Services").forEach(function(o) {
            if (!o.ID) return;
            es.services.push({
                id: String(o.ID), name: String(o.Name || ""), category: String(o.Category || "General"), type: String(o.Type || "regular"),
                gov_fee: Number(o.Gov_Fee) || 0, direct_charge: Number(o.Direct_Charge) || 0, agent_charge: Number(o.Agent_Charge) || 0,
                required_documents: String(o.Required_Documents || ""), icon: String(o.Icon || "")
            });
        });
        rowsToObjects("ES_Customers").forEach(function(o) {
            if (!o.ID) return;
            es.customers.push({
                id: String(o.ID), name: String(o.Name || ""), phone: String(o.Phone || ""), balance: Number(o.Balance) || 0,
                type: String(o.Type || "Direct"), created_at: String(o.Created_At || "")
            });
        });
        rowsToObjects("ES_Enquiries").forEach(function(o) {
            if (!o.ID) return;
            es.enquiries.push({
                id: String(o.ID), customer_id: String(o.Customer_ID || ""), service_name: String(o.Service_Name || ""),
                quoted_amount: Number(o.Quoted_Amount) || 0, advance: Number(o.Advance_Received) || 0, status: String(o.Status || "pending"),
                notes: String(o.Notes || ""), created_at: String(o.Created_At || "")
            });
        });
        rowsToObjects("ES_Works").forEach(function(o) {
            if (!o.ID) return;
            let stages = [];
            try { if (o.Stages_Json) stages = JSON.parse(String(o.Stages_Json)); } catch (e) { stages = []; }
            es.works.push({
                id: String(o.ID), enquiry_id: String(o.Transaction_ID || ""), customer_id: String(o.Customer_ID || ""),
                service_name: String(o.Service_Name || ""), status: String(o.Status || "pending"), stages: stages,
                document_url: String(o.Document_Url || ""), completed_at: String(o.Completed_At || ""), created_at: String(o.Created_At || "")
            });
        });
        rowsToObjects("ES_Transactions").forEach(function(o) {
            if (!o.ID) return;
            es.transactions.push({
                id: String(o.ID), customer_id: String(o.Customer_ID || ""), total_amount: Number(o.Total_Amount) || 0,
                received_amount: Number(o.Received_Amount) || 0, balance_diff: Number(o.Balance_Diff) || 0,
                payment_mode: String(o.Payment_Mode || "Cash"), gov_bank: String(o.Gov_Bank || "SBI"),
                status: String(o.Status || "finished"), date: String(o.Created_At || "")
            });
        });
        rowsToObjects("ES_Ledger").forEach(function(o) {
            es.ledgerEntries.push({
                date: String(o.Date || ""), type: String(o.Type || ""), category: String(o.Category || ""),
                description: String(o.Description || ""), amount: Number(o.Amount) || 0, account: String(o.Account || "")
            });
        });

        return importBackupPayloadAction({ nanban: { students: students, expenses: expenses }, esevai: es });
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function restoreFromNamedXlsxBackup() {
    try {
        let names = ["Nanban_ERP_Database(4).xlsx", "Nanban_ERP_Database.xlsx", "Nanban_ERP_Database"];
        let file = null;
        for (let i = 0; i < names.length && !file; i++) {
            let files = DriveApp.getFilesByName(names[i]);
            if (files.hasNext()) file = files.next();
        }
        if (!file) return { status: "error", message: "Drive-ல் Nanban_ERP_Database backup file கிடைக்கவில்லை" };
        return restoreFromDriveXlsxToFirebase(file.getId());
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

/**
 * One-time migration: move old Firebase nodes into new RTDB paths.
 */
function migrateDataToFirebase() {
    try {
        return migrateLegacyAllDataToFirebase(false);
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function mergeByKey_(baseArr, addArr, keyName) {
    let out = [];
    let map = {};
    (baseArr || []).forEach(function(x) {
        let k = String((x && x[keyName]) || "").trim();
        if (!k) return;
        map[k] = Object.assign({}, x);
    });
    (addArr || []).forEach(function(x) {
        let k = String((x && x[keyName]) || "").trim();
        if (!k) return;
        map[k] = Object.assign({}, map[k] || {}, x);
    });
    Object.keys(map).forEach(function(k) { out.push(map[k]); });
    return out;
}

function readLegacyNanbanFromSheets_() {
    return { students: [], expenses: [] };
}

function readLegacyESevaiFromSheets_() {
    return { services: [], customers: [], enquiries: [], works: [], transactions: [], ledgerEntries: [], settings: {} };
}

/**
 * One-time recovery:
 * - old Firebase paths: /nanban/main, /esevai/main
 * -> merged into /nanban_driving_school and /esevai
 */
function migrateLegacyAllDataToFirebase(forceOverwrite) {
    try {
        if (!useFirebaseRtdb_()) return { status: "error", message: "Firebase RTDB URL not configured" };

        let oldNanban = fbGet_("nanban/main", {}) || {};
        let oldESevai = fbGet_("esevai/main", {}) || {};
        let sheetNanban = readLegacyNanbanFromSheets_();
        let sheetESevai = readLegacyESevaiFromSheets_();
        let newNanban = forceOverwrite ? {} : (getNanbanSnapshot_() || {});
        let newESevai = forceOverwrite ? {} : (getESevaiSnapshot_() || {});

        newNanban.students = mergeByKey_(newNanban.students || [], oldNanban.students || [], "id");
        newNanban.students = mergeByKey_(newNanban.students || [], sheetNanban.students || [], "id");
        newNanban.expenses = (newNanban.expenses || []).concat(oldNanban.expenses || [], sheetNanban.expenses || []);
        if (!newNanban.appSettingsBundle) newNanban.appSettingsBundle = oldNanban.appSettingsBundle || getAppSettings();

        newESevai.services = mergeByKey_(newESevai.services || [], oldESevai.services || [], "id");
        newESevai.services = mergeByKey_(newESevai.services || [], sheetESevai.services || [], "id");
        newESevai.customers = mergeByKey_(newESevai.customers || [], oldESevai.customers || [], "id");
        newESevai.customers = mergeByKey_(newESevai.customers || [], sheetESevai.customers || [], "id");
        newESevai.enquiries = mergeByKey_(newESevai.enquiries || [], oldESevai.enquiries || [], "id");
        newESevai.enquiries = mergeByKey_(newESevai.enquiries || [], sheetESevai.enquiries || [], "id");
        newESevai.works = mergeByKey_(newESevai.works || [], oldESevai.works || [], "id");
        newESevai.works = mergeByKey_(newESevai.works || [], sheetESevai.works || [], "id");
        newESevai.transactions = mergeByKey_(newESevai.transactions || [], oldESevai.transactions || [], "id");
        newESevai.transactions = mergeByKey_(newESevai.transactions || [], sheetESevai.transactions || [], "id");
        newESevai.ledgerEntries = (newESevai.ledgerEntries || []).concat(oldESevai.ledgerEntries || [], sheetESevai.ledgerEntries || []);
        newESevai.settings = Object.assign({}, oldESevai.settings || {}, sheetESevai.settings || {}, newESevai.settings || {});
        if (!newESevai.balances) newESevai.balances = oldESevai.balances || { Cash: 0, SBI: 0, "Federal 1": 0, "Federal 2": 0, Paytm: 0 };
        if (!newESevai.openingBalance) newESevai.openingBalance = oldESevai.openingBalance || null;

        saveNanbanSnapshot_(newNanban);
        saveESevaiSnapshot_(newESevai);

        return {
            status: "success",
            message: "Legacy data merged to new Firebase paths.",
            counts: {
                students: (newNanban.students || []).length,
                expenses: (newNanban.expenses || []).length,
                services: (newESevai.services || []).length,
                customers: (newESevai.customers || []).length,
                enquiries: (newESevai.enquiries || []).length,
                works: (newESevai.works || []).length
            }
        };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function cleanPhoneNumber(phone) { 
    if (!phone) return ""; 
    let p = phone.toString().replace(/\D/g, ''); 
    // Always ensure 91 prefix for India (10 digits)
    if (p.length === 10) return "91" + p; 
    if (p.length > 10 && p.startsWith("91")) return p; 
    // Fallback: If it's already more than 10 digits and doesn't start with 91, try to fix it
    if (p.length > 10) return p.substring(p.length - 12); // Handle 91+10 digits cases
    return p; 
}

function getDisplayPhoneNumber(phone) {
    if (!phone) return "";
    let p = phone.toString().replace(/\D/g, '');
    return p.length >= 10 ? p.substring(p.length - 10) : p;
}

// ------------------------------------------------------------------------------
// 2.5. ஏலம் எடுத்தவர் ஆட்டோ-டெக்ஷன் சிஸ்டம்
// ------------------------------------------------------------------------------

function getAuctionWinnerByMonth(groupName, monthNo) {
    try {
        let bidSheet = getDB().getSheetByName("Chit_Live_Bids");
        if (!bidSheet) return null;
        
        let data = bidSheet.getDataRange().getValues();
        let headers = data[0];
        
        // Find winner for specific month and group
        for (let i = 1; i < data.length; i++) {
            let row = data[i];
            let rowGroup = row[headers.indexOf("Group")] || "";
            let rowMonth = row[headers.indexOf("Month")] || "";
            let bidAmount = row[headers.indexOf("BidAmount")] || 0;
            let memberName = row[headers.indexOf("MemberName")] || "";
            let memberPhone = row[headers.indexOf("MemberPhone")] || "";
            
            if (rowGroup === groupName && rowMonth == monthNo && bidAmount > 0) {
                return {
                    name: memberName,
                    phone: getDisplayPhoneNumber(memberPhone),
                    bidAmount: bidAmount,
                    month: monthNo,
                    group: groupName
                };
            }
        }
        return null;
    } catch (e) { return null; }
}

/**
 * 📝 ஆன்லைன் அட்மிஷன் பதிவு (Online Admission Submission)
 */
function submitAdmissionAction(formData) {
    try {
        let sheet = getDB().getSheetByName("Students");
        let id = "STU" + Date.now();
        let today = getISTDate();
        
        // Metadata structure for Enquiry
        let metadata = {
            id: id,
            name: formData.name,
            phone: cleanPhoneNumber(formData.phone),
            gender: formData.gender || "",
            service: formData.service || "4 வீலர்",
            location: formData.location || "",
            dateJoined: today,
            status: "Processing",
            type: "Enquiry",
            source: "OnlineAdmission",
            classesAttended: 0,
            totalFee: 0,
            advance: 0,
            adminRemarks: [{date: today, text: "📝 Online Admission Form மூலமாக பதிவு செய்யப்பட்டது."}]
        };
        
        sheet.appendRow([
            id,
            formData.name,
            cleanPhoneNumber(formData.phone),
            formData.service || "4 வீலர்",
            today,
            "Enquiry",
            JSON.stringify(metadata)
        ]);
        
        // Notify Admins
        notifyAdmins(`📝 *NEW ENQUIRY:* ${formData.name} (${formData.phone}) online அட்மிஷன் செய்துள்ளார்!`);
        
        // Send Welcome Message if possible
        sendWhatsAppMessage(cleanPhoneNumber(formData.phone), `வணக்கம் ${formData.name}, நண்பன் டிரைவிங் ஸ்கூலில் உங்கள் ஆன்லைன் பதிவு வெற்றிகரமாக முடிந்தது! விரைவில் உங்களை அழைப்போம். 🚗🚦`);
        
        return { status: 'success', id: id };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function getAllAuctionWinners(groupName) {
    try {
        let bidSheet = getDB().getSheetByName("Chit_Live_Bids");
        if (!bidSheet) return [];
        
        let data = bidSheet.getDataRange().getValues();
        let headers = data[0];
        let winners = [];
        
        for (let i = 1; i < data.length; i++) {
            let row = data[i];
            let rowGroup = row[headers.indexOf("Group")] || "";
            let rowMonth = row[headers.indexOf("Month")] || "";
            let bidAmount = row[headers.indexOf("BidAmount")] || 0;
            let memberName = row[headers.indexOf("MemberName")] || "";
            let memberPhone = row[headers.indexOf("MemberPhone")] || "";
            
            if (rowGroup === groupName && bidAmount > 0) {
                winners.push({
                    name: memberName,
                    phone: getDisplayPhoneNumber(memberPhone),
                    bidAmount: bidAmount,
                    month: rowMonth,
                    group: groupName
                });
            }
        }
        
        // Sort by month
        winners.sort((a, b) => parseInt(a.month) - parseInt(b.month));
        return winners;
    } catch(e) {
        Logger.log("Error getting all auction winners: " + e.toString());
        return [];
    }
}

function formatAuctionWinnersList(groupName) {
    try {
        let winners = getAllAuctionWinners(groupName);
        if (winners.length === 0) {
            return `📋 *ஏலம் எடுத்தவர்கள் பட்டியல்*\n\nகுழு: ${groupName}\n\nஇதுவரை யாரும் ஏலம் எடுக்கவில்லை.`;
        }
        
        let message = `📋 *ஏலம் எடுத்தவர்கள் பட்டியல்*\n\nகுழு: ${groupName}\n\n`;
        
        winners.forEach(winner => {
            message += `🏆 *மாதம் ${winner.month}*: ${winner.name}\n`;
            message += `   💰 ஏலத் தொகை: ₹${winner.bidAmount}\n`;
            message += `   📱 போன்: ${winner.phone}\n\n`;
        });
        
        message += `📊 மொத்தம்: ${winners.length} பேர் ஏலம் எடுத்துள்ளனர்`;
        
        return message;
    } catch(e) {
        return "பட்டியலைப் பெறுவதில் பிழை: " + e.toString();
    }
}

// ------------------------------------------------------------------------------
// 2.6. ஸ்மார்ட் ட்ராப் டவுன் செலக்டர் சிஸ்டம்
// ------------------------------------------------------------------------------

function getChitGroups() {
    try {
        let db = getChitData().data;
        let groups = [...new Set(db.members.map(m => m.group).filter(g => g))];
        return groups.sort();
    } catch(e) {
        Logger.log("Error getting chit groups: " + e.toString());
        return [];
    }
}

function getMonthsForGroup(groupName) {
    try {
        let db = getChitData().data;
        let groups = db.groups.find(g => g.name === groupName);
        if (groups && groups.months) {
            return groups.months.map(m => `மாதம் ${m}`).sort();
        }
        return [];
    } catch(e) {
        Logger.log("Error getting months for group: " + e.toString());
        return [];
    }
}

function getMembersForGroup(groupName) {
    try {
        let db = getChitData().data;
        let members = db.members
            .filter(m => m.group === groupName && m.name)
            .map(m => ({
                name: m.name,
                phone: getDisplayPhoneNumber(m.phone),
                id: m.phone
            }))
            .sort((a, b) => a.name.localeCompare(b.name));
        return members;
    } catch(e) {
        Logger.log("Error getting members for group: " + e.toString());
        return [];
    }
}

function generateAuctionFormOptions() {
    try {
        let groups = getChitGroups();
        Logger.log("Groups found: " + JSON.stringify(groups));
        
        let formOptions = {
            groups: groups,
            months: {},
            members: {}
        };
        
        // Generate months for each group
        groups.forEach(group => {
            formOptions.months[group] = getMonthsForGroup(group);
            formOptions.members[group] = getMembersForGroup(group);
            Logger.log("Group " + group + " members: " + JSON.stringify(formOptions.members[group]));
        });
        
        Logger.log("Final form options: " + JSON.stringify(formOptions));
        return formOptions;
    } catch(e) {
        Logger.log("Error generating auction form options: " + e.toString());
        return { groups: [], months: {}, members: {} };
    }
}

function createSmartAuctionForm() {
    try {
        let options = generateAuctionFormOptions();
        
        // If no data, use hardcoded test data
        if (!options.groups || options.groups.length === 0) {
            options = {
                groups: ["1 லட்சம் சீட்டு", "1,00,000 சீட்டு"],
                months: {
                    "1 லட்சம் சீட்டு": ["மாதம் 1", "மாதம் 2", "மாதம் 3"],
                    "1,00,000 சீட்டு": ["மாதம் 1", "மாதம் 2", "மாதம் 3"]
                },
                members: {
                    "1 லட்சம் சீட்டு": [
                        {name: "ரஞ்சித்", phone: "9942391870"},
                        {name: "நந்தகுமார்", phone: "9092036666"},
                        {name: "குமார்", phone: "9876543210"}
                    ],
                    "1,00,000 சீட்டு": [
                        {name: "ரமேஷ்", phone: "9876543211"},
                        {name: "சுரேஷ்", phone: "9876543212"},
                        {name: "விஜய்", phone: "9876543213"}
                    ]
                }
            };
        }
        
        let html = '<!DOCTYPE html>' +
'<html>' +
'<head>' +
'    <base target="_top">' +
'    <title>நண்பன் சீட்டு - புதிய ஏலம்</title>' +
'    <style>' +
'        body { font-family: Arial, sans-serif; padding: 20px; background: #f5f5f5; }' +
'        .container { max-width: 600px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }' +
'        h1 { color: #2c3e50; text-align: center; margin-bottom: 30px; }' +
'        .form-group { margin-bottom: 20px; }' +
'        label { display: block; margin-bottom: 5px; font-weight: bold; color: #34495e; }' +
'        select, input { width: 100%; padding: 12px; border: 2px solid #ddd; border-radius: 5px; font-size: 16px; }' +
'        select:focus, input:focus { border-color: #3498db; outline: none; }' +
'        button { background: #3498db; color: white; padding: 12px 30px; border: none; border-radius: 5px; font-size: 16px; cursor: pointer; width: 100%; }' +
'        button:hover { background: #2980b9; }' +
'        .hidden { display: none; }' +
'    </style>' +
'</head>' +
'<body>' +
'    <div class="container">' +
'        <h1>🏆 புதிய ஏலம்</h1>' +
        
'        <div class="form-group">' +
'            <label for="group">குழு தேர்ந்தெடுக்கவும்:</label>' +
'            <select id="group" name="group" onchange="updateMonthsAndMembers()">' +
'                <option value="">குழுவைத் தேர்ந்தெடுக்கவும்...</option>' +
'                ' + options.groups.map(function(g) { return '<option value="' + g + '">' + g + '</option>'; }).join('') + '' +
'            </select>' +
'        </div>' +
        
'        <div class="form-group">' +
'            <label for="month">மாதம் தேர்ந்தெடுக்கவும்:</label>' +
'            <select id="month" name="month" class="hidden">' +
'                <option value="">முதலில் குழுவைத் தேர்ந்தெடுக்கவும்...</option>' +
'            </select>' +
'        </div>' +
        
'        <div class="form-group">' +
'            <label for="winner">ஏலம் எடுத்தவர் தேர்ந்தெடுக்கவும்:</label>' +
'            <select id="winner" name="winner">' +
'                <option value="">ஏலம் எடுத்தவரைத் தேர்ந்தெடுக்கவும்...</option>' +
'            </select>' +
'        </div>' +
        
'        <div class="form-group">' +
'            <label for="bidAmount">ஏலத் தொகை:</label>' +
'            <input type="number" id="bidAmount" name="bidAmount" placeholder="எ.கா: 15000" min="1000">' +
'        </div>' +
        
'        <button onclick="submitAuction()">ஏலத்தைச் சமர்ப்பிக்கவும்</button>' +
'    </div>' +

'<script>' +
'        const options = ' + JSON.stringify(options) + ';' +
        
'        function updateMonthsAndMembers() {' +
'            const groupSelect = document.getElementById("group");' +
'            const monthSelect = document.getElementById("month");' +
'            const winnerSelect = document.getElementById("winner");' +
'            const selectedGroup = groupSelect.value;' +
            
'            // Clear previous options' +
'            monthSelect.innerHTML = \'<option value="">மாதத்தைத் தேர்ந்தெடுக்கவும்...</option>\';' +
'            winnerSelect.innerHTML = \'<option value="">ஏலம் எடுத்தவரைத் தேர்ந்தெடுக்கவும்...</option>\';' +
            
'            if (selectedGroup) {' +
'                // Update months' +
'                if (options.months[selectedGroup]) {' +
'                    monthSelect.classList.remove("hidden");' +
'                    options.months[selectedGroup].forEach(function(month) {' +
'                        const option = document.createElement("option");' +
'                        option.value = month.replace("மாதம் ", "");' +
'                        option.textContent = month;' +
'                        monthSelect.appendChild(option);' +
'                    });' +
'                } else {' +
'                    monthSelect.classList.add("hidden");' +
'                }' +

'                // Update members for selected group ONLY' +
'                if (options.members[selectedGroup]) {' +
'                    options.members[selectedGroup].forEach(function(member) {' +
'                        const option = document.createElement("option");' +
'                        option.value = member.name;' +
'                        option.textContent = member.name + " (" + member.phone + ")";' +
'                        winnerSelect.appendChild(option);' +
'                    });' +
'                }' +
'            } else {' +
'                monthSelect.classList.add("hidden");' +
'                // When no group is selected, winner dropdown should be reset' +
'                winnerSelect.innerHTML = \'<option value="">முதலில் குழுவைத் தேர்ந்தெடுக்கவும்...</option>\';' +
'            }' +
'        }' +
        
'        function submitAuction() {' +
'            const group = document.getElementById("group").value;' +
'            const month = document.getElementById("month").value;' +
'            const winner = document.getElementById("winner").value;' +
'            const bidAmount = document.getElementById("bidAmount").value;' +
            
'            if (!group || !month || !winner || !bidAmount) {' +
'                alert("அனைத்து புலங்களையும் நிரப்பவும்!");' +
'                return;' +
'            }' +
            
'            // Submit to Google Apps Script' +
'            google.script.run' +
'                .withSuccessHandler(() => {' +
'                    alert("ஏலம் வெற்றிகரமாகச் சமர்ப்பிக்கப்பட்டது!");' +
'                    google.script.host.close();' +
'                })' +
'                .withFailureHandler(error => {' +
'                    alert("பிழை: " + error);' +
'                })' +
'                .saveSmartAuction({' +
'                    group: group,' +
'                    month: month,' +
'                    winner: winner,' +
'                    bidAmount: bidAmount' +
'                });' +
'        }' +
'<\/script>' +
'<\/body>' +
'<\/html>';
        
        return html;
    } catch(e) {
        Logger.log("Error creating smart auction form: " + e.toString());
        return "Error creating form: " + e.toString();
    }
}

function saveSmartAuction(auctionData) {
    try {
        // Calculate auction details
        let bidAmount = parseFloat(auctionData.bidAmount);
        let chitAmount = 50000; // Default chit amount
        let discount = chitAmount - bidAmount;
        let commission = Math.round(discount * 0.05); // 5% commission
        let perHead = Math.round((chitAmount - commission) / 20); // Assuming 20 members
        let interestRate = Math.round((discount / chitAmount) * 100 * 12 / parseInt(auctionData.month));
        
        let auctionObj = {
            group: auctionData.group,
            monthNo: auctionData.month,
            winner: auctionData.winner,
            bidAmount: bidAmount,
            discount: discount,
            commission: commission,
            perHead: perHead,
            interestRate: interestRate
        };
        
        // Save to sheet
        saveChitAuction(auctionObj, false);
        
        return { status: 'success', message: 'Auction saved successfully' };
    } catch(e) {
        Logger.log("Error saving smart auction: " + e.toString());
        return { status: 'error', message: e.toString() };
    }
}

function showSmartAuctionForm() {
    try {
        let html = createSmartAuctionForm();
        let title = 'நண்பன் சீட்டு - புதிய ஏலம்';
        
        HtmlService.createHtmlOutput(html)
            .setTitle(title)
            .setWidth(650)
            .setHeight(500)
            .showAsModal(HtmlService.createHtmlOutput('<p>Loading...</p>'));
            
    } catch(e) {
        Logger.log("Error showing smart auction form: " + e.toString());
        uiAlert_('படிவத்தைத் திறப்பதில் பிழை: ' + e.toString());
    }
}

/**
 * 🛠 UTILITY: Get financial year for a given date string (DD/MM/YYYY)
 * Starts April 1st.
 */
function getFinancialYear(dateStr) {
    if (!dateStr) return null;
    let parts = dateStr.split('/');
    if (parts.length !== 3) return null;
    let month = parseInt(parts[1]);
    let year = parseInt(parts[2]);
    if (month >= 4) {
        return `${year}-${year + 1}`;
    } else {
        return `${year - 1}-${year}`;
    }
}

/**
 * 🛠 UTILITY: Check if a date falls in the current financial year.
 */
function isCurrentFY(dateStr) {
    let now = new Date();
    let currentMonth = now.getMonth() + 1;
    let currentYear = now.getFullYear();
    let currentFY = currentMonth >= 4 ? `${currentYear}-${currentYear + 1}` : `${currentYear - 1}-${currentYear}`;
    return getFinancialYear(dateStr) === currentFY;
}

function getISTDate() { 
    let tz = new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" }); 
    let ist = new Date(tz); 
    let day = String(ist.getDate()).padStart(2, '0');
    let month = String(ist.getMonth() + 1).padStart(2, '0');
    let year = ist.getFullYear();
    return `${day}/${month}/${year}`; 
}

function getTomorrowDateYYYYMMDD() {
    let tz = new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" }); 
    let ist = new Date(tz);
    ist.setDate(ist.getDate() + 1); // Add 1 day for tomorrow
    let day = String(ist.getDate()).padStart(2, '0');
    let month = String(ist.getMonth() + 1).padStart(2, '0');
    let year = ist.getFullYear();
    return `${year}-${month}-${day}`; // HTML Date format required for comparison
}

function formatYMDToDDMMYYYY(ymd) {
    try {
        let m = String(ymd || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (!m) return String(ymd || "");
        return `${m[3]}/${m[2]}/${m[1]}`;
    } catch (e) {
        return String(ymd || "");
    }
}

function getTemplateAndReminderConfig() {
    try {
        let settings = getAppSettings();
        let a = (settings && settings.appSettings) ? settings.appSettings : {};
        return {
            enquiryTemplate: String(a.enquiryTemplate || "enquiry_welcom").trim(),
            welcomeTemplate: String(a.welcomeTemplate || "welcome_admission").trim(),
            llrTemplate: String(a.llrAdmissionTemplate || "welcome_admission").trim(),
            rtoTemplate: String(a.rtoReminderTemplate || "rto_test_reminder").trim(),
            rtoTomorrowTemplate: String(a.rtoTomorrowTemplate || "rto_test_tomorrow").trim(),
            dailyClassTemplate: String(a.dailyClassTemplate || "daily_class_alert").trim(),
            chitAuctionTemplate: String(a.chitAuctionTemplate || "chit_auction_alert").trim(),
            chitDueTemplate: String(a.chitDueTemplate || "chit_due_reminder").trim(),
            chitReceiptTemplate: String(a.chitReceiptTemplate || "chit_payment_receipt").trim(),
            bulkTemplate: String(a.bulkTemplate || "bulk_announcement").trim(),
            adminUniversalTemplate: String(a.adminUniversalTemplate || "admin_universal_alert").trim(),
            paymentReminderTemplate: String(a.paymentReminderTemplate || "payment_reminder_nds").trim(),
            dayCloseTemplate: String(a.dayCloseTemplate || "day_close_report").trim(),
            businessUpi: String(a.businessUpi || "9942391870@oksbi").trim(),
            googleReviewLink: String(a.googleReviewLink || "https://g.page/r/your-id/review").trim(),
            inspectorTime: String(a.inspectorTime || "").trim(),
            rtoReminderText: String(a.rtoReminderText || "").trim(),
            partnerPhone: String(a.partnerPhone || "919092036666").trim(),
            whyNanbanText: String(a.whyNanbanText || "✨ *ஏன் நண்பன் டிரைவிங் ஸ்கூல்?*\n\n✅ 100% பொறுப்பான பயிற்சி.\n✅ டிஜிட்டல் சிலபஸ் டிராக்கிங் வசதி.\n✅ அனுபவம் வாய்ந்த பயிற்சியாளர்கள்.\n✅ குறுகிய காலத்தில் சிறந்த முறையில் கார் ஓட்ட கற்றுக்கொள்ளலாம்!").trim(),
            quizTemplate: String(a.quizTemplate || "daily_quiz_btn").trim()
        };
    } catch (e) {
        return {
            enquiryTemplate: "enquiry_welcom",
            welcomeTemplate: "welcome_admission",
            llrTemplate: "welcome_admission",
            rtoTemplate: "rto_test_reminder",
            rtoTomorrowTemplate: "rto_test_tomorrow",
            dailyClassTemplate: "daily_class_alert",
            chitAuctionTemplate: "chit_auction_alert",
            chitDueTemplate: "chit_due_reminder",
            chitReceiptTemplate: "chit_payment_receipt",
            bulkTemplate: "bulk_announcement",
            adminUniversalTemplate: "admin_universal_alert",
            paymentReminderTemplate: "payment_reminder_nds",
            dayCloseTemplate: "day_close_report",
            businessUpi: "9942391870@oksbi",
            googleReviewLink: "https://g.page/r/your-id/review",
            inspectorTime: "",
            rtoReminderText: "",
            partnerPhone: "919092036666",
            quizTemplate: "daily_quiz_btn"
        };
    }
}

function validateTemplateSettingsAction() {
    try {
        let cfg = getTemplateAndReminderConfig();
        let items = [];
        let keys = [
            ["enquiryTemplate", "Enquiry"],
            ["welcomeTemplate", "Welcome"],
            ["llrTemplate", "LLR"],
            ["rtoTemplate", "RTO Reminder"],
            ["rtoTomorrowTemplate", "RTO Tomorrow"],
            ["dailyClassTemplate", "Daily Class"],
            ["chitAuctionTemplate", "Chit Auction"],
            ["chitDueTemplate", "Chit Due"],
            ["chitReceiptTemplate", "Chit Receipt"],
            ["bulkTemplate", "Bulk"],
            ["adminUniversalTemplate", "Admin Alert"],
            ["paymentReminderTemplate", "Payment Reminder"],
            ["dayCloseTemplate", "Day Close"]
        ];

        function isValidTemplateName(n) {
            let s = String(n || "").trim();
            if (!s) return false;
            return /^[a-z0-9_]+$/.test(s);
        }

        keys.forEach(function(k) {
            let key = k[0];
            let label = k[1];
            let val = cfg && cfg[key] ? String(cfg[key]).trim() : "";
            let ok = isValidTemplateName(val);
            items.push({ key: key, label: label, value: val, ok: ok });
        });

        let warnings = [];
        if (!cfg.inspectorTime) warnings.push("Inspector time empty (RTO message fallback will still work).");
        if (!cfg.rtoReminderText) warnings.push("Custom RTO text empty (default reminder text will be used).");

        return { status: "success", items: items, warnings: warnings, cfg: cfg };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function getChatbotServicePricingConfig_() {
    return {
        TW_FULL: {
            key: "TW_FULL",
            title_ta: "இருசக்கர வாகனம் - முழு பேக்கேஜ்",
            title_en: "Two-Wheeler Full Package",
            pricing: { llr: 1000, license: 1800 }
        },
        FW_LICENSE_ONLY: {
            key: "FW_LICENSE_ONLY",
            title_ta: "நான்கு சக்கர வாகனம் - லைசென்ஸ் மட்டும்",
            title_en: "Four-Wheeler License Only",
            pricing: { llr: 2000, license: 2500 }
        },
        FW_TRAINING_ONLY: {
            key: "FW_TRAINING_ONLY",
            title_ta: "நான்கு சக்கர வாகனம் - பயிற்சி மட்டும்",
            title_en: "Four-Wheeler Training Only",
            pricing: { training_per_day: 200, training_days: 15 }
        },
        FW_LICENSE_TRAINING: {
            key: "FW_LICENSE_TRAINING",
            title_ta: "நான்கு சக்கர வாகனம் - லைசென்ஸ் + பயிற்சி",
            title_en: "Four-Wheeler License + Training",
            pricing: { llr: 2000, license: 2500, training_per_day: 200, training_days: 15 }
        },
        COMBO_2W_4W: {
            key: "COMBO_2W_4W",
            title_ta: "காம்போ (2W + 4W)",
            title_en: "Combo (2W + 4W)",
            includes: ["TW_FULL", "FW_LICENSE_ONLY"]
        }
    };
}

function formatInr_(amt) {
    let n = Number(amt) || 0;
    return "₹" + Math.round(n);
}

function normalizeServiceKey_(txt) {
    let t = String(txt || "").trim().toUpperCase();
    if (t && getChatbotServicePricingConfig_()[t]) return t;
    let low = String(txt || "").toLowerCase();
    if (low.indexOf("two") !== -1 || low.indexOf("bike") !== -1 || low.indexOf("2w") !== -1 || low.indexOf("இருசக்கர") !== -1) return "TW_FULL";
    if (low.indexOf("combo") !== -1 || low.indexOf("2+4") !== -1) return "COMBO_2W_4W";
    if (low.indexOf("training only") !== -1 || low.indexOf("பயிற்சி மட்டும்") !== -1) return "FW_TRAINING_ONLY";
    if (low.indexOf("license + training") !== -1 || low.indexOf("லைசென்ஸ் + பயிற்சி") !== -1) return "FW_LICENSE_TRAINING";
    if (low.indexOf("license only") !== -1 || low.indexOf("லைசென்ஸ் மட்டும்") !== -1) return "FW_LICENSE_ONLY";
    if (low.indexOf("four") !== -1 || low.indexOf("car") !== -1 || low.indexOf("4w") !== -1 || low.indexOf("நான்கு") !== -1) return "FW_LICENSE_ONLY";
    return "TW_FULL";
}

function computeServiceBreakdown_(serviceKey, cfg, stack) {
    cfg = cfg || getChatbotServicePricingConfig_();
    stack = stack || {};
    let key = normalizeServiceKey_(serviceKey);
    if (!cfg[key]) return { key: key, title: key, lines: [], total: 0 };
    if (stack[key]) return { key: key, title: cfg[key].title_en || key, lines: [], total: 0 };
    stack[key] = true;

    let svc = cfg[key];
    let lines = [];
    let total = 0;
        if (Array.isArray(svc.includes) && svc.includes.length) {
        svc.includes.forEach(function(childKey) {
            let child = computeServiceBreakdown_(childKey, cfg, stack);
            if (child.total > 0) lines.push("  • " + (child.title_ta || child.title || childKey) + ": " + formatInr_(child.total));
            total += child.total;
        });
    } else {
        let p = svc.pricing || {};
        if (Number(p.llr) > 0) {
            lines.push("  • LLR கட்டணம்: " + formatInr_(p.llr));
            total += Number(p.llr) || 0;
        }
        if (Number(p.license) > 0) {
            lines.push("  • ஓட்டுநர் உரிமம்: " + formatInr_(p.license));
            total += Number(p.license) || 0;
        }
        if (Number(p.training_per_day) > 0 && Number(p.training_days) > 0) {
            let tr = (Number(p.training_per_day) || 0) * (Number(p.training_days) || 0);
            lines.push("  • பயிற்சி: " + formatInr_(p.training_per_day) + " × " + Number(p.training_days) + " நாட்கள் = " + formatInr_(tr));
            total += tr;
        }
        if (Number(p.fixed_total) > 0 && total <= 0) {
            lines.push("  • பேக்கேஜ் மொத்தம்: " + formatInr_(p.fixed_total));
            total += Number(p.fixed_total) || 0;
        }
    }
    return {
        key: key,
        title: svc.title_en || key,
        title_ta: svc.title_ta || svc.title_en || key,
        lines: lines,
        total: total
    };
}

function buildDynamicFeeMessageByServices_(serviceKeys, heading) {
    let cfg = getChatbotServicePricingConfig_();
    let keys = Array.isArray(serviceKeys) ? serviceKeys.map(normalizeServiceKey_) : [];
    let dedup = {};
    keys = keys.filter(function(k) { if (!k || dedup[k]) return false; dedup[k] = true; return !!cfg[k]; });
    if (!keys.length) keys = ["TW_FULL"];

    let blocks = [];
    let grand = 0;
    keys.forEach(function(k) {
        let b = computeServiceBreakdown_(k, cfg, {});
        blocks.push(b);
        grand += Number(b.total) || 0;
    });

    let text = (heading ? String(heading) + "\n\n" : "") + "*கட்டண விவரம்*\n";
    blocks.forEach(function(b) {
        text += "\n▸ *" + b.title_ta + "*\n";
        if (!b.lines.length) text += "  • விவரம் தற்போது கிடைக்கவில்லை\n";
        else text += b.lines.join("\n") + "\n";
        text += "  மொத்தம்: *" + formatInr_(b.total) + "*\n";
    });
    if (blocks.length > 1) text += "\nஒட்டுமொத்த கட்டணம்: *" + formatInr_(grand) + "*\n";
    return text.trim();
}

function getChatbotServiceListRows_() {
    let cfg = getChatbotServicePricingConfig_();
    return Object.keys(cfg).map(function(k) {
        let svc = cfg[k];
        let total = computeServiceBreakdown_(k, cfg, {}).total;
        return {
            id: "FEE_SEL::" + k,
            title: "💠 " + (svc.title_ta || svc.title_en || k),
            description: "Total: " + formatInr_(total)
        };
    });
}

function getChatbotUserState_(phone) {
    try {
        let p = cleanPhoneNumber(phone);
        if (!p) return { selected_services: [], last_service_key: "TW_FULL" };
        let raw = PropertiesService.getScriptProperties().getProperty("WA_BOT_STATE_" + p);
        if (!raw) return { selected_services: [], last_service_key: "TW_FULL" };
        let obj = JSON.parse(raw) || {};
        if (!Array.isArray(obj.selected_services)) obj.selected_services = [];
        if (!obj.last_service_key) obj.last_service_key = "TW_FULL";
        return obj;
    } catch (e) {
        return { selected_services: [], last_service_key: "TW_FULL" };
    }
}

function setChatbotUserState_(phone, obj) {
    try {
        let p = cleanPhoneNumber(phone);
        if (!p) return;
        let payload = Object.assign({}, obj || {}, { saved_at: new Date().toISOString() });
        PropertiesService.getScriptProperties().setProperty("WA_BOT_STATE_" + p, JSON.stringify(payload));
    } catch (e) {}
}

/** Meta enquiry_welcom body {{2}} — Tamil service phrase (matches Firebase dynamicPricingEngine). */
function enquiryWelcomServiceLabelTa_(serviceRaw, vehicleTypeEn) {
    var s = String(serviceRaw || "");
    var low = s.toLowerCase();
    if (low.indexOf("combo") !== -1 || s.indexOf("காம்போ") !== -1 || low.indexOf("2w + 4w") !== -1)
        return "2W + 4W காம்போ பயிற்சி";
    if (low.indexOf("2") !== -1 || low.indexOf("two") !== -1 || s.indexOf("டூ") !== -1 || s.indexOf("இரு") !== -1)
        return "இருசக்கர வாகன பயிற்சி";
    if (low.indexOf("4") !== -1 || low.indexOf("four") !== -1 || s.indexOf("கார்") !== -1 || s.indexOf("நான்கு") !== -1)
        return "கார் பயிற்சி";
    if (String(vehicleTypeEn || "") === "Two-Wheeler") return "இருசக்கர வாகன பயிற்சி";
    var t = s.trim();
    return t || "ஓட்டுநர் பயிற்சி";
}

function buildFirstInquiryWelcomeText_(name, serviceTextOrKey) {
    let n = String(name || "மாணவரே");
    let key = normalizeServiceKey_(serviceTextOrKey);
    let cfg = getChatbotServicePricingConfig_();
    let svc = cfg[key] || cfg.TW_FULL;
    let feePreview = buildDynamicFeeMessageByServices_([key]);
    return (
        `வணக்கம் ${n},\n\n` +
        `*நண்பன் டிரைவிங் ஸ்கூல்* — உங்கள் விசாரணை பதிவு செய்யப்பட்டுள்ளது.\n\n` +
        `*தேர்ந்தெடுக்கப்பட்ட சேவை*\n` +
        `▸ ${svc.title_ta}\n\n` +
        `*எங்கள் தளத்தின் முக்கிய அம்சங்கள்*\n` +
        `• முழுமையான டிஜிட்டல் கண்காணிப்பு — பிரத்தியேக மொபைல் செயலி வழி\n` +
        `• ஒவ்வொரு வகுப்பு நிறைவுக்குப் பிறகு WhatsApp அறிவிப்பு மற்றும் சுருக்க அறிக்கை\n` +
        `• விரிவான வீடியோ வழிகாட்டி (Video Tutor)\n` +
        `• நவீன, பாதுகாப்பான பயிற்சி முறைகள்\n\n` +
        feePreview +
        `\n\nமேலும் விவரம் அல்லது அட்மிஷன் தொடர்பாக கீழுள்ள பொத்தான்களைப் பயன்படுத்தவும். நன்றி.`
    );
}

function sendAllTemplateTestsAction(toPhone, loggedBy) {
    try {
        if (loggedBy && !isPrivilegedName(loggedBy)) return { status: 'error', message: 'Not allowed' };

        let cfg = getTemplateAndReminderConfig();
        let phone = cleanPhoneNumber(toPhone || "");
        if (!phone) return { status: 'error', message: 'Phone required' };

        let templates = [
            cfg.enquiryTemplate,
            cfg.welcomeTemplate,
            cfg.llrTemplate,
            cfg.rtoTemplate,
            cfg.rtoTomorrowTemplate,
            cfg.dailyClassTemplate,
            cfg.chitAuctionTemplate,
            cfg.chitDueTemplate,
            cfg.chitReceiptTemplate,
            cfg.bulkTemplate
        ].map(x => String(x || "").trim()).filter(Boolean);

        let results = [];
        for (let i = 0; i < templates.length; i++) {
            let t = templates[i];
            if (i > 0) Utilities.sleep(1200);
            let r = sendTemplateTestAction(t, phone);
            results.push({ template: t, status: (r && r.status) ? r.status : "unknown", msg: (r && r.msg) ? r.msg : "" });
        }

        try { logAuditEvent('TEMPLATE_TEST_ALL', 'Template', phone, "", "", { count: templates.length, by: loggedBy || "" }); } catch (e) {}
        return { status: 'success', results: results, count: results.length };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🛠️ வாட்ஸ்அப் ஆரோக்கியத்தை சரிபார்க்கும் கருவி (Health Check)
 */
function checkWhatsAppHealthAction() {
    try {
        let phone = ADMINS[0]; // ரஞ்சித் அண்ணா போன் எண்
        let results = [];

        // 1. Direct Message Test
        let r1 = sendWhatsAppMessage(phone, "🛠️ Nanban ERP: WhatsApp Connection Test (Direct Text)");
        results.push({ test: "Direct Text", status: r1.status, details: r1.body || r1.message || "OK" });

        // 2. Template Test
        let cfg = getTemplateAndReminderConfig();
        let r2 = sendTemplateMsg(phone, cfg.adminUniversalTemplate, ["Test Alert"], null);
        results.push({ test: "Template (" + cfg.adminUniversalTemplate + ")", status: r2.status, details: r2.body || r2.message || "OK" });

        return { 
            status: 'success', 
            results: results,
            info: {
                phoneId: WA_PHONE_ID,
                tokenPrefix: WA_TOKEN.substring(0, 10) + "...",
                messagesEnabled: isMessagingEnabled()
            }
        };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function sendTemplateMsg(toPhone, templateName, bodyParams, docId) {
    let url = "https://graph.facebook.com/v20.0/" + WA_PHONE_ID + "/messages";
    let components = [];
    
    // 🎯 Use "id" if we have one, otherwise fall back to link
    let docPayload = {};
    if (docId) {
        let finalId = docId;
        // If it's a URL or Drive ID, we try to upload it first for better native opening
        if (String(docId).length > 20) {
            try {
                let mediaId = getWhatsAppMediaId(docId);
                if (mediaId) {
                    docPayload = { "id": mediaId, "filename": "Nanban_Document.pdf" };
                } else if (String(docId).startsWith("http")) {
                    docPayload = { "link": docId, "filename": "Nanban_Document.pdf" };
                }
            } catch(e) {
                if (String(docId).startsWith("http")) {
                    docPayload = { "link": docId, "filename": "Nanban_Document.pdf" };
                }
            }
        }

        if (Object.keys(docPayload).length === 0) {
            docPayload = { "id": docId, "filename": "Nanban_Document.pdf" };
        }

        components.push({
            "type": "header",
            "parameters": [{
                "type": "document",
                "document": docPayload
            }]
        });
    }
    
    if (bodyParams && bodyParams.length > 0) {
        components.push({
            "type": "body",
            "parameters": bodyParams.map(function(p) {
                return { "type": "text", "text": sanitizeTemplateParamText_(p) };
            })
        });
    }

    let payload = {
        "messaging_product": "whatsapp",
        "to": cleanPhoneNumber(toPhone),
        "type": "template",
        "template": {
            "name": templateName,
            "language": { "code": "ta" },
            "components": components
        }
    };

    let options = {
        "method": "post",
        "headers": {
            "Authorization": "Bearer " + getCleanToken(),
            "Content-Type": "application/json"
        },
        "payload": JSON.stringify(payload),
        "muteHttpExceptions": true
    };

    try {
        let response = UrlFetchApp.fetch(url, options);
        let responseCode = response.getResponseCode();
        let responseBody = response.getContentText();
        try { logWaOutboundLocal_("template", toPhone, templateName, payload, responseCode, responseBody); } catch (e) {}
        
        if (responseCode >= 200 && responseCode < 300) {
            return { status: 'success', body: responseBody };
        } else {
            console.error("Template Send Failed: " + responseBody);
            try { logBotActivity("WA_TEMPLATE_FAIL", `Template: ${templateName} | To: ${toPhone} | Error: ${responseBody}`); } catch(logErr){}
            return { status: 'error', message: responseBody, code: responseCode };
        }
    } catch (e) {
        console.error("Template API Error: " + e.toString());
        try { logBotActivity("WA_API_ERROR", e.toString()); } catch(logErr){}
        return { status: 'error', message: e.toString() };
    }
}

function sendTemplateWithParamFallback(toPhone, templateName, paramCandidates, docId) {
    let isAdmin = ADMINS.includes(cleanPhoneNumber(toPhone));
    if (!isAdmin && !isMessagingEnabled()) return { status: 'error', message: 'Messaging is disabled by Admin.' };
    try {
        let list = Array.isArray(paramCandidates) ? paramCandidates : [];
        for (let i = 0; i < list.length; i++) {
            let arr = Array.isArray(list[i]) ? list[i] : [];
            let r = sendTemplateMsg(toPhone, templateName, arr, docId || null);
            if (r && r.status === 'success') return r;
        }
    } catch (e) {}
    return { status: 'error' };
}

function buildRtoReminderMessage(studentName, d, t) {
    let cfg = getTemplateAndReminderConfig();
    let timeVal = t || cfg.inspectorTime || "";
    return cfg.rtoReminderText
        .replace(/\{name\}/g, String(studentName || "மாணவர்"))
        .replace(/\{date\}/g, d)
        .replace(/\{time\}/g, timeVal);
}

function generateUPILink(amount, studentName) {
    let cfg = getTemplateAndReminderConfig();
    let upiId = cfg.businessUpi || "9942391870@oksbi";
    let name = encodeURIComponent("Nanban Driving School");
    let tnCode = encodeURIComponent("Fees - " + studentName);
    // Note: upi:// protocol works on mobile apps.
    return `upi://pay?pa=${upiId}&pn=${name}&am=${amount}&tn=${tnCode}&cu=INR`;
}

// ------------------------------------------------------------------------------
// Helpers for PDFs, WhatsApp document messages, Contacts auto-save
// ------------------------------------------------------------------------------

function getDrivePdfDownloadUrl(fileId) {
    if (!fileId) return "";
    return "https://docs.google.com/uc?id=" + fileId + "&export=download";
}

/**
 * 🎯 NATIVE MEDIA UPLOAD: The ultimate fix for PDF opening errors.
 * Uploads a Drive file or URL to Meta and returns a Media ID.
 */
function getWhatsAppMediaId(sourceId) {
    if (!sourceId) return null;
    try {
        let blob;
        if (String(sourceId).startsWith("http")) {
            // It's a URL
            blob = UrlFetchApp.fetch(sourceId).getBlob();
        } else {
            // It's a Drive ID
            blob = DriveApp.getFileById(sourceId).getBlob();
        }
        
        let url = "https://graph.facebook.com/v20.0/" + WA_PHONE_ID + "/media";
        let options = {
            "method": "post",
            "headers": { "Authorization": "Bearer " + WA_TOKEN },
            "payload": {
                "messaging_product": "whatsapp",
                "file": blob
            },
            "muteHttpExceptions": true
        };
        let res = UrlFetchApp.fetch(url, options);
        let resContent = res.getContentText();
        let json = JSON.parse(resContent);
        
        if (json.error) {
            logBotActivity("MEDIA_UPLOAD_API_ERR", resContent);
            return null;
        }
        return json.id || null;
    } catch(e) {
        logBotActivity("MEDIA_UPLOAD_FAIL", e.toString());
        return null;
    }
}

function getISTMonthLabelYear() {
    let tz = new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" });
    let ist = new Date(tz);
    let monthLabel = ist.toLocaleString("en-US", { month: "short" }); // e.g. Mar
    let year = ist.getFullYear();
    return { monthLabel, year };
}

function getVehicleKeyFromService(service) {
    let s = String(service || "").toLowerCase();
    if (s.includes("combo")) return "COMBO";
    if (s.includes("2")) return "2W";
    if (s.includes("4")) return "4W";
    return String(service || "").trim().replace(/\s+/g, "_") || "VEH";
}

function buildNDSContactName(service) {
    let v = getVehicleKeyFromService(service);
    let mt = getISTMonthLabelYear();
    return `NDS-${v}-${mt.monthLabel}`;
}

function autoSaveGoogleContact(student) {
    try {
        if (!student || !student.phone) return "No phone provided";

        let phoneDigits = cleanPhoneNumber(student.phone);
        if (!phoneDigits || phoneDigits.length < 10) return "Invalid phone digits";
        
        let phoneLocal10 = phoneDigits.slice(-10);
        let phonePlus = "+91" + phoneLocal10;
        let contactTag = buildNDSContactName(student.service || "");
        let studentName = String(student.name || "Customer").trim();
        let displayName = `${studentName} - ${contactTag}`;

        // Method 1: Try with ContactsApp (Standard)
        if (typeof ContactsApp !== 'undefined') {
            try {
                let existing = ContactsApp.getContactsByPhoneNumber(phonePlus);
                if (existing && existing.length > 0) return "Exists (ContactsApp)";

                let contact = ContactsApp.createContact(displayName, "", "");
                contact.addPhone(ContactsApp.Field.MOBILE_PHONE, phonePlus);
                
                let group = ContactsApp.getContactGroup("Nanban_Contacts");
                if (!group) group = ContactsApp.createContactGroup("Nanban_Contacts");
                if (group) group.addContact(contact);
                
                return "Success (ContactsApp)";
            } catch(e) {
                Logger.log("ContactsApp Error: " + e.toString());
            }
        }

        // Method 2: Try with People API (Modern Backup)
        if (typeof People !== 'undefined') {
            try {
                let contact = {
                    names: [{ givenName: displayName }],
                    phoneNumbers: [{ value: phonePlus, type: 'mobile' }]
                };
                People.People.createContact(contact);
                return "Success (People API)";
            } catch(e) {
                Logger.log("People API Error: " + e.toString());
            }
        }

        return "Error: No Contact Service Enabled. Please add 'Google Contacts API' in Services (+).";
    } catch (err) {
        return "Critical Error: " + err.toString();
    }
}

// 🎯 TEST FUNCTION: Run this in Apps Script to verify
function TEST_CONTACT_SAVE() {
  let testStudent = {
    name: "Test Nanban",
    phone: "9876543210",
    service: "4W"
  };
  let res = autoSaveGoogleContact(testStudent);
  Logger.log("Test Result: " + res);
}

function sendWhatsAppDocumentMessage(toPhone, docLink, filename, captionText) {
    let isAdmin = ADMINS.includes(cleanPhoneNumber(toPhone));
        if (!isAdmin && !isMessagingEnabled()) return { status: 'error', message: 'Messaging is disabled by Admin.' };
        try {
            if (!docLink) return;
        let url = "https://graph.facebook.com/v20.0/" + WA_PHONE_ID + "/messages";
        // 🎯 NATIVE MEDIA FIX: Upload to Meta for reliable opening on mobile
        // This ensures the phone doesn't see a Google Drive redirect, avoiding ARCore errors.
        let mediaId = getWhatsAppMediaId(docLink);
        let docPayload = { "filename": filename || "Document.pdf" };
        if (mediaId) {
            docPayload.id = mediaId;
        } else {
            docPayload.link = docLink;
        }

        let payload = {
            messaging_product: "whatsapp",
            to: cleanPhoneNumber(toPhone),
            type: "document",
            document: docPayload
        };

        if (captionText) payload.document.caption = captionText;

        let resp = UrlFetchApp.fetch(url, {
            method: "post",
            headers: {
                "Authorization": "Bearer " + getCleanToken(),
                "Content-Type": "application/json"
            },
            payload: JSON.stringify(payload),
            muteHttpExceptions: true
        });
        
        let code = resp.getResponseCode();
        let body = resp.getContentText();
        try { logWaOutboundLocal_("document", toPhone, "", payload, code, body); } catch (e) {}
        if (code >= 200 && code < 300) return { status: 'success' };
        
        Logger.log("WA Document Failed [" + code + "]: " + body);
        return { status: 'error', message: body, code: code };
    } catch (error) {
        Logger.log("WA Document Error: " + error.toString());
        return { status: 'error', message: error.toString() };
    }
}

function AUTHORIZE_SCRIPT() { 
    DriveApp.getRootFolder(); 
    let folders = DriveApp.getFoldersByName("Nanban_Uploads");
    if (!folders.hasNext()) {
        DriveApp.createFolder("Nanban_Uploads");
    }
    ensureChitSheets(); 
    Logger.log("✅ Google Permissions Granted Successfully!");
}

function notifyAdmins(msg) { 
    console.log("🔔 notifyAdmins() TRIGGERED. Content: " + String(msg).substring(0, 100));
    
    if (!ADMINS || ADMINS.length === 0) {
        console.error("❌ CRITICAL ERROR: No ADMINS defined at top of script.");
        return;
    }
    
    let cfg = getTemplateAndReminderConfig();
    let templateName = cfg.adminUniversalTemplate || "admin_universal_alert";
    
    // 🔥 Multi-Admin Sync: Combine static ADMINS + dynamic Partner Phone
    let adminList = [...ADMINS];
    let pPhone = cleanPhoneNumber(cfg.partnerPhone);
    if (pPhone && !adminList.includes(pPhone)) {
        adminList.push(pPhone);
    }

    // Split message if it exceeds 1024 characters (Meta limit per parameter)
    let chunks = [];
    let rawMsg = String(msg || "");
    const MAX_LEN = 1000;
    
    if (rawMsg.length <= MAX_LEN) {
        chunks.push(rawMsg);
    } else {
        let lines = rawMsg.split('\n');
        let currentChunk = "";
        
        for (let i = 0; i < lines.length; i++) {
            let line = lines[i];
            
            // If a single line is greater than MAX_LEN, we have to blind split it
            if (line.length > MAX_LEN) {
                // push existing chunk
                if (currentChunk.trim().length > 0) {
                    chunks.push(currentChunk.trim());
                    currentChunk = "";
                }
                // blind split the huge line
                for (let j = 0; j < line.length; j += MAX_LEN) {
                    chunks.push(line.substring(j, j + MAX_LEN));
                }
            } else if ((currentChunk.length + line.length + 1) > MAX_LEN) {
                // adding this line would exceed max length, push current chunk
                if (currentChunk.trim().length > 0) {
                    chunks.push(currentChunk.trim());
                }
                currentChunk = line + "\n";
            } else {
                currentChunk += line + "\n";
            }
        }
        
        if (currentChunk.trim().length > 0) {
            chunks.push(currentChunk.trim());
        }
    }

    // Add pagination indicators if message was split
    if (chunks.length > 1) {
        let formattedChunks = [];
        let total = chunks.length;
        for (let idx = 0; idx < total; idx++) {
            let prefix = `[Part ${idx + 1}/${total}]\n`;
            // Ensure adding the prefix doesn't blow the 1024 limit
            // (1000 + ~13 chars = 1013 chars, which is < 1024, so it's safe)
            formattedChunks.push(prefix + chunks[idx]);
        }
        chunks = formattedChunks;
    }

    for (let j = 0; j < adminList.length; j++) {
        let phone = adminList[j];
        for (let k = 0; k < chunks.length; k++) {
            try { 
                if (j > 0 || k > 0) Utilities.sleep(2000); 
                let chunkMsg = chunks[k];
                let chunkMsgForTemplate = sanitizeTemplateParamText_(chunkMsg);
                
                // Using admin template to bypass 24h window
                // Template should have one variable: 🔔 நண்பன் ERP அலர்ட்: {{1}}
                let res = sendTemplateMsg(phone, templateName, [chunkMsgForTemplate], null);
                
                if (res && res.status === 'success') {
                    console.log(`✅ Admin alert chunk ${k+1} sent to ${phone}`);
                } else {
                    console.warn(`⚠️ Admin Template chunk ${k+1} failed for ${phone}. Trying Direct Message fallback.`);
                    sendWhatsAppMessage(phone, "🔔 *நண்பன் ERP அலர்ட்:*\n\n" + chunkMsg);
                }
            } catch(error) {
                console.error(`🚨 Admin WA Failed for ${phone} (Chunk ${k+1}): ` + error.message);
            } 
        }
    }
}

// WhatsApp template variables cannot contain newline/tab chars
function sanitizeTemplateParamText_(txt) {
    try {
        let s = String(txt || "");
        s = s.replace(/[\r\n\t]+/g, " | ");
        s = s.replace(/\s{5,}/g, "    "); // hard cap to avoid >4 consecutive spaces
        s = s.replace(/\s+\|\s+/g, " | ");
        return s.trim();
    } catch (e) {
        return String(txt || "");
    }
}

function appendJsonLogProp_(propKey, entryObj, maxItems) {
    try {
        let p = PropertiesService.getScriptProperties();
        let raw = String(p.getProperty(propKey) || "[]");
        let arr = [];
        try { arr = JSON.parse(raw); } catch (e) { arr = []; }
        if (!Array.isArray(arr)) arr = [];
        arr.unshift(entryObj || {});
        let cap = Number(maxItems) || 120;
        if (arr.length > cap) arr = arr.slice(0, cap);
        p.setProperty(propKey, JSON.stringify(arr));
    } catch (e) {}
}

function logWaOutboundLocal_(kind, toPhone, templateName, payloadObj, responseCode, responseBody) {
    try {
        appendJsonLogProp_("WA_OUTBOUND_LOG", {
            at: new Date().toISOString(),
            kind: String(kind || "unknown"),
            to: cleanPhoneNumber(toPhone || ""),
            template: String(templateName || ""),
            code: Number(responseCode || 0),
            status: (Number(responseCode) >= 200 && Number(responseCode) < 300) ? "sent_api_ok" : "sent_api_fail",
            response: String(responseBody || "").substring(0, 900),
            payload: payloadObj || {}
        }, 300);
    } catch (e) {}
}

function captureWaWebhookStatusesLocal_(jsonBody) {
    try {
        let entries = Array.isArray(jsonBody && jsonBody.entry) ? jsonBody.entry : [];
        let list = [];
        entries.forEach(function(ent) {
            let changes = Array.isArray(ent && ent.changes) ? ent.changes : [];
            changes.forEach(function(ch) {
                let value = (ch && ch.value) ? ch.value : {};
                let sts = Array.isArray(value.statuses) ? value.statuses : [];
                sts.forEach(function(s) {
                    list.push({
                        at: new Date().toISOString(),
                        wa_id: String(s && s.id || ""),
                        to: cleanPhoneNumber((s && s.recipient_id) || ""),
                        status: String(s && s.status || ""),
                        ts: String(s && s.timestamp || ""),
                        conversation_id: String(s && s.conversation && s.conversation.id || ""),
                        category: String(s && s.pricing && s.pricing.category || ""),
                        errors: (s && s.errors) ? s.errors : []
                    });
                });
            });
        });
        if (list.length) {
            list.forEach(function(item) {
                appendJsonLogProp_("WA_STATUS_LOG", item, 500);
            });
            try {
                PropertiesService.getScriptProperties().setProperty("WA_LAST_PARSE_NOTE", "Webhook status events captured: " + list.length);
            } catch (e) {}
        }
    } catch (e) {}
}

function getWhatsAppDeliveryStatusLocalAction(phone, limit) {
    try {
        let p = PropertiesService.getScriptProperties();
        let statusLog = [];
        let outbound = [];
        try { statusLog = JSON.parse(String(p.getProperty("WA_STATUS_LOG") || "[]")); } catch (e) { statusLog = []; }
        try { outbound = JSON.parse(String(p.getProperty("WA_OUTBOUND_LOG") || "[]")); } catch (e) { outbound = []; }
        if (!Array.isArray(statusLog)) statusLog = [];
        if (!Array.isArray(outbound)) outbound = [];
        let key = cleanPhoneNumber(phone || "");
        let lim = Math.max(1, Math.min(200, Number(limit) || 50));
        let fStatus = key ? statusLog.filter(function(x) { return cleanPhoneNumber(x && x.to || "") === key; }) : statusLog.slice();
        let fOutbound = key ? outbound.filter(function(x) { return cleanPhoneNumber(x && x.to || "") === key; }) : outbound.slice();
        return {
            status: "success",
            phone: key || "",
            delivery_events: fStatus.slice(0, lim),
            outbound_events: fOutbound.slice(0, lim)
        };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function getTemplateVariableAuditAction() {
    try {
        let cfg = getTemplateAndReminderConfig();
        let audit = [
            { key: "enquiryTemplate", name: cfg.enquiryTemplate, vars: ["name", "service", "vehicle_type"], fallbackCount: 3 },
            { key: "welcomeTemplate", name: cfg.welcomeTemplate, vars: ["name", "service", "join_date", "total_fee", "advance", "balance"], fallbackCount: 1 },
            { key: "llrTemplate", name: cfg.llrTemplate, vars: ["name", "service", "join_date", "total_fee", "advance", "balance"], fallbackCount: 1 },
            { key: "rtoTemplate", name: cfg.rtoTemplate, vars: ["name", "test_date", "time"], fallbackCount: 4 },
            { key: "rtoTomorrowTemplate", name: cfg.rtoTomorrowTemplate, vars: ["name"], fallbackCount: 2 },
            { key: "dailyClassTemplate", name: cfg.dailyClassTemplate, vars: ["name", "class_count", "performance"], fallbackCount: 2 },
            { key: "chitAuctionTemplate", name: cfg.chitAuctionTemplate, vars: ["member_name", "group_name", "month"], fallbackCount: 3 },
            { key: "chitDueTemplate", name: cfg.chitDueTemplate, vars: ["member_name", "group_name"], fallbackCount: 3 },
            { key: "chitReceiptTemplate", name: cfg.chitReceiptTemplate, vars: ["member_name", "amount", "date"], fallbackCount: 3 },
            { key: "bulkTemplate", name: cfg.bulkTemplate, vars: ["message"], fallbackCount: 2 },
            { key: "paymentReminderTemplate", name: cfg.paymentReminderTemplate, vars: ["name", "balance"], fallbackCount: 4 },
            { key: "dayCloseTemplate", name: cfg.dayCloseTemplate, vars: ["trainer_name", "cash", "online", "km"], fallbackCount: 2 },
            { key: "quizTemplate", name: cfg.quizTemplate, vars: ["name", "question"], fallbackCount: 2 },
            { key: "adminUniversalTemplate", name: cfg.adminUniversalTemplate, vars: ["alert_text"], fallbackCount: 1 },
            // Additional approved/static templates used in fallback paths:
            { key: "static_passport", name: "passport_admission", vars: ["name", "service", "date", "fee", "advance", "balance"], fallbackCount: 1 },
            { key: "static_divorce", name: "divorce_admission", vars: ["name", "service", "date", "fee", "advance", "balance"], fallbackCount: 1 },
            { key: "static_rto_test_tomorrow", name: "rto_test_tomorrow", vars: ["name"], fallbackCount: 2 },
            { key: "static_daily_quiz_btn", name: "daily_quiz_btn", vars: ["name", "question"], fallbackCount: 2 },
            { key: "static_admin_universal_alert", name: "admin_universal_alert", vars: ["alert_text"], fallbackCount: 1 }
        ];
        let invalid = audit.filter(function(a) { return !/^[a-z0-9_]+$/.test(String(a.name || "")); });
        return {
            status: "success",
            total_templates_checked: audit.length,
            invalid_templates: invalid,
            templates: audit
        };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function diagnoseCustomerDeliveryLocalAction(phone) {
    try {
        let info = getWhatsAppDeliveryStatusLocalAction(phone, 80);
        if (!info || info.status !== "success") return info;
        let events = Array.isArray(info.delivery_events) ? info.delivery_events : [];
        let outbound = Array.isArray(info.outbound_events) ? info.outbound_events : [];
        let latestOutbound = outbound.length ? outbound[0] : null;
        let latestDelivery = events.length ? events[0] : null;
        let verdict = "unknown";
        if (latestDelivery && String(latestDelivery.status || "").toLowerCase() === "failed") verdict = "api_failed";
        else if (latestDelivery && (String(latestDelivery.status || "").toLowerCase() === "delivered" || String(latestDelivery.status || "").toLowerCase() === "read")) verdict = "customer_received";
        else if (latestOutbound && String(latestOutbound.status || "") === "sent_api_ok" && !latestDelivery) verdict = "sent_no_delivery_event_yet";
        else if (latestOutbound && String(latestOutbound.status || "") === "sent_api_fail") verdict = "api_failed";
        return {
            status: "success",
            phone: info.phone,
            verdict: verdict,
            latest_outbound: latestOutbound,
            latest_delivery: latestDelivery,
            note: "Verdict is based only on local webhook + outbound logs (no live API calls)."
        };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function previewChatbotFlowLocalAction(pid) {
    try {
        let p = String(pid || "").trim();
        if (p === "MENU_BIKE") {
            return { status: "success", pid: p, message: buildFirstInquiryWelcomeText_("மாணவரே", "TW_FULL") };
        }
        if (p === "MENU_CAR") {
            return { status: "success", pid: p, message: buildFirstInquiryWelcomeText_("மாணவரே", "FW_LICENSE_ONLY") };
        }
        if (p === "MENU_FEES") {
            return { status: "success", pid: p, message: buildDynamicFeeMessageByServices_(["TW_FULL"]) };
        }
        if (p.indexOf("FEE_SEL::") === 0) {
            let k = String(p).split("::")[1] || "TW_FULL";
            return { status: "success", pid: p, message: buildDynamicFeeMessageByServices_([k]) };
        }
        return { status: "success", pid: p, message: "No preview configured for this pid. Try MENU_BIKE / MENU_CAR / MENU_FEES / FEE_SEL::FW_LICENSE_TRAINING" };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

// ------------------------------------------------------------------------------
// 3. FRONTEND CONNECTION (SAFE HTML OUTPUT)
// ------------------------------------------------------------------------------

function doGet(e) {
    // Bridge GET fallback for cross-origin/redirect-safe calls (JSONP style).
    try {
        let a = String((e && e.parameter && e.parameter.action) || "");
        if (a === "api_bridge_get") {
            let fn = String((e.parameter && e.parameter.fn) || "");
            let key = String((e.parameter && e.parameter.key) || "");
            let cb = String((e.parameter && e.parameter.cb) || "");
            let argsRaw = String((e.parameter && e.parameter.args) || "[]");
            let args = [];
            try { args = JSON.parse(argsRaw); } catch (pErr) { args = []; }

            let bridgeRes = handleApiBridgeCall_({ action: "api_bridge", fn: fn, key: key, args: args });
            let payload = bridgeRes && bridgeRes.getContent ? bridgeRes.getContent() : String(bridgeRes || "{}");
            if (!/^[A-Za-z_$][A-Za-z0-9_$\.]*$/.test(cb || "")) {
                return ContentService.createTextOutput(payload).setMimeType(ContentService.MimeType.JSON);
            }
            return ContentService
                .createTextOutput(cb + "(" + payload + ");")
                .setMimeType(ContentService.MimeType.JAVASCRIPT);
        }
    } catch (bridgeGetErr) {}

    // Meta webhook verification handshake
    try {
        let mode = String((e && e.parameter && e.parameter["hub.mode"]) || "");
        let challenge = String((e && e.parameter && e.parameter["hub.challenge"]) || "");
        if (mode === "subscribe" && challenge) {
            return ContentService.createTextOutput(challenge);
        }
    } catch (vhErr) {}

    // 🔍 Debug Diagnostic
    if (e.parameter.debug) {
        let dbgMode = String(e.parameter.debug || "").toLowerCase();
        if (dbgMode === "webhook" || dbgMode === "local") {
            let local = getWebhookDebugStatus();
            return ContentService.createTextOutput(JSON.stringify(local, null, 2)).setMimeType(ContentService.MimeType.JSON);
        }
        let tokenStatus = "Not Tested";
        try {
            let testRes = UrlFetchApp.fetch("https://graph.facebook.com/v20.0/" + WA_PHONE_ID, {
                headers: { "Authorization": "Bearer " + getCleanToken() },
                muteHttpExceptions: true
            });
            tokenStatus = testRes.getResponseCode() === 200 ? "Valid ✅" : "Error ❌: " + testRes.getContentText();
        } catch(f) { tokenStatus = "Crash 🛠️: " + f.toString(); }

        let status = {
            bot_alive: true,
            api_token_status: tokenStatus,
            time: new Date().toLocaleString(),
            token_snippet: WA_TOKEN.substring(0, 10) + "...",
            phone_id: WA_PHONE_ID,
            storage_mode: "firebase_rtdb_only"
        };
        return ContentService.createTextOutput(JSON.stringify(status, null, 2)).setMimeType(ContentService.MimeType.JSON);
    }

    let page = e.parameter.page || 'index';
    if (page === 'admission') {
        return HtmlService.createHtmlOutputFromFile('Admission')
            .setTitle('Nanban - Online Admission')
            .addMetaTag('viewport', 'width=device-width, initial-scale=1')
            .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
    }
    
    return HtmlService.createHtmlOutputFromFile('index')
        .setTitle('Nanban Pro - World Class ERP')
        .addMetaTag('viewport', 'width=device-width, initial-scale=1')
        .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function getWebhookDebugStatus() {
    try {
        let p = PropertiesService.getScriptProperties();
        let recentStatus = [];
        let recentOutbound = [];
        try { recentStatus = JSON.parse(String(p.getProperty("WA_STATUS_LOG") || "[]")); } catch (e) { recentStatus = []; }
        try { recentOutbound = JSON.parse(String(p.getProperty("WA_OUTBOUND_LOG") || "[]")); } catch (e) { recentOutbound = []; }
        if (!Array.isArray(recentStatus)) recentStatus = [];
        if (!Array.isArray(recentOutbound)) recentOutbound = [];
        return {
            status: "ok",
            lastHitAt: p.getProperty("WA_LAST_HIT_AT") || "",
            lastMessageFrom: p.getProperty("WA_LAST_FROM") || "",
            lastMessageType: p.getProperty("WA_LAST_TYPE") || "",
            lastParseNote: p.getProperty("WA_LAST_PARSE_NOTE") || "",
            lastError: p.getProperty("WA_LAST_ERROR") || "",
            lastBodySnippet: p.getProperty("WA_LAST_BODY_SNIPPET") || "",
            recentDelivery: recentStatus.slice(0, 10),
            recentOutbound: recentOutbound.slice(0, 10)
        };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function bridgePingAction() {
    return {
        status: "success",
        serverTime: new Date().toISOString(),
        storage_mode: "firebase_rtdb_only"
    };
}

// ------------------------------------------------------------------------------
// 4. USER & SETTINGS MANAGEMENT 
// ------------------------------------------------------------------------------

function getAppUsers(tenantId) {
    try {
        if (!useFirebaseRtdb_()) return [];
        let tid = ensureTenantBootstrap_(tenantId);
        let pinCfg = getAdminPinConfig_(tid);
        let rows = tenantFbGet_(tid, "users", null);
        if (!rows && tid === getDefaultTenantId_()) rows = fbGet_("users", {});
        rows = rows || {};
        let out = [];
        let seen = {};
        Object.keys(rows || {}).forEach(function(k) {
            let d = rows[k] || {};
            if (!d.name) return;
            let roleVal = String(d.role || "Staff").trim();
            let nameVal = String(d.name || "").trim();
            let businesses = Array.isArray(d.businesses) ? d.businesses : [];
            if (!businesses.length) businesses = (roleVal === "Admin") ? ["Nanban", "ESevai"] : ["Nanban"];
            if (roleVal === "Partner") businesses = ["Nanban"];

            let pin = String(d.pin || "").trim();
            if (!pin) {
                if (pinCfg.byUser[nameVal] !== undefined) pin = String(pinCfg.byUser[nameVal] || "").trim();
                else if (pinCfg.byRole[roleVal] !== undefined) pin = String(pinCfg.byRole[roleVal] || "").trim();
                else if (roleVal === "Admin") pin = pinCfg.adminPin || pinCfg.defaultPin || "";
                else if (roleVal === "Partner") pin = pinCfg.partnerPin || pinCfg.defaultPin || "";
                else pin = pinCfg.defaultPin || "";
            }
            if (!pin) return;
            seen[nameVal.toLowerCase()] = true;
            out.push({
                name: nameVal,
                phone: String(d.phone || "").trim(),
                role: roleVal,
                pin: pin,
                businesses: businesses,
                tenant_id: normalizeTenantId_(d.tenant_id || d.tenantId || tid) || tid
            });
        });

        // Fallback 1: build users directly from PIN.byUser map when /users path is empty.
        if (!out.length && pinCfg.byUser && typeof pinCfg.byUser === "object") {
            Object.keys(pinCfg.byUser).forEach(function(nameKey) {
                let nm = String(nameKey || "").trim();
                let p = String(pinCfg.byUser[nameKey] || "").trim();
                if (!nm || !p) return;
                if (seen[nm.toLowerCase()]) return;
                seen[nm.toLowerCase()] = true;
                out.push({
                    name: nm,
                    phone: "",
                    role: "Admin",
                    pin: p,
                    businesses: ["Nanban", "ESevai"],
                    tenant_id: tid
                });
            });
        }

        // Fallback 2: guaranteed emergency admin entries when only role/default pins exist.
        if (!out.length) {
            let adminPin = String(pinCfg.adminPin || pinCfg.defaultPin || "").trim();
            let partnerPin = String(pinCfg.partnerPin || pinCfg.defaultPin || "").trim();
            if (adminPin) {
                out.push({ name: "ரஞ்சித்", phone: "", role: "Admin", pin: adminPin, businesses: ["Nanban", "ESevai"], tenant_id: tid });
                out.push({ name: "நந்தகுமார்", phone: "", role: "Admin", pin: adminPin, businesses: ["Nanban", "ESevai"], tenant_id: tid });
            }
            if (partnerPin && partnerPin !== adminPin) {
                out.push({ name: "பார்ட்னர்", phone: "", role: "Partner", pin: partnerPin, businesses: ["Nanban"], tenant_id: tid });
            }
        }

        return out;
    } catch (error) { 
        return [];
    }
}

function getAppSettings() {
  let obj = { 
    appSettings: { 
      services: ["2 வீலர்", "4 வீலர்", "Combo"], 
      expenseCategories: ["பெட்ரோல்", "சம்பளம்"], 
      incomeCategories: ["வரவு"], 
      referrers: [], 
      trainerAlertPhone: "", 
      openingBalance: 0 
    }, 
    serviceSplits: {}, 
    vehicleKm: { current: 0, lastService: 0, nextService: 5000 } 
  };
  try {
    if (!useFirebaseRtdb_()) return obj;
    let snap = getNanbanSnapshot_() || {};
    if (snap.appSettingsBundle && typeof snap.appSettingsBundle === "object") {
      obj = Object.assign(obj, snap.appSettingsBundle);
    }

    let props = PropertiesService.getScriptProperties();
    let opBal = props.getProperty('OPENING_BALANCE');
    if (opBal !== null) {
      obj.appSettings.openingBalance = parseInt(opBal) || 0;
    }
    
    let vehCur = props.getProperty('VEHICLE_KM_CURRENT');
    if (vehCur !== null) obj.vehicleKm.current = parseInt(vehCur) || 0;
    
    let vehNext = props.getProperty('VEHICLE_KM_NEXT');
    if (vehNext !== null) obj.vehicleKm.nextService = parseInt(vehNext) || 5000;

  } catch(e) {
    Logger.log("getAppSettings error: " + e.toString());
  }
  return obj;
}

function saveOpeningBalanceAction(val) {
  try {
    let props = PropertiesService.getScriptProperties();
    props.setProperty('OPENING_BALANCE', String(val));
    
    // Update the Settings sheet as well for long-term storage
    let settings = getAppSettings();
    if (!settings.appSettings) settings.appSettings = {};
    settings.appSettings.openingBalance = val;
    saveAppSettings('appSettings', settings.appSettings);
    
    return { status: 'success' };
  } catch (e) {
    return { status: 'error', message: e.toString() };
  }
}

function saveAppSettings(key, valueObj) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        let snap = getNanbanSnapshot_() || {};
        let bundle = snap.appSettingsBundle || getAppSettings();
        let beforeVal = (typeof bundle[key] === 'object') ? JSON.stringify(bundle[key]) : String(bundle[key] || "");
        let saveVal = (typeof valueObj === 'object') ? JSON.stringify(valueObj) : String(valueObj || "");
        bundle[key] = valueObj;
        snap.appSettingsBundle = bundle;
        if (!Array.isArray(snap.students)) snap.students = [];
        if (!Array.isArray(snap.expenses)) snap.expenses = [];
        saveNanbanSnapshot_(snap);
        try { logAuditEvent('SAVE_SETTINGS', 'Settings', key, beforeVal, saveVal, { key: key }); } catch (e) {}
        return { status: 'success' };
    } catch (error) { 
        return { status: 'error', message: error.toString() }; 
    }
}

function setAppSettingsAction(subKey, val) {
    try {
        let settings = getAppSettings();
        if (!settings.appSettings) settings.appSettings = {};
        settings.appSettings[subKey] = val;
        return saveAppSettings('appSettings', settings.appSettings);
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

function normalizePhone10(phone) {
    try {
        if (!phone) return "";
        let p = String(phone).replace(/\D/g, "");
        if (p.length >= 10) return p.slice(-10);
        return p;
    } catch (e) {
        return "";
    }
}

function parseStudentDate_(raw) {
    try {
        if (!raw) return null;
        if (raw instanceof Date && !isNaN(raw.getTime())) return raw;
        let s = String(raw).trim();
        if (!s) return null;

        // ISO / timestamp
        if (s.indexOf("T") !== -1) {
            let dIso = new Date(s);
            if (!isNaN(dIso.getTime())) return dIso;
        }

        // DD/MM/YYYY or DD-MM-YYYY
        if (/^\d{2}[\/-]\d{2}[\/-]\d{4}$/.test(s)) {
            let p = s.split(/[\/-]/);
            let d1 = new Date(parseInt(p[2], 10), parseInt(p[1], 10) - 1, parseInt(p[0], 10));
            if (!isNaN(d1.getTime())) return d1;
        }

        // YYYY-MM-DD
        if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
            let p2 = s.split("-");
            let d2 = new Date(parseInt(p2[0], 10), parseInt(p2[1], 10) - 1, parseInt(p2[2], 10));
            if (!isNaN(d2.getTime())) return d2;
        }
    } catch (e) {}
    return null;
}

function getDaysSinceStudentJoin_(s) {
    try {
        let d = parseStudentDate_(s && s.dateJoined ? s.dateJoined : "");
        if (!d) return 0;
        d.setHours(0, 0, 0, 0);
        let t = new Date();
        t.setHours(0, 0, 0, 0);
        let diff = Math.floor((t - d) / (1000 * 60 * 60 * 24));
        return diff < 0 ? 0 : diff;
    } catch (e) {
        return 0;
    }
}

function getQuizDayByJoinDate_(s) {
    // Day 1 on joined date, Day 5 on 5th day, etc.
    let days = getDaysSinceStudentJoin_(s);
    return Math.max(1, days + 1);
}

function ensureAuditSheet() {
    let ss = getDB();
    let sh = ss.getSheetByName("Audit_Log");
    if (sh) return sh;
    sh = ss.insertSheet("Audit_Log");
    sh.appendRow(["Timestamp", "Time", "Actor", "Action", "Entity", "EntityId", "Before", "After", "Meta"]);
    return sh;
}

function logAuditEvent(action, entity, entityId, beforeStr, afterStr, metaObj) {
    try {
        let sh = ensureAuditSheet();
        let ts = new Date();
        let d = getISTDate();
        let t = Utilities.formatDate(ts, "GMT+5:30", "HH:mm:ss");
        let actor = "";
        try { actor = Session.getActiveUser().getEmail() || ""; } catch (e) { actor = ""; }
        let meta = "";
        try { meta = metaObj ? JSON.stringify(metaObj) : ""; } catch (e) { meta = ""; }
        sh.appendRow([d, t, actor, String(action || ""), String(entity || ""), String(entityId || ""), String(beforeStr || ""), String(afterStr || ""), meta]);
        flushNoop_();
    } catch (e) {}
}

function ensureFilingSheet() {
    let ss = getDB();
    let sh = ss.getSheetByName("Filing_Index");
    if (sh) return sh;
    sh = ss.insertSheet("Filing_Index");
    sh.appendRow(["Timestamp", "Time", "Month", "ReportType", "Url", "Meta", "Actor"]);
    return sh;
}

function logFilingEntry(monthKey, reportType, url, metaObj) {
    try {
        let sh = ensureFilingSheet();
        let ts = new Date();
        let d = getISTDate();
        let t = Utilities.formatDate(ts, "GMT+5:30", "HH:mm:ss");
        let actor = "";
        try { actor = Session.getActiveUser().getEmail() || ""; } catch (e) { actor = ""; }
        let meta = "";
        try { meta = metaObj ? JSON.stringify(metaObj) : ""; } catch (e) { meta = ""; }
        sh.appendRow([d, t, String(monthKey || ""), String(reportType || ""), String(url || ""), meta, actor]);
        flushNoop_();
    } catch (e) {}
}

function generatePdfReportFromHtml(html, fileName, reportType, monthKey, metaObj) {
    try {
        let safeName = String(fileName || "Nanban_Report.pdf").trim();
        if (!safeName.toLowerCase().endsWith(".pdf")) safeName += ".pdf";
        let out = HtmlService.createHtmlOutput(html || "<html><body>Empty</body></html>");
        let blob = out.getBlob().setName(safeName).getAs("application/pdf");
        let folders = DriveApp.getFoldersByName("Nanban_Reports");
        let folder = folders.hasNext() ? folders.next() : DriveApp.createFolder("Nanban_Reports");
        let file = folder.createFile(blob);
        file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
        let fileId = file.getId();
        let url = getDrivePdfDownloadUrl(fileId);
        try { logFilingEntry(monthKey || "", reportType || "Report", url, metaObj || {}); } catch (e) {}
        try { logAuditEvent('GENERATE_PDF', 'Report', fileId, "", url, { reportType: reportType || "", month: monthKey || "" }); } catch (e) {}
        return { status: "success", id: fileId, url: url };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function monthKeyFromISTDate(ddmmyyyy) {
    try {
        let p = String(ddmmyyyy || '').split('/');
        if (p.length !== 3) return "";
        let dd = parseInt(p[0], 10);
        let mm = parseInt(p[1], 10);
        let yy = parseInt(p[2], 10);
        if (!dd || !mm || !yy) return "";
        return `${yy}-${String(mm).padStart(2, '0')}`;
    } catch (e) {
        return "";
    }
}

function generateTestPassCertificatePdf(studentObj, trainerName, issuedDate) {
    try {
        let s = studentObj || {};
        let name = String(s.name || "Student").trim();
        let service = String(s.service || "-").trim();
        let issue = String(issuedDate || getISTDate()).trim();
        let testDateLabel = "-";
        try {
            if (s.testDate) testDateLabel = formatYMDToDDMMYYYY(s.testDate);
        } catch (e) {}
        if (!testDateLabel || testDateLabel === "-") testDateLabel = issue;

        let certNo = `NDS-${String(s.id || '').slice(-6)}-${issue.replace(/\D/g, '')}`;
        let mk = monthKeyFromISTDate(issue);

        // A4 Paper Size approximately 800x1130 for internal rendering
        let html = `
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <style>
                    @import url('https://fonts.googleapis.com/css2?family=Kavivanar&family=Mukta+Malar:wght@400;700&family=Tiro+Tamil:ital@0;1&display=swap');
                    body { 
                        margin: 0; padding: 0; background: #ffffff; 
                        font-family: 'Mukta Malar', Arial, sans-serif;
                    }
                    .cert-page {
                        width: 800px;
                        height: 1120px; 
                        background-color: #0f172a;
                        background-image: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #0f172a 100%);
                        position: relative;
                        padding: 40px;
                        box-sizing: border-box;
                        color: #ffffff;
                        overflow: hidden;
                        margin: 0 auto;
                    }
                    .cert-border {
                        position: absolute; top: 25px; bottom: 25px; left: 25px; right: 25px;
                        border: 3px solid #fbbf24; border-radius: 12px; z-index: 1;
                    }
                    .cert-border-inner {
                        position: absolute; top: 35px; bottom: 35px; left: 35px; right: 35px;
                        border: 1px solid rgba(251, 191, 36, 0.4); border-radius: 8px; z-index: 1;
                    }
                    .watermark {
                        position: absolute; top: 40%; left: 50%; transform: translate(-50%, -50%);
                        font-size: 140px; color: rgba(255,255,255,0.03); font-weight: 900; z-index: 0; white-space: nowrap;
                    }
                    .content { position: relative; z-index: 10; text-align: center; width: 100%; height: 100%; }
                    
                    .header-logo {
                        font-family: 'Kavivanar', cursive; font-size: 50px; font-weight: 900; color: #fbbf24;
                        margin-top: 50px; text-shadow: 0 4px 10px rgba(0,0,0,0.5);
                    }
                    .header-sub { font-size: 18px; color: #94a3b8; letter-spacing: 2px; margin-top: 5px; margin-bottom: 60px; }
                    
                    .cert-title {
                        font-family: 'Tiro Tamil', serif; font-size: 55px; color: #ffffff; margin-bottom: 25px;
                    }
                    .presentation { font-size: 20px; color: #cbd5e1; margin-bottom: 50px; letter-spacing: 1px; }
                    
                    .student-name {
                        font-family: 'Kavivanar', cursive; font-size: 70px; color: #fbbf24; font-weight: 900;
                        margin-bottom: 40px; text-shadow: 0 4px 15px rgba(251, 191, 36, 0.3);
                        border-bottom: 2px solid rgba(251, 191, 36, 0.4); padding-bottom: 10px; display: inline-block; min-width: 70%;
                    }
                    
                    .achievement-text { font-size: 22px; color: #e2e8f0; line-height: 1.6; max-width: 85%; margin: 0 auto 50px; }
                    
                    .grid-box {
                        margin: 0 auto 50px auto; width: 85%;
                    }
                    table.details {
                        width: 100%; text-align: left; background: rgba(255, 255, 255, 0.05); padding: 25px; border-radius: 16px; border: 1px solid rgba(255, 255, 255, 0.1);
                    }
                    table.details td { padding: 15px; vertical-align: top; }
                    .dl { font-size: 14px; color: #94a3b8; text-transform: uppercase; font-weight: 700; margin-bottom: 5px; letter-spacing:1px; }
                    .dv { font-size: 22px; color: #ffffff; font-weight: 900; }
                    
                    .seal-sig-wrapper {
                        width: 100%; margin-top: 50px;
                    }
                    table.sig-table { width: 90%; margin: 0 auto; text-align: center; }
                    table.sig-table td { vertical-align: bottom; width: 33%; }
                    
                    .sig-line { width: 180px; height: 2px; background: #cbd5e1; margin: 0 auto 10px auto; position: relative; }
                    .sig-title { font-size: 14px; color: #94a3b8; font-weight: 700; letter-spacing: 1px; }
                    .badge-seal {
                        width: 130px; height: 130px; border-radius: 50%; border: 3px solid #fbbf24; margin: 0 auto;
                        display: block; background: rgba(251, 191, 36, 0.1); padding: 5px;
                    }
                    .badge-inner {
                        width: 116px; height: 116px; border-radius: 50%; border: 2px dashed #fbbf24;
                        margin: 0 auto; display: block; color: #fbbf24;
                    }
                    .b-icon { font-size: 40px; margin-top: 15px; }
                    .b-txt { font-size: 12px; font-weight: 900; line-height: 1.2; text-transform: uppercase; margin-top: 5px; letter-spacing: 1px; }
                    
                    .cert-footer { font-size: 14px; color: #475569; margin-top: 30px; font-weight: 600; letter-spacing: 2px; }
                </style>
            </head>
            <body>
                <div class="cert-page">
                    <div class="watermark">NANBAN</div>
                    <div class="cert-border"></div>
                    <div class="cert-border-inner"></div>
                    <div class="content">
                        <div class="header-logo">நண்பன் டிரைவிங் ஸ்கூல்</div>
                        <div class="header-sub">அரசு அங்கீகாரம் பெற்ற ஓட்டுநர் பயிற்சிப் பள்ளி</div>
                        
                        <div class="cert-title">ஓட்டுநர் வெற்றிச் சான்றிதழ்</div>
                        <div class="presentation">இந்தச் சான்றிதழ் பெருமையுடன் வழங்கப்படுவது</div>
                        
                        <div class="student-name">${name}</div>
                        
                        <div class="achievement-text">
                            வட்டாரப் போக்குவரத்து அலுவலக (RTO) ஓட்டுநர் தேர்வில் வெற்றிகரமாகத் தேர்ச்சி பெற்றமைக்காகவும், பாதுகாப்பான பயண விதிகளைச் சிறப்பாகப் பயின்றமைக்காகவும் இச்சான்றிதழ் வழங்கப்படுகிறது. 🚦
                        </div>
                        
                        <div class="grid-box">
                            <table class="details">
                                <tr>
                                    <td><div class="dl">பயிற்சி பெற்ற வாகனம்</div><div class="dv">${service}</div></td>
                                    <td><div class="dl">தேர்வு தேதி</div><div class="dv">${testDateLabel}</div></td>
                                </tr>
                                <tr>
                                    <td><div class="dl">பயிற்றுநர்</div><div class="dv">${String(trainerName || "-")}</div></td>
                                    <td><div class="dl">சான்றிதழ் எண்</div><div class="dv">${certNo}</div></td>
                                </tr>
                            </table>
                        </div>
                        
                        <div class="seal-sig-wrapper">
                            <table class="sig-table">
                                <tr>
                                    <td>
                                        <div style="font-family:cursive; font-size:30px; color:#ffffff; margin-bottom:-5px; transform:rotate(-5deg);">${String(trainerName || "Trainer")}</div>
                                        <div class="sig-line"></div>
                                        <div class="sig-title">பயிற்றுநர் கையொப்பம்</div>
                                    </td>
                                    <td>
                                        <div class="badge-seal">
                                            <div class="badge-inner">
                                                <div class="b-icon">🎖️</div>
                                                <div class="b-txt">அங்கீகரிக்கப்பட்ட<br>ஓட்டுநர்</div>
                                            </div>
                                        </div>
                                    </td>
                                    <td>
                                        <div style="font-family:cursive; font-size:30px; color:#fbbf24; margin-bottom:-5px; transform:rotate(-5deg);">Nanban</div>
                                        <div class="sig-line"></div>
                                        <div class="sig-title">அங்கீகரிக்கப்பட்ட கையொப்பம்</div>
                                    </td>
                                </tr>
                            </table>
                        </div>
                        
                        <div class="cert-footer">Verify authenticity at nanbandrivingschool.com</div>
                    </div>
                </div>
            </body>
            </html>
        `;

        // Generate as "Nanban_Success_Report" instead of "Test_Certificate" to prevent Google Wallet hijack
        return generatePdfReportFromHtml(html, `Nanban_Success_Report_${name}_${issue.replace(/\//g,'')}.pdf`, 'TestCertificate', mk || '', { studentId: s.id || "", name: name, service: service, trainer: trainerName || "", testDate: testDateLabel });
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function getRoleByName(name) {
    try {
        let n = String(name || "").trim();
        if (!n) return "";
        let users = getAppUsers();
        for (let i = 0; i < users.length; i++) {
            if (String(users[i].name || "").trim() === n) return String(users[i].role || "");
        }
    } catch (e) {}
    return "";
}

function isPrivilegedName(name) {
    let r = String(getRoleByName(name) || "");
    return (r === "Admin" || r === "Partner");
}

function saveCashOpeningAction(monthKey, ranjithAmt, nandhaAmt, officeAmt, loggedBy) {
    try {
        let mk = String(monthKey || "").trim();
        if (!mk) return { status: 'error', message: 'Month required' };
        if (loggedBy && !isPrivilegedName(loggedBy)) return { status: 'error', message: 'Not allowed' };
        let settings = getAppSettings();
        if (!settings || !settings.appSettings) settings = { appSettings: {} };
        if (!settings.appSettings.cashOpeningByMonth) settings.appSettings.cashOpeningByMonth = {};
        let before = "";
        try {
            before = JSON.stringify(settings.appSettings.cashOpeningByMonth[mk] || {});
        } catch (e) { before = ""; }
        settings.appSettings.cashOpeningByMonth[mk] = {
            ranjith: parseInt(ranjithAmt) || 0,
            nandha: parseInt(nandhaAmt) || 0,
            office: parseInt(officeAmt) || 0,
            by: String(loggedBy || "")
        };
        saveAppSettings('appSettings', settings.appSettings);
        try { logAuditEvent('SAVE_OPENING', 'OpeningBalance', mk, before, JSON.stringify(settings.appSettings.cashOpeningByMonth[mk]), { month: mk }); } catch (e) {}
        return { status: 'success' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function getAuditLogAction(fromDt, toDt, limit) {
    try {
        let sh = getDB().getSheetByName("Audit_Log");
        if (!sh) return { status: 'success', items: [] };
        let data = sh.getDataRange().getDisplayValues();
        let max = parseInt(limit) || 200;
        let f = String(fromDt || "").trim();
        let t = String(toDt || "").trim();

        function parseDDMMYYYY(s) {
            let p = String(s || '').split('/');
            if (p.length !== 3) return null;
            let dd = parseInt(p[0], 10);
            let mm = parseInt(p[1], 10) - 1;
            let yy = parseInt(p[2], 10);
            let d = new Date(yy, mm, dd);
            return isNaN(d.getTime()) ? null : d;
        }

        function inRange(dateStr) {
            if (!f || !t) return true;
            let d = parseDDMMYYYY(dateStr);
            if (!d) return false;
            return !(d < new Date(f) || d > new Date(t));
        }

        let items = [];
        for (let i = data.length - 1; i >= 1; i--) {
            let row = data[i];
            let d = row[0] || "";
            if (!inRange(d)) continue;
            items.push({
                date: row[0] || "",
                time: row[1] || "",
                actor: row[2] || "",
                action: row[3] || "",
                entity: row[4] || "",
                entityId: row[5] || "",
                before: row[6] || "",
                after: row[7] || "",
                meta: row[8] || ""
            });
            if (items.length >= max) break;
        }
        return { status: 'success', items: items };
    } catch (e) {
        return { status: 'error', message: e.toString(), items: [] };
    }
}

function getFilingEntriesAction(monthKey) {
    try {
        let mk = String(monthKey || "").trim();
        let sh = getDB().getSheetByName("Filing_Index");
        if (!sh) return { status: 'success', items: [] };
        let data = sh.getDataRange().getDisplayValues();
        let items = [];
        for (let i = 1; i < data.length; i++) {
            let m = String(data[i][2] || "").trim();
            if (mk && m !== mk) continue;
            items.push({
                date: data[i][0] || "",
                time: data[i][1] || "",
                month: data[i][2] || "",
                type: data[i][3] || "",
                url: data[i][4] || "",
                meta: data[i][5] || "",
                actor: data[i][6] || ""
            });
        }
        items.reverse();
        return { status: 'success', items: items };
    } catch (e) {
        return { status: 'error', message: e.toString(), items: [] };
    }
}

function generateFilingIndexPdfAction(monthKey) {
    try {
        let mk = String(monthKey || "").trim();
        let res = getFilingEntriesAction(mk);
        if (!res || res.status !== 'success') return { status: 'error', message: 'Unable to load filing index' };
        let items = res.items || [];
        let rows = "";
        for (let i = 0; i < items.length; i++) {
            let it = items[i];
            rows += `<tr>
                <td style="padding:8px 10px; border-bottom:1px solid #e2e8f0; font-weight:900;">${i + 1}</td>
                <td style="padding:8px 10px; border-bottom:1px solid #e2e8f0; font-weight:800;">${it.date}</td>
                <td style="padding:8px 10px; border-bottom:1px solid #e2e8f0; font-weight:800;">${it.time}</td>
                <td style="padding:8px 10px; border-bottom:1px solid #e2e8f0; font-weight:1000;">${it.type}</td>
                <td style="padding:8px 10px; border-bottom:1px solid #e2e8f0; word-break:break-word;">${it.url ? `<a href="${it.url}" target="_blank">${it.url}</a>` : '-'}</td>
            </tr>`;
        }
        let html = `
            <html><head><meta charset="utf-8"><style>
                body{font-family:Arial, sans-serif; color:#0f172a; padding:18px;}
                h1{margin:0; font-size:18px;}
                .sub{color:#64748b; font-size:11px; font-weight:700; margin-top:4px;}
                table{width:100%; border-collapse:collapse; font-size:11px; table-layout:fixed;}
                th{background:#f8fafc; text-align:left; padding:10px; border-bottom:1px solid #e2e8f0; overflow-wrap:anywhere;}
                td{overflow-wrap:anywhere;}
            </style></head>
            <body>
                <h1>Nanban Filing Index</h1>
                <div class="sub">Month: ${mk || '-'} | Generated: ${getISTDate()}</div>
                <table>
                    <thead><tr><th style="width:35px;">#</th><th style="width:80px;">Date</th><th style="width:60px;">Time</th><th style="width:140px;">Report</th><th>URL</th></tr></thead>
                    <tbody>${rows || `<tr><td colspan="5" style="padding:14px; text-align:center; color:gray; font-weight:900;">No entries</td></tr>`}</tbody>
                </table>
            </body></html>
        `;
        return generatePdfReportFromHtml(html, `Filing_Index_${mk || 'All'}.pdf`, 'FilingIndex', mk || '', { count: items.length });
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function generateFullAuditPdfAction(fromDt, toDt, loggedBy) {
    try {
        let f = String(fromDt || "").trim();
        let t = String(toDt || "").trim();
        if (!f || !t) return { status: 'error', message: 'Date range required' };
        if (loggedBy && !isPrivilegedName(loggedBy)) return { status: 'error', message: 'Not allowed' };
        let mk = f.length >= 7 ? f.slice(0, 7) : "";

        function parseDDMMYYYY(s) {
            let p = String(s || '').split('/');
            if (p.length !== 3) return null;
            let dd = parseInt(p[0], 10);
            let mm = parseInt(p[1], 10) - 1;
            let yy = parseInt(p[2], 10);
            let d = new Date(yy, mm, dd);
            return isNaN(d.getTime()) ? null : d;
        }

        function inRangeDDMM(ddmmyyyy) {
            let d = parseDDMMYYYY(ddmmyyyy);
            if (!d) return false;
            if (f && d < new Date(f)) return false;
            if (t && d > new Date(t)) return false;
            return true;
        }

        function ymdInRange(ymd) {
            if (!ymd) return false;
            let d = new Date(ymd);
            if (isNaN(d.getTime())) return false;
            if (f && d < new Date(f)) return false;
            if (t && d > new Date(t)) return false;
            return true;
        }

        function personFromNote(text, fallback) {
            let tx = String(text || '').toLowerCase();
            if (tx.includes('நந்தகுமார்')) return 'நந்தகுமார்';
            if (tx.includes('ஆபீஸ்') || tx.includes('office')) return 'Office';
            if (tx.includes('ரஞ்சித்')) return 'ரஞ்சித்';
            return fallback || 'ரஞ்சித்';
        }

        let db = getDatabaseData();
        let ch = getChitData();
        let students = (db && db.students) ? db.students : [];
        let expenses = (db && db.expenses) ? db.expenses : [];
        let chit = (ch && ch.status === 'success' && ch.data) ? ch.data : { auctions: [] };

        let admissions = [];
        let payRows = [];
        let expRows = [];
        let chitRows = [];
        let testRows = [];
        let totalIn = 0;
        let totalOut = 0;

        students.filter(s => s && s.status !== 'Deleted').forEach(function(s) {
            if (s.type !== 'Enquiry' && inRangeDDMM(s.dateJoined)) {
                let adv = parseInt(s.advance) || 0;
                let tot = parseInt(s.totalFee) || 0;
                let bal = tot - adv;
                admissions.push({ date: s.dateJoined, name: s.name || '-', phone: s.phone || '-', service: s.service || '-', receiver: s.receiver || '-', total: tot, adv: adv, bal: bal < 0 ? 0 : bal });
            }

            (Array.isArray(s.paymentHistory) ? s.paymentHistory : []).forEach(function(p) {
                if (!inRangeDDMM(p.date)) return;
                let amt = parseInt(p.amount) || 0;
                if (amt <= 0) return;
                let who = personFromNote(p.note, 'ரஞ்சித்');
                totalIn += amt;
                payRows.push({ date: p.date, who: who, name: s.name || '-', service: s.service || '-', note: p.note || '-', amt: amt });
            });

            if (s.testDate && ymdInRange(s.testDate)) {
                let st = String(s.testStatus || '').trim();
                if (st) {
                    testRows.push({ date: formatYMDToDDMMYYYY(s.testDate), name: s.name || '-', phone: s.phone || '-', service: s.service || '-', status: st });
                }
            }
        });

        expenses.forEach(function(e) {
            if (!e || !inRangeDDMM(e.date)) return;
            let amt = parseInt(e.amt) || 0;
            if (amt <= 0) return;
            let cat = String(e.cat || '-');
            let isIncome = cat.includes("வரவு") || cat.includes("(In)") || cat.includes("Spot Collection");
            let isTransfer = cat.includes("பரிமாற்றம்");
            if (isIncome && !isTransfer) totalIn += amt;
            if (!isIncome && !isTransfer && !cat.includes("Spot Pending")) totalOut += amt;
            expRows.push({ date: e.date, who: e.spender || '-', cat: cat, desc: e.desc || '-', amt: amt, kind: isIncome ? 'IN' : 'OUT', isTransfer: isTransfer });
        });

        (Array.isArray(chit.auctions) ? chit.auctions : []).forEach(function(a) {
            if (!a || !inRangeDDMM(a.date)) return;
            let comm = parseInt(a.commission) || 0;
            let ex = parseInt(a.expenses) || 0;
            let net = parseInt(a.netProfit) || (comm - ex);
            chitRows.push({ date: a.date || '-', group: a.group || '-', month: a.month || '-', winner: a.winner || '-', perHead: parseInt(a.perHead) || 0, comm: comm, exp: ex, net: net });
        });

        admissions.sort((a,b) => (parseDDMMYYYY(a.date) || 0) - (parseDDMMYYYY(b.date) || 0));
        payRows.sort((a,b) => (parseDDMMYYYY(a.date) || 0) - (parseDDMMYYYY(b.date) || 0));
        expRows.sort((a,b) => (parseDDMMYYYY(a.date) || 0) - (parseDDMMYYYY(b.date) || 0));
        chitRows.sort((a,b) => (parseDDMMYYYY(a.date) || 0) - (parseDDMMYYYY(b.date) || 0));
        testRows.sort((a,b) => (parseDDMMYYYY(a.date) || 0) - (parseDDMMYYYY(b.date) || 0));

        let settings = getAppSettings();
        let ob = 0, on = 0, oo = 0;
        try {
            let obm = settings && settings.appSettings && settings.appSettings.cashOpeningByMonth ? settings.appSettings.cashOpeningByMonth : {};
            let row = mk && obm[mk] ? obm[mk] : null;
            if (row) { ob = parseInt(row.ranjith) || 0; on = parseInt(row.nandha) || 0; oo = parseInt(row.office) || 0; }
        } catch (e) {}

        function money(n) { return `₹${parseInt(n) || 0}`; }

        function table(title, cols, bodyHtml) {
            return `
                <div style="border:1px solid #e2e8f0; border-radius:14px; overflow:hidden; margin-top:12px;">
                    <div style="background:#0f172a; color:white; padding:10px 12px; font-weight:900; font-size:12px;">${title}</div>
                    <table style="width:100%; border-collapse:collapse; font-size:10px; table-layout:fixed;">
                        <thead><tr style="background:#f8fafc; text-align:left;">${cols.map(c => `<th style="padding:8px 10px; border-bottom:1px solid #e2e8f0; overflow-wrap:anywhere;">${c}</th>`).join('')}</tr></thead>
                        <tbody>${bodyHtml || `<tr><td colspan="${cols.length}" style="padding:12px; text-align:center; color:gray; font-weight:900;">No entries</td></tr>`}</tbody>
                    </table>
                </div>
            `;
        }

        let admBody = admissions.map((r,i) => `<tr>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${i+1}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.date}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.name}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.phone}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.service}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.receiver}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:900;">${money(r.total)}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:900; color:#16a34a;">${money(r.adv)}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:900; color:#b91c1c;">${money(r.bal)}</td>
        </tr>`).join('');

        let payBody = payRows.map((r,i) => `<tr>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${i+1}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.date}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.who}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.name}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.service}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; color:#475569;">${r.note}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:900; color:#16a34a;">${money(r.amt)}</td>
        </tr>`).join('');

        let expBody = expRows.map((r,i) => {
            let col = r.isTransfer ? '#0f172a' : (r.kind === 'IN' ? '#16a34a' : '#b91c1c');
            let tag = r.isTransfer ? 'TRANSFER' : r.kind;
            return `<tr>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${i+1}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.date}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900; color:${col};">${tag}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.who}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.cat}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; color:#475569;">${r.desc}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:900; color:${col};">${money(r.amt)}</td>
            </tr>`;
        }).join('');

        let chitBody = chitRows.map((r,i) => `<tr>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${i+1}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.date}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.group}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">M-${r.month}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.winner}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:900;">${money(r.perHead)}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:900; color:#16a34a;">${money(r.comm)}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:900; color:#b91c1c;">${money(r.exp)}</td>
            <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:900; color:#1d4ed8;">${money(r.net)}</td>
        </tr>`).join('');

        let testBody = testRows.map((r,i) => {
            let col = (String(r.status).toLowerCase().includes('pass')) ? '#16a34a' : '#b91c1c';
            return `<tr>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${i+1}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.date}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.name}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.phone}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0;">${r.service}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900; color:${col};">${r.status}</td>
            </tr>`;
        }).join('');

        let net = totalIn - totalOut;
        let html = `
            <html><head><meta charset="utf-8"><style>
                body{font-family:Arial, sans-serif; color:#0f172a; background:white; padding:18px;}
                .hdr{display:flex; justify-content:space-between; align-items:flex-start; border-bottom:3px solid #0f172a; padding-bottom:10px; margin-bottom:14px;}
                .t1{font-size:18px; font-weight:900;}
                .t2{font-size:11px; color:#64748b; font-weight:900; margin-top:4px;}
                .grid{display:grid; grid-template-columns: 1fr 1fr 1fr; gap:10px; margin:10px 0 12px;}
                .box{border-radius:14px; padding:12px; border:1px solid #e2e8f0;}
                .lab{font-size:10px; font-weight:900; color:#64748b;}
                .val{font-size:18px; font-weight:900;}
                table{table-layout:fixed; width:100%;}
                td,th{overflow-wrap:anywhere; word-break:break-word;}
            </style></head>
            <body>
                <div class="hdr">
                    <div>
                        <div class="t1">நண்பன் டிரைவிங் ஸ்கூல்</div>
                        <div class="t2">A-Z FULL AUDIT REPORT • ${f} முதல் ${t} வரை</div>
                    </div>
                    <div class="t2" style="text-align:right;">Generated: ${getISTDate()}<br>${loggedBy ? `By: ${loggedBy}` : ''}</div>
                </div>

                <div class="grid">
                    <div class="box" style="background:#f0fdf4; border-color:#bbf7d0;">
                        <div class="lab">TOTAL IN</div><div class="val" style="color:#16a34a;">${money(totalIn)}</div>
                    </div>
                    <div class="box" style="background:#fef2f2; border-color:#fecaca;">
                        <div class="lab">TOTAL OUT</div><div class="val" style="color:#b91c1c;">${money(totalOut)}</div>
                    </div>
                    <div class="box" style="background:#eff6ff; border-color:#bfdbfe;">
                        <div class="lab">NET</div><div class="val" style="color:#1d4ed8;">${money(net)}</div>
                    </div>
                </div>

                <div class="grid" style="grid-template-columns: 1fr 1fr 1fr;">
                    <div class="box">
                        <div class="lab">Opening (ரஞ்சித்)</div><div class="val">${money(ob)}</div>
                    </div>
                    <div class="box">
                        <div class="lab">Opening (நந்தகுமார்)</div><div class="val">${money(on)}</div>
                    </div>
                    <div class="box">
                        <div class="lab">Opening (Office)</div><div class="val">${money(oo)}</div>
                    </div>
                </div>

                ${table('1) Admissions', ['#','Date','Name','Phone','Service','Receiver','Total','Advance','Balance'], admBody)}
                ${table('2) Collections (Payment History)', ['#','Date','Account','Student','Service','Note','Amount'], payBody)}
                ${table('3) Income/Expense Entries', ['#','Date','Type','Account','Category','Description','Amount'], expBody)}
                ${table('4) Chit Auctions', ['#','Date','Group','Month','Winner','Per Head','Commission','Expense','Net'], chitBody)}
                ${table('5) Test Results', ['#','Date','Student','Phone','Service','Result'], testBody)}
            </body></html>
        `;
        return generatePdfReportFromHtml(html, `A-Z_Full_Report_${mk || 'Period'}.pdf`, 'FullAudit', mk || '', { from: f, to: t, net: net });
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function generateMonthlyCashbookPdfAction(fromDt, toDt, loggedBy) {
    try {
        let f = String(fromDt || "").trim();
        let t = String(toDt || "").trim();
        if (!f || !t) return { status: 'error', message: 'Date range required' };
        if (loggedBy && !isPrivilegedName(loggedBy)) return { status: 'error', message: 'Not allowed' };
        let mk = f.length >= 7 ? f.slice(0, 7) : "";

        function parseDDMMYYYY(s) {
            let p = String(s || '').split('/');
            if (p.length !== 3) return null;
            let dd = parseInt(p[0], 10);
            let mm = parseInt(p[1], 10) - 1;
            let yy = parseInt(p[2], 10);
            let d = new Date(yy, mm, dd);
            return isNaN(d.getTime()) ? null : d;
        }

        function inRangeDDMM(ddmmyyyy) {
            let d = parseDDMMYYYY(ddmmyyyy);
            if (!d) return false;
            if (f && d < new Date(f)) return false;
            if (t && d > new Date(t)) return false;
            return true;
        }

        function personFromNote(text, fallback) {
            let tx = String(text || '').toLowerCase();
            if (tx.includes('நந்தகுமார்')) return 'நந்தகுமார்';
            if (tx.includes('ஆபீஸ்') || tx.includes('office')) return 'Office';
            if (tx.includes('ரஞ்சித்')) return 'ரஞ்சித்';
            return fallback || 'ரஞ்சித்';
        }

        let db = getDatabaseData();
        let students = (db && db.students) ? db.students : [];
        let expenses = (db && db.expenses) ? db.expenses : [];

        let rows = [];
        let sumIn = 0;
        let sumOut = 0;

        students.filter(s => s && s.status !== 'Deleted').forEach(function(s) {
            (Array.isArray(s.paymentHistory) ? s.paymentHistory : []).forEach(function(p) {
                if (!inRangeDDMM(p.date)) return;
                let amt = parseInt(p.amount) || 0;
                if (amt <= 0) return;
                let who = personFromNote(p.note, 'ரஞ்சித்');
                sumIn += amt;
                rows.push({ date: p.date, kind: 'IN', account: who, cat: `🎓 Admission - ${s.service || '-'}`, desc: `${s.name || '-'} | ${p.note || '-'}`, amt: amt });
            });
        });

        expenses.forEach(function(e) {
            if (!e || !inRangeDDMM(e.date)) return;
            let amt = parseInt(e.amt) || 0;
            if (amt <= 0) return;
            let cat = String(e.cat || '-');
            let isIncome = cat.includes("வரவு") || cat.includes("(In)");
            if (isIncome) sumIn += amt;
            else if (!cat.includes("Spot Pending")) sumOut += amt;
            rows.push({ date: e.date, kind: isIncome ? 'IN' : 'OUT', account: e.spender || '-', cat: cat, desc: e.desc || '-', amt: amt });
        });

        rows.sort(function(a, b) {
            let ad = parseDDMMYYYY(a.date);
            let bd = parseDDMMYYYY(b.date);
            if (!ad || !bd) return 0;
            return ad - bd;
        });

        function money(n) { return `₹${parseInt(n) || 0}`; }
        let tr = rows.map(function(r, idx) {
            let col = (r.kind === 'IN') ? '#16a34a' : '#b91c1c';
            return `<tr>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${idx + 1}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:800;">${r.date}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:1100; color:${col};">${r.kind}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.account}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; font-weight:900;">${r.cat}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; color:#475569;">${r.desc}</td>
                <td style="padding:7px 8px; border-bottom:1px solid #e2e8f0; text-align:right; font-weight:1100; color:${col};">${money(r.amt)}</td>
            </tr>`;
        }).join('');

        let html = `
            <html><head><meta charset="utf-8"><style>
                body{font-family:Arial, sans-serif; color:#0f172a; background:white; padding:18px;}
                .hdr{display:flex; justify-content:space-between; align-items:flex-start; border-bottom:3px solid #0f172a; padding-bottom:10px; margin-bottom:14px;}
                .t1{font-size:18px; font-weight:900;}
                .t2{font-size:11px; color:#64748b; font-weight:900; margin-top:4px;}
                .grid{display:grid; grid-template-columns: 1fr 1fr 1fr; gap:10px; margin:10px 0 12px;}
                .box{border-radius:14px; padding:12px; border:1px solid #e2e8f0;}
                .lab{font-size:10px; font-weight:900; color:#64748b;}
                .val{font-size:18px; font-weight:900;}
                table{width:100%; border-collapse:collapse; font-size:10px; table-layout:fixed;}
                td,th{overflow-wrap:anywhere; word-break:break-word;}
                th{background:#f8fafc; text-align:left; padding:8px 10px; border-bottom:1px solid #e2e8f0;}
            </style></head>
            <body>
                <div class="hdr">
                    <div>
                        <div class="t1">நண்பன் டிரைவிங் ஸ்கூல்</div>
                        <div class="t2">MONTHLY CASHBOOK • ${f} முதல் ${t} வரை</div>
                    </div>
                    <div class="t2" style="text-align:right;">Generated: ${getISTDate()}<br>${loggedBy ? `By: ${loggedBy}` : ''}</div>
                </div>

                <div class="grid">
                    <div class="box" style="background:#f0fdf4; border-color:#bbf7d0;">
                        <div class="lab">TOTAL IN</div><div class="val" style="color:#16a34a;">${money(sumIn)}</div>
                    </div>
                    <div class="box" style="background:#fef2f2; border-color:#fecaca;">
                        <div class="lab">TOTAL OUT</div><div class="val" style="color:#b91c1c;">${money(sumOut)}</div>
                    </div>
                    <div class="box" style="background:#eff6ff; border-color:#bfdbfe;">
                        <div class="lab">NET</div><div class="val" style="color:#1d4ed8;">${money(sumIn - sumOut)}</div>
                    </div>
                </div>

                <div style="border:1px solid #e2e8f0; border-radius:14px; overflow:hidden;">
                    <div style="background:#0f172a; color:white; padding:10px 12px; font-weight:900; font-size:12px;">Transactions</div>
                    <table>
                        <thead><tr>
                            <th style="width:35px;">#</th>
                            <th style="width:70px;">Date</th>
                            <th style="width:50px;">Type</th>
                            <th style="width:90px;">Account</th>
                            <th style="width:140px;">Category</th>
                            <th>Description</th>
                            <th style="width:80px; text-align:right;">Amount</th>
                        </tr></thead>
                        <tbody>${tr || `<tr><td colspan="7" style="padding:12px; text-align:center; color:gray; font-weight:900;">No entries</td></tr>`}</tbody>
                    </table>
                </div>
            </body></html>
        `;
        return generatePdfReportFromHtml(html, `Cashbook_${mk || 'Period'}.pdf`, 'Cashbook', mk || '', { from: f, to: t, net: sumIn - sumOut });
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function generateMonthlyPdfPackAction(fromDt, toDt, loggedBy) {
    try {
        let f = String(fromDt || "").trim();
        let t = String(toDt || "").trim();
        if (!f || !t) return { status: 'error', message: 'Date range required' };
        if (loggedBy && !isPrivilegedName(loggedBy)) return { status: 'error', message: 'Not allowed' };
        let full = generateFullAuditPdfAction(f, t, loggedBy);
        let cash = generateMonthlyCashbookPdfAction(f, t, loggedBy);
        let out = { status: 'success', full: full, cashbook: cash };
        return out;
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

// ------------------------------------------------------------------------------
// 5. WHATSAPP ENGINE
// ------------------------------------------------------------------------------

function isMessagingEnabled() {
    if (typeof MASTER_MSG_ON !== 'undefined' && !MASTER_MSG_ON) return false;
    try {
        let settings = getAppSettings();
        if (settings && settings.appSettings && settings.appSettings.messagesEnabled === false) return false;
    } catch(e) {}
    return true;
}

/**
 * 🔘 வாட்ஸ்அப் பட்டன் மெசேஜ் (Interactive Buttons)
 */
function sendWhatsAppInteractiveButtons(to, bodyText, buttons) {
    try {
        let url = "https://graph.facebook.com/v20.0/" + WA_PHONE_ID + "/messages";
        let payload = {
            messaging_product: "whatsapp",
            recipient_type: "individual",
            to: cleanPhoneNumber(to),
            type: "interactive",
            interactive: {
                type: "button",
                body: { text: bodyText },
                action: {
                    buttons: buttons.slice(0, 3).map(b => ({
                        type: "reply",
                        reply: { id: b.id, title: b.title }
                    }))
                }
            }
        };

        let options = {
            method: "post",
            headers: {
                "Authorization": "Bearer " + getCleanToken(),
                "Content-Type": "application/json"
            },
            payload: JSON.stringify(payload),
            muteHttpExceptions: true
        };

        let response = UrlFetchApp.fetch(url, options);
        return { status: 'success', body: response.getContentText() };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 📋 லிஸ்ட் மெசேஜ் அனுப்புதல் (List Message Helper)
 */
function sendWhatsAppListMessage(to, bodyText, buttonText, sections) {
    try {
        let url = "https://graph.facebook.com/v20.0/" + WA_PHONE_ID + "/messages";
        let payload = {
            messaging_product: "whatsapp",
            recipient_type: "individual",
            to: cleanPhoneNumber(to),
            type: "interactive",
            interactive: {
                type: "list",
                header: { type: "text", text: "Nanban Driving School" },
                body: { text: bodyText },
                action: {
                    button: buttonText || "Menu",
                    sections: sections
                }
            }
        };
        let options = {
            method: "post",
            headers: {
                "Authorization": "Bearer " + getCleanToken(),
                "Content-Type": "application/json"
            },
            payload: JSON.stringify(payload),
            muteHttpExceptions: true
        };
        let res = UrlFetchApp.fetch(url, options);
        return { status: 'success', body: res.getContentText() };
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

function setLastQuizState_(phone, stateObj) {
    try {
        let p = cleanPhoneNumber(phone);
        if (!p) return;
        let key = "QUIZ_LAST_" + p;
        let payload = Object.assign({}, stateObj || {}, { savedAt: new Date().toISOString() });
        PropertiesService.getScriptProperties().setProperty(key, JSON.stringify(payload));
    } catch (e) {}
}

function getLastQuizState_(phone) {
    try {
        let p = cleanPhoneNumber(phone);
        if (!p) return null;
        let key = "QUIZ_LAST_" + p;
        let raw = PropertiesService.getScriptProperties().getProperty(key);
        if (!raw) return null;
        let obj = JSON.parse(raw);
        return obj || null;
    } catch (e) {
        return null;
    }
}

function resolveQuizCorrectAnswerText_(qRow, correctNo) {
    try {
        let qCfg = getQuizBankConfig();
        let a = String(qRow[qCfg.o1] || "").trim();
        let b = String(qRow[qCfg.o2] || "").trim();
        let c = String(qRow[qCfg.o3] || "").trim();
        if (parseInt(correctNo) === 1) return a || "முதல் விடை";
        if (parseInt(correctNo) === 2) return b || "இரண்டாம் விடை";
        if (parseInt(correctNo) === 3) return c || "மூன்றாம் விடை";
        return "";
    } catch (e) {
        return "";
    }
}

function resolveCorrectAnswerFromPid_(pid) {
    try {
        if (!pid || pid.indexOf("QUIZ_") !== 0) return null;
        let parts = String(pid).split("_"); // QUIZ_CORRECT_RowIndex_ChoiceNo
        if (parts.length < 4) return null;
        let rowIndex = parseInt(parts[2]);
        if (!rowIndex || rowIndex < 1) return null;
        let qSheet = getDB().getSheetByName("QuizBank");
        if (!qSheet) return null;
        let row = qSheet.getRange(rowIndex, 1, 1, qSheet.getLastColumn()).getValues()[0];
        let qCfg = getQuizBankConfig();
        let correctText = String(row[qCfg.ansText] || "").trim();
        let correctNo = parseInt(correctText);
        if (isNaN(correctNo) || correctNo < 1 || correctNo > 3) {
            let ct = cleanQuizText(correctText);
            let a = String(row[qCfg.o1] || "");
            let b = String(row[qCfg.o2] || "");
            let c = String(row[qCfg.o3] || "");
            if (ct && ct === cleanQuizText(a)) correctNo = 1;
            else if (ct && ct === cleanQuizText(b)) correctNo = 2;
            else if (ct && ct === cleanQuizText(c)) correctNo = 3;
            else correctNo = 1;
        }
        return {
            rowIndex: rowIndex,
            question: String(row[qCfg.ques] || "").trim(),
            correctNo: correctNo,
            correctAnswerText: resolveQuizCorrectAnswerText_(row, correctNo)
        };
    } catch (e) {
        return null;
    }
}

function resolveLlrDocumentUrl_(s) {
    try {
        if (!s) return "";
        let list = [];
        if (s.llrDocUrl) list.push(String(s.llrDocUrl).trim());
        if (s.llrDocId) list.push(getDrivePdfDownloadUrl(s.llrDocId));
        if (s.llrDriveLink) list.push(String(s.llrDriveLink).trim());
        if (s.llrLink) list.push(String(s.llrLink).trim());
        if (s.documentUrl) list.push(String(s.documentUrl).trim());
        for (let i = 0; i < list.length; i++) {
            let u = String(list[i] || "").trim();
            if (u && u.indexOf("http") === 0) return u;
        }
        return "";
    } catch (e) {
        return "";
    }
}

/**
 * 🎮 வினாடி-வினா மெசேஜ் (Quiz Interactive Message)
 */
function sendQuizInteractiveMsg(to, bodyText, headerUrl, correctChoiceNo, dayNo, studentName, qRow, rowIndex) {
    try {
        let url = "https://graph.facebook.com/v20.0/" + WA_PHONE_ID + "/messages";
        
        // Use Row Index to ensure 100% accurate validation even with multiple questions per day
        let rIdx = rowIndex || 0;
        let buttons = [
            { id: (correctChoiceNo == 1 ? "QUIZ_CORRECT_" : "QUIZ_WRONG_") + rIdx + "_1", title: "முதல் விடை" },
            { id: (correctChoiceNo == 2 ? "QUIZ_CORRECT_" : "QUIZ_WRONG_") + rIdx + "_2", title: "இரண்டாம் விடை" },
            { id: (correctChoiceNo == 3 ? "QUIZ_CORRECT_" : "QUIZ_WRONG_") + rIdx + "_3", title: "மூன்றாம் விடை" }
        ];
        let interactive = {
            type: "button",
            body: { text: bodyText },
            action: {
                buttons: buttons.map(b => ({
                    type: "reply",
                    reply: { id: b.id, title: b.title }
                }))
            }
        };
        if (headerUrl && headerUrl.trim()) {
            interactive.header = {
                type: "image",
                image: { link: headerUrl.trim() }
            };
        }
        let payload = {
            messaging_product: "whatsapp",
            recipient_type: "individual",
            to: cleanPhoneNumber(to),
            type: "interactive",
            interactive: interactive
        };
        let options = {
            method: "post",
            headers: {
                "Authorization": "Bearer " + getCleanToken(),
                "Content-Type": "application/json"
            },
            payload: JSON.stringify(payload),
            muteHttpExceptions: true
        };
        let res = UrlFetchApp.fetch(url, options);
        // Save last quiz state so text-based replies can still be evaluated.
        setLastQuizState_(to, {
            correctNo: parseInt(correctChoiceNo) || 1,
            dayNo: parseInt(dayNo) || 1,
            rowIndex: parseInt(rIdx) || 0,
            question: String((qRow && qRow[2]) || "").trim(),
            correctAnswerText: resolveQuizCorrectAnswerText_(qRow || [], parseInt(correctChoiceNo) || 1)
        });
        return { status: 'success', body: res.getContentText() };
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🚨 அட்மின் அலர்ட் (Admin Alert Helper)
 */
function triggerAdminBotAlert(senderPhone, btnName) {
    let now = new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" });
    let alertMsg = `🚨 *புதிய கஸ்டமர் அலர்ட்!*\n\nஎண்: ${senderPhone}\nதேவை: ${btnName}\nநேரம்: ${now}\n\n👉 உடனே தொடர்புகொள்ளவும்!`;
    notifyAdmins(alertMsg);
}

function triggerAdminLeadAlert_(senderPhone, clickedLabel, dryRun, outbox) {
    let msg = "User " + String(senderPhone || "-") + " clicked " + String(clickedLabel || "-") + " - Potential Lead";
    if (dryRun) {
        if (Array.isArray(outbox)) outbox.push({ type: "admin_alert", text: msg });
        return { status: "mocked", message: msg };
    }
    try {
        notifyAdmins("🚨 " + msg);
        return { status: "success", message: msg };
    } catch (e) {
        return { status: "error", message: String(e) };
    }
}

function chatbotSendText_(to, text, dryRun, outbox) {
    if (dryRun) {
        if (Array.isArray(outbox)) outbox.push({ type: "text", to: to, text: text });
        return { status: "mocked" };
    }
    return sendWhatsAppMessage(to, text);
}

function chatbotSendButtons_(to, bodyText, buttons, dryRun, outbox) {
    if (dryRun) {
        if (Array.isArray(outbox)) outbox.push({ type: "buttons", to: to, body: bodyText, buttons: buttons || [] });
        return { status: "mocked" };
    }
    return sendWhatsAppInteractiveButtons(to, bodyText, buttons || []);
}

function chatbotSendList_(to, bodyText, buttonText, sections, dryRun, outbox) {
    if (dryRun) {
        if (Array.isArray(outbox)) outbox.push({ type: "list", to: to, body: bodyText, button: buttonText, sections: sections || [] });
        return { status: "mocked" };
    }
    return sendWhatsAppListMessage(to, bodyText, buttonText, sections || []);
}

function handleDynamicPricingChatbotFlow_(from, rawMsg, pid, dryRun, outbox) {
    let msg = String(rawMsg || "").toLowerCase().trim();
    let state = getChatbotUserState_(from);
    if (!Array.isArray(state.selected_services)) state.selected_services = [];
    if (!state.last_service_key) state.last_service_key = "TW_FULL";

    // Explicit service package picked from dynamic fee list.
    if (pid && String(pid).indexOf("FEE_SEL::") === 0) {
        let key = normalizeServiceKey_(String(pid).split("::")[1] || "");
        let idx = state.selected_services.indexOf(key);
        if (idx >= 0) state.selected_services.splice(idx, 1);
        else state.selected_services.push(key);
        state.last_service_key = key;
        setChatbotUserState_(from, state);

        let fee = buildDynamicFeeMessageByServices_(state.selected_services.length ? state.selected_services : [key], "✅ தேர்வு புதுப்பிக்கப்பட்டது");
        chatbotSendButtons_(from, fee, [
            { id: "FEE_SHOW_TOTAL", title: "🧮 Grand Total" },
            { id: "MENU_FEES", title: "📋 More Services" },
            { id: "FEE_RESET", title: "♻️ Reset" }
        ], dryRun, outbox);
        return { handled: true, state: state };
    }

    if (pid === "FEE_SHOW_TOTAL") {
        let keys = state.selected_services.length ? state.selected_services : [state.last_service_key || "TW_FULL"];
        chatbotSendButtons_(from, buildDynamicFeeMessageByServices_(keys, "📌 Selected Services"), [
            { id: "MENU_FEES", title: "📋 சேவைகள்" },
            { id: "ADM_HELP", title: "📞 Admin Help" },
            { id: "GOTO_MENU", title: "🔙 Main Menu" }
        ], dryRun, outbox);
        return { handled: true, state: state };
    }

    if (pid === "FEE_RESET") {
        state.selected_services = [];
        state.last_service_key = "TW_FULL";
        setChatbotUserState_(from, state);
        chatbotSendText_(from, "சரி ✅ உங்கள் தேர்வுகள் reset செய்யப்பட்டது.", dryRun, outbox);
        chatbotSendList_(from, "கீழே இருந்து சேவைகளை தேர்வு செய்யவும் (multiple selection supported by repeated taps):", "சேவைகள்", [
            { title: "Driving School Services", rows: getChatbotServiceListRows_().concat([
                { id: "FEE_SHOW_TOTAL", title: "🧮 Grand Total பார்க்க", description: "Selected services total" }
            ]) }
        ], dryRun, outbox);
        return { handled: true, state: state };
    }

    if (pid === "MENU_BIKE") {
        state.last_service_key = "TW_FULL";
        if (state.selected_services.indexOf("TW_FULL") === -1) state.selected_services = ["TW_FULL"];
        setChatbotUserState_(from, state);
        let bikeMsg = buildFirstInquiryWelcomeText_("மாணவரே", "TW_FULL");
        chatbotSendButtons_(from, bikeMsg, [
            { id: "MENU_FEES", title: "💰 கட்டண விவரம்" },
            { id: "ADM_DEMO", title: "✅ அட்மிஷன் / Demo" },
            { id: "GOTO_MENU", title: "🔙 Main Menu" }
        ], dryRun, outbox);
        return { handled: true, state: state };
    }

    if (pid === "MENU_CAR") {
        // For 4W users, show exact package options dynamically.
        state.last_service_key = "FW_LICENSE_ONLY";
        if (!state.selected_services.length) state.selected_services = ["FW_LICENSE_ONLY"];
        setChatbotUserState_(from, state);
        chatbotSendList_(from, "🚗 நான்கு சக்கர சேவை வகையைத் தேர்வு செய்யவும்:", "4W Packages", [
            {
                title: "Four-Wheeler Plans",
                rows: [
                    { id: "FEE_SEL::FW_LICENSE_ONLY", title: "💠 லைசென்ஸ் மட்டும்", description: "LLR + License" },
                    { id: "FEE_SEL::FW_TRAINING_ONLY", title: "💠 பயிற்சி மட்டும்", description: "₹200/day x 15 days" },
                    { id: "FEE_SEL::FW_LICENSE_TRAINING", title: "💠 லைசென்ஸ் + பயிற்சி", description: "Complete 4W package" }
                ]
            }
        ], dryRun, outbox);
        chatbotSendButtons_(from, "👇 சேவையை தேர்வு செய்ததும் துல்லியமான கட்டண breakdown கிடைக்கும்.", [
            { id: "MENU_FEES", title: "💰 Fee Details" },
            { id: "ADM_DEMO", title: "✅ Demo" },
            { id: "GOTO_MENU", title: "🔙 Main Menu" }
        ], dryRun, outbox);
        return { handled: true, state: state };
    }

    if (pid === "MENU_FEES" || msg.indexOf("கட்டண") !== -1 || msg.indexOf("fee") !== -1 || msg.indexOf("fees") !== -1) {
        let selected = state.selected_services.length ? state.selected_services : [state.last_service_key || "TW_FULL"];
        let exact = buildDynamicFeeMessageByServices_(selected, "📣 உங்கள் தேர்வுக்கான கட்டண விவரம்");
        chatbotSendButtons_(from, exact, [
            { id: "FEE_SHOW_TOTAL", title: "🧮 Grand Total" },
            { id: "ADM_HELP", title: "📞 Admin Help" },
            { id: "GOTO_MENU", title: "🔙 Main Menu" }
        ], dryRun, outbox);
        chatbotSendList_(from, "மேலும் சேவைகள் சேர்க்க/நீக்க கீழே தேர்வு செய்யவும்:", "Service List", [
            { title: "Driving School Services", rows: getChatbotServiceListRows_().concat([
                { id: "FEE_SHOW_TOTAL", title: "🧮 Grand Total பார்க்க", description: "Selected services total" },
                { id: "FEE_RESET", title: "♻️ Reset Selection", description: "Clear selected services" }
            ]) }
        ], dryRun, outbox);
        return { handled: true, state: state };
    }

    return { handled: false, state: state };
}

function simulateChatbotWebhookMockAction(mockPayload) {
    try {
        let json = mockPayload;
        if (typeof json === "string") json = JSON.parse(json || "{}");
        json = json || {};

        let from = "";
        let rawMsg = "";
        let pid = "";

        if (json.from || json.pid || json.rawMsg) {
            from = String(json.from || "");
            rawMsg = String(json.rawMsg || "");
            pid = String(json.pid || "");
        } else {
            let entries = Array.isArray(json.entry) ? json.entry : [];
            let msgObj = null;
            for (let ei = 0; ei < entries.length && !msgObj; ei++) {
                let changes = Array.isArray(entries[ei] && entries[ei].changes) ? entries[ei].changes : [];
                for (let ci = 0; ci < changes.length && !msgObj; ci++) {
                    let value = (changes[ci] && changes[ci].value) ? changes[ci].value : {};
                    let msgs = Array.isArray(value.messages) ? value.messages : [];
                    if (msgs.length > 0) msgObj = msgs[0];
                }
            }
            if (!msgObj) return { status: "error", message: "No inbound message in payload" };
            from = String(msgObj.from || "");
            if (msgObj.type === "text") rawMsg = String((msgObj.text && msgObj.text.body) || "");
            else if (msgObj.type === "button") {
                pid = String((msgObj.button && (msgObj.button.payload || msgObj.button.text)) || "");
                rawMsg = String((msgObj.button && msgObj.button.text) || "");
            } else if (msgObj.type === "interactive") {
                if (msgObj.interactive && msgObj.interactive.type === "button_reply") {
                    pid = String((msgObj.interactive.button_reply && msgObj.interactive.button_reply.id) || "");
                    rawMsg = String((msgObj.interactive.button_reply && msgObj.interactive.button_reply.title) || "");
                } else if (msgObj.interactive && msgObj.interactive.type === "list_reply") {
                    pid = String((msgObj.interactive.list_reply && msgObj.interactive.list_reply.id) || "");
                    rawMsg = String((msgObj.interactive.list_reply && msgObj.interactive.list_reply.title) || "");
                }
            }
        }

        let outbox = [];
        if (pid) triggerAdminLeadAlert_(from, (rawMsg || pid), true, outbox);
        let r = handleDynamicPricingChatbotFlow_(from, rawMsg, pid, true, outbox);
        return {
            status: "success",
            handled: !!(r && r.handled),
            inbound: { from: from, pid: pid, rawMsg: rawMsg },
            state: (r && r.state) ? r.state : null,
            outbound: outbox
        };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function sendWhatsAppMessage(toPhone, messageText) {
    try {
        let url = "https://graph.facebook.com/v20.0/" + WA_PHONE_ID + "/messages";
        let cleanText = String(messageText || "");
        let payload = {
            "messaging_product": "whatsapp",
            "to": cleanPhoneNumber(toPhone),
            "type": "text",
            "text": { "body": cleanText }
        };
        let options = {
            "method": "post",
            "headers": { 
                "Authorization": "Bearer " + getCleanToken(),
                "Content-Type": "application/json" 
            },
            "payload": JSON.stringify(payload),
            "muteHttpExceptions": true
        };
        let response = UrlFetchApp.fetch(url, options);
        let code = response.getResponseCode();
        let body = response.getContentText();
        try { logWaOutboundLocal_("text", toPhone, "", payload, code, body); } catch (e) {}
        return { status: (code >= 200 && code < 300) ? 'success' : 'error', body: body, code: code };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🖼️ Send WhatsApp Image Message
 */
function sendWhatsAppImageMessage(toPhone, imageUrl, caption) {
    try {
        var payload = {
            "messaging_product": "whatsapp",
            "to": cleanPhoneNumber(toPhone),
            "type": "image",
            "image": { "link": imageUrl, "caption": caption || "" }
        };
        UrlFetchApp.fetch("https://graph.facebook.com/v20.0/" + WA_PHONE_ID + "/messages", {
            "method": "post",
            "headers": { "Authorization": "Bearer " + getCleanToken(), "Content-Type": "application/json" },
            "payload": JSON.stringify(payload),
            "muteHttpExceptions": true
        });
    } catch(e) {}
}

/**
 * 📊 Calculate Student Journey Progress %
 */
function getStudentProgress(s) {
    let p = 5;
    if (s.llrStatus === 'Yes' || s.llrDate) p = 33;
    if ((parseInt(s.classesAttended)||0) > 0) {
        let trainP = (parseInt(s.classesAttended)||0) / 15;
        if (trainP > 1) trainP = 1;
        p = Math.floor(33 + (trainP * 33));
    }
    if (s.testStatus === 'Pass' || s.status === 'License_Completed') p = 100;
    return p;
}


// ------------------------------------------------------------------------------
// 6. WEBHOOKS, AUTO-REPLIES & AUTO FEEDBACK
// -----------------------------------------------------------------------------// 🚀 Bulk Send WhatsApp Reminders
function bulkSendRemindersAction(studentIds, type) {
    if (!Array.isArray(studentIds) || studentIds.length === 0) return { status: 'error', msg: 'No students selected.' };
    
    let db = getDatabaseData();
    let sentCount = 0;
    let failCount = 0;
    
    studentIds.forEach(id => {
        let s = db.students.find(x => String(x.id) === String(id));
        if (!s || !s.phone) return;
        
        try {
            let res;
            if (type === 'today_30') {
                res = sendTemplateMsg(s.phone, "llr_30_days_reminder", [s.name]);
            } else if (type === 'llr_expiring') {
                // Using bulk_announcement if no specialized LLR expiry template
                res = sendTemplateMsg(s.phone, "bulk_announcement", ["உங்கள் LLR இன்னும் சில நாட்களில் காலாவதியாக உள்ளது. தயவுசெய்து புதுப்பிக்கவும்."]);
            }
            
            if (res && res.status === 'success') sentCount++;
            else failCount++;
        } catch (e) {
            failCount++;
        }
    });
    
    return { status: 'success', msg: `${sentCount} மெசேஜ்கள் அனுப்பப்பட்டன. (தோல்வி: ${failCount})` };
}

// @ts-ignore
function doPost(e) { 
  try {
    try {
      var rawInbound = (e && e.postData && e.postData.contents) ? String(e.postData.contents) : "";
      var rawSnippet = rawInbound.length > 1200 ? rawInbound.substring(0, 1200) + "...[truncated]" : rawInbound;
      PropertiesService.getScriptProperties().setProperty("WA_LAST_HIT_AT", new Date().toISOString());
      PropertiesService.getScriptProperties().setProperty("WA_LAST_BODY_SNIPPET", rawSnippet);
      PropertiesService.getScriptProperties().setProperty("WA_LAST_ERROR", "");
    } catch (dbg0) {}

    var body = (e && e.postData && e.postData.contents) ? e.postData.contents : "{}";
    var json = JSON.parse(body || "{}");
    try { captureWaWebhookStatusesLocal_(json); } catch (wsErr) {}
    // Backup import endpoint (secure key required)
    if (json && json.action === "import_backup_payload") {
      var reqKey = String(json.key || "");
      if (!reqKey || reqKey !== getImportBackupKey_()) {
        return jsonOut_({ status: "error", message: "Unauthorized" });
      }
      var resultImport = importBackupPayloadAction(json.payload || {});
      return jsonOut_(resultImport);
    }

    // Web App API Bridge endpoint (for Firebase-hosted frontend)
    if (json && json.action === "api_bridge") {
      return handleApiBridgeCall_(json);
    }

    var entries = Array.isArray(json.entry) ? json.entry : [];
    var msgObj = null;

    // Robust parser: pick first inbound message from any entry/change.
    for (var ei = 0; ei < entries.length && !msgObj; ei++) {
      var changes = Array.isArray(entries[ei] && entries[ei].changes) ? entries[ei].changes : [];
      for (var ci = 0; ci < changes.length && !msgObj; ci++) {
        var value = (changes[ci] && changes[ci].value) ? changes[ci].value : {};
        var msgs = Array.isArray(value.messages) ? value.messages : [];
        if (msgs.length > 0) msgObj = msgs[0];
      }
    }
    if (!msgObj) {
      try { PropertiesService.getScriptProperties().setProperty("WA_LAST_PARSE_NOTE", "No inbound messages (status/event only)"); } catch (dbg1) {}
      return ContentService.createTextOutput("OK");
    }

    var from = msgObj.from;
    var rawMsg = "";
    var pid = "";
    try {
      PropertiesService.getScriptProperties().setProperty("WA_LAST_FROM", String(from || ""));
      PropertiesService.getScriptProperties().setProperty("WA_LAST_TYPE", String(msgObj.type || ""));
      PropertiesService.getScriptProperties().setProperty("WA_LAST_PARSE_NOTE", "Inbound message parsed");
    } catch (dbg2) {}

    if (msgObj.type === "text") {
      rawMsg = msgObj.text.body;
    } else if (msgObj.type === "button") {
      // Some clients send quick reply as type=button
      pid = (msgObj.button && (msgObj.button.payload || msgObj.button.text)) ? String(msgObj.button.payload || msgObj.button.text) : "";
      rawMsg = (msgObj.button && msgObj.button.text) ? msgObj.button.text : "";
    } else if (msgObj.type === "interactive") {
      if (msgObj.interactive.type === "button_reply") {
        pid = msgObj.interactive.button_reply.id;
        rawMsg = msgObj.interactive.button_reply.title;
      } else if (msgObj.interactive.type === "list_reply") {
        pid = msgObj.interactive.list_reply.id;
        rawMsg = msgObj.interactive.list_reply.title;
      }
    }
    if (!rawMsg && msgObj.text && msgObj.text.body) rawMsg = msgObj.text.body;
    if (!rawMsg && msgObj.button && msgObj.button.text) rawMsg = msgObj.button.text;
    var msg = String(rawMsg).toLowerCase().trim();
    var leadAlertSent = false;
    if (pid) {
      try {
        triggerAdminLeadAlert_(from, (rawMsg || pid), false, null);
        leadAlertSent = true;
      } catch (laErr) {}
    }

    // New dynamic pricing/service flow (handles MENU_BIKE, MENU_CAR, MENU_FEES, FEE_SEL::*)
    var dynFlow = handleDynamicPricingChatbotFlow_(from, rawMsg, pid, false, null);
    if (dynFlow && dynFlow.handled) {
      return ContentService.createTextOutput("OK");
    }

    // --- WELCOME / MENU ---
    var isWelcome = (msg === "hi" || msg === "hello" || msg === "hlo" || msg === "hai" || msg === "hey" ||
                     msg === "\u0bb9\u0bbf" || msg === "\u0bb5\u0ba3\u0b95\u0bcd\u0b95\u0bae\u0bcd" ||
                     msg === "\u0bb9\u0bbe\u0baf\u0bcd" || msg === "\u0bb9\u0bb2\u0bcb" ||
                     msg.indexOf("menu") !== -1 || msg.indexOf("\u0bae\u0bc6\u0ba9\u0bc1") !== -1 ||
                     pid === "GOTO_MENU");
    if (isWelcome) {
      var isStudent = false; var student = null;
      try {
        var db = getDatabaseData();
        student = (db.students || []).find(function(x) { return cleanPhoneNumber(x.phone) === cleanPhoneNumber(from); });
        isStudent = !!student;
      } catch(dbErr) {}
      var wMsg = "\u0bb5\u0ba3\u0b95\u0bcd\u0b95\u0bae\u0bcd! \uD83D\uDE4F *\u0ba8\u0ba3\u0bcd\u0baa\u0ba9\u0bcd \u0b9f\u0bbf\u0bb0\u0bc8\u0bb5\u0bbf\u0b99\u0bcd \u0bb8\u0bcd\u0b95\u0bc2\u0bb2\u0bc1\u0b95\u0bcd\u0b95\u0bc1* \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bc8 \u0b85\u0ba9\u0bcd\u0baa\u0bc1\u0b9f\u0ba9\u0bcd \u0bb5\u0bb0\u0bb5\u0bc7\u0bb1\u0bcd\u0b95\u0bbf\u0bb1\u0bcb\u0bae\u0bcd.\n\n\u0bb5\u0bbe\u0b95\u0ba9\u0bae\u0bcd \u0b93\u0b9f\u0bcd\u0b9f\u0bc1\u0bb5\u0ba4\u0bc1 \u0bb5\u0bc6\u0bb1\u0bc1\u0bae\u0bcd \u0ba4\u0bbf\u0bb1\u0bae\u0bc8\u0baf\u0bb2\u0bcd\u0bb2, \u0b85\u0ba4\u0bc1 \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bbf\u0ba9\u0bcd \u0b9a\u0bc1\u0ba4\u0ba8\u0bcd\u0ba4\u0bbf\u0bb0\u0bae\u0bcd! \uD83D\uDEE3\uFE0F";
      if (isStudent && student) {
        wMsg = "\u0bb5\u0ba3\u0b95\u0bcd\u0b95\u0bae\u0bcd *" + student.name + "*! \uD83D\uDE4F \u0ba8\u0ba3\u0bcd\u0baa\u0ba9\u0bcd \u0b9f\u0bbf\u0bb0\u0bc8\u0bb5\u0bbf\u0b99\u0bcd \u0bb8\u0bcd\u0b95\u0bc2\u0bb2\u0bcd \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bc8 \u0b85\u0ba9\u0bcd\u0baa\u0bc1\u0b9f\u0ba9\u0bcd \u0bb5\u0bb0\u0bb5\u0bc7\u0bb1\u0bcd\u0b95\u0bbf\u0bb1\u0ba4\u0bc1. \uD83D\uDE97\uD83D\uDCA8";
      }
      var rows = [
        { id: "MENU_CAR", title: "\uD83D\uDE97 \u0b95\u0bbe\u0bb0\u0bcd \u0b93\u0b9f\u0bcd\u0b9f \u0b95\u0bb1\u0bcd\u0b95", description: "Car Training" },
        { id: "MENU_BIKE", title: "\uD83D\uDEF5 \u0b9f\u0bc2-\u0bb5\u0bc0\u0bb2\u0bb0\u0bcd \u0b95\u0bb1\u0bcd\u0b95", description: "Two-Wheeler Training" },
        { id: "MENU_RTO", title: "\uD83E\uDEAA RTO & E-Sevai", description: "RTO Services" },
        { id: "MENU_FEES", title: "\uD83D\uDCB0 \u0b95\u0b9f\u0bcd\u0b9f\u0ba3\u0bae\u0bcd & \u0ba8\u0bc7\u0bb0\u0bae\u0bcd", description: "Fees & Timings" },
        { id: "MENU_FAQ", title: "\u2753 \u0b9a\u0ba8\u0bcd\u0ba4\u0bc7\u0b95\u0b99\u0bcd\u0b95\u0bb3\u0bcd", description: "FAQ" }
      ];
      if (isStudent) {
        rows.push({ id: "MENU_MY_STATUS", title: "\uD83D\uDCCA \u0b8e\u0ba9\u0ba4\u0bc1 \u0bb5\u0bbf\u0bb5\u0bb0\u0bae\u0bcd", description: "Progress & Fees" });
        rows.push({ id: "MENU_ROAD_SAFETY", title: "\uD83D\uDEA6 \u0b9a\u0bbe\u0bb2\u0bc8 \u0bb5\u0bbf\u0ba4\u0bbf\u0b95\u0bb3\u0bcd", description: "Road Safety" });
      }
      var res = sendWhatsAppListMessage(from, wMsg + "\n\n\u0b95\u0bc0\u0bb4\u0bc7 \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bcd \u0ba4\u0bc7\u0bb5\u0bc8\u0baf\u0bc8 \u0ba4\u0bc7\u0bb0\u0bcd\u0ba8\u0bcd\u0ba4\u0bc6\u0b9f\u0bc1\u0b95\u0bcd\u0b95\u0bb5\u0bc1\u0bae\u0bcd \uD83D\uDC47", "Menu", [{ title: "\u0b9a\u0bc7\u0bb5\u0bc8\u0b95\u0bb3\u0bcd", rows: rows }]);
      if (!res || res.status === "error") {
        sendWhatsAppMessage(from, wMsg + "\n\n(\u0ba4\u0baf\u0bb5\u0bc1\u0b9a\u0bc6\u0baf\u0bcd\u0ba4\u0bc1 'menu' \u0b8e\u0ba9\u0bcd\u0bb1\u0bc1 \u0b85\u0ba9\u0bc1\u0baa\u0bcd\u0baa\u0bb5\u0bc1\u0bae\u0bcd)");
      }
      return ContentService.createTextOutput("OK");
    }

    // --- ADMIN ALERT BUTTONS ---
    if (pid === "ADM_DEMO" || pid === "ADM_HELP" || pid === "ADM_UPLOAD" || pid === "LLR_READY" || pid === "LLR_CALL") {
      var bName = "Help";
      if (pid === "ADM_DEMO") bName = "Admissions";
      else if (pid === "ADM_UPLOAD") bName = "Uploads";
      else if (pid === "LLR_READY") bName = "LLR 30-Day: Ready for Test";
      else if (pid === "LLR_CALL") bName = "LLR 30-Day: Request Call";
      
      if (!leadAlertSent) triggerAdminBotAlert(from, bName);
      if (pid === "ADM_DEMO") {
         var demoMsg = buildFirstInquiryWelcomeText_("மாணவரே", "இருசக்கர வாகனம் (Two-Wheeler)") +
           "\n\n👇 அடுத்தது என்ன செய்ய வேண்டும்?\n• உங்கள் பெயர் + நகரம் share செய்யவும்.\n• Demo/Admission slot உடனே confirm செய்யலாம்.";
         sendWhatsAppInteractiveButtons(from, demoMsg, [
            { id: "MENU_FEES", title: "💰 Fee Details" },
            { id: "ADM_HELP", title: "📞 Call Admin" },
            { id: "GOTO_MENU", title: "🔙 Main Menu" }
         ]);
         return ContentService.createTextOutput("OK");
      }
      if (pid === "LLR_READY" || pid === "LLR_CALL") {
         sendWhatsAppMessage(from, "\u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bcd \u0ba4\u0b95\u0bb5\u0bb2\u0bcd \u0b85\u0b9f\u0bcd\u0bae\u0bbf\u0ba9\u0bc1\u0b95\u0bcd\u0b95\u0bc1 \u0b85\u0ba9\u0bc1\u0baa\u0bcd\u0baa\u0baa\u0bcd\u0baa\u0b9f\u0bcd\u0b9f\u0ba4\u0bc1. \u0bb5\u0bbf\u0bb0\u0bc8\u0bb5\u0bbf\u0bb2\u0bcd \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bc8 \u0b85\u0bb4\u0bc8\u0baa\u0bcd\u0baa\u0bcb\u0bae\u0bcd! \uD83D\uDE97");
      }
      return ContentService.createTextOutput("OK");
    }

    // --- MENU BRANCHES ---
    if (pid === "MENU_CAR") {
      // 🖼️ Send car training image first
      try { sendWhatsAppImageMessage(from, "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a0/Maruti_Suzuki_S-Presso_2019.jpg/1200px-Maruti_Suzuki_S-Presso_2019.jpg", "நண்பன் டிரைவிங் ஸ்கூல் - Maruti S-Presso பயிற்சி 🚗"); } catch(imgErr) {}
      sendWhatsAppInteractiveButtons(from, "சூப்பர் சாய்ஸ்! 🚗✨\n\nMaruti S-Presso காரில் 100% Practical பயிற்சி.\nகியர், கிளட்ச், பிரேக் கண்ட்ரோல் — எல்லாமே ஒரே இடத்தில் கற்றுக்கொள்ளலாம்!\n\n💪 நம்பிக்கையோடு ஸ்டீயரிங் பிடி!", [{ id: "ADM_DEMO", title: "✅ அட்மிஷன் / Demo" }, { id: "MENU_FEES", title: "💰 கட்டணம்" }, { id: "GOTO_MENU", title: "🔙 Main Menu" }]);
      return ContentService.createTextOutput("OK");
    }
    if (pid === "MENU_BIKE") {
      // 🖼️ Send bike training image first
      try { sendWhatsAppImageMessage(from, "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5c/Honda_Activa_6G_2021.jpg/1200px-Honda_Activa_6G_2021.jpg", "நண்பன் டிரைவிங் ஸ்கூல் - Two-Wheeler பயிற்சி 🛵"); } catch(imgErr) {}
      let bikeMsg = buildFirstInquiryWelcomeText_("மாணவரே", "இருசக்கர வாகனம் (Two-Wheeler)");
      sendWhatsAppInteractiveButtons(from, bikeMsg, [{ id: "ADM_DEMO", title: "✅ அட்மிஷன் / Demo" }, { id: "MENU_FEES", title: "💰 கட்டணம்" }, { id: "GOTO_MENU", title: "🔙 Main Menu" }]);
      return ContentService.createTextOutput("OK");
    }
    if (pid === "MENU_RTO") {
      sendWhatsAppInteractiveButtons(from, "\u0bb2\u0bc8\u0b9a\u0bc6\u0ba9\u0bcd\u0b9a\u0bcd \u0b8e\u0b9f\u0bc1\u0baa\u0bcd\u0baa\u0ba4\u0bc1 \u0b87\u0ba9\u0bbf \u0b88\u0b9a\u0bbf! RTO \u0bb5\u0bc7\u0bb2\u0bc8\u0b95\u0bb3\u0bc8 \u0ba8\u0bbe\u0b99\u0bcd\u0b95\u0bb3\u0bc7 \u0bae\u0bc1\u0b9f\u0bbf\u0ba4\u0bcd\u0ba4\u0bc1\u0ba4\u0bcd \u0ba4\u0bb0\u0bc1\u0b95\u0bbf\u0bb1\u0bcb\u0bae\u0bcd. \uD83D\uDCDD\n\n\u2705 \u0baa\u0bc1\u0ba4\u0bbf\u0baf LLR & License\n\u2705 Renewal & Address Change\n\u2705 E-Sevai Services", [{ id: "ADM_HELP", title: "\uD83D\uDCDE \u0b85\u0b9f\u0bcd\u0bae\u0bbf\u0ba9\u0bcd \u0b89\u0ba4\u0bb5\u0bbf" }, { id: "GOTO_MENU", title: "\uD83D\uDD19 Main Menu" }]);
      return ContentService.createTextOutput("OK");
    }
    if (pid === "MENU_FEES") {
      let feeMsg =
        "💰 *கட்டண விவரம் (Clear & Transparent)*\n\n" +
        "• Two-Wheeler Full Package (Training + License): *₹2800*\n" +
        "• LLR + Basic Training மட்டும்: *₹1800*\n\n" +
        "✨ *ஏன் நண்பன் டிரைவிங் ஸ்கூல்?*\n" +
        "• 100% வெளிப்படைத்தன்மை கொண்ட பிரத்தியேக மொபைல் ஆப்.\n" +
        "• ஒவ்வொரு கிளாஸ் முடிந்ததும் WhatsApp-ல் ஆட்டோமேட்டிக் அலர்ட் & ரிப்போர்ட்.\n" +
        "• வேறு எங்கும் இல்லாத Video Tutor.\n" +
        "• நவீன தொழில்நுட்பத்தில் பயிற்சி.";
      sendWhatsAppInteractiveButtons(from, feeMsg, [{ id: "ADM_DEMO", title: "✅ அட்மிஷன் / Demo" }, { id: "ADM_HELP", title: "📞 Admin Help" }, { id: "GOTO_MENU", title: "🔙 Main Menu" }]);
      return ContentService.createTextOutput("OK");
    }
    if (pid === "MENU_FAQ") {
      sendWhatsAppInteractiveButtons(from, "\u0baa\u0bcb\u0ba4\u0bc1 \u0b95\u0bc7\u0bb3\u0bcd\u0bb5\u0bbf\u0b95\u0bb3\u0bcd:\n\n\uD83D\uDD39 \u0baa\u0baf\u0bbf\u0bb1\u0bcd\u0b9a\u0bbf \u0ba8\u0bbe\u0b9f\u0bcd\u0b95\u0bb3\u0bcd: 10-15 \u0ba8\u0bbe\u0b9f\u0bcd\u0b95\u0bb3\u0bcd\n\uD83D\uDD39 \u0bb5\u0baf\u0ba4\u0bc1 \u0bb5\u0bb0\u0bae\u0bcd\u0baa\u0bc1: LLR-\u0b95\u0bcd\u0b95\u0bc1 18+\n\uD83D\uDD39 \u0baa\u0bbe\u0ba4\u0bc1\u0b95\u0bbe\u0baa\u0bcd\u0baa\u0bc1: Dual-Control \u0b95\u0bbe\u0bb0\u0bcd", [{ id: "ADM_DEMO", title: "\u2705 \u0b85\u0b9f\u0bcd\u0bae\u0bbf\u0bb7\u0ba9\u0bcd / Demo" }, { id: "GOTO_MENU", title: "\uD83D\uDD19 Main Menu" }]);
      return ContentService.createTextOutput("OK");
    }
    if (pid === "MENU_MY_STATUS") {
      try {
        var db2 = getDatabaseData();
        var s2 = (db2.students || []).find(function(x) { return cleanPhoneNumber(x.phone) === cleanPhoneNumber(from); });
        if (s2) {
          var bal2 = (parseInt(s2.totalFee) || 0) - (parseInt(s2.advance) || 0) - (parseInt(s2.discount) || 0);
          var prog2 = getStudentProgress(s2);
          var balStr = bal2 > 0 ? "\u26A0 \u0ba8\u0bbf\u0bb2\u0bc1\u0bb5\u0bc8: \u20B9" + bal2 : "\u2705 \u0ba8\u0bbf\u0bb2\u0bc1\u0bb5\u0bc8 \u0b87\u0bb2\u0bcd\u0bb2\u0bc8";
          var llrStatusStr = (s2.llrStatus === "Yes" || s2.llrDate) ? "\u2705 LLR \u0baa\u0bc6\u0bb1\u0bcd\u0bb1\u0bc1\u0bb3\u0bcd\u0bb3\u0bc0\u0bb0\u0bcd" : "\u23F3 LLR \u0baa\u0bc6\u0bb1\u0bb5\u0bbf\u0bb2\u0bcd\u0bb2\u0bc8";

          // 1. Details card
          sendWhatsAppInteractiveButtons(from,
            "\uD83D\uDCCA *\u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bbf\u0ba9\u0bcd \u0bb5\u0bbf\u0bb5\u0bb0\u0b99\u0bcd\u0b95\u0bb3\u0bcd:*\n\n" +
            "\uD83D\uDC64 \u0baa\u0bc6\u0baf\u0bb0\u0bcd: *" + s2.name + "*\n" +
            "\uD83D\uDCB0 " + balStr + "\n" +
            "\uD83C\uDFAF Quiz: *" + ((parseInt(s2.quizMarks) || parseInt(s2.marks) || 0)) + "*\n" +
            "\uD83C\uDF9B " + llrStatusStr + "\n" +
            "\uD83D\uDEE3 \u0baa\u0baf\u0bbf\u0bb1\u0bcd\u0b9a\u0bbf: *" + prog2 + "% \u0bae\u0bc1\u0b9f\u0bbf\u0ba8\u0bcd\u0ba4\u0ba4\u0bc1*\n\n" +
            "\u0ba8\u0ba3\u0bcd\u0baa\u0ba9\u0bcd \u0b9f\u0bbf\u0bb0\u0bc8\u0bb5\u0bbf\u0b99\u0bcd \u0bb8\u0bcd\u0b95\u0bc2\u0bb2\u0bcd \uD83D\uDE97",
            [{ id: "GOTO_MENU", title: "\uD83D\uDD19 Main Menu" }]
          );

          // 2. Send Passport link (always)
          try {
            var portalUrl2 = ScriptApp.getService().getUrl() + "?id=" + s2.id;
            if (portalUrl2 && portalUrl2.indexOf("http") === 0) {
              sendWhatsAppMessage(from, "📱 உங்கள் டிஜிட்டல் பாஸ்போர்ட்:\n👉 " + portalUrl2);
            }
          } catch (pErr) {}

          // 3. Send LLR copy if available
          Utilities.sleep(800);
          var llrDocUrl = resolveLlrDocumentUrl_(s2);
          if (llrDocUrl) {
            try {
              var llrRes = sendWhatsAppDocumentMessage(from, llrDocUrl, "LLR_" + s2.name + ".pdf", "\uD83D\uDCCB \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bcd LLR \u0b95\u0bbe\u0baa\u0bcd\u0baa\u0bbf");
              if (!llrRes || llrRes.status !== "success") {
                sendWhatsAppMessage(from, "📄 LLR PDF இணைப்பு:\n" + llrDocUrl);
              }
            } catch(llrErr) {
              sendWhatsAppMessage(from, "📄 LLR PDF இணைப்பு:\n" + llrDocUrl);
            }
          } else {
            sendWhatsAppMessage(from, "📄 உங்கள் LLR PDF இன்னும் upload செய்யப்படவில்லை.\nஉதவி வேண்டுமெனில் Admin-ஐ தொடர்பு கொள்ளுங்கள்.");
          }

          // 4. 30-day rule: Calculate days since joining
          Utilities.sleep(800);
          var testStatus = String(s2.testStatus || "").trim();
          var testDate = s2.testDate || "";

          if (testStatus === "Pass") {
            sendWhatsAppInteractiveButtons(from,
              "\uD83C\uDF89 *\u0bb5\u0bbe\u0bb4\u0bcd\u0ba4\u0bcd\u0ba4\u0bc1\u0b95\u0bcd\u0b95\u0bb3\u0bc3!*\nRTO \u0b9f\u0bc6\u0bb8\u0bcd\u0b9f\u0bcd Pass \u0b86\u0b95\u0bbf\u0bb5\u0bbf\u0b9f\u0bcd\u0b9f\u0ba4\u0bc1! \uD83E\uDEAA\n\u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bcd License \u0bb5\u0bbf\u0bb0\u0bc8\u0bb5\u0bbf\u0bb2\u0bcd \u0bb5\u0bb0\u0bc1\u0bae\u0bcd.",
              [{ id: "ADM_HELP", title: "\uD83D\uDCDE Admin Call" }, { id: "GOTO_MENU", title: "\uD83D\uDD19 Main Menu" }]
            );
          } else {
            // Calculate days attended from joining date
            var daysCompleted = getDaysSinceStudentJoin_(s2);
            var daysRemaining = Math.max(0, 30 - daysCompleted);

            if (daysCompleted >= 30 || testDate) {
              // 30 days done OR date already fixed - show test date info
              if (testDate) {
                sendWhatsAppInteractiveButtons(from,
                  "📅 *RTO டெஸ்ட் தேதி:* " + testDate + "\n\n💪 தயார் ஆகுங்கள்! சந்தேகம் இருந்தால் Admin-ஐ அழைக்கவும்.",
                  [{ id: "ADM_HELP", title: "\uD83D\uDCDE Admin-\u0b90 call" }, { id: "GOTO_MENU", title: "\uD83D\uDD19 Main Menu" }]
                );
              } else {
                sendWhatsAppInteractiveButtons(from,
                  "🎉 *30 நாள் பயிற்சி முடிந்தது!* 💪\n\nஇப்போது நீங்கள் RTO டெஸ்ட் தேதிக்குத் தயாராக இருக்கிறீர்கள்.\n\nதேதி fix செய்ய Admin-ஐ அழைக்கவும் — நாங்களே arrange செய்து விடுவோம்!",
                  [{ id: "ADM_HELP", title: "\uD83D\uDCDE Date fix \u0baa\u0ba3\u0bcd\u0ba3\u0bb2\u0bbe\u0bae\u0bcd" }, { id: "GOTO_MENU", title: "\uD83D\uDD19 Main Menu" }]
                );
              }
            } else {
              // Less than 30 days - tell them how many days remaining
              sendWhatsAppInteractiveButtons(from,
                "🔆 *RTO டெஸ்ட் தேதி பற்றி...*\n\n" +
                "RTO டெஸ்ட் எடுக்க *30 நாள் பயிற்சி* முடிக்க வேண்டும்.\n" +
                "உங்களுக்கு இன்னும் *" + daysRemaining + " நாள்* மீதம் உள்ளது! ⏳\n\n" +
                "30 நாள் முடிந்தவுடன் எங்களை அழைக்கவும்.\nநாங்களே RTO டெஸ்ட் பதிவு செய்து விடுவோம்! 💪",
                [{ id: "ADM_HELP", title: "\uD83D\uDCDE Admin-\u0b90 call" }, { id: "GOTO_MENU", title: "\uD83D\uDD19 Main Menu" }]
              );
            }
          }
        } else {
          sendWhatsAppMessage(from, "\u0bae\u0ba9\u0bcd\u0ba9\u0bbf\u0b95\u0bcd\u0b95\u0bb5\u0bc1\u0bae\u0bcd! \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bcd \u0bb5\u0bbf\u0bb5\u0bb0\u0b99\u0bcd\u0b95\u0bb3\u0bcd \u0b95\u0bbf\u0b9f\u0bc8\u0b95\u0bcd\u0b95\u0bb5\u0bbf\u0bb2\u0bcd\u0bb2\u0bc8. \u0b85\u0b9f\u0bcd\u0bae\u0bbf\u0ba9\u0bc8\u0ba4\u0bcd \u0ba4\u0bcb\u0b9f\u0bb0\u0bcd\u0baa\u0bc1 \u0b95\u0bca\u0bb3\u0bcd\u0bb3\u0bb5\u0bc1\u0bae\u0bcd. \uD83D\uDE4F");
        }
      } catch(e3) {
        sendWhatsAppMessage(from, "\u0ba4\u0bb1\u0bcd\u0b95\u0bbe\u0bb2\u0bbf\u0b95\u0bae\u0bbe\u0b95 \u0b95\u0bbf\u0b9f\u0bc8\u0b95\u0bcd\u0b95\u0bb5\u0bbf\u0bb2\u0bcd\u0bb2\u0bc8. \u0b95\u0bca\u0b9e\u0bcd\u0b9a\u0bae\u0bcd \u0ba8\u0bc7\u0bb0\u0bae\u0bcd \u0baa\u0bbf\u0bb1\u0b95\u0bc1 \u0bae\u0bc1\u0baf\u0bb1\u0bcd\u0b9a\u0bbf\u0baf\u0bc1\u0b99\u0bcd\u0b95. \uD83D\uDE4F");
      }
      return ContentService.createTextOutput("OK");
    }
    if (pid === "MENU_ROAD_SAFETY" || pid === "ROAD_MARKINGS" || pid === "MANDATORY_SIGNS" || pid.indexOf("CARD_") === 0) {
      if (pid === "MENU_ROAD_SAFETY") {
        sendWhatsAppInteractiveButtons(from, "\uD83D\uDEA6 *\u0ba8\u0ba3\u0bcd\u0baa\u0ba9\u0bcd \u0b9a\u0bbe\u0bb2\u0bc8 \u0baa\u0bbe\u0ba4\u0bc1\u0b95\u0bbe\u0baa\u0bcd\u0baa\u0bc1 \u0bae\u0bc8\u0baf\u0bae\u0bcd*\n\n\u0b9a\u0bbe\u0bb2\u0bc8 \u0bb5\u0bbf\u0ba4\u0bbf\u0b95\u0bb3\u0bc8\u0baa\u0bcd \u0baa\u0bb1\u0bcd\u0bb1\u0bbf \u0b85\u0bb1\u0bbf\u0baf \u0bb5\u0bbf\u0bb0\u0bc1\u0bae\u0bcd\u0baa\u0bc1\u0bae\u0bcd \u0baa\u0b95\u0bc1\u0ba4\u0bbf\u0baf\u0bc8 \u0ba4\u0bc7\u0bb0\u0bcd\u0ba4\u0bc6\u0b9f\u0bc1\u0b95\u0bcd\u0b95\u0bb5\u0bc1\u0bae\u0bcd:", [{ id: "ROAD_MARKINGS", title: "\uD83D\uDEA7 \u0b9a\u0bbe\u0bb2\u0bc8\u0b95\u0bcd \u0b95\u0bcb\u0b9f\u0bc1\u0b95\u0bb3\u0bcd" }, { id: "MANDATORY_SIGNS", title: "\uD83D\uDED1 \u0bae\u0bc1\u0b95\u0bcd\u0b95\u0bbf\u0baf \u0b9a\u0bbf\u0ba9\u0bcd\u0ba9\u0b99\u0bcd\u0b95\u0bb3\u0bcd" }, { id: "GOTO_MENU", title: "\uD83D\uDD19 Main Menu" }]);
      } else if (pid === "ROAD_MARKINGS") {
        sendWhatsAppInteractiveButtons(from, "\uD83D\uDEE3\uFE0F *\u0b9a\u0bbe\u0bb2\u0bc8\u0b95\u0bcd \u0b95\u0bcb\u0b9f\u0bc1\u0b95\u0bb3\u0bcd*\n\n\u0b95\u0bc0\u0bb4\u0bc7 \u0b95\u0bbe\u0ba3 \u0bb5\u0bbf\u0bb0\u0bc1\u0bae\u0bcd\u0baa\u0bc1\u0bae\u0bcd \u0b95\u0bcb\u0b9f\u0bcd\u0b9f\u0bc8 \u0ba4\u0bc7\u0bb0\u0bcd\u0ba8\u0bcd\u0ba4\u0bc6\u0b9f\u0bc1\u0b95\u0bcd\u0b95\u0bb5\u0bc1\u0bae\u0bcd:", [{ id: "CARD_ZEBRA", title: "\uD83E\uDD93 Zebra Crossing" }, { id: "CARD_STOPLINE", title: "\uD83D\uDEA6 Stop Line" }, { id: "MENU_ROAD_SAFETY", title: "\uD83D\uDD19 Back" }]);
      } else if (pid === "MANDATORY_SIGNS") {
        sendWhatsAppInteractiveButtons(from, "\uD83D\uDED1 *\u0bae\u0bc1\u0b95\u0bcd\u0b95\u0bbf\u0baf \u0b9a\u0bbf\u0ba9\u0bcd\u0ba9\u0b99\u0bcd\u0b95\u0bb3\u0bcd*\n\n\u0b95\u0bc0\u0bb4\u0bc7 \u0ba4\u0bc7\u0bb0\u0bcd\u0ba4\u0bc6\u0b9f\u0bc1\u0b95\u0bcd\u0b95\u0bb5\u0bc1\u0bae\u0bcd:", [{ id: "CARD_STOP", title: "\uD83D\uDED1 STOP \u0b9a\u0bbf\u0ba9\u0bcd\u0ba9\u0bae\u0bcd" }, { id: "CARD_NO_OVERTAKE", title: "\uD83D\uDEB3 \u0b93\u0bb5\u0bb0\u0bcd-\u0b9f\u0bc7\u0b95\u0bcd \u0b95\u0bc2\u0b9f\u0bbe\u0ba4\u0bc1" }, { id: "MENU_ROAD_SAFETY", title: "\uD83D\uDD19 Back" }]);
      } else {
        sendRoadSafetyCard(from, pid);
      }
      return ContentService.createTextOutput("OK");
    }

    if (pid === "GOTO_MENU") {
      var isStudent3 = false; var student3 = null;
      try {
        var db3 = getDatabaseData();
        student3 = (db3.students || []).find(function(x) { return cleanPhoneNumber(x.phone) === cleanPhoneNumber(from); });
        isStudent3 = !!student3;
      } catch(dbErr) {}
      var wMsg3 = "\u0bb5\u0ba3\u0b95\u0bcd\u0b95\u0bae\u0bcd! \uD83D\uDE4F *\u0ba8\u0ba3\u0bcd\u0baa\u0ba9\u0bcd \u0b9f\u0bbf\u0bb0\u0bc8\u0bb5\u0bbf\u0b99\u0bcd \u0bb8\u0bcd\u0b95\u0bc2\u0bb2\u0bc1\u0b95\u0bcd\u0b95\u0bc1* \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bc8 \u0b85\u0ba9\u0bcd\u0baa\u0bc1\u0b9f\u0ba9\u0bcd \u0bb5\u0bb0\u0bb5\u0bc7\u0bb1\u0bcd\u0b95\u0bbf\u0bb1\u0bcb\u0bae\u0bcd.\n\n\u0bb5\u0bbe\u0b95\u0ba9\u0bae\u0bcd \u0b93\u0b9f\u0bcd\u0b9f\u0bc1\u0bb5\u0ba4\u0bc1 \u0bb5\u0bc6\u0bb1\u0bc1\u0bae\u0bcd \u0ba4\u0bbf\u0bb1\u0bae\u0bc8\u0baf\u0bb2\u0bcd\u0bb2, \u0b85\u0ba4\u0bc1 \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bbf\u0ba9\u0bcd \u0b9a\u0bc1\u0ba4\u0ba8\u0bcd\u0ba4\u0bbf\u0bb0\u0bae\u0bcd! \uD83D\uDEE3\uFE0F";
      if (isStudent3 && student3) {
        wMsg3 = "\u0bb5\u0ba3\u0b95\u0bcd\u0b95\u0bae\u0bcd *" + student3.name + "*! \uD83D\uDE4F \u0ba8\u0ba3\u0bcd\u0baa\u0ba9\u0bcd \u0b9f\u0bbf\u0bb0\u0bc8\u0bb5\u0bbf\u0b99\u0bcd \u0bb8\u0bcd\u0b95\u0bc2\u0bb2\u0bcd \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bc8 \u0b85\u0ba9\u0bcd\u0baa\u0bc1\u0b9f\u0ba9\u0bcd \u0bb5\u0bb0\u0bb5\u0bc7\u0bb1\u0bcd\u0b95\u0bbf\u0bb1\u0ba4\u0bc1. \uD83D\uDE97\uD83D\uDCA8";
      }
      var rows3 = [
        { id: "MENU_CAR", title: "\uD83D\uDE97 \u0b95\u0bbe\u0bb0\u0bcd \u0b93\u0b9f\u0bcd\u0b9f \u0b95\u0bb1\u0bcd\u0b95", description: "Car Training" },
        { id: "MENU_BIKE", title: "\uD83D\uDEF5 \u0b9f\u0bc2-\u0bb5\u0bc0\u0bb2\u0bb0\u0bcd \u0b95\u0bb1\u0bcd\u0b95", description: "Two-Wheeler Training" },
        { id: "MENU_RTO", title: "\uD83E\uDEAA RTO & E-Sevai", description: "RTO Services" },
        { id: "MENU_FEES", title: "\uD83D\uDCB0 \u0b95\u0b9f\u0bcd\u0b9f\u0ba3\u0bae\u0bcd & \u0ba8\u0bc7\u0bb0\u0bae\u0bcd", description: "Fees & Timings" },
        { id: "MENU_FAQ", title: "\u2753 \u0b9a\u0ba8\u0bcd\u0ba4\u0bc7\u0b95\u0b99\u0bcd\u0b95\u0bb3\u0bcd", description: "FAQ" }
      ];
      if (isStudent3) {
        rows3.push({ id: "MENU_MY_STATUS", title: "\uD83D\uDCCA \u0b8e\u0ba9\u0ba4\u0bc1 \u0bb5\u0bbf\u0bb5\u0bb0\u0bae\u0bcd", description: "Progress & Fees" });
        rows3.push({ id: "MENU_ROAD_SAFETY", title: "\uD83D\uDEA6 \u0b9a\u0bbe\u0bb2\u0bc8 \u0bb5\u0bbf\u0ba4\u0bbf\u0b95\u0bb3\u0bcd", description: "Road Safety" });
      }
      sendWhatsAppListMessage(from, wMsg3 + "\n\n\u0b95\u0bc0\u0bb4\u0bc7 \u0b89\u0b99\u0bcd\u0b95\u0bb3\u0bcd \u0ba4\u0bc7\u0bb5\u0bc8\u0baf\u0bc8 \u0ba4\u0bc7\u0bb0\u0bcd\u0ba8\u0bcd\u0ba4\u0bc6\u0b9f\u0bc1\u0b95\u0bcd\u0b95\u0bb5\u0bc1\u0bae\u0bcd \uD83D\uDC47", "Menu", [{ title: "\u0b9a\u0bc7\u0bb5\u0bc8\u0b95\u0bb3\u0bcd", rows: rows3 }]);
      return ContentService.createTextOutput("OK");
    }

    // --- QUIZ ---
    var isQuizInput = false; var isCorrect = false; var debugStr = "";
    try {
      var dbQ = getDatabaseData();
      var sQ = (dbQ.students || []).find(function(x) { return cleanPhoneNumber(x.phone) === cleanPhoneNumber(from); });
        
        // 🎯 NEW: High-Precision ID-Based Validation (Correct/Wrong embedded in Button ID)
        if (pid && (pid.includes("QUIZ_CORRECT") || pid.includes("QUIZ_WRONG"))) {
          isQuizInput = true;
          isCorrect = pid.includes("QUIZ_CORRECT");
          let parts = pid.split("_"); // QUIZ_CORRECT_RowIndex_ChoiceNo
          let rIdx = parseInt(parts[2]);
          let userChoice = parseInt(parts[3]);
          // debugStr = `\n\n_(DEBUG: R${rIdx}:U${userChoice}:IC${isCorrect ? 1 : 0})_`;
        }
        
        // 🎯 FALLBACK-A: If text contains option but no PID, use last quiz state.
        if (!isQuizInput) {
          let userChoiceByText = 0;
          let cleanedMsgA = cleanQuizText(msg);
          if (cleanedMsgA === "1" || msg.indexOf("\u0bae\u0bc1\u0ba4\u0bb2\u0bcd") !== -1) userChoiceByText = 1;
          else if (cleanedMsgA === "2" || msg.indexOf("\u0b87\u0bb0\u0ba3\u0bcd\u0b9f\u0bbe\u0bae\u0bcd") !== -1) userChoiceByText = 2;
          else if (cleanedMsgA === "3" || msg.indexOf("\u0bae\u0bc3\u0ba9\u0bcd\u0bb1\u0bbe\u0bae\u0bcd") !== -1 || msg.indexOf("\u0bae\u0bc3\u0ba9\u0bcd\u0bb1\u0bc1") !== -1) userChoiceByText = 3;
          if (userChoiceByText > 0) {
            let st = getLastQuizState_(from);
            if (st && st.correctNo) {
              isQuizInput = true;
              isCorrect = (parseInt(st.correctNo) === userChoiceByText);
            }
          }
        }

        // 🎯 FALLBACK-B: Old Sheet-based validation (if student types manually or uses old buttons)
        if (!isQuizInput && sQ && sQ.quizDay) {
          var qSheet = getDB().getSheetByName("QuizBank");
          var qData = qSheet ? qSheet.getDataRange().getValues() : [];
          var qDayCurrent = parseInt(sQ.quizDay) || 1;
          var qDayPrev = Math.max(1, qDayCurrent - 1);
          
          let srv = (sQ.service || "").toLowerCase();
          var cats = [];
          if (srv.includes("2 வீலர்") || srv.includes("2w")) cats = ["2W", "General"];
          else if (srv.includes("4 வீலர்") || srv.includes("4w")) cats = ["4W", "General"];
          else cats = ["General", "4W", "2W"];
          
          var qCfg = getQuizBankConfig();
          
          // Search questions for Day and Day-1
          let searchDays = [qDayPrev, qDayCurrent];
          let possibleMatches = [];
          for (let di = 0; di < searchDays.length; di++) {
            for (let ri = 0; ri < qData.length; ri++) {
              let r = qData[ri];
              if (parseInt(r[qCfg.day]) === searchDays[di] && (cats.indexOf(String(r[qCfg.cat]).trim()) !== -1)) {
                possibleMatches.push({ row: r, day: searchDays[di] });
              }
            }
          }

          if (possibleMatches.length > 0) {
            let userChoice = 0;
            let cleanedMsg = cleanQuizText(msg);
            
            if (cleanedMsg === "1" || msg.indexOf("\u0bae\u0bc1\u0ba4\u0bb2\u0bcd") !== -1) userChoice = 1;
            else if (cleanedMsg === "2" || msg.indexOf("\u0b87\u0bb0\u0ba3\u0bcd\u0b9f\u0bbe\u0bae\u0bcd") !== -1) userChoice = 2;
            else if (cleanedMsg === "3" || msg.indexOf("\u0bae\u0bc3\u0ba9\u0bcd\u0bb1\u0bbe\u0bae\u0bcd") !== -1 || msg.indexOf("\u0bae\u0bc3\u0ba9\u0bcd\u0bb1\u0bc1") !== -1) userChoice = 3;

            if (userChoice > 0) {
              isQuizInput = true;
              // Check against all possible questions of those days
              for (let i = 0; i < possibleMatches.length; i++) {
                let qRow = possibleMatches[i].row;
                let optA = String(qRow[qCfg.o1] || "");
                let optB = String(qRow[qCfg.o2] || "");
                let optC = String(qRow[qCfg.o3] || "");
                let correctText = String(qRow[qCfg.ansText] || "").trim();
                
                var correctNo = parseInt(correctText);
                if (isNaN(correctNo) || correctNo < 1 || correctNo > 3) {
                    let ct = cleanQuizText(correctText);
                    if (ct && ct === cleanQuizText(optA)) correctNo = 1;
                    else if (ct && ct === cleanQuizText(optB)) correctNo = 2;
                    else if (ct && ct === cleanQuizText(optC)) correctNo = 3;
                }
                
                if (userChoice === correctNo) {
                  isCorrect = true;
                  // debugStr = `\n\n_(DEBUG: FALLBACK: D${possibleMatches[i].day}:C${correctNo}:IC1)_`;
                  break;
                }
              }
            }
          }
        }
    } catch(qErr) {
      console.error("Quiz Logic Error: " + qErr.toString());
      // debugStr = "\n\n(DEBUG: Logic Error)";
    }

    if (isQuizInput) {
      if (isCorrect) {
        sendWhatsAppMessage(from, "🎉 சபாஷ்! சரியான விடை.\nஉங்களின் விழிப்புணர்வு மதிப்பெண் ஏறியுள்ளது. நாளை அடுத்த கேள்வியுடன் சந்திப்போம்! 🚗\n- நண்பன் டிரைவிங் ஸ்கூல்");
        updateMarksInBackground(from, true);
      } else {
        let reveal = resolveCorrectAnswerFromPid_(pid);
        if (!reveal) {
          let st = getLastQuizState_(from);
          if (st && st.correctAnswerText) {
            reveal = { correctNo: st.correctNo, correctAnswerText: st.correctAnswerText };
          }
        }
        let wrongMsg = "❌ தவறான விடை.\n";
        if (reveal && reveal.correctAnswerText) {
          wrongMsg += "✅ சரியான விடை: *" + reveal.correctAnswerText + "* (விடை " + (reveal.correctNo || "-") + ")\n";
        }
        wrongMsg += "பரவாயில்லை, நாளை மீண்டும் முயற்சி செய்வோம்! விழிப்புணர்வுடன் ஓட்டுங்கள். 🚦\n- நண்பன் டிரைவிங் ஸ்கூல்";
        sendWhatsAppMessage(from, wrongMsg);
        updateMarksInBackground(from, false);
      }
      return ContentService.createTextOutput("OK");
    }

    // --- LIVE BID ---
    if (/^\d+$/.test(msg) && parseInt(msg) >= 1000) {
      saveLiveBidToSheet(from, msg);
      sendWhatsAppMessage(from, "\uD83D\uDC4D \u0b8f\u0bb2\u0ba4\u0bcd \u0ba4\u0bcb\u0b95\u0bc8 (\u20B9" + msg + ") \u0baa\u0ba4\u0bbf\u0bb5\u0bbe\u0ba9\u0ba4\u0bc1. \u0b8f\u0bb2\u0bae\u0bcd \u0bae\u0bc1\u0b9f\u0bbf\u0ba8\u0bcd\u0ba4\u0ba4\u0bc1\u0bae\u0bcd \u0bae\u0bc1\u0b9f\u0bbf\u0bb5\u0bc1\u0b95\u0bb3\u0bcd \u0b85\u0bb1\u0bbf\u0bb5\u0bbf\u0b95\u0bcd\u0b95\u0baa\u0bcd\u0baa\u0b9f\u0bc1\u0bae\u0bcd.");
      return ContentService.createTextOutput("OK");
    }

    // --- BALANCE ---
    if (msg.indexOf("balance") !== -1 || msg.indexOf("\u0baa\u0bc7\u0bb2\u0ba9\u0bcd\u0bb8\u0bcd") !== -1) {
      try {
        var dbB = getDatabaseData();
        var sB = (dbB.students || []).find(function(x) { return cleanPhoneNumber(x.phone) === cleanPhoneNumber(from); });
        if (sB) {
          var balB = (parseInt(sB.totalFee) || 0) - (parseInt(sB.advance) || 0) - (parseInt(sB.discount) || 0);
          sendWhatsAppMessage(from, "\uD83D\uDCB0 *\u0ba8\u0bbf\u0bb2\u0bc1\u0bb5\u0bc8:* \u20B9" + balB + "\n\u0ba8\u0ba9\u0bcd\u0bb1\u0bbf! - \u0ba8\u0ba3\u0bcd\u0baa\u0ba9\u0bcd \u0b9f\u0bbf\u0bb0\u0bc8\u0bb5\u0bbf\u0b99\u0bcd \u0bb8\u0bcd\u0b95\u0bc2\u0bb2\u0bcd");
        }
      } catch(bErr) {}
      return ContentService.createTextOutput("OK");
    }

    // --- DEFAULT ---
    sendWhatsAppInteractiveButtons(from, "\u0bae\u0ba9\u0bcd\u0ba9\u0bbf\u0b95\u0bcd\u0b95\u0bb5\u0bc1\u0bae\u0bcd, \u0baa\u0bc1\u0bb0\u0bbf\u0baf\u0bb5\u0bbf\u0bb2\u0bcd\u0bb2\u0bc8. \u0b95\u0bc0\u0bb4\u0bc7 \u0bae\u0bc6\u0ba9\u0bc1 \u0baa\u0b9f\u0bcd\u0b9f\u0ba9\u0bc8\u0baa\u0bcd \u0baa\u0baf\u0ba9\u0bcd\u0baa\u0b9f\u0bc1\u0ba4\u0bcd\u0ba4\u0bb5\u0bc1\u0bae\u0bcd. \uD83D\uDC47", [{ id: "GOTO_MENU", title: "\uD83D\uDD19 Main Menu" }]);
  } catch(err) {
    console.error("doPost error: " + err.toString());
    try { PropertiesService.getScriptProperties().setProperty("WA_LAST_ERROR", String(err)); } catch (dbg3) {}
    try {
      var from2 = JSON.parse(e.postData.contents).entry[0].changes[0].value.messages[0].from;
      sendWhatsAppMessage(from2, "\u26A0\uFE0F \u0ba4\u0bca\u0bb4\u0bbf\u0bb2\u0bcd\u0ba8\u0bc1\u0b9f\u0bcd\u0baa\u0b95\u0bcd \u0b95\u0bcb\u0bb3\u0bbe\u0bb1\u0bc1. \u0b85\u0b9f\u0bcd\u0bae\u0bbf\u0ba9\u0bcd \u0b9a\u0bb0\u0bbf\u0b9a\u0bc6\u0baf\u0bcd\u0ba4\u0bc1 \u0b95\u0bca\u0ba3\u0bcd\u0b9f\u0bbf\u0bb0\u0bc1\u0b95\u0bcd\u0b95\u0bbf\u0bb1\u0bbe\u0bb0\u0bcd. \uD83D\uDE4F");
    } catch(e2) {}
  }
  return ContentService.createTextOutput("OK");
}
function saveLiveBidToSheet(phone, amount) {
    try {
        ensureChitSheets(); 
        let ss = getDB(); 
        let sh = ss.getSheetByName("Chit_Live_Bids");
        
        if (!sh) { 
            sh = ss.insertSheet("Chit_Live_Bids"); 
            sh.appendRow(["Phone", "BidAmount", "Time"]); 
        }
        
        sh.appendRow([phone, amount, new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" })]);
        flushNoop_();
    } catch(error) {}
}

/**
 * 🧪 TEST: Send Quiz to Admin manually
 * This allows admin to check how questions and images appear on WhatsApp.
 */
function testSendQuizToAdmin() {
    let adminPhone = "919942391870"; // Change this to your number if needed
    let testDay = 1; // Change this to test different days (e.g., 2, 3...)
    let testCategory = "General"; // Options: "2W", "4W", "General"
    
    console.log(`🧪 Testing Quiz for Admin: ${adminPhone} | Day: ${testDay} | Cat: ${testCategory}`);
    
    try {
        let quizSheet = getDB().getSheetByName("QuizBank");
        let quizData = quizSheet ? quizSheet.getDataRange().getValues() : [];
        
        if (quizData.length === 0) {
            console.error("❌ QuizBank sheet is empty or not found!");
            return;
        }

        // 🎯 Find questions for the test day and category
        let questions = [];
        for (let i = 0; i < quizData.length; i++) {
            let row = quizData[i];
            let rowCat = String(row[0]).trim();
            let rowDay = parseInt(row[1]);
            
            if (rowDay === testDay && (rowCat === testCategory || rowCat === "General")) {
                questions.push({ row: row, index: i + 1 });
            }
        }

        if (questions.length === 0) {
            console.warn(`⚠️ No questions found for Day ${testDay} and Category ${testCategory}`);
            return;
        }

        // 🎯 Send the first found question for testing
        let qObj = questions[0];
        let sMock = { name: "Admin Test", phone: adminPhone };
        let titlePrefix = "🧪 TEST QUIZ";
        
        let result = sendQuizStep(sMock, qObj.row, testDay, titlePrefix, qObj.index || 1);
        console.log("✅ Test result:", JSON.stringify(result));
        
    } catch (e) {
        console.error("❌ Test function error: " + e.toString());
    }
}

/**
 * 🎯 SEND LLR 30 DAY REMINDER (MANUAL)
 */
function sendLLR30DayReminder(studentId) {
    try {
        let dbData = getDatabaseData();
        // 🎯 FIX: Robust ID comparison (String vs Number)
        let s = (dbData.students || []).find(function(x) { return String(x.id) === String(studentId); });
        if (!s) return { status: 'error', message: 'மாணவர் கிடைக்கவில்லை! (ID: ' + studentId + ')' };
        
        let btns = [
          { id: "LLR_READY", title: "\u0ba8\u0bbe\u0ba9\u0bcd \u0ba4\u0baf\u0bbe\u0bb0\u0bcd" },
          { id: "LLR_CALL", title: "\u0b95\u0bbe\u0bb2\u0bcd \u0baa\u0ba3\u0bcd\u0ba3\u0bc1\u0b99\u0bcd\u0b95" }
        ];
        
        let msg = `வணக்கம் ${s.name}! 🎉
உங்கள் LLR பதிவு செய்து இன்று 30 நாள் நிறைவடைந்துள்ளது.

இப்போது நீங்கள் RTO டெஸ்ட் தேதி பதிவு செய்ய தயாராக இருக்கிறீர்கள். 🚗
தேதி fix செய்ய கீழே உள்ள பட்டனை அழுத்துங்கள் 👇`;
        
        let res = sendWhatsAppInteractiveButtons(s.phone, msg, btns);
        if (res && res.status === 'success') {
            return { status: 'success', message: '30 நாள் நினைவூட்டல் மெசேஜ் அனுப்பப்பட்டது! ✅' };
        } else {
            return { status: 'error', message: 'மெசேஜ் அனுப்ப முடியவில்லை. ' + (res.body || "") };
        }
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🎯 SEND LLR EXPIRE REMINDER (MANUAL)
 */
function sendLLRExpireReminder(studentId) {
    try {
        let dbData = getDatabaseData();
        // 🎯 FIX: Robust ID comparison (String vs Number)
        let s = (dbData.students || []).find(function(x) { return String(x.id) === String(studentId); });
        if (!s) return { status: 'error', message: 'மாணவர் கிடைக்கவில்லை! (ID: ' + studentId + ')' };
        
        let msg = "உங்கள் LLR இன்னும் சில நாட்களில் காலாவதியாக உள்ளது. தயவுசெய்து உடனே புதுப்பிக்கவும். - நண்பன் டிரைவிங் ஸ்கூல்";
        let res = sendTemplateMsg(s.phone, "bulk_announcement", [msg]);
        if (res && res.status === 'success') {
            return { status: 'success', message: 'LLR காலாவதி நினைவூட்டல் அனுப்பப்பட்டது! ✅' };
        } else {
            return { status: 'error', message: 'மெசேஜ் அனுப்ப முடியவில்லை. ' + (res.body || "") };
        }
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

function sendDailyQuiz() {
    return runDailyQuizWithSafeguard_(false);
}

function sendDailyQuizBackup() {
    return runDailyQuizWithSafeguard_(false);
}

function runDailyQuizNowForce() {
    return runDailyQuizWithSafeguard_(true);
}

function resetTodayQuizDispatchLock() {
    try {
        let props = PropertiesService.getScriptProperties();
        let today = getISTDate();
        props.deleteProperty("QUIZ_LAST_DISPATCH_DATE");
        props.deleteProperty("QUIZ_LAST_DISPATCH_AT");
        props.deleteProperty("QUIZ_LAST_DISPATCH_RESULT");
        return { status: "success", message: "இன்றைய Quiz dispatch lock reset செய்யப்பட்டது.", date: today };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function runDailyQuizWithSafeguard_(forceRun) {
    try {
        let props = PropertiesService.getScriptProperties();
        let now = new Date();
        let today = getISTDate();
        let hourIST = parseInt(Utilities.formatDate(now, "Asia/Kolkata", "H"), 10);
        let lastDate = String(props.getProperty("QUIZ_LAST_DISPATCH_DATE") || "");
        let force = !!forceRun;

        // Avoid accidental early sends before morning window (except force mode).
        if (!force && hourIST < 8) {
            return { status: "skipped", reason: "before_8am_ist", hourIST: hourIST, date: today };
        }

        // Idempotent daily protection: do not send twice in same day.
        if (!force && lastDate === today) {
            return { status: "skipped", reason: "already_sent_today", date: today };
        }

        let res = dailyMorningCron();
        if (res && res.status === "success") {
            props.setProperty("QUIZ_LAST_DISPATCH_DATE", today);
            props.setProperty("QUIZ_LAST_DISPATCH_AT", now.toISOString());
            props.setProperty("QUIZ_LAST_DISPATCH_RESULT", JSON.stringify(res));
        }
        return res;
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

// Run this once manually or from Settings to setup daily morning trigger (8:00 AM IST)
function setupDailyMorningTrigger() {
    try {
        let triggers = ScriptApp.getProjectTriggers();
        for (let i = 0; i < triggers.length; i++) {
            let func = triggers[i].getHandlerFunction();
            if (func === 'dailyMorningCron' || func === 'sendDailyQuiz' || func === 'sendDailyQuizBackup') {
                ScriptApp.deleteTrigger(triggers[i]);
            }
        }
        
        // Primary: Daily Quiz at 8:00 AM IST
        ScriptApp.newTrigger('sendDailyQuiz')
            .timeBased()
            .atHour(8)
            .nearMinute(0)
            .everyDays(1)
            .inTimezone("Asia/Kolkata")
            .create();

        // Backup: hourly trigger to catch missed run; safeguarded to send only once/day.
        ScriptApp.newTrigger('sendDailyQuizBackup')
            .timeBased()
            .everyHours(1)
            .inTimezone("Asia/Kolkata")
            .create();
            
        return { status: 'success', message: "✅ காலை 8:00 Quiz trigger + backup hourly catch-up trigger உருவாக்கப்பட்டது." };
    } catch(e) {
        return { status: 'error', message: "ட்ரிகர் அமைப்பதில் பிழை: " + e.toString() };
    }
}

function setupDailyEveningTrigger() {
    try {
        let triggers = ScriptApp.getProjectTriggers();
        for (let i = 0; i < triggers.length; i++) {
            if (triggers[i].getHandlerFunction() === 'sendDailyAdminSummary') {
                ScriptApp.deleteTrigger(triggers[i]);
            }
        }
        
        // Daily Admin Summary (7:00 PM IST)
        ScriptApp.newTrigger('sendDailyAdminSummary')
            .timeBased()
            .atHour(19)
            .nearMinute(0)
            .everyDays(1)
            .inTimezone("Asia/Kolkata")
            .create();
            
        return { status: 'success', message: "✅ மாலை 7:00 (Summary) ட்ரிகர் உருவாக்கப்பட்டுவிட்டது!" };
    } catch(e) {
        return { status: 'error', message: "ட்ரிகர் அமைப்பதில் பிழை: " + e.toString() };
    }
}

function setupAllDailyTriggers() {
    try {
        setupDailyMorningTrigger();
        setupDailyEveningTrigger();
        setupESevaiReminderTrigger();
        return { status: 'success', message: "✅ Quiz + Summary + E-Sevai reminder ட்ரிகர்கள் தயார்!" };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function dailyMorningCron() {
    console.log("🚀 Daily Morning Cron STARTED...");
    try {
        let dbData = getDatabaseData(); 
        let students = dbData.students;
        let quizSheet = getDB().getSheetByName("QuizBank"); 
        let quizData = quizSheet ? quizSheet.getDataRange().getValues() : [];
        let today = new Date(); 
        today.setHours(0,0,0,0);
        
        let tomorrowStr = getTomorrowDateYYYYMMDD(); 
        let sentCount = 0;

        for (let i = 0; i < students.length; i++) {
            try {
                let s = students[i];
                if (s.status === 'Deleted') continue;

                // --- 1. 🚦 SMART AWARENESS QUIZ (WORLD CLASS TARGETED FEED) ---
                // No longer just LLR_Admission - can be Everyone if desired. 
                // But for now keeping it for learners or until License_Completed.
                if (s.status !== 'License_Completed' && quizData.length > 0) {
                    let srv = (s.service || "").toLowerCase();
                    let quizIdx = getQuizDayByJoinDate_(s);
                    
                    // Logic: Send 3 Questions every morning
                    // 2W/4W Users: 2 category-specific questions + 1 General question = 3 total
                    
                    let targetCats = [];
                    if (srv.includes("2 வீலர்") || srv.includes("2w")) targetCats.push("2W");
                    else if (srv.includes("4 வீலர்") || srv.includes("4w")) targetCats.push("4W");
                    else if (srv.includes("combo") || srv.includes("duo")) {
                        // For combo, use day-based selection (alternate or both)
                        targetCats.push(quizIdx % 2 === 0 ? "4W" : "2W");
                    }
                    if (targetCats.length === 0) targetCats.push("General");

                    // 🎯 Get questions for today with their original row indexes
                    let todayQuestions = [];
                    for (let ri = 0; ri < quizData.length; ri++) {
                        let row = quizData[ri];
                        let rowCat = String(row[0]).trim();
                        let rowDay = parseInt(row[1]);
                        if (rowDay === quizIdx && (targetCats.includes(rowCat) || rowCat === "General")) {
                            todayQuestions.push({ row: row, index: ri + 1 });
                        }
                    }

                    // 🎯 Sort: Target Category questions first, then General
                    todayQuestions.sort((a, b) => {
                        let catA = String(a.row[0]).trim();
                        let catB = String(b.row[0]).trim();
                        if (targetCats.includes(catA) && !targetCats.includes(catB)) return -1;
                        if (!targetCats.includes(catA) && targetCats.includes(catB)) return 1;
                        return 0;
                    });

                    // 🎯 Send 3 Questions (if available)
                    for (let q = 0; q < todayQuestions.length && q < 3; q++) {
                        let qObj = todayQuestions[q];
                        let qRow = qObj.row;
                        let rowIndex = qObj.index;
                        
                        let titlePrefix = (String(qRow[0]).trim() === "General") ? "🚦 பொது விழிப்புணர்வு" : "🎯 இன்றைய விசேஷ வினா";
                        sendQuizStep(s, qRow, quizIdx, titlePrefix, rowIndex);
                        Utilities.sleep(3000); // 3 seconds delay between questions
                    }

                    // Answer Reveal for YESTERDAY (Psychology)
                    if (quizIdx > 1) {
                        let prevIdx = quizIdx - 1;
                        revealYesterdayAnswers(s, quizData, prevIdx, targetCats);
                    }

                    // Keep state synced with real day progression from join date
                    s.quizDay = quizIdx + 1; 
                    updateStudentDataSilent(s);
                    sentCount++;
                }

                // --- 2. ALUMNI ANNIVERSARY ---
                if (s.status === 'License_Completed' && s.dateJoined) {
                    let parts = s.dateJoined.split('/');
                    if (parts.length === 3) {
                        let joinDate = new Date(parts[2], parts[1]-1, parts[0]); 
                        joinDate.setHours(0,0,0,0);
                        let diffDays = Math.round((today - joinDate) / (1000 * 60 * 60 * 24));
                        if (diffDays === 365 || diffDays === 730 || diffDays === 1095) {
                            sendTemplateWithParamFallback(s.phone, "alumni_anniversary", [[String(s.name || "நண்பரே")], []], null);
                        }
                    }
                }

                // --- 3. RTO TEST TOMORROW ALERT ---
                if (s.testStatus === 'Pending' && s.testDate === tomorrowStr) {
                    let cfg = getTemplateAndReminderConfig();
                    sendTemplateWithParamFallback(s.phone, cfg.rtoTomorrowTemplate || "rto_test_tomorrow", [[String(s.name || "-")], []], null);
                }

            } catch(loopErr) {
                console.error("Cron Loop Error for Student " + i + ": " + loopErr);
            }
        }
        console.log("✅ Daily Morning Cron COMPLETED. Sent: " + sentCount);
        return { status: 'success', count: sentCount };
    } catch(e) { 
        console.error("CRITICAL CRON ERROR: " + e.toString());
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🛠 Internal Helper: Send a single Quiz message
 */
function sendQuizStep(s, qRow, dayNo, titlePrefix, rowIndex) {
    try {
        let cfg = getTemplateAndReminderConfig();
        let qCfg = getQuizBankConfig(); // NEW: Get dynamic column indexes
        let optA = String(qRow[qCfg.o1] || "");
        let optB = String(qRow[qCfg.o2] || "");
        let optC = String(qRow[qCfg.o3] || "");
        let correctText = String(qRow[qCfg.ansText] || "").trim();

        // 🎯 ROBUST: Determine correctNo from Column G (Number or Text)
        var correctNo = parseInt(correctText);
        if (isNaN(correctNo) || correctNo < 1 || correctNo > 3) {
            let ct = cleanQuizText(correctText);
            if (ct && ct === cleanQuizText(optA)) correctNo = 1;
            else if (ct && ct === cleanQuizText(optB)) correctNo = 2;
            else if (ct && ct === cleanQuizText(optC)) correctNo = 3;
            else correctNo = 1; // Final fallback if absolutely no match
        }
        
        let quizHeader = `${titlePrefix} - (நாள் ${dayNo})`;
        let qText = String(qRow[qCfg.ques] || "");
        let imgUrl = qCfg.img !== -1 ? String(qRow[qCfg.img] || "").trim() : "";
        
        console.log(`🖼️ Quiz Image Debug: Day ${dayNo} | RowIndex: ${rowIndex} | imgUrl: ${imgUrl}`);
        
        // 🎯 Use Interactive Buttons with Header Image for 100% accurate validation and better UX
        let msg = quizHeader + ` 🚗🚦\n\n${qText}\n\n1️⃣ ${optA}\n2️⃣ ${optB}\n3️⃣ ${optC}\n\n_சரியான விடையைத் தேர்ந்தெடுக்கவும்_ 👇`;
        
        // If image is available, pass it as headerUrl to sendQuizInteractiveMsg
        let headerUrl = (imgUrl && imgUrl.startsWith("http")) ? imgUrl : null;
        return sendQuizInteractiveMsg(s.phone, msg, headerUrl, correctNo, dayNo, s.name, qRow, rowIndex);
        
    } catch(e) { 
        try { logBotActivity("QUIZ_STEP_ERROR", e.toString()); } catch(logErr){}
        return null; 
    }
}

/**
 * 🛠 Internal Helper: Reveal logic for World Class Engagement
 */
function revealYesterdayAnswers(s, quizData, prevIdx, cats) {
    try {
        let relevantRows = quizData.filter(row => (cats.includes(String(row[0]).trim()) || String(row[0]).trim() === "General") && parseInt(row[1]) === prevIdx);
        for (let row of relevantRows) {
            let correctAns = String(row[6] || "").trim();
            let exp = row[8] ? String(row[8]) : "பாதுகாப்பாக ஓட்டுங்கள்!";
            let revealMsg = `✅ *நேற்றைய விடை (${row[0]}):*\n\n❓ ${row[2]}\n✔️ *விடை: ${correctAns}*\n💡 *விளக்கம்:* ${exp}`;
            sendWhatsAppMessage(s.phone, revealMsg);
            Utilities.sleep(1000);
        }
    } catch(e) {}
}

// 🌟 FEATURE: DAILY EVENING SUMMARY (7:00 PM - 8:00 PM)
function sendDailyAdminSummary() {
    Logger.log("🚀 Starting sendDailyAdminSummary...");
    try {
        try { logCronRun('sendDailyAdminSummary', 'START', '', {}); } catch (e) {}
        let today = getISTDate();
        Logger.log("📅 Date: " + today);
        let dbData = getDatabaseData();
        if (!dbData || !dbData.students) {
            Logger.log("⚠️ ERROR: getDatabaseData() returned null or invalid data.");
            return { status: 'error', message: 'DB Fetch Failed' };
        }
        let students = dbData.students;
        let expenses = dbData.expenses;
        
        let settings = getAppSettings();
        let splits = settings.serviceSplits || {};
        
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
        
        students.forEach(s => {
            
            // Total Pending Balance Logic — same as frontend (totalFee - advance - discount)
            if (s.status !== 'License_Completed' && s.status !== 'Hold') {
                // 🚫 Skip Deleted/Inactive/OLD/Enquiry (same as practical dashboard expectation)
                if (s.status === 'Deleted' || s.status === 'Inactive' || s.type === 'Enquiry' || String(s.id || '').includes('OLD')) return;
                let adv = parseInt(s.advance) || 0;
                let disc = parseInt(s.discount) || 0;
                let tFee = parseInt(s.totalFee) || 0;
                // Fallback: if totalFee missing, compute from feeSplit
                if (tFee === 0) {
                    let sSplit = s.feeSplit || splits[s.service] || {llr:0, train:0, test:0};
                    tFee = (parseInt(sSplit.llr)||0) + (parseInt(sSplit.train)||0) + (parseInt(sSplit.test)||0);
                }
                let bal = tFee - adv - disc;
                // Guard against abnormal garbage values
                if (bal > 0 && bal < 1000000) totalPendingBalance += bal;
            }
            
            // Today's Payments
            let pays = Array.isArray(s.paymentHistory) ? s.paymentHistory : [];
            pays.forEach(p => {
                if (p.date === today) {
                    let amt = (parseInt(p.amount) || 0);
                    colToday += amt;
                    let note = (p.note || "").toLowerCase();
                    if (note.includes("ரஞ்சித்")) colRanjith += amt;
                    else if (note.includes("நந்தகுமார்")) colNandha += amt;
                    if ((s.service || "").toLowerCase().includes("test") || note.includes("test") || note.includes("டெஸ்ட்")) {
                        colTest += amt;
                    }
                }
            });
            
            // Attendance
            let att = Array.isArray(s.attendanceHistory) ? s.attendanceHistory : [];
            att.forEach(a => { if (a.includes(today) && a.includes('✅')) classesToday++; });
            
            // Admissions
            if (s.dateJoined === today) {
                countToday++;
                let srv = (s.service || "").toLowerCase();
                if (srv.includes("combo")) countCombo++;
                else if (srv.includes("2")) count2W++;
                else if (srv.includes("4")) count4W++;
                else if (s.type === 'Test_Admission' || srv.includes("test")) countTest++;
                else if (s.type === 'Training_Admission') countTrainOnly++;
            }
        });
        
        // Expenses
        expenses.forEach(e => {
            let isInc = (e.cat || "").includes("வரவு") || (e.cat || "").includes("(In)");
            if (!isInc && e.date === today && !(e.cat || "").includes("Spot Pending")) {
                let amt = (parseInt(e.amt) || 0);
                expToday += amt;
                let desc = (e.desc || "").toLowerCase();
                let cat = (e.cat || "").toLowerCase();
                if (desc.includes("test") || desc.includes("டெஸ்ட்") || cat.includes("test") || cat.includes("டெஸ்ட்")) expTest += amt;
            }
        });
        
        let props = PropertiesService.getScriptProperties();
        let kmDt = props.getProperty('NANBAN_KM_TODAY_DATE') || '';
        let kmVal = (kmDt === today) ? props.getProperty('NANBAN_KM_TODAY_VALUE') : '0';
        
        let cfg = getTemplateAndReminderConfig();
        let templateName = cfg.dayCloseTemplate || "day_close_report";
        
        let summaryMsg = `📊 *இன்றைய அறிக்கை (${today})*\n\n` +
            `📝 பதிவுகள்: ${countToday} (2W:${count2W}, 4W:${count4W}, Tst:${countTest}, Cmb:${countCombo})\n` +
            `🚗 பயிற்சி: ${classesToday} பேர் | ${kmVal} KM\n` +
            `💰 வசூல்: ₹${colToday} (R:${colRanjith}, N:${colNandha})\n` +
            `🔴 செலவு: ₹${expToday}\n` +
            `💵 கை இருப்பு: *₹${colToday - expToday}*\n` +
            `🎯 டெஸ்ட்: In:₹${colTest} | Out:₹${expTest}\n` +
            `📉 மொத்த நிலுவை: ₹${totalPendingBalance}\n\n` +
            `- நண்பன் ERP ஆட்டோமேஷன் 🤖`;
            
        // Use Template if configured
        notifyAdmins(summaryMsg);
        
        // Evening test reminders to students
        try {
            let tomorrowStr = getTomorrowDateYYYYMMDD();
            let testStudents = students.filter(s => s.status !== 'Deleted' && s.testStatus === 'Pending' && s.testDate === tomorrowStr && s.phone);
            testStudents.forEach(function(s, idx) {
                try {
                    if (idx > 0) Utilities.sleep(2000);
                    let testMsg = `🌙 *வணக்கம் ${s.name}!*\n\nநாளை உங்களுக்கு *RTO டெஸ்ட்* இருக்கிறது! 🚗\n\n⏰ *காலை 8:00 மணிக்கு* முன்பாக வந்துவிடுங்கள்.\n\n📋 *கார்டு, போட்டோ, LLR* கொண்டு வர மறக்காதீங்க! 💪`;
                    sendWhatsAppMessage(s.phone, testMsg);
                } catch(msgErr) {}
            });
        } catch(eveningTestErr) {}
        
        try { logCronRun('sendDailyAdminSummary', 'SUCCESS', 'Summary sent.', { date: today, col: colToday, km: kmVal }); } catch (e) {}
        return { status: 'success' };
    } catch(e) {
        Logger.log("Daily Evening Summary Error: " + e.toString());
        try { logCronRun('sendDailyAdminSummary', 'ERROR', e.toString(), { stage: 'main' }); } catch (e2) {}
        return { status: 'error' };
    }
}

function ensureCronLogSheet() {
    let ss = getDB();
    let sh = ss.getSheetByName("Cron_Log");
    if (sh) return sh;
    sh = ss.insertSheet("Cron_Log");
    sh.appendRow(["Timestamp", "Time", "Function", "Status", "Message", "Meta"]);
    return sh;
}

function logCronRun(fnName, status, message, metaObj) {
    try {
        let sh = ensureCronLogSheet();
        let ts = new Date();
        let d = getISTDate();
        let t = Utilities.formatDate(ts, "GMT+5:30", "HH:mm:ss");
        let meta = "";
        try { meta = metaObj ? JSON.stringify(metaObj) : ""; } catch (e) { meta = ""; }
        sh.appendRow([d, t, String(fnName || ""), String(status || ""), String(message || ""), meta]);
        flushNoop_();
    } catch (e) {}
}

function runDailyAdminSummaryNowAction(loggedBy) {
    try {
        if (loggedBy && !isPrivilegedName(loggedBy)) return { status: 'error', message: 'Not allowed' };
        let r = sendDailyAdminSummary();
        return r || { status: 'error' };
    } catch (e) {
        try { logCronRun('runDailyAdminSummaryNowAction', 'ERROR', e.toString(), {}); } catch (e2) {}
        return { status: 'error', message: e.toString() };
    }
}

/**
 * ===============================================================
 * 🧪 DIAGNOSTIC TEST FUNCTION SET (ONE-CLICK HEALTH REPORT)
 * ===============================================================
 */

function testAdminAlerts() {
    try {
        let cfg = getTemplateAndReminderConfig();
        let templateName = cfg.adminUniversalTemplate || "admin_universal_alert";
        let adminList = [].concat(ADMINS || []);
        let pPhone = cleanPhoneNumber(cfg.partnerPhone || "");
        if (pPhone && adminList.indexOf(pPhone) === -1) adminList.push(pPhone);

        // Normalize unique admin numbers
        let unique = {};
        let targets = [];
        adminList.forEach(function(p) {
            let c = cleanPhoneNumber(p);
            if (c && !unique[c]) {
                unique[c] = true;
                targets.push(c);
            }
        });

        let sent = 0;
        let failed = 0;
        let details = [];
        let testMsg = "🧪 Admin Alert Diagnostic Test - Nanban ERP";

        for (let i = 0; i < targets.length; i++) {
            let ph = targets[i];
            let ok = false;
            let reason = "";
            try {
                let res = sendTemplateMsg(ph, templateName, [testMsg], null);
                if (res && res.status === 'success') {
                    ok = true;
                } else {
                    // fallback direct message
                    let fb = sendWhatsAppMessage(ph, "🔔 *Admin Test Alert*\n\n" + testMsg);
                    ok = !!(fb && fb.status === 'success');
                    reason = ok ? "template-failed-but-fallback-success" : "template-and-fallback-failed";
                }
            } catch (e1) {
                reason = String(e1);
            }

            if (ok) sent++; else failed++;
            details.push({ phone: ph, ok: ok, reason: reason || "template-success" });
            if (i < targets.length - 1) Utilities.sleep(1200);
        }

        return {
            status: failed === 0 ? 'success' : 'partial',
            targets: targets.length,
            sent: sent,
            failed: failed,
            details: details
        };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function testDailyQuizDayMapping() {
    try {
        let db = getDatabaseData();
        let students = (db && Array.isArray(db.students)) ? db.students : [];

        let checked = 0;
        let mismatch = 0;
        let invalidDate = 0;
        let examples = [];

        students.forEach(function(s) {
            if (!s || s.status === 'Deleted') return;
            if (s.status === 'License_Completed') return;

            checked++;
            let d = parseStudentDate_(s.dateJoined);
            if (!d) {
                invalidDate++;
                if (examples.length < 20) {
                    examples.push({
                        id: s.id || "",
                        name: s.name || "",
                        issue: "invalid-dateJoined",
                        dateJoined: s.dateJoined || ""
                    });
                }
                return;
            }

            let expected = getQuizDayByJoinDate_(s);
            let stored = parseInt(s.quizDay) || 1;
            if (expected !== stored) {
                mismatch++;
                if (examples.length < 20) {
                    examples.push({
                        id: s.id || "",
                        name: s.name || "",
                        dateJoined: s.dateJoined || "",
                        expectedQuizDay: expected,
                        storedQuizDay: stored
                    });
                }
            }
        });

        return {
            status: 'success',
            checked: checked,
            mismatch: mismatch,
            invalidDate: invalidDate,
            ok: (mismatch === 0 && invalidDate === 0),
            samples: examples
        };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * Auto-fix quizDay using dateJoined-based day mapping.
 * - dryRun=true  : only report
 * - dryRun=false : update students in DB
 */
function fixQuizDayMappingFromJoinDate(dryRun, runBy) {
    try {
        if (runBy && !isPrivilegedName(runBy)) {
            return { status: 'error', message: 'Not allowed' };
        }
        let onlyReport = (dryRun !== false);
        let db = getDatabaseData();
        let students = (db && Array.isArray(db.students)) ? db.students : [];
        let changed = 0;
        let invalidDate = 0;
        let samples = [];

        for (let i = 0; i < students.length; i++) {
            let s = students[i];
            if (!s || s.status === 'Deleted' || s.status === 'License_Completed') continue;
            let d = parseStudentDate_(s.dateJoined);
            if (!d) { invalidDate++; continue; }
            let expected = getQuizDayByJoinDate_(s);
            let current = parseInt(s.quizDay) || 1;
            if (expected !== current) {
                changed++;
                if (samples.length < 30) {
                    samples.push({
                        id: s.id || "",
                        name: s.name || "",
                        oldQuizDay: current,
                        newQuizDay: expected,
                        dateJoined: s.dateJoined || ""
                    });
                }
                if (!onlyReport) {
                    s.quizDay = expected;
                    updateStudentData(s);
                }
            }
        }

        return {
            status: 'success',
            dryRun: onlyReport,
            changed: changed,
            invalidDate: invalidDate,
            samples: samples
        };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function testPendingSummaryBreakdown() {
    try {
        let db = getDatabaseData();
        let students = (db && Array.isArray(db.students)) ? db.students : [];
        let settings = getAppSettings();
        let splits = settings.serviceSplits || {};

        let totalPending = 0;
        let rawPending = 0;
        let excluded = {
            deleted: 0,
            inactive: 0,
            enquiry: 0,
            hold: 0,
            completed: 0,
            oldId: 0,
            abnormal: 0
        };
        let topPending = [];

        students.forEach(function(s) {
            if (!s) return;
            let adv = parseInt(s.advance) || 0;
            let disc = parseInt(s.discount) || 0;
            let tFee = parseInt(s.totalFee) || 0;
            if (tFee === 0) {
                let sp = s.feeSplit || splits[s.service] || { llr: 0, train: 0, test: 0 };
                tFee = (parseInt(sp.llr) || 0) + (parseInt(sp.train) || 0) + (parseInt(sp.test) || 0);
            }
            let bal = tFee - adv - disc;
            if (bal > 0) rawPending += bal;

            if (s.status === 'Deleted') { excluded.deleted++; return; }
            if (s.status === 'Inactive') { excluded.inactive++; return; }
            if (s.type === 'Enquiry') { excluded.enquiry++; return; }
            if (s.status === 'Hold') { excluded.hold++; return; }
            if (s.status === 'License_Completed') { excluded.completed++; return; }
            if (String(s.id || '').indexOf('OLD') !== -1) { excluded.oldId++; return; }
            if (bal >= 1000000) { excluded.abnormal++; return; }
            if (bal <= 0) return;

            totalPending += bal;
            topPending.push({
                id: s.id || "",
                name: s.name || "",
                phone: s.phone || "",
                balance: bal,
                service: s.service || "",
                status: s.status || ""
            });
        });

        topPending.sort(function(a, b) { return b.balance - a.balance; });

        return {
            status: 'success',
            totalPending: totalPending,
            rawPending: rawPending,
            differenceByFilters: rawPending - totalPending,
            excluded: excluded,
            topPending: topPending.slice(0, 25)
        };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * One-click health report runner
 * - includeAdminMessageTest=true என்றால் test alerts அனுப்பும்
 * - runBy optional: privileged check
 */
function runDiagnosticHealthReport(includeAdminMessageTest, runBy) {
    try {
        if (runBy && !isPrivilegedName(runBy)) {
            return { status: 'error', message: 'Not allowed' };
        }

        let report = {
            status: 'success',
            runAt: new Date().toISOString(),
            includeAdminMessageTest: !!includeAdminMessageTest,
            adminAlertTest: { status: 'skipped' },
            quizDayMapping: testDailyQuizDayMapping(),
            pendingSummary: testPendingSummaryBreakdown()
        };

        if (includeAdminMessageTest) {
            report.adminAlertTest = testAdminAlerts();
        }

        // concise summary for admins
        let q = report.quizDayMapping || {};
        let p = report.pendingSummary || {};
        let a = report.adminAlertTest || {};
        let msg =
            `🧪 *System Health Report*\n` +
            `🕒 ${getISTDate()}\n\n` +
            `📚 Quiz Day Mapping:\n` +
            `• Checked: ${q.checked || 0}\n` +
            `• Mismatch: ${q.mismatch || 0}\n` +
            `• Invalid Date: ${q.invalidDate || 0}\n\n` +
            `💰 Pending Summary:\n` +
            `• Filtered Pending: ₹${p.totalPending || 0}\n` +
            `• Raw Pending: ₹${p.rawPending || 0}\n` +
            `• Filter Difference: ₹${p.differenceByFilters || 0}\n\n` +
            `🔔 Admin Alert Test:\n` +
            `• Status: ${a.status || 'skipped'}\n` +
            `• Sent: ${a.sent || 0} / ${a.targets || 0}`;

        notifyAdmins(msg);
        return report;
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

// ✅ Resend Today's Messages to all new students added today
function resendTodayMessagesAction(loggedBy) {
    try {
        let today = getISTDate();
        let db = getDatabaseData();
        let students = db.students || [];
        let count = 0;
        let skipped = 0;
        let portalBase = ScriptApp.getService().getUrl();

        students.forEach(function(s) {
            if (s.status === 'Deleted') return;
            // Check if student was created/added today (DD/MM/YYYY)
            let sDt = s.dateJoined || s.createdDate || s.date || '';
            let sDtStr = String(sDt);
            
            // Handle both exact match and .includes for timestamp strings
            if (!sDtStr.includes(today)) return;
            if (!s.phone) { skipped++; return; }

            try {
                let portalUrl = portalBase + '?id=' + s.id;
                let bal = (parseInt(s.totalFee) || 0) - (parseInt(s.advance) || 0) - (parseInt(s.discount) || 0);
                let directMsg = '';

                if (s.type === 'Enquiry') {
                    directMsg = `🔔 வணக்கம் ${s.name}!\n\nநண்பன் டிரைவிங் ஸ்கூலில் நீங்கள் பதிவு செய்துள்ளீர்கள்.\n\n📱 உங்கள் விவரங்கள்:\n👉 ${portalUrl}\n\nவிரைவில் தொடர்பு கொள்வோம்! 🚗\n- நண்பன் டிரைவிங் ஸ்கூல்`;
                } else {
                    directMsg = `🎉 வணக்கம் ${s.name},\nநண்பன் டிரைவிங் ஸ்கூலில் உங்கள் பதிவு வெற்றிகரமாக முடிந்தது!\n\nபயிற்சி: ${s.service}\nகட்டணம்: ₹${s.totalFee}\nஅட்வான்ஸ்: ₹${s.advance}\nமீதம்: ₹${bal}\n\n📱 உங்கள் டிஜிட்டல் பாஸ்போர்ட்:\n👉 ${portalUrl}\n\nவிரைவில் உங்களை அழைப்போம். 🚗`;
                }

                sendWhatsAppMessage(s.phone, directMsg);
                count++;
                if (count % 3 === 0) Utilities.sleep(2000);
            } catch (e) {
                skipped++;
            }
        });

        notifyAdmins(`📨 *இன்றைய மெசேஜ் மீண்டும் அனுப்பப்பட்டது!*\n\nமொத்தம்: ${count + skipped} | அனுப்பப்பட்டது: ${count} | தவறியது: ${skipped}\n\nBy: ${loggedBy || 'Admin'}`);
        return { status: 'success', msg: `✅ ${count} மாணவர்களுக்கு மெசேஜ் அனுப்பப்பட்டது!`, count: count, skipped: skipped };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function getTriggerStatusAction() {
    try {
        let triggers = ScriptApp.getProjectTriggers();
        let items = [];
        let hasDailyMorning = false;
        let hasDailyEvening = false;
        
        for (let i = 0; i < triggers.length; i++) {
            let tr = triggers[i];
            let handler = tr.getHandlerFunction();
            if (handler === 'dailyMorningCron') hasDailyMorning = true;
            if (handler === 'sendDailyAdminSummary') hasDailyEvening = true;

            items.push({
                handler: handler,
                source: String(tr.getTriggerSource()),
                eventType: String(tr.getEventType()),
                active: true
            });
        }
        return { 
            status: 'success', 
            items: items, 
            summary: {
                dailyMorning: hasDailyMorning,
                dailyEvening: hasDailyEvening
            }
        };
    } catch (e) {
        return { status: 'error', message: e.toString(), items: [] };
    }
}

function runQuizDiagnosticsAction() {
    try {
        let db = getDB();
        let qSheet = db.getSheetByName("QuizBank") || db.getSheetByName("Quiz Bank") || db.getSheetByName("quizbank");
        if (!qSheet) return { status: 'error', message: '❌ "QuizBank" sheet not found in spreadsheet. please name the sheet "QuizBank"' };
        
        let qData = qSheet.getDataRange().getValues();
        if (qData.length <= 1) return { status: 'error', message: '⚠️ "QuizBank" has no questions (only header or empty).' };
        
        let qCfg = getQuizBankConfig();
        let breakdown = {};
        let warnings = [];
        
        for (let i = 1; i < qData.length; i++) {
            let cat = String(qData[i][qCfg.cat] || "Unknown").trim();
            if (!cat) {
              warnings.push(`Row ${i+1}: Category is empty.`);
              cat = "Unknown";
            }
            breakdown[cat] = (breakdown[cat] || 0) + 1;
        }
        
        let stats = { 
            totalQuestions: qData.length - 1, 
            categoriesCount: Object.keys(breakdown).length,
            categoryBreakdown: breakdown 
        };
        
        return { 
            status: 'success', 
            message: `✅ "QuizBank" OK: ${stats.totalQuestions} questions found.`,
            stats: stats,
            warnings: warnings
        };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🧪 MANUAL TEST: Run this to verify the daily_quiz_btn template.
 * It uses the sender's own number or a provided test number.
 */
function testDailyQuizForAdmin(testPhone) {
    let phone = testPhone || "919942391870"; // Default or override
    let dummyStudent = { name: "ரஞ்சித்", phone: phone, quizDay: 1 };
    let qHeader = "🚦 நண்பன் தினசரி விழிப்புணர்வு - Day 1 🚦";
    let qText = "சிகப்பு விளக்கு எதைக் குறிக்கிறது?";
    let o1 = "நிற்க வேண்டும்";
    let o2 = "செல்ல வேண்டும்";
    let o3 = "கவனிக்க வேண்டும்";
    
    let dummyQRow = [
      "General", // Category (0)
      1,         // Day (1)
      qText,     // Question (2)
      o1,        // A (3)
      o2,        // B (4)
      o3,        // C (5)
      o1,        // Correct text (6)
      "https://i.postimg.cc/B6MY8zGj/Nanban.jpg", // Image URL (7)
      "சிகப்பு விளக்கு நிறுத்தத்தைக் குறிக்கிறது." // Explanation (8)
    ];
    
    console.log("🧪 Testing daily_quiz_btn for: " + phone);
    let res = sendQuizStep(dummyStudent, dummyQRow, 1, "🚦 நண்பன் தினசரி விழிப்புணர்வு", 1);
    console.log("Response: " + JSON.stringify(res));
    return res;
}

// 🌐 Check WhatsApp Connection (Token + Status)
function checkWhatsAppConnectionAction() {
    try {
        let url = "https://graph.facebook.com/v20.0/" + WA_PHONE_ID;
        let options = {
            "method": "get",
            "headers": { "Authorization": "Bearer " + getCleanToken() },
            "muteHttpExceptions": true
        };
        let resp = UrlFetchApp.fetch(url, options);
        let code = resp.getResponseCode();
        let body = JSON.parse(resp.getContentText());

        if (code === 200) {
            return { status: 'success', message: '✅ Connection OK! Token is Valid.', name: body.name || 'WhatsApp Business Account' };
        } else {
            let errorMsg = (body.error && body.error.message) ? body.error.message : 'Unknown Error';
            return { status: 'error', message: '❌ Connection Failed: ' + errorMsg + ' (Code: ' + code + ')' };
        }
    } catch (e) {
        return { status: 'error', message: '❌ System Error: ' + e.toString() };
    }
}

// 🔔 Test Admin Notification Delivery
function testAdminNotificationAction() {
    try {
        notifyAdmins("🔔 இது ஒரு சிஸ்டம் டெஸ்ட் மெசேஜ் (System Test Message). அட்மின் அலர்ட் வேலை செய்கிறதா என்பதை உறுதிப்படுத்த இந்த மெசேஜ் அனுப்பப்பட்டது.");
        return { status: 'success', message: 'Test notification triggered via Template/Fallback logic.' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

// ------------------------------------------------------------------------------
// 8. CORE DRIVING SCHOOL DB ACTIONS
// ------------------------------------------------------------------------------

// Feature: Old Data Crash Fix (fallback defaults for missing fields)
function normalizeStudentObject(s) {
    try {
        if (!s || typeof s !== 'object') return s;

        s.type = s.type || "Admission";
        s.status = s.status || "Processing";
        s.name = s.name || "";
        s.phone = s.phone || "";
        s.gender = s.gender || "";
        s.service = s.service || "";

        s.totalFee = parseInt(s.totalFee) || 0;
        s.advance = parseInt(s.advance) || 0;

        s.classesAttended = parseInt(s.classesAttended) || 0;
        s.marks = parseInt(s.marks) || 0;
        s.quizDay = parseInt(s.quizDay) || 1;

        s.testDate = s.testDate || "";
        s.testStatus = s.testStatus || "";
        s.llrStatus = s.llrStatus || "No";
        s.llrDate = s.llrDate || "";
        
        // Logical flags for UI filters
        s.is30DayCompleted = false;
        s.isLlrExpiring = false;
        
        if (s.llrStatus === 'Yes' && s.llrDate) {
            let today = new Date();
            let dParts = String(s.llrDate).split('-');
            if (dParts.length === 3) {
                let d = new Date(dParts[0], dParts[1]-1, dParts[2]);
                let diff = Math.floor((today - d) / (1000 * 60 * 60 * 24));
                if (diff >= 30) s.is30DayCompleted = true;
                
                // Expiry (180 days - 15 days window)
                let expDate = new Date(d.getTime() + (180 * 24 * 60 * 60 * 1000));
                let daysToExpir = Math.ceil((expDate - today) / (1000 * 60 * 60 * 24));
                if (daysToExpir >= 0 && daysToExpir <= 15) s.isLlrExpiring = true;
            }
        }

        s.llrDocId = s.llrDocId || "";
        // Fix old llrDocUrl to always be direct PDF link (if we have file id)
        if (s.llrDocId) {
            s.llrDocUrl = getDrivePdfDownloadUrl(s.llrDocId);
        } else {
            s.llrDocUrl = s.llrDocUrl || "";
        }

        s.feeSplit = s.feeSplit || {};
        s.referral = s.referral || "";
        s.receiver = s.receiver || "";

        if (!Array.isArray(s.attendanceHistory)) s.attendanceHistory = [];
        if (!Array.isArray(s.paymentHistory)) s.paymentHistory = [];
        if (!Array.isArray(s.adminRemarks)) s.adminRemarks = [];

        // Misc flags used in logic
        s.feedbackSent = !!s.feedbackSent;
        return s;
    } catch (e) {
        return s;
    }
}

function getDatabaseData() {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured', students: [], expenses: [] };
        let snap = tryRecoverNanbanDataIfEmpty_() || {};
        let students = Array.isArray(snap.students) ? snap.students.map(function(s) { return normalizeStudentObject(s); }) : [];
        let expenses = Array.isArray(snap.expenses) ? snap.expenses : [];
        return { status: 'success', students: students, expenses: expenses };
    } catch (e) {
        return { status: 'error', message: e.toString(), students: [], expenses: [] };
    }
}

function saveStudentData(student) {
    try {
        if (!student.id) {
            student.id = new Date().getTime();
        }
        if (!student.dateJoined) {
            student.dateJoined = getISTDate();
        }

        student.phone = normalizePhone10(student.phone);
        if (student.type !== 'Enquiry' && String(student.phone || "").length !== 10) {
            return { status: 'error', message: 'Invalid phone (10-digit required)' };
        }

        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        let snap = getNanbanSnapshot_() || {};
        let students = Array.isArray(snap.students) ? snap.students : [];
        students = students.filter(function(x) { return String(x.id) !== String(student.id); });
        students.unshift(normalizeStudentObject(student));
        snap.students = students;
        if (!Array.isArray(snap.expenses)) snap.expenses = [];
        if (!snap.appSettingsBundle) snap.appSettingsBundle = getAppSettings();
        saveNanbanSnapshot_(snap);

        try { logAuditEvent('CREATE_STUDENT', 'Student', student.id, "", JSON.stringify(student), { phone: student.phone, name: student.name, type: student.type, service: student.service }); } catch (e) {}

        // Feature 1: Google Contacts Auto-Save (admission only)
        try {
            autoSaveGoogleContact(student);
        } catch(e) {}
        
        triggerWelcomeMessage(student);
        return { status: 'success' };
    } catch(err) { 
        return { status: 'error', message: err.toString() }; 
    }
}

function sendWelcomeMessageAction(studentId) {
    try {
        let db = getDatabaseData();
        let s = (db.students || []).find(x => String(x.id) === String(studentId));
        if (!s) return { status: 'error', message: 'Student Not Found' };
        triggerWelcomeMessage(s);
        return { status: 'success' };
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

function triggerWelcomeMessage(student) {
    try {
        let bal = (parseInt(student.totalFee) || 0) - (parseInt(student.advance) || 0) - (parseInt(student.discount) || 0);
        
        if (student.type === 'Enquiry') {
            notifyAdmins(`📞 *புதிய Enquiry:*\nபெயர்: ${student.name}\nமொபைல்: ${student.phone}\nகோர்ஸ்: ${student.service}`);
        } else {
            notifyAdmins(`📝 *புதிய பதிவு:*\nபெயர்: ${student.name}\nகோர்ஸ்: ${student.service}\nமுன்பணம்: ₹${student.advance}\nமீதம்: ₹${bal}`);
        }
        
        let portalUrl = ScriptApp.getService().getUrl() + "?id=" + student.id;

        try {
            let directMsg = "";
            let templateParams = [
                String(student.name || "-"), 
                String(student.service || "-"), 
                String(student.dateJoined || "-"), 
                String(student.totalFee || "0"), 
                String(student.advance || "0"), 
                String(bal || "0"),
                String(portalUrl || "-")
            ];
            
            if (student.type === 'Enquiry') {
                // 🚀 Immediate welcome + CTA buttons for walk-in / online enquiry
                let cfg = getTemplateAndReminderConfig();
                let tEnq = cfg.enquiryTemplate || "enquiry_welcom";
                let vehicleType = (String(student.service || "").toLowerCase().indexOf("2") !== -1 || String(student.service || "").toLowerCase().indexOf("two") !== -1 || String(student.service || "").indexOf("டூ") !== -1)
                    ? "Two-Wheeler"
                    : String(student.service || "Driving Course");
                var serviceTa = enquiryWelcomServiceLabelTa_(student.service, vehicleType);
                let resT = sendTemplateWithParamFallback(
                    student.phone,
                    tEnq,
                    [
                        [String(student.name || "-"), serviceTa],
                        [String(student.name || "-"), String(vehicleType), "₹2800", "₹1800"],
                        [String(student.name || "-"), String(vehicleType)],
                        [String(student.name || "-")],
                        []
                    ],
                    null
                );
                if (!resT || resT.status === 'error') {
                    // Template failed — send direct message with interactive buttons
                    let directMsg = buildFirstInquiryWelcomeText_(student.name, vehicleType);
                    let btnRes = sendWhatsAppInteractiveButtons(student.phone, directMsg, [
                        { id: 'ADM_DEMO', title: '✅ Confirm Admission' },
                        { id: 'MENU_FEES', title: '💰 கட்டணம் பார்க்க' },
                        { id: 'GOTO_MENU', title: '❓ More Info' }
                    ]);
                    if (!btnRes || btnRes.status === 'error') {
                        // Final fallback: plain text
                        sendWhatsAppMessage(student.phone, directMsg);
                    }
                }
            } else if (student.type === 'Test_Admission') {
                directMsg = `🎉 வணக்கம் ${student.name},\nநண்பன் டிரைவிங் ஸ்கூலில் உங்கள் பதிவு வெற்றிகரமாக முடிந்தது!\n\nபயிற்சி: ${student.service}\nகட்டணம்: ₹${student.totalFee}\nஅட்வான்ஸ்: ₹${student.advance}\nமீதம்: ₹${bal}\n\n📱 *உங்கள் டிஜிட்டல் பாஸ்போர்ட்:* (விவரங்களை அறிய)\n👉 ${portalUrl}\n\nவிரைவில் உங்களை அழைப்போம். 🚗`;
                let cfg = getTemplateAndReminderConfig();
                let dateLabel = formatYMDToDDMMYYYY(student.testDate || "");
                let tRto = cfg.rtoTemplate || "rto_test_reminder";
                let resT = sendTemplateWithParamFallback(
                    student.phone,
                    tRto,
                    [
                        [String(student.name || "-"), String(dateLabel || "-"), String(cfg.inspectorTime || "-")],
                        [String(student.name || "-"), String(dateLabel || "-")],
                        [String(student.name || "-")],
                        []
                    ],
                    null
                );
                if (!resT || resT.status === 'error') {
                    sendWhatsAppMessage(student.phone, directMsg + "\n\n" + buildRtoReminderMessage(student.name, student.testDate));
                }
            } else if (student.type === 'LLR_Admission') {
                directMsg = `📝 வணக்கம் ${student.name}, உங்கள் LLR அட்மிஷன் பதிவு செய்யப்பட்டுள்ளது.\nதேதி: ${student.dateJoined}\nமொத்தம்: ₹${student.totalFee}\nகட்டியது: ₹${student.advance}\nமீதம்: ₹${bal}\n\n👉 பாஸ்போர்ட்: ${portalUrl}`;
                let cfg = getTemplateAndReminderConfig();
                let tLlr = cfg.llrTemplate || "welcome_admission";
                
                // 🎯 FIX: Remove guessed domain. Use student document or specific setting ONLY.
                let docId = student.llrDocId || cfg.welcomeHeaderId || null; 
                
                let r1 = sendTemplateWithParamFallback(
                    student.phone, 
                    tLlr, 
                    [ templateParams.slice(0, 6) ], 
                    docId
                );
                if (!r1 || r1.status === 'error') {
                    sendWhatsAppMessage(student.phone, directMsg);
                }
            } else {
                // Standard Admissions (4 Wheeler, Licenses etc)
                directMsg = `🎉 வணக்கம் ${student.name},\nநண்பன் டிரைவிங் ஸ்கூலில் உங்கள் பதிவு வெற்றிகரமாக முடிந்தது!\n\nபயிற்சி: ${student.service}\nகட்டணம்: ₹${student.totalFee}\nஅட்வான்ஸ்: ₹${student.advance}\nமீதம்: ₹${bal}\n\n📱 *உங்கள் டிஜிட்டல் பாஸ்போர்ட்:* (விவரங்களை அறிய)\n👉 ${portalUrl}\n\nவிரைவில் உங்களை அழைப்போம். 🚗`;
                if (student.llrDocUrl) directMsg += `\n\n📄 ஆவணம்: ${student.llrDocUrl}`;

                let cfg = getTemplateAndReminderConfig();
                let templateName = cfg.welcomeTemplate || "welcome_admission";
                let st = String(student.type || "").toLowerCase();
                if (st.includes("passport")) templateName = "passport_admission";
                else if (st.includes("divorce")) templateName = "divorce_admission";

                let res = sendTemplateWithParamFallback(
                    student.phone, 
                    templateName, 
                    [ templateParams.slice(0, 6) ], 
                    student.llrDocId || cfg.welcomeHeaderId || null
                );
                
                if (!res || res.status === 'error') {
                    sendWhatsAppMessage(student.phone, directMsg);
                }

                // Phase 1.2: Welcome Payment Help
                let uCfg = getTemplateAndReminderConfig();
                if (uCfg.businessUpi && bal > 0) {
                    try {
                        let upiL = generateUPILink(bal, student.name);
                        Utilities.sleep(2000);
                        sendWhatsAppMessage(student.phone, `💳 *நேரடியாக பணம் செலுத்த (UPI):*\n\nமீதமுள்ள கட்டணம் ₹${bal} செலுத்த இந்த லிங்கை கிளிக் செய்யவும்: \n👉 ${upiL} \n\n(அல்லது) UPI ID: *${uCfg.businessUpi}*`);
                    } catch(e) {}
                }
            }

            if (student.type !== 'Enquiry' && parseInt(student.advance) > 0) {
                try {
                    sendDigitalFeeReceiptForPayment(student, parseInt(student.advance) || 0, student.receiver || "", student.receiver || "");
                } catch(e) {
                    try { logBotActivity("WELCOME_RECEIPT_ERR", e.toString()); } catch(le){}
                }
            }
            try { logBotActivity("WELCOME_COMPLETE", student.name); } catch(le){}
        } catch(waErr) {
            Logger.log("WA Welcome error: " + waErr.toString());
            try { logBotActivity("WELCOME_CATASTROPHIC", waErr.toString()); } catch(le){}
        }
        // ... (trainer alert part remains)


        try {
            let settings = getAppSettings(); 
            let trPhone = settings.appSettings ? settings.appSettings.trainerAlertPhone : "";
            let trFee = student.feeSplit ? (parseInt(student.feeSplit.train) || 0) : 0;
            
            if (trPhone && (student.service.includes("4 வீலர்") || student.service.includes("Combo") || student.type === 'Training_Admission') && trFee > 0) {
                sendWhatsAppMessage(trPhone, `🚗 *புதிய பயிற்சி அட்மிஷன்:*\nபெயர்: ${student.name}\nமொபைல்: ${student.phone}\nகோர்ஸ்: ${student.service}`);
            }
        } catch(err) {
            Logger.log("Trainer alert error");
        }
    } catch(e) {
        Logger.log("Trigger Welcome Global Error: " + e.toString());
    }
}


function updateStudentData(s) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        let snap = getNanbanSnapshot_() || {};
        let students = Array.isArray(snap.students) ? snap.students : [];
        let found = false;
        s.phone = normalizePhone10(s.phone);
        students = students.map(function(x) {
            if (String(x.id) === String(s.id)) { found = true; return normalizeStudentObject(s); }
            return x;
        });
        if (!found) return { status: 'error' };
        snap.students = students;
        if (!Array.isArray(snap.expenses)) snap.expenses = [];
        saveNanbanSnapshot_(snap);
        try { autoSaveGoogleContact(s); } catch (e) {}
        try { logAuditEvent('UPDATE_STUDENT', 'Student', s.id, "", JSON.stringify(s), { phone: s.phone, name: s.name, status: s.status }); } catch (e) {}
        return { status: 'success' };
    } catch(e) { 
        return { status: 'error' }; 
    }
}

function updateStudentDataSilent(s) { 
    updateStudentData(s); 
}

function updateMarksInBackground(phone, isCorrect) { 
    try { 
        let res = getDatabaseData(); 
        let students = res.students; 
        let foundStudent = null; 
        
        for (let i = 0; i < students.length; i++) { 
            if (cleanPhoneNumber(students[i].phone) === cleanPhoneNumber(phone)) { 
                foundStudent = students[i]; 
                break; 
            } 
        } 
        
        if (foundStudent) { 
            if (!foundStudent.quizStats) foundStudent.quizStats = { total: 0, correct: 0 };
            foundStudent.quizStats.total = (parseInt(foundStudent.quizStats.total) || 0) + 1;
            
            if (isCorrect) {
                foundStudent.quizStats.correct = (parseInt(foundStudent.quizStats.correct) || 0) + 1;
                foundStudent.marks = (parseInt(foundStudent.marks) || 0) + 1; 
                foundStudent.quizMarks = (parseInt(foundStudent.quizMarks) || 0) + 1;
            }
            
            // Add automated remark for tracking
            if (!Array.isArray(foundStudent.adminRemarks)) foundStudent.adminRemarks = [];
            let statusText = isCorrect ? "✅ Quiz Correct" : "❌ Quiz Wrong";
            foundStudent.adminRemarks.unshift({
                date: getISTDate(), 
                time: Utilities.formatDate(new Date(), "GMT+5:30", "HH:mm:ss"),
                text: `${statusText} (Day ${foundStudent.quizDay - 1})`
            });
            
            updateStudentDataSilent(foundStudent); 
        } 
    } catch(e) {} 
}

function resetQuizDayAndRunTodayNow() {
    let out = { sync: null, send: null };
    out.sync = fixQuizDayMappingFromJoinDate(false, "ரஞ்சித்");
    out.send = dailyMorningCron();
    Logger.log(JSON.stringify(out, null, 2));
    return out;
}

/**
 * 📊 வாராந்திர வினாடி-வினா ரிப்போர்ட் (Weekly Quiz Summary)
 */
function sendWeeklyQuizSummaryAction() {
    try {
        let res = getDatabaseData();
        let students = res.students || [];
        let count = 0;
        
        for (let s of students) {
            if (s.quizStats && s.quizStats.total > 0 && s.status !== 'Pass') {
                let correct = s.quizStats.correct || 0;
                let total = s.quizStats.total || 0;
                let percentage = Math.round((correct / total) * 100);
                
                let msg = `📊 *நண்பன் வினாடி-வினா - வாராந்திர ரிப்போர்ட்*\n\nவணக்கம் ${s.name},\n\nஇந்த வாரம் நீங்கள் கேட்ட கேள்விகளில் *${total}*-க்கு *${correct}* சரியான பதில்களைக் கூறியுள்ளீர்கள்! 🎯\n\nஉங்கள் முன்னேற்றம்: *${percentage}%*\n\n${percentage >= 80 ? "அருமையான முன்னேற்றம்! இதே வேகத்தில் தொடருங்கள். 🚀" : "சிறந்த முயற்சி! இன்னும் கொஞ்சம் முயற்சி செய்தால் நீங்கள் 100% பெறலாம். 👍"}\n\nவாழ்த்துக்கள்! 🚗🚦`;
                
                sendWhatsAppMessage(s.phone, msg);
                count++;
                Utilities.sleep(2000); // Avoid rate limit
            }
        }
        return { status: 'success', sentCount: count };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function deleteStudent(studentId) { 
    try { 
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        let snap = getNanbanSnapshot_() || {};
        let students = Array.isArray(snap.students) ? snap.students : [];
        let beforeLen = students.length;
        students = students.filter(function(x) { return String(x.id) !== String(studentId); });
        if (students.length === beforeLen) return { status: 'error' };
        snap.students = students;
        if (!Array.isArray(snap.expenses)) snap.expenses = [];
        saveNanbanSnapshot_(snap);
        return { status: 'success' };
    } catch(e) { 
        return { status: 'error' }; 
    } 
}

// 🎯 FIX 1: Passport ID Mismatch Issue (String enforcement)
function getStudentPassportData(studentId) {
    try {
        let db = getDatabaseData(); 
        // Ensure both are compared as strings to avoid type mismatch
        let s = db.students.find(x => String(x.id) === String(studentId));
        
        if (s) {
            return { status: 'success', data: s };
        }
        return { status: 'error', message: 'Student Not Found' };
    } catch(e) { 
        return { status: 'error', message: e.toString() }; 
    }
}

// ------------------------------------------------------------------------------
// 9. TRAINER & TEST ACTIONS
// ------------------------------------------------------------------------------

function processTrainerEntry(studentId, type, att, perf, amt, trainer) {
    try {
        let db = getDatabaseData(); 
        let s = db.students.find(x => String(x.id) === String(studentId)); 
        
        if (!s) {
            return {status: 'error', message: 'Student Not Found'}; 
        }
        
        let d = getISTDate(); 
        let admMsg = `✅ *டிரெய்னர் பதிவு (${trainer}):*\nமாணவர்: ${s.name}\n`;
        
        if (type === 'absent') { 
            if (!s.attendanceHistory) s.attendanceHistory = []; 
            s.attendanceHistory.unshift(`❌ ${d} (Absent)`); 
            admMsg += `ஸ்டேட்டஸ்: இன்று வரவில்லை (Absent)\n`; 
        } else { 
            // 🔄 AUTO-REACTIVATE: If student was on Hold or Completed, make them active again
            if (s.status === 'Hold' || s.status === 'License_Completed') {
                s.status = 'Training'; // Reset to active training status
                admMsg += `⚠️ மாணவர் மீண்டும் பயிற்சிக்கு வந்துள்ளார் (Re-Activated)\n`;
            }

            if (att > 0) { 
                s.classesAttended = (parseInt(s.classesAttended)||0) + parseInt(att); 
                if (!s.attendanceHistory) s.attendanceHistory = []; 
                s.attendanceHistory.unshift(`✅ ${d} (Class ${s.classesAttended} - ${perf})`); 
                admMsg += `வகுப்பு: ${att} கிளாஸ்\nசெயல்பாடு: ${perf}\n`; 
            } 
            
            if (amt > 0) { 
                if (!Array.isArray(s.paymentHistory)) s.paymentHistory = [];
                let already = s.paymentHistory.find(p => (p && p.date === d && parseInt(p.amount) === parseInt(amt) && String(p.note || "").includes("பயிற்சியாளர்") && String(p.note || "").includes(trainer)));
                if (already) return { status: 'error', message: 'Duplicate payment detected' };
                
                s.advance = (parseInt(s.advance)||0) + amt; 
                s.paymentHistory.unshift({date: d, amount: amt, note: `பயிற்சியாளர் ${trainer} கையில்`}); 
                admMsg += `பணம் வசூல்: ₹${amt}\n`; 
            } 
        }

        // 🔄 1. SAVE DATA FIRST (Reliability Priority)
        updateStudentData(s); 
        
        // 📢 2. NOTIFICATIONS (Non-Blocking)
        try { notifyAdmins(admMsg); } catch(e) {}
        
        try {
            if (type === 'absent') {
                 sendWhatsAppMessage(s.phone, `🚫 வணக்கம் ${s.name}, இன்று நீங்கள் பயிற்சிக்கு வரவில்லை என பதிவாகியுள்ளது.`);
            } else {
                if (att > 0) {
                    let nextDay = s.classesAttended + 1; 
                    let syllabusText = (nextDay <= 15) ? CAR_SYLLABUS[nextDay - 1] : "அனைத்து பயிற்சிகளும் முடிந்தது! இனி டெஸ்டுக்குத் தயாராகலாம்."; 
                    
                    let cfg = getTemplateAndReminderConfig();
                    let dailyRes = sendTemplateWithParamFallback(
                        s.phone,
                        cfg.dailyClassTemplate || "daily_class_alert",
                        [[String(s.name || "-"), String(att || 1), String(perf || "-"), String(syllabusText || "-")], [String(s.name || "-")]],
                        null
                    );
                    if (!dailyRes || dailyRes.status === 'error') {
                        sendWhatsAppMessage(s.phone, `🚗 வணக்கம் ${s.name}, இன்று உங்கள் ${att} வகுப்பு முடிந்தது. செயல்பாடு: ${perf}\n\n📅 நாளைக்கான பயிற்சி: ${syllabusText}`);
                    }
                    
                    if (s.classesAttended === 15 && s.feedbackSent !== true) {
                        let feedbackMsg = `🙏 வணக்கம் ${s.name},\n\nநண்பன் டிரைவிங் ஸ்கூலில் உங்களுடைய 15 நாள் பயிற்சி இன்றுடன் நிறைவடைகிறது. எங்கள் பயிற்சி மற்றும் டிரெய்னரின் அணுகுமுறை உங்களுக்கு எப்படி இருந்தது?\n\nஉங்கள் மதிப்பெண்ணை (1 முதல் 5 வரை) Type செய்து ரிப்ளை செய்யவும். (உதா: 5)`;
                        sendWhatsAppMessage(s.phone, feedbackMsg);
                        s.feedbackSent = true;
                        updateStudentData(s); // Save feedback sent status
                    }
                }
                
                if (amt > 0) {
                    let bal = (parseInt(s.totalFee) || 0) - (parseInt(s.advance) || 0) - (parseInt(s.discount) || 0); 
                    sendWhatsAppMessage(s.phone, `💰 வணக்கம் ${s.name},\nஉங்களிடம் இருந்து ₹${amt} பெறப்பட்டது.\nபயிற்சியாளர்: ${trainer}\nமீதமுள்ள தொகை: ₹${bal}\n\nநன்றி! - நண்பன் டிரைவிங் ஸ்கூல்`); 
                    try { sendDigitalFeeReceiptForPayment(s, amt, trainer, trainer); } catch(e) {}
                }
            }
        } catch(msgErr) {
            Logger.log("Trainer Notify Error: " + msgErr.toString());
        }
        
        return { status: 'success' };
    } catch(e) { 
        return { status: 'error', message: e.toString() }; 
    }
}

function processReTestUpdate(studentId, testFee, advancePaid, newDate, adminName) {
    try {
        let db = getDatabaseData(); 
        let s = db.students.find(x => String(x.id) === String(studentId)); 
        
        s.totalFee = (parseInt(s.totalFee) || 0) + parseInt(testFee);
        
        if (advancePaid > 0) { 
            if (!Array.isArray(s.paymentHistory)) s.paymentHistory = [];
            let already = s.paymentHistory.find(p => (p && p.date === getISTDate() && parseInt(p.amount) === parseInt(advancePaid) && String(p.note || "").includes("Re-Test") && String(p.note || "").includes(adminName)));
            if (already) {
                return { status: 'error', message: 'Duplicate payment detected' };
            }
            s.advance = (parseInt(s.advance) || 0) + parseInt(advancePaid); 
            if (!Array.isArray(s.paymentHistory)) {
                s.paymentHistory = []; 
            }
            s.paymentHistory.unshift({ date: getISTDate(), amount: advancePaid, note: `Re-Test கட்டணம் (${adminName})` }); 
        }
        
        s.testDate = newDate; 
        s.testStatus = 'Pending'; 
        s.status = 'Ready_for_Test'; 
        
        if (!Array.isArray(s.adminRemarks)) {
            s.adminRemarks = []; 
        }
        s.adminRemarks.unshift({ date: getISTDate(), text: `🔄 Re-Test பதிவு: ₹${testFee}. தேதி: ${newDate}` }); 
        
        updateStudentData(s); 
        notifyAdmins(`🔄 *Re-Test பதிவு:*\nமாணவர்: ${s.name}\nகட்டணம்: ₹${testFee}\nதேதி: ${newDate}`); 
        let cfg = getTemplateAndReminderConfig();
        let dateLabel = formatYMDToDDMMYYYY(newDate || "");
        let r3 = sendTemplateWithParamFallback(
            s.phone,
            cfg.rtoTemplate,
            [
                [String(s.name || "-"), String(dateLabel || "-"), String(cfg.inspectorTime || "-")],
                [String(s.name || "-"), String(dateLabel || "-")],
                [String(s.name || "-")],
                []
            ],
            null
        );
        if (!r3 || r3.status === 'error') {
            sendWhatsAppMessage(s.phone, buildRtoReminderMessage(s.name, newDate));
        }
        
        return { status: 'success' };
    } catch(e) { 
        return { status: 'error' }; 
    }
}

// 🎯 FIX 5: SPOT CASH HANDOVER LOGIC (Converts "🟡 Spot Pending" to actual receiver's income)
function processDayCloseHandover(trainer, receiver, expAmt, expDesc, runKm, testResultsJson) {
    Logger.log(`🏁 Starting processDayCloseHandover. Trainer: ${trainer}, Receiver: ${receiver}, Exp: ${expAmt}, KM: ${runKm}`);
    try {
        let testResults = [];
        try { if (testResultsJson) testResults = JSON.parse(testResultsJson); } catch(e) {}
        Logger.log("🎯 Test Results found: " + testResults.length);

        let today = getISTDate(); 
        // Store today's KM globally (server-side), not browser localStorage.
        try {
            let props = PropertiesService.getScriptProperties();
            props.setProperty('NANBAN_KM_TODAY_DATE', today);
            props.setProperty('NANBAN_KM_TODAY_VALUE', String(parseInt(runKm) || 0));
            props.setProperty('NANBAN_KM_SESSION_DATE', today);
            props.setProperty('NANBAN_KM_SESSION_ACTIVE', '0');
            props.setProperty('NANBAN_KM_SESSION_END', String(parseInt(runKm) || 0));
        } catch (e) {}

        let settings = getAppSettings(); 
        let oldKm = settings.vehicleKm.current; 
        
        settings.vehicleKm.current += (parseInt(runKm) || 0); 
        saveAppSettings("vehicleKm", settings.vehicleKm);
        
        if (Math.floor(settings.vehicleKm.current / settings.vehicleKm.nextService) > Math.floor(oldKm / settings.vehicleKm.nextService)) {
            notifyAdmins(`⚠️ *வண்டி சர்வீஸ் அலர்ட்!* வண்டி ${settings.vehicleKm.current} KM ஓடிவிட்டது. சர்வீஸ் செய்ய வேண்டிய நேரம் வந்துவிட்டது! 🛠️🚗`);
        }
        
        if (expAmt > 0) {
            saveExpenseData({ date: today, spender: trainer, cat: '🔴 செலவு - வண்டி செலவுகள்', amt: expAmt, desc: expDesc });
        }
        
        let sheetSt = getDB().getSheetByName("Students"); 
        let data = sheetSt.getDataRange().getValues(); 
        let totalCollected = 0; 
        let classesTaken = 0;
        
        for (let i = 1; i < data.length; i++) { 
            if (!data[i][6]) continue; 
            
            let s = JSON.parse(data[i][6]); 
            let updated = false; 
            
            // 🎯 Check if this student has a test result to update
            let resObj = testResults.find(r => r.id.toString() === s.id.toString());
            if (resObj) {
                s.testStatus = resObj.result;
                if (!Array.isArray(s.adminRemarks)) s.adminRemarks = [];
                s.adminRemarks.unshift({date: today, text: `🏁 டெஸ்ட் முடிவு: ${resObj.result} (பதிவு செய்தவர்: ${trainer})`});
                if (String(resObj.result) === 'Pass') {
                    s.status = 'License_Completed';
                    s.adminRemarks.unshift({date: today, text: `🏆 RTO டெஸ்ட் பாஸ்! (Trainer: ${trainer})`});
                    if (s.phone) {
                        if (!s.passCertificateSentDate || String(s.passCertificateSentDate) !== today) {
                            let cert = generateTestPassCertificatePdf(s, trainer, today);
                            if (cert && cert.status === 'success' && cert.url) {
                                sendWhatsAppDocumentMessage(s.phone, cert.url, "Nanban_Success_Report.pdf", `🏆 வாழ்த்துக்கள் ${s.name}!\n\nஉங்களின் RTO Test PASS Certificate.\n\nஇதைக் WhatsApp Status-ல் share பண்ணுங்க 🙏\n- Nanban Driving School`);
                                sendWhatsAppMessage(s.phone, `📣 Status Caption (copy):\n\n✅ இன்று நான் Nanban Driving School-ல் RTO Test PASS பண்ணிட்டேன்!\nநன்றி Nanban Team 🙏\n\n#NanbanDrivingSchool`);
                            } else {
                                sendWhatsAppMessage(s.phone, `🎉 வெற்றி பெற்றீர்கள் ${s.name}! ஓட்டுநர் தேர்வில் இன்று சிறப்பாக செயல்பட்டதற்கு வாழ்த்துக்கள்! 🏆`);
                            }
                            s.passCertificateSentDate = today;
                        }
                    }
                    notifyAdmins(`🏆 *TEST PASS:* ${s.name} தேர்ச்சி பெற்றுவிட்டார்!`);
                }
                updated = true;
            }

            let att = Array.isArray(s.attendanceHistory) ? s.attendanceHistory : []; 
            for (let a = 0; a < att.length; a++) { 
                if (att[a].includes(today) && att[a].includes('✅')) {
                    classesTaken++; 
                }
            } 
            
            let pays = Array.isArray(s.paymentHistory) ? s.paymentHistory : []; 
            for (let j = 0; j < pays.length; j++) { 
                if (pays[j].date === today && (pays[j].note||"").includes("பயிற்சியாளர்") && (pays[j].note||"").includes(trainer)) { 
                    totalCollected += parseInt(pays[j].amount); 
                    pays[j].note = `பயிற்சியாளர் வசூல் -> ${receiver}`; 
                    updated = true; 
                } 
            } 
            
            if (updated) { 
                s.paymentHistory = pays; 
                sheetSt.getRange(i + 1, 7).setValue(JSON.stringify(s)); 
            } 
        }
        
        // 🎯 Catching the "Pending Spot" and converting it to Actual Income
        let spotIncome = 0; 
        let sheetExpRead = getDB().getSheetByName("Expenses"); 
        let expData = sheetExpRead.getDataRange().getValues(); 
        
        for (let i = 1; i < expData.length; i++) { 
            if (expData[i][0] === today && expData[i][1] === trainer && expData[i][2].includes('🟡 Spot Pending')) { 
                let amt = parseInt(expData[i][3]) || 0;
                spotIncome += amt; 
                // Feature 2: Wallet Transfer Fix - settle spot cash into receiver wallet.
                sheetExpRead.getRange(i + 1, 2).setValue(receiver); // spender -> receiver wallet
                sheetExpRead.getRange(i + 1, 3).setValue(`🟢 வரவு - Spot Collection (${receiver})`);
                sheetExpRead.getRange(i + 1, 5).setValue(`Spot Collection Settled (${receiver}) - Trainer: ${trainer}`);
            } 
        } 
        
        flushNoop_();
        
        let expectedHandover = totalCollected + spotIncome - expAmt;
        
        let closeMsg = `🏁 *DAY CLOSE REPORT (${trainer})*\n\n`;
        closeMsg += `📅 தேதி: ${today}\n`;
        closeMsg += `🚗 பயிற்சி பெற்றவர்கள்: ${classesTaken} பேர்\n`;
        closeMsg += `🚗 ஓடியது: ${runKm} KM (Total: ${settings.vehicleKm.current} KM)\n`;
        closeMsg += `💰 மாணவர் வசூல்: ₹${totalCollected}\n`;
        closeMsg += `💸 இதர வரவு (Spot): ₹${spotIncome}\n`;
        closeMsg += `🔴 செலவு: ₹${expAmt} (${expDesc})\n`;
        
        if (testResults.length > 0) {
            closeMsg += `--------------------\n`;
            closeMsg += `🎯 *டெஸ்ட் முடிவுகள்:*`;
            testResults.forEach(r => {
                let s = getStudentById(r.id);
                closeMsg += `\n- ${s ? s.name : 'Unknown'}: ${r.result === 'Pass' ? '✅ PASS' : '❌ FAIL'}`;
            });
        }

        closeMsg += `\n--------------------\n`;
        closeMsg += `🤝 ஒப்படைத்த பணம்: *₹${expectedHandover}*\n`;
        closeMsg += `(To: ${receiver})`;
        
        notifyAdmins(closeMsg); 

        // 🌟 Also notify the trainer themself
        try {
            let allUsers = getAppUsers();
            let trainerUser = allUsers.find(u => u.name === trainer);
            if (trainerUser && trainerUser.phone) {
                Logger.log("Sending Close Report copy to trainer: " + trainerUser.phone);
                let cfg = getTemplateAndReminderConfig();
                let resT = sendTemplateWithParamFallback(
                    trainerUser.phone,
                    cfg.dayCloseTemplate || "day_close_report",
                    [[closeMsg], []],
                    null
                );
                if (!resT || resT.status === 'error') {
                    sendWhatsAppMessage(trainerUser.phone, closeMsg);
                }
            }
        } catch (e) {
            Logger.log("Error sending shift close copy to trainer: " + e.toString());
        }

        try { logAuditEvent('DAY_CLOSE', 'DayClose', today, "", "", { trainer: trainer, receiver: receiver, runKm: parseInt(runKm) || 0, collected: totalCollected, spot: spotIncome, expense: parseInt(expAmt) || 0, expected: expectedHandover, testCount: testResults.length }); } catch (e) {}
        return { status: 'success' };
        
    } catch(e) { 
        return { status: 'error', message: e.toString() }; 
    }
}

function getKmTodayAction() {
    try {
        let today = getISTDate();
        let props = PropertiesService.getScriptProperties();
        let dt = props.getProperty('NANBAN_KM_TODAY_DATE') || '';
        let val = props.getProperty('NANBAN_KM_TODAY_VALUE') || '0';
        let km = 0;
        if (dt === today) {
            km = parseInt(val) || 0;
        }
        return { status: 'success', km: km };
    } catch (e) {
        return { status: 'error', km: 0, message: e.toString() };
    }
}

function getTrainerKmSessionAction() {
    try {
        let today = getISTDate();
        let props = PropertiesService.getScriptProperties();
        let dt = props.getProperty('NANBAN_KM_SESSION_DATE') || '';
        let active = (props.getProperty('NANBAN_KM_SESSION_ACTIVE') || '0') === '1';
        let startKm = parseInt(props.getProperty('NANBAN_KM_SESSION_START') || '0') || 0;
        let startedBy = props.getProperty('NANBAN_KM_SESSION_BY') || '';
        let endKm = parseInt(props.getProperty('NANBAN_KM_SESSION_END') || '0') || 0;

        if (dt !== today) {
            return { status: 'success', active: false, date: today, startKm: 0, startedBy: '', endKm: 0 };
        }

        if (!active || startKm <= 0) {
            return { status: 'success', active: false, date: today, startKm: 0, startedBy: startedBy, endKm: endKm };
        }

        return { status: 'success', active: true, date: today, startKm: startKm, startedBy: startedBy, endKm: endKm };
    } catch (e) {
        return { status: 'error', message: e.toString(), active: false, startKm: 0 };
    }
}

function startTrainerKmSessionAction(startKm, trainerName) {
    try {
        let today = getISTDate();
        let st = parseInt(startKm) || 0;
        if (st <= 0) return { status: 'error', message: 'Invalid Start KM' };

        let props = PropertiesService.getScriptProperties();
        let dt = props.getProperty('NANBAN_KM_SESSION_DATE') || '';
        let active = (props.getProperty('NANBAN_KM_SESSION_ACTIVE') || '0') === '1';
        let existing = parseInt(props.getProperty('NANBAN_KM_SESSION_START') || '0') || 0;
        let by = props.getProperty('NANBAN_KM_SESSION_BY') || '';

        if (dt === today && active && existing > 0) {
            return { status: 'exists', date: today, active: true, startKm: existing, startedBy: by };
        }

        props.setProperty('NANBAN_KM_SESSION_DATE', today);
        props.setProperty('NANBAN_KM_SESSION_ACTIVE', '1');
        props.setProperty('NANBAN_KM_SESSION_START', String(st));
        props.setProperty('NANBAN_KM_SESSION_BY', String(trainerName || 'Trainer'));
        props.setProperty('NANBAN_KM_SESSION_END', '0');

        return { status: 'success', date: today, active: true, startKm: st, startedBy: String(trainerName || 'Trainer') };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function clearTrainerKmSessionAction() {
    try {
        let today = getISTDate();
        let props = PropertiesService.getScriptProperties();
        props.setProperty('NANBAN_KM_SESSION_DATE', today);
        props.setProperty('NANBAN_KM_SESSION_ACTIVE', '0');
        props.setProperty('NANBAN_KM_SESSION_START', '0');
        props.setProperty('NANBAN_KM_SESSION_BY', '');
        props.setProperty('NANBAN_KM_SESSION_END', '0');
        return { status: 'success' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function markTestResultActionEx(studentId, resultStr, trainerName, nextDate) { 
    try { 
        let sheet = getDB().getSheetByName("Students"); 
        let data = sheet.getDataRange().getValues(); 
        let today = getISTDate();
        
        for (let i = 1; i < data.length; i++) { 
            if (data[i][0].toString() === studentId.toString()) { 
                let s = JSON.parse(data[i][6]); 
                s.testStatus = resultStr; 
                
                if (!Array.isArray(s.adminRemarks)) {
                    s.adminRemarks = []; 
                }
                
                if (resultStr === 'Pass') { 
                    s.status = 'License_Completed'; 
                    s.adminRemarks.unshift({date: today, text: `🏆 RTO டெஸ்ட் பாஸ்! (Trainer: ${trainerName})`}); 
                    sendWhatsAppMessage(s.phone, `🎉 வெற்றி பெற்றீர்கள் ${s.name}! ஓட்டுநர் தேர்வில் இன்று சிறப்பாக செயல்பட்டதற்கு வாழ்த்துக்கள்! 🏆 உங்கள் லைசென்ஸ் கார்டு விரைவில் கைக்கு வரும்.`); 
                    if (s.phone) {
                        if (!s.passCertificateSentDate || String(s.passCertificateSentDate) !== today) {
                            let cert = generateTestPassCertificatePdf(s, trainerName, today);
                            if (cert && cert.status === 'success' && cert.url) {
                                sendWhatsAppDocumentMessage(s.phone, cert.url, "Nanban_Success_Report.pdf", `🏆 வாழ்த்துக்கள் ${s.name}!\n\nஉங்களின் RTO Test PASS Certificate.\n\nஇதைக் WhatsApp Status-ல் share பண்ணுங்க 🙏\n- Nanban Driving School`);
                                sendWhatsAppMessage(s.phone, `📣 Status Caption (copy):\n\n✅ இன்று நான் Nanban Driving School-ல் RTO Test PASS பண்ணிட்டேன்!\nநன்றி Nanban Team 🙏\n\n#NanbanDrivingSchool`);
                            }
                            s.passCertificateSentDate = today;
                        }
                    }

                    let cfg = getTemplateAndReminderConfig();
                    if (cfg.googleReviewLink && cfg.googleReviewLink.includes("http")) {
                        Utilities.sleep(2000);
                        sendWhatsAppMessage(s.phone, `🌟 எங்கள் பயிற்சி உங்களுக்கு பிடித்திருந்ததா? \n\nதயவுசெய்து உங்கள் மேலான கருத்துக்களை கூகுளில் (Google Review) பகிருங்கள். இது எங்களுக்கு பெரும் உதவியாக இருக்கும்: \n👉 ${cfg.googleReviewLink}\n\nமிக்க நன்றி! 🙏`);
                    }

                    notifyAdmins(`🏆 *TEST PASS:* ${s.name} தேர்ச்சி பெற்றுவிட்டார்!`); 
                } else { 
                    let resText = resultStr === 'Fail' ? 'ஃபெயில்' : 'வரவில்லை (Absent)'; 
                    let dtTxt = nextDate ? `அடுத்த டெஸ்ட்: ${nextDate}` : `தேதி முடிவாகவில்லை`; 
                    s.adminRemarks.unshift({date: today, text: `❌ RTO டெஸ்ட் ${resText}. ${dtTxt} (Trainer: ${trainerName})`}); 
                    
                    if (nextDate) { 
                        s.testDate = nextDate; 
                        s.testStatus = 'Pending'; 
                    } 
                    
                    notifyAdmins(`❌ *TEST ${resultStr.toUpperCase()}:* ${s.name} டெஸ்டில் ${resText}. ${dtTxt}`); 
                } 
                
                sheet.getRange(i + 1, 7).setValue(JSON.stringify(s)); 
                flushNoop_(); 
                
                return { status: 'success' }; 
            } 
        } 
        return { status: 'error' }; 
    } catch(e) { 
        return { status: 'error' }; 
    }
}

/**
 * 🎓 சிலபஸ் முன்னேற்றத்தை பதிவு செய்தல் (Mark Syllabus Progress)
 */
function markSyllabusAction(studentId, itemKey, isCompleted) {
    try {
        let sheet = getDB().getSheetByName("Students");
        let data = sheet.getDataRange().getValues();
        for (let i = 1; i < data.length; i++) {
            if (data[i][0].toString() === studentId.toString()) {
                let s = JSON.parse(data[i][6]);
                if (!s.syllabus) s.syllabus = {};
                s.syllabus[itemKey] = !!isCompleted;
                s.syllabusLastUpdate = getISTDate();
                
                sheet.getRange(i + 1, 7).setValue(JSON.stringify(s));
                flushNoop_();
                return { status: 'success', syllabus: s.syllabus };
            }
        }
        return { status: 'error', message: 'Student not found' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function getSyllabusItems() {
    return [
        { key: 'clutch', label: 'Clutch Control (கிளட்ச்)', category: 'Basics' },
        { key: 'gear', label: 'Gear Shifting (கியர்)', category: 'Basics' },
        { key: 'steering', label: 'Steering (ஸ்டீயரிங்)', category: 'Basics' },
        { key: 'braking', label: 'Braking (பிரேக்கிங்)', category: 'Basics' },
        { key: 'reverse', label: 'Reverse (ரிவர்ஸ்)', category: 'Advanced' },
        { key: 'track8', label: '8-Track (8 போடுதல்)', category: 'Test' },
        { key: 'trackH', label: 'H-Track (H போடுதல்)', category: 'Test' },
        { key: 'slope', label: 'Slope Start (ஏற்றம்)', category: 'Advanced' },
        { key: 'parking', label: 'Parking (பார்க்கிங்)', category: 'Advanced' },
        { key: 'city', label: 'City Driving (நகர பயணம்)', category: 'Road' },
        { key: 'highway', label: 'Highway (நெடுஞ்சாலை)', category: 'Road' },
        { key: 'night', label: 'Night Driving (இரவு பயணம்)', category: 'Road' },
        { key: 'signals', label: 'Traffic Signals (சிக்னல்கள்)', category: 'Rules' },
        { key: 'maintenance', label: 'Maintenance (பராமரிப்பு)', category: 'Basics' },
        { key: 'safety', label: 'Safety Rules (பாதுகாப்பு)', category: 'Rules' }
    ];
}

// ------------------------------------------------------------------------------
// 10. EXPENSE, ALERTS & SYNC
// ------------------------------------------------------------------------------

function saveExpenseData(exp) { 
    console.log("💸 saveExpenseData() STARTED. Data: ", JSON.stringify(exp));
    try { 
        exp = exp || {};
        exp.date = exp.date || getISTDate();
        exp.spender = exp.spender || "";
        exp.cat = exp.cat || "";
        exp.amt = parseInt(exp.amt) || 0;
        exp.desc = exp.desc || "";
        exp.receiptUrl = exp.receiptUrl || ""; // 📎 New Receipt Field!

        if (useFirebaseRtdb_()) {
            let snap = getNanbanSnapshot_() || {};
            let expenses = Array.isArray(snap.expenses) ? snap.expenses : [];
            let isDup = expenses.some(function(r) {
                return String(r.date || '').trim() === exp.date &&
                    String(r.spender || '').trim() === exp.spender &&
                    String(r.cat || '').trim() === exp.cat &&
                    (parseInt(r.amt) || 0) === exp.amt &&
                    String(r.desc || '').trim() === exp.desc;
            });
            if (isDup) return { status: 'error', message: 'Duplicate expense detected' };
            expenses.unshift(exp);
            snap.expenses = expenses;
            if (!Array.isArray(snap.students)) snap.students = [];
            saveNanbanSnapshot_(snap);
        } else {
            let sheet = getDB().getSheetByName("Expenses");
            // Check for duplicates
            try {
                let lastRow = sheet.getLastRow();
                let start = Math.max(2, lastRow - 60);
                if (lastRow >= 2) {
                    let vals = sheet.getRange(start, 1, lastRow - start + 1, 5).getValues();
                    for (let i = 0; i < vals.length; i++) {
                        let r = vals[i];
                        if (String(r[0] || '').trim() === exp.date && String(r[1] || '').trim() === exp.spender && String(r[2] || '').trim() === exp.cat && parseInt(r[3]) === exp.amt && String(r[4] || '').trim() === exp.desc) {
                            console.warn("🚫 Duplicate expense blocked.");
                            return { status: 'error', message: 'Duplicate expense detected' };
                        }
                    }
                }
            } catch (e) {}
            console.log("📝 Appending Expense to sheet...");
            sheet.appendRow([exp.date, exp.spender, exp.cat, exp.amt, exp.desc, exp.receiptUrl]); // 6th Col added!
            flushNoop_();
        }
        
        let typeIcon = "🔴";
        if(exp.cat.includes("வரவு")) typeIcon = "🟢";
        if(exp.cat.includes("Spot Pending")) typeIcon = "🟡";
        
        let alertMsg = `💸 *கணக்கு பதிவு (${exp.spender}):*\nவகை: ${typeIcon} ${exp.cat}\nதொகை: ₹${exp.amt}\nவிவரம்: ${exp.desc}`;
        console.log("📣 Notifying Admins with message: " + alertMsg);
        
        try {
            notifyAdmins(alertMsg);
        } catch (adminErr) {
            console.error("Admin Notification Function Call Failed: ", adminErr.message);
        }
        
        return { status: 'success' }; 
    } catch(e) { 
        console.error("🚨 saveExpenseData() FATAL ERROR: ", e.message);
        return { status: 'error' }; 
    } 
}

// Admin-only: update an existing expense row (matched by all fields).
function updateExpenseDataAction(expObj, newAmt, newDesc, loggedBy) {
    try {
        if (loggedBy && !isPrivilegedName(loggedBy)) return { status: 'error', message: 'Not allowed' };
        if (!expObj) return { status: 'error', message: 'Missing expense object' };

        let oldDate = String(expObj.date || '').trim();
        let oldSpender = String(expObj.spender || '').trim();
        let oldCat = String(expObj.cat || '').trim();
        let oldAmt = parseInt(expObj.amt) || 0;
        let oldDesc = String(expObj.desc || '').trim();

        let amt2 = parseInt(newAmt) || 0;
        let desc2 = String(newDesc || '').trim();

        if (useFirebaseRtdb_()) {
            let snap = getNanbanSnapshot_() || {};
            let expenses = Array.isArray(snap.expenses) ? snap.expenses : [];
            for (let i = 0; i < expenses.length; i++) {
                let row = expenses[i] || {};
                let d = String(row.date || '').trim();
                let sp = String(row.spender || '').trim();
                let c = String(row.cat || '').trim();
                let a = parseInt(row.amt) || 0;
                let ds = String(row.desc || '').trim();
                if (d === oldDate && sp === oldSpender && c === oldCat && a === oldAmt && ds === oldDesc) {
                    let before = JSON.stringify(row);
                    row.amt = amt2;
                    row.desc = desc2;
                    expenses[i] = row;
                    snap.expenses = expenses;
                    saveNanbanSnapshot_(snap);
                    notifyAdmins(`📝 Expense Updated:\n${d} | ${sp}\nCat: ${c}\nAmount: ₹${amt2}\nDesc: ${desc2}`);
                    try { logAuditEvent('UPDATE_EXPENSE', 'Expense', `${d}|${sp}|${c}`, before, JSON.stringify(row), { spender: sp, cat: c }); } catch (e) {}
                    return { status: 'success' };
                }
            }
        } else {
            let sheet = getDB().getSheetByName("Expenses");
            if (!sheet) return { status: 'error', message: 'Expenses sheet not found' };
            let lastRow = sheet.getLastRow();
            for (let r = 2; r <= lastRow; r++) {
                let row = sheet.getRange(r, 1, 1, 5).getValues()[0];
                let d = String(row[0] || '').trim();
                let sp = String(row[1] || '').trim();
                let c = String(row[2] || '').trim();
                let a = parseInt(row[3]) || 0;
                let ds = String(row[4] || '').trim();
                if (d === oldDate && sp === oldSpender && c === oldCat && a === oldAmt && ds === oldDesc) {
                    let before = JSON.stringify({ date: d, spender: sp, cat: c, amt: a, desc: ds });
                    sheet.getRange(r, 4).setValue(amt2);
                    sheet.getRange(r, 5).setValue(desc2);
                    flushNoop_();
                    notifyAdmins(`📝 Expense Updated:\n${d} | ${sp}\nCat: ${c}\nAmount: ₹${amt2}\nDesc: ${desc2}`);
                    try { logAuditEvent('UPDATE_EXPENSE', 'Expense', `${d}|${sp}|${c}`, before, JSON.stringify({ date: d, spender: sp, cat: c, amt: amt2, desc: desc2 }), { spender: sp, cat: c }); } catch (e) {}
                    return { status: 'success' };
                }
            }
        }

        return { status: 'error', message: 'Matching expense row not found' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function processFundTransfer(fromPerson, toPerson, amt, desc, loggedBy) { 
    try { 
        if (loggedBy && !isPrivilegedName(loggedBy)) return { status: 'error', message: 'Not allowed' };
        let d = getISTDate(); 
        if (useFirebaseRtdb_()) {
            let snap = getNanbanSnapshot_() || {};
            let expenses = Array.isArray(snap.expenses) ? snap.expenses : [];
            expenses.unshift({ date: d, spender: fromPerson, cat: '🔄 பணப் பரிமாற்றம் (Out)', amt: parseInt(amt) || 0, desc: `To ${toPerson}: ${desc} (By ${loggedBy})` });
            expenses.unshift({ date: d, spender: toPerson, cat: '🔄 பணப் பரிமாற்றம் (In)', amt: parseInt(amt) || 0, desc: `From ${fromPerson}: ${desc} (By ${loggedBy})` });
            snap.expenses = expenses;
            if (!Array.isArray(snap.students)) snap.students = [];
            saveNanbanSnapshot_(snap);
        } else {
            let sheet = getDB().getSheetByName("Expenses");
            sheet.appendRow([d, fromPerson, '🔄 பணப் பரிமாற்றம் (Out)', amt, `To ${toPerson}: ${desc} (By ${loggedBy})`]); 
            sheet.appendRow([d, toPerson, '🔄 பணப் பரிமாற்றம் (In)', amt, `From ${fromPerson}: ${desc} (By ${loggedBy})`]); 
            flushNoop_();
        }
        notifyAdmins(`🔄 *பணப் பரிமாற்றம் (Transfer)*\n\nகொடுத்தவர்: ${fromPerson}\nபெற்றவர்: ${toPerson}\nதொகை: ₹${amt}\nவிவரம்: ${desc}\nபதிவு: ${loggedBy}`); 
        try { logAuditEvent('TRANSFER', 'Transfer', `${d}|${fromPerson}|${toPerson}|${parseInt(amt)||0}`, "", "", { from: fromPerson, to: toPerson, amt: parseInt(amt) || 0, desc: desc, by: loggedBy }); } catch (e) {}
        
        return { status: 'success' }; 
    } catch(e) { 
        return { status: 'error', message: e.toString() }; 
    } 
}

function sendBulkMessageAction(msgText) { 
    try { 
        let db = getDatabaseData(); 
        let count = 0; 
        let cfg = getTemplateAndReminderConfig();
        
        db.students.forEach(s => { 
            if (s.status !== 'Deleted' && s.type !== 'Enquiry' && s.status !== 'License_Completed' && s.status !== 'Hold') { 
                let r = sendTemplateWithParamFallback(s.phone, cfg.bulkTemplate || "bulk_announcement", [[String(msgText || "")], []], null);
                if (!r || r.status === 'error') {
                    sendWhatsAppMessage(s.phone, "📢 *அறிவிப்பு:*\n\n" + msgText + "\n\n- நிர்வாகம், நண்பன் டிரைவிங் ஸ்கூல்");
                }
                count++; 
            } 
        }); 
        
        return {status: 'success', msg: `${count} பேருக்கு மெசேஜ் அனுப்பப்பட்டது!`}; 
    } catch(e) { 
        return {status: 'error', msg: e.toString()}; 
    } 
}

function sendPaymentReminderAction(studentId, adminName) { 
    try { 
        let db = getDatabaseData(); 
        let s = db.students.find(x => String(x.id) === String(studentId)); 
        let bal = (parseInt(s.totalFee) || 0) - (parseInt(s.advance) || 0) - (parseInt(s.discount) || 0); 
        
        if (bal > 0) { 
            let cfg = getTemplateAndReminderConfig();
            let res = sendTemplateWithParamFallback(
                s.phone,
                cfg.paymentReminderTemplate || "payment_reminder_nds",
                [[String(s.name || "நண்பரே"), String(bal)], [String(s.name || "நண்பரே")], []],
                null
            );

            if (!res || res.status === 'error') {
                sendWhatsAppMessage(s.phone, `🔔 *கட்டண நினைவூட்டல் (Reminder)*\n\nவணக்கம் ${s.name},\nஉங்கள் ஓட்டுநர் பயிற்சி கட்டணத்தில் ₹${bal} நிலுவையில் உள்ளது. தயவுசெய்து மீதமுள்ள தொகையை விரைவில் செலுத்துமாறு கேட்டுக்கொள்கிறோம்.\n\nநன்றி! 🙏\n- நண்பன் டிரைவிங் ஸ்கூல்`); 
            }

            // Phase 1.2: Follow-up with UPI Link for easy payment
            if (cfg.businessUpi) {
                try {
                    let upiLink = generateUPILink(bal, s.name);
                    Utilities.sleep(1500);
                    sendWhatsAppMessage(s.phone, `💳 *சுலபமாக பணம் செலுத்த (Online UPI):*\n\nகீழே உள்ள லிங்கை கிளிக் செய்து நேரடியாக பணம் செலுத்தலாம்: \n👉 ${upiLink} \n\nஅல்லது இந்த UPI ID-க்கு அனுப்பலாம்: \n🆔 *${cfg.businessUpi}*`);
                } catch(upiErr) {}
            }
            
            if (!Array.isArray(s.adminRemarks)) {
                s.adminRemarks = []; 
            }
            
            s.adminRemarks.unshift({date: getISTDate(), text: `🔔 Payment Reminder அனுப்பப்பட்டது (${adminName})`}); 
            updateStudentData(s); 
            
            return {status: 'success', msg: 'மெசேஜ் அனுப்பப்பட்டது!'}; 
        } 
        return {status: 'error', msg: 'இவருக்கு பேலன்ஸ் ஏதும் இல்லை.'}; 
    } catch(e) { 
        return {status: 'error', msg: e.toString()}; 
    } 
}

function triggerTrainerLeaveAlert() { 
    try { 
        let db = getDatabaseData(); 
        let count = 0; 
        
        for (let i = 0; i < db.students.length; i++) { 
            if (db.students[i].status === 'Processing' && db.students[i].type !== 'Enquiry') { 
                sendWhatsAppMessage(db.students[i].phone, "🚫 *முக்கிய அறிவிப்பு:*\n\nஇன்று உங்களின் ஓட்டுநர் பயிற்சியாளர் விடுப்பு என்பதால், உங்களுக்கான பயிற்சி வகுப்பு இன்று செயல்படாது.\n\n- நிர்வாகம், நண்பன் டிரைவிங் ஸ்கூல்."); 
                count++; 
            } 
        } 
        return {status: 'success', msg: count + ' மாணவர்களுக்கு விடுப்பு மெசேஜ் அனுப்பப்பட்டது!'}; 
    } catch(e) { 
        return {status: 'error'}; 
    } 
}

function syncOldStudentsData() { 
    return {status: 'success', msg: 'பழைய டேட்டா சின்க் செய்யப்பட்டது!'}; 
}

// ------------------------------------------------------------------------------
// 11. CHIT FUND ENGINE (WORLD CLASS EDITION)
// ------------------------------------------------------------------------------

function ensureChitSheets() { 
    let ss = getDB(); 
    if (!ss.getSheetByName("Chit_Groups")) {
        ss.insertSheet("Chit_Groups").appendRow(["ID", "GroupName", "TotalAmount", "Months", "MembersCount", "Status", "RanjithQuota", "NandhaQuota", "CompanyQuota"]); 
    }
    if (!ss.getSheetByName("Chit_Members")) {
        ss.insertSheet("Chit_Members").appendRow(["ID", "Name", "Phone", "GroupName", "JoinedBy", "DateJoined"]); 
    }
    if (!ss.getSheetByName("Chit_Auctions")) {
        ss.insertSheet("Chit_Auctions").appendRow(["ID", "GroupName", "MonthNo", "Date", "WinnerName", "InterestRate", "DiscountAmount", "CompanyCommission", "PerHeadAmount", "Status", "Bidders", "Expenses", "NetProfit"]); 
    }
    if (!ss.getSheetByName("Chit_Payments")) {
        ss.insertSheet("Chit_Payments").appendRow(["ID", "AuctionID", "MemberName", "PaidAmount", "Receiver", "Date"]); 
    }
    if (!ss.getSheetByName("Chit_Live_Bids")) {
        ss.insertSheet("Chit_Live_Bids").appendRow(["Phone", "BidAmount", "Time"]); 
    }
    if (!ss.getSheetByName("Chit_Schedule")) {
        ss.insertSheet("Chit_Schedule").appendRow(["GroupName", "DisplayDate", "RawDate", "SavedOn"]);
    }
}

function getChitData() { 
    ensureChitSheets(); 
    let ss = getDB(); 
    let result = { members: [], auctions: [], payments: [], bids: [], groups: [] }; 
    
    try { 
        let shG = ss.getSheetByName("Chit_Groups");
        if (shG && shG.getLastRow() > 0) {
            let g = shG.getDataRange().getDisplayValues();
            for(let i = 1; i < g.length; i++) {
                if (g[i][0] && g[i][0] !== 'ID') {
                    result.groups.push({ 
                        id: g[i][0], 
                        name: g[i][1], 
                        total: g[i][2], 
                        months: g[i][3], 
                        members: g[i][4], 
                        status: g[i][5],
                        ranjithQuota: parseInt(g[i][6]) || 0,
                        nandhaQuota: parseInt(g[i][7]) || 0,
                        companyQuota: parseInt(g[i][8]) || 0
                    });
                }
            }
        }

        let shM = ss.getSheetByName("Chit_Members"); 
        if (shM && shM.getLastRow() > 0) { 
            let m = shM.getDataRange().getDisplayValues(); 
            for(let i = 1; i < m.length; i++) { 
                if (m[i][0] && m[i][0] !== 'ID') {
                    result.members.push({ id: m[i][0], name: m[i][1], phone: m[i][2], group: m[i][3], joinedBy: m[i][4], date: m[i][5] }); 
                }
            } 
        } 
        
        let shA = ss.getSheetByName("Chit_Auctions"); 
        if (shA && shA.getLastRow() > 0) { 
            let a = shA.getDataRange().getDisplayValues(); 
            for(let i = 1; i < a.length; i++) { 
                if (a[i][0] && a[i][0] !== 'ID') {
                    result.auctions.push({ 
                        id: a[i][0], 
                        group: a[i][1], 
                        month: a[i][2], 
                        date: a[i][3],
                        winner: a[i][4], 
                        interestRate: a[i][5], 
                        discount: a[i][6], 
                        commission: a[i][7], 
                        perHead: a[i][8], 
                        status: a[i][9],
                        expenses: a[i][11] || "0",
                        netProfit: a[i][12] || "0"
                    }); 
                }
            } 
        } 
        
        let shP = ss.getSheetByName("Chit_Payments"); 
        if (shP && shP.getLastRow() > 0) { 
            let p = shP.getDataRange().getDisplayValues(); 
            for(let i = 1; i < p.length; i++) { 
                if (p[i][0] && p[i][0] !== 'ID') {
                    // 🎯 ROBUST: Handle different column counts (Standard: 6, Historical: 9)
                    let pObj = { 
                        id: p[i][0],
                        auctionId: p[i][1], 
                        memberName: p[i][2], 
                        phone: p[i][3] || "",
                        amount: 0,
                        receiver: ""
                    };
                    
                    if (p[i].length > 6) {
                        // Historical/Old Entry format (9 cols)
                        pObj.amount = p[i][5];
                        pObj.receiver = p[i][8] || "Historical";
                    } else {
                        // Standard format (6 cols)
                        pObj.amount = p[i][3]; // Wait, saveChitPayment had amount at Index 3? Let me re-check.
                        pObj.receiver = p[i][4];
                    }
                    
                    // Actually, let's look at saveChitPayment again: [id, auctionId, memberName, amount, receiver, d]
                    // Index 3 is amount. Index 4 is receiver.
                    // Let's re-map standard carefully:
                    if (p[i].length <= 6) {
                        pObj.amount = p[i][3];
                        pObj.receiver = p[i][4];
                    }
                    
                    result.payments.push(pObj); 
                }
            } 
        } 
        
        let shB = ss.getSheetByName("Chit_Live_Bids"); 
        if (shB && shB.getLastRow() > 0) { 
            let b = shB.getDataRange().getDisplayValues(); 
            for(let i = 1; i < b.length; i++) { 
                if (b[i][0] && b[i][0] !== 'Phone') {
                    result.bids.push({ phone: b[i][0], amount: b[i][1], time: b[i][2] }); 
                }
            } 
        } 
    } catch(e) { 
        Logger.log("Chit DB Error: " + e.toString()); 
    } 
    return { status: 'success', data: result }; 
}

function saveChitGroup(gObj) {
    try {
        ensureChitSheets();
        let sheet = getDB().getSheetByName("Chit_Groups");
        let data = sheet.getDataRange().getValues();
        let foundRow = -1;
        let before = "";
        
        let idToUse = gObj.id || new Date().getTime();
        
        if (gObj.id) {
            for (let i = 1; i < data.length; i++) {
                if (data[i][0].toString() === gObj.id.toString()) {
                    foundRow = i + 1;
                    before = JSON.stringify({
                        id: data[i][0],
                        name: data[i][1],
                        total: data[i][2],
                        months: data[i][3],
                        members: data[i][4],
                        status: data[i][5],
                        rQuota: data[i][6],
                        nQuota: data[i][7],
                        cQuota: data[i][8]
                    });
                    break;
                }
            }
        }
        
        let rowData = [
            idToUse,
            gObj.name,
            gObj.total,
            gObj.months,
            gObj.members,
            gObj.status,
            gObj.rQuota || 0,
            gObj.nQuota || 0,
            gObj.cQuota || 0
        ];
        
        if (foundRow > -1) {
            sheet.getRange(foundRow, 1, 1, rowData.length).setValues([rowData]);
        } else {
            sheet.appendRow(rowData);
        }
        
        flushNoop_();
        try { logAuditEvent(foundRow > -1 ? 'UPDATE_CHIT_GROUP' : 'CREATE_CHIT_GROUP', 'ChitGroup', idToUse, before, JSON.stringify(gObj), { group: gObj.name }); } catch (e) {}
        return { status: 'success' };
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

function saveChitMember(memberObj) { 
    try { 
        ensureChitSheets(); 
        let sheet = getDB().getSheetByName("Chit_Members"); 
        let id = new Date().getTime(); 
        let d = getISTDate(); 
        
        memberObj = memberObj || {};
        memberObj.phone = normalizePhone10(memberObj.phone);
        if (String(memberObj.phone || "").length !== 10) {
            return { status: 'error', message: 'Invalid phone (10-digit required)' };
        }
        sheet.appendRow([ id, memberObj.name, memberObj.phone, memberObj.group, memberObj.joinedBy, d ]); 
        flushNoop_(); 
        try { logAuditEvent('CREATE_CHIT_MEMBER', 'ChitMember', id, "", JSON.stringify(memberObj), { group: memberObj.group, joinedBy: memberObj.joinedBy }); } catch (e) {}
        
        return { status: 'success' }; 
    } catch(e) { 
        return { status: 'error', message: e.toString() }; 
    } 
}

function editChitMemberData(memberObj) { 
    try { 
        let sheet = getDB().getSheetByName("Chit_Members"); 
        let data = sheet.getDataRange().getValues(); 
        
        for (let i = 1; i < data.length; i++) { 
            if (data[i][0].toString() === memberObj.id.toString()) { 
                let before = JSON.stringify({ id: data[i][0], name: data[i][1], phone: data[i][2], group: data[i][3], joinedBy: data[i][4] });
                memberObj.phone = normalizePhone10(memberObj.phone);
                if (String(memberObj.phone || "").length !== 10) {
                    return { status: 'error', message: 'Invalid phone (10-digit required)' };
                }
                sheet.getRange(i + 1, 2, 1, 4).setValues([[ memberObj.name, memberObj.phone, memberObj.group, memberObj.joinedBy ]]); 
                flushNoop_(); 
                try { logAuditEvent('UPDATE_CHIT_MEMBER', 'ChitMember', memberObj.id, before, JSON.stringify(memberObj), { group: memberObj.group, joinedBy: memberObj.joinedBy }); } catch (e) {}
                return { status: 'success' }; 
            } 
        } 
        return { status: 'error' }; 
    } catch(e) { 
        return { status: 'error', message: e.toString() }; 
    } 
}

function deleteChitMemberData(id) { 
    try { 
        let sheet = getDB().getSheetByName("Chit_Members"); 
        let data = sheet.getDataRange().getValues(); 
        
        for (let i = 1; i < data.length; i++) { 
            if (data[i][0].toString() === id.toString()) { 
                let before = JSON.stringify({ id: data[i][0], name: data[i][1], phone: data[i][2], group: data[i][3], joinedBy: data[i][4] });
                sheet.deleteRow(i + 1); 
                flushNoop_(); 
                try { logAuditEvent('DELETE_CHIT_MEMBER', 'ChitMember', id, before, "", {}); } catch (e) {}
                return { status: 'success' }; 
            } 
        } 
        return { status: 'error' }; 
    } catch(e) { 
        return { status: 'error', message: e.toString() }; 
    } 
}

function saveChitAuction(auctionObj, isOldHistory) { 
    try { 
        ensureChitSheets(); 
        let sheet = getDB().getSheetByName("Chit_Auctions"); 
        let id = new Date().getTime(); 
        let d = getISTDate(); 
        
        let expenses = parseInt(auctionObj.expenses) || 0;
        let netProfit = (parseInt(auctionObj.commission) || 0) - expenses;

        // If old history auction, save as 'Settled' directly. Otherwise 'Active'.
        let auctionStatus = isOldHistory ? "Settled" : "Active";
        
        sheet.appendRow([ 
            id, 
            auctionObj.group, 
            auctionObj.monthNo, 
            d, 
            auctionObj.winner, 
            auctionObj.interestRate || "0", 
            auctionObj.discount, 
            auctionObj.commission, 
            auctionObj.perHead, 
            auctionStatus,
            auctionObj.bidders || "",
            expenses,
            netProfit
        ]); 
        try { logAuditEvent('CREATE_CHIT_AUCTION', 'ChitAuction', id, "", JSON.stringify(auctionObj), { group: auctionObj.group, monthNo: auctionObj.monthNo, isOld: !!isOldHistory }); } catch (e) {}
        
        if (isOldHistory) {
            // 🔇 SILENT MODE: Auto-mark all group members as paid. No WhatsApp messages sent.
            try {
                let db = getChitData().data;
                let members = db.members.filter(m => m.group === auctionObj.group);
                let paymentSheet = getDB().getSheetByName("Chit_Payments");
                if (paymentSheet && members.length > 0) {
                    let perHead = parseInt(auctionObj.perHead) || 0;
                    members.forEach(m => {
                        try {
                            paymentSheet.appendRow([
                                new Date().getTime(),   // Payment ID
                                id,                     // Auction ID (🎯 FIX: Use the actual Auction ID instead of Group Name)
                                m.name,                 // Member Name
                                m.phone || "",          // Phone
                                auctionObj.monthNo,     // Month No
                                perHead,                // Amount Paid
                                d,                      // Date
                                "Paid",                 // Status
                                "Historical Entry"      // Note
                            ]);
                        } catch(pe) {}
                    });
                }
            } catch(autoPayErr) {}
        } else { 
            if (parseInt(auctionObj.commission) > 0) {
                saveExpenseData({ date: d, spender: "Office", cat: "🟢 வரவு - சீட்டு கமிஷன்", amt: auctionObj.commission, desc: `${auctionObj.group} (Month ${auctionObj.monthNo})` }); 
            }
            
            if (expenses > 0) {
                saveExpenseData({ date: d, spender: "Office", cat: "🔴 செலவு - சீட்டு செலவு", amt: expenses, desc: `${auctionObj.group} (Month ${auctionObj.monthNo})` });
            }
            
            let bidSheet = getDB().getSheetByName("Chit_Live_Bids"); 
            if(bidSheet && bidSheet.getLastRow() > 1) { 
                bidSheet.deleteRows(2, bidSheet.getLastRow() - 1); 
            } 
            
            // 🌟 FEATURE 2: PDF E-INVOICE TO ALL MEMBERS
            let db = getChitData().data;
            let members = db.members.filter(m => m.group === auctionObj.group);
            let invoiceUrl = generateAuctionInvoicePDF(auctionObj);
            
            // 🏆 ஆட்டோ-டெக்ஷன்: ஏலம் எடுத்தவர் பெயரைக் கண்டறிதல்
            let winnerInfo = getAuctionWinnerByMonth(auctionObj.group, auctionObj.monthNo);
            let winnerName = winnerInfo ? winnerInfo.name : (auctionObj.winner || "தகவல் இல்லை");
            let winnerPhone = winnerInfo ? winnerInfo.phone : "";
            
            let winMsg = `📢 *சீட்டு ஏல முடிவு - Month ${auctionObj.monthNo}*\n\nகுழு: ${auctionObj.group}\n🏆 ஏலம் எடுத்தவர்: *${winnerName}*${winnerPhone ? `\n📱 போன்: ${winnerPhone}` : ""}\n💰 ஏலத் தொகை: ₹${winnerInfo ? winnerInfo.bidAmount : "தகவல் இல்லை"}\nதள்ளுபடி: ₹${auctionObj.discount}\n\nஇந்த மாதம் ஒவ்வொருவரும் கட்ட வேண்டிய தொகை: *₹${auctionObj.perHead}*\n\nதயவுசெய்து தொகையைச் செலுத்தவும். 🙏`;
            
            members.forEach((m, idx) => {
                if(m.phone) {
                    try {
                        if(idx > 0) Utilities.sleep(3000);
                        
                        let cfg = getTemplateAndReminderConfig();
                        let aucRes = sendTemplateWithParamFallback(
                            m.phone,
                            cfg.chitAuctionTemplate || "chit_auction_alert",
                            [[String(auctionObj.group || "-"), String(d || "-")], [String(auctionObj.group || "-")], []],
                            null
                        );
                        if (!aucRes || aucRes.status === 'error') {
                            sendWhatsAppMessage(m.phone, winMsg);
                        }
                        if (invoiceUrl) {
                            sendWhatsAppDocumentMessage(m.phone, getDrivePdfDownloadUrl(invoiceUrl), `Auction_Invoice_M${auctionObj.monthNo}.pdf`, `சீட்டு விவரம் - ${auctionObj.group}`);
                        }
                    } catch(err){}
                }
            });
        } 
        
        flushNoop_(); 
        return { status: 'success', auctionId: id }; 
    } catch(e) { 
        return { status: 'error' }; 
    } 
}

function settleAuctionWinner(auctionId, loggedBy) {
    try {
        let sheet = getDB().getSheetByName("Chit_Auctions");
        if (!sheet) return { status: 'error', message: 'Sheet not found' };
        let data = sheet.getDataRange().getValues();
        for (let i = 1; i < data.length; i++) {
            if (data[i][0].toString() === auctionId.toString()) {
                let currentStatus = data[i][9];
                if (currentStatus === "Settled") return { status: 'error', message: 'Already settled' };
                sheet.getRange(i + 1, 10).setValue("Settled");
                flushNoop_();
                try { logAuditEvent('SETTLE_CHIT_AUCTION', 'ChitAuction', auctionId, "", "Settled", { by: loggedBy }); } catch (e) {}
                notifyAdmins(`🤝 *சீட்டு பட்டுவாடா நிறைவு*\n\nஏல எண்: ${auctionId}\nநிலை: Settled`);
                return { status: 'success' };
            }
        }
        return { status: 'error', message: 'Auction not found' };
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

function deleteChitAuction(auctionId, loggedBy) {
    try {
        if (loggedBy && !isPrivilegedName(loggedBy)) return { status: 'error', message: 'Not allowed' };
        let sheet = getDB().getSheetByName("Chit_Auctions");
        if (!sheet) return { status: 'error', message: 'Sheet not found' };
        let data = sheet.getDataRange().getValues();
        for (let i = 1; i < data.length; i++) {
            if (data[i][0].toString() === auctionId.toString()) {
                sheet.deleteRow(i + 1);
                flushNoop_();
                try { logAuditEvent('DELETE_CHIT_AUCTION', 'ChitAuction', auctionId, "", 'Deleted', { by: loggedBy }); } catch (e) {}
                return { status: 'success' };
            }
        }
        return { status: 'error', message: 'Auction not found' };
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

// 🌟 FEATURE 2.1: PDF Generation for Auction Invoice
function generateAuctionInvoicePDF(aucObj) {
    try {
        let htmlTemplate = `
            <html>
            <head>
                <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;800&display=swap" rel="stylesheet">
                <style>
                    body { font-family: 'Plus Jakarta Sans', sans-serif; background: #f8fafc; padding: 40px; }
                    .card { background: white; border-radius: 24px; padding: 40px; box-shadow: 0 10px 30px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; max-width: 600px; margin: auto; position: relative; overflow: hidden; }
                    .header-strip { position: absolute; top: 0; left: 0; right: 0; height: 8px; background: #8b5cf6; }
                    h1 { color: #0f172a; margin: 0; font-size: 28px; font-weight: 800; }
                    .subtitle { color: #64748b; font-size: 14px; font-weight: 600; margin-top: 5px; text-transform: uppercase; letter-spacing: 1px; }
                    .divider { border: none; border-top: 1px dashed #e2e8f0; margin: 25px 0; }
                    .info-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; text-align: left; }
                    .info-item { display: flex; flex-direction: column; gap: 4px; }
                    .label { font-size: 11px; color: #94a3b8; font-weight: 800; text-transform: uppercase; }
                    .value { font-size: 16px; color: #1e293b; font-weight: 700; }
                    .amount-box { background: #f5f3ff; border: 1px solid #ddd6fe; border-radius: 16px; padding: 20px; margin-top: 25px; text-align: center; }
                    .amount-label { font-size: 13px; color: #7c3aed; font-weight: 800; margin-bottom: 5px; }
                    .amount-value { font-size: 32px; color: #6d28d9; font-weight: 800; }
                    .footer { font-size: 11px; color: #94a3b8; margin-top: 30px; font-weight: 600; }
                </style>
            </head>
            <body>
                <div class="card">
                    <div class="header-strip"></div>
                    <h1>நண்பன் சீட்டு நிறுவனம்</h1>
                    <p class="subtitle">ஏல விவரம் • MONTH ${aucObj.monthNo}</p>
                    <div class="divider"></div>
                    <div class="info-grid">
                        <div class="info-item"><span class="label">குழு</span><span class="value">${aucObj.group}</span></div>
                        <div class="info-item"><span class="label">ஏலம் எடுத்தவர்</span><span class="value" style="color:#3b82f6;">${aucObj.winner}</span></div>
                        <div class="info-item"><span class="label">வட்டி (%)</span><span class="value">${aucObj.interestRate}%</span></div>
                        <div class="info-item"><span class="label">தள்ளுபடி</span><span class="value" style="color:#ef4444;">₹${aucObj.discount}</span></div>
                    </div>
                    <div class="amount-box">
                        <div class="amount-label">நீங்கள் செலுத்த வேண்டியது</div>
                        <div class="amount-value">₹${aucObj.perHead}</div>
                    </div>
                    <div class="footer">
                        * இது கணினியால் உருவாக்கப்பட்ட டிஜிட்டல் அறிவிப்பு. <br>
                        தாமதமின்றி பணத்தைச் செலுத்தும்படி கேட்டுக்கொள்கிறோம்.
                    </div>
                </div>
            </body>
            </html>
        `;
        
        let blob = Utilities.newBlob(htmlTemplate, MimeType.HTML).getAs(MimeType.PDF).setName("Auction_Invoice_M" + aucObj.monthNo + ".pdf");
        
        let folders = DriveApp.getFoldersByName("Nanban_Uploads"); 
        let folder = folders.hasNext() ? folders.next() : DriveApp.createFolder("Nanban_Uploads");
        
        let file = folder.createFile(blob); 
        file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW); 
        Utilities.sleep(3000); 
        
        return file.getId();
    } catch(e) { 
        return null; 
    }
}

function generateChitPDFReceipt(payObj) { 
    try { 
        let htmlTemplate = ` 
            <html>
            <head>
                <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;800&display=swap" rel="stylesheet">
                <style>
                    body { font-family: 'Plus Jakarta Sans', sans-serif; background: #f8fafc; padding: 40px; }
                    .card { background: white; border-radius: 24px; padding: 40px; box-shadow: 0 10px 30px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; max-width: 600px; margin: auto; position: relative; overflow: hidden; }
                    .header-strip { position: absolute; top: 0; left: 0; right: 0; height: 8px; background: #10b981; }
                    h1 { color: #0f172a; margin: 0; font-size: 28px; font-weight: 800; }
                    .subtitle { color: #64748b; font-size: 14px; font-weight: 600; margin-top: 5px; text-transform: uppercase; letter-spacing: 1px; }
                    .divider { border: none; border-top: 1px dashed #e2e8f0; margin: 25px 0; }
                    .info-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; text-align: left; }
                    .info-item { display: flex; flex-direction: column; gap: 4px; }
                    .label { font-size: 11px; color: #94a3b8; font-weight: 800; text-transform: uppercase; }
                    .value { font-size: 16px; color: #1e293b; font-weight: 700; }
                    .amount-box { background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 16px; padding: 20px; margin-top: 25px; text-align: center; }
                    .amount-label { font-size: 13px; color: #059669; font-weight: 800; margin-bottom: 5px; }
                    .amount-value { font-size: 32px; color: #047857; font-weight: 800; }
                    .footer { font-size: 11px; color: #94a3b8; margin-top: 30px; font-weight: 600; }
                </style>
            </head>
            <body>
                <div class="card">
                    <div class="header-strip"></div>
                    <h1>நண்பன் சீட்டு நிறுவனம்</h1>
                    <p class="subtitle">அதிகாரப்பூர்வ மின்-ரசீது (E-RECEIPT)</p>
                    <div class="divider"></div>
                    <div class="info-grid">
                        <div class="info-item"><span class="label">தேதி</span><span class="value">${getISTDate()}</span></div>
                        <div class="info-item"><span class="label">பெயர்</span><span class="value">${payObj.memberName}</span></div>
                        <div class="info-item"><span class="label">குழு</span><span class="value">${payObj.groupName || "-"}</span></div>
                        <div class="info-item"><span class="label">வசூலர்</span><span class="value">${payObj.receiver}</span></div>
                    </div>
                    <div class="amount-box">
                        <div class="amount-label">பெறப்பட்ட தொகை</div>
                        <div class="amount-value">₹${payObj.amount}</div>
                    </div>
                    <div class="footer">
                        * இது கணினியால் உருவாக்கப்பட்ட டிஜிட்டல் ரசீது. <br>
                        நண்பன் சீட்டு நிறுவனத்துடன் இணைந்தமைக்கு நன்றி.
                    </div>
                </div>
            </body>
            </html>
        `; 
        
        let blob = Utilities.newBlob(htmlTemplate, MimeType.HTML).getAs(MimeType.PDF).setName("Nanban_Chit_Receipt_" + payObj.memberName + ".pdf"); 
        
        let folders = DriveApp.getFoldersByName("Nanban_Uploads"); 
        let folder; 
        
        if (folders.hasNext()) { 
            folder = folders.next(); 
        } else { 
            folder = DriveApp.createFolder("Nanban_Uploads"); 
        } 
        
        let file = folder.createFile(blob); 
        file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW); 
        Utilities.sleep(3000); 
        
        return file.getId(); 
    } catch(e) { 
        return null; 
    } 
}

function saveChitPayment(payObj) { 
    try { 
        ensureChitSheets(); 
        let sheet = getDB().getSheetByName("Chit_Payments"); 
        let id = new Date().getTime(); 
        let d = getISTDate(); 
        payObj = payObj || {};
        payObj.phone = normalizePhone10(payObj.phone);
        if (payObj.phone && String(payObj.phone).length !== 10) {
            return { status: 'error', message: 'Invalid phone (10-digit required)' };
        }
        sheet.appendRow([ id, payObj.auctionId, payObj.memberName, payObj.amount, payObj.receiver, d ]); 
        flushNoop_(); 
        try { logAuditEvent('CREATE_CHIT_PAYMENT', 'ChitPayment', id, "", JSON.stringify(payObj), { receiver: payObj.receiver, auctionId: payObj.auctionId }); } catch (e) {}
        
        let pdfId = generateChitPDFReceipt(payObj); 
        
        if (payObj.phone) { 
            let msg = `💰 வணக்கம் ${payObj.memberName},\nஉங்களின் இந்த மாத சீட்டுத் தொகை ₹${payObj.amount} பெறப்பட்டது. (வசூலர்: ${payObj.receiver})\nநன்றி! 🙏\n- நண்பன் சீட்டு நிறுவனம்`; 
            
            if (pdfId) { 
                let docLink = getDrivePdfDownloadUrl(pdfId); 
                let cfg = getTemplateAndReminderConfig();
                let components = [ 
                    { "type": "header", "parameters": [{ "type": "document", "document": { "link": docLink, "filename": "Chit_Receipt.pdf" } }] }, 
                    { "type": "body", "parameters": [ { "type": "text", "text": payObj.memberName }, { "type": "text", "text": payObj.amount.toString() }, { "type": "text", "text": String(payObj.receiver || "-") } ] } 
                ]; 
                
                let payload = { 
                    "messaging_product": "whatsapp", 
                    "to": cleanPhoneNumber(payObj.phone), 
                    "type": "template", 
                    "template": { "name": (cfg.chitReceiptTemplate || "chit_payment_receipt"), "language": { "code": "ta" }, "components": components } 
                }; 
                
                try {
                    let res = UrlFetchApp.fetch("https://graph.facebook.com/v20.0/" + WA_PHONE_ID + "/messages", { 
                        "method": "post", 
                        "headers": { "Authorization": "Bearer " + getCleanToken(), "Content-Type": "application/json" }, 
                        "payload": JSON.stringify(payload), 
                        "muteHttpExceptions": true 
                    }); 
                    if (res.getResponseCode() !== 200) {
                        sendWhatsAppMessage(payObj.phone, msg);
                        try { sendWhatsAppDocumentMessage(payObj.phone, docLink, "Chit_Receipt.pdf", "சீட்டு ரசீது"); } catch(e){}
                    }
                } catch(e) {
                    sendWhatsAppMessage(payObj.phone, msg);
                    try { sendWhatsAppDocumentMessage(payObj.phone, docLink, "Chit_Receipt.pdf", "சீட்டு ரசீது"); } catch(e){}
                }
            } else { 
                sendWhatsAppMessage(payObj.phone, msg); 
            } 
        } 
        
        notifyAdmins(`💰 *சீட்டு வசூல்!*\n\nமெம்பர்: ${payObj.memberName}\nதொகை: ₹${payObj.amount}\nபெற்றவர்: ${payObj.receiver}`); 

        
        return { status: 'success' }; 
    } catch(e) { 
        return { status: 'error', message: e.toString() }; 
    } 
}

function sendChitBulkAlert(phonesArray, msgTemplate) { 
    try { 
        let successCount = 0; 
        let cfg = getTemplateAndReminderConfig();
        for (let i = 0; i < phonesArray.length; i++) { 
            let item = phonesArray[i];
            let phone = (typeof item === 'object' && item) ? item.phone : item;
            let name = (typeof item === 'object' && item) ? item.name : "";
            let group = (typeof item === 'object' && item) ? item.group : "";
            if (phone) {
                if (i > 0) Utilities.sleep(3000); 
                let t = sendTemplateWithParamFallback(phone, cfg.chitDueTemplate || "chit_due_reminder", [[String(name || "நண்பரே"), String(group || "சீட்டு குழு")], [String(name || "நண்பரே")], []], null);
                if (!t || t.status === 'error') {
                    sendWhatsAppMessage(phone, msgTemplate);
                }
                successCount++; 
            } 
        } 
        return { status: 'success', msg: `${successCount} பேருக்கு மெசேஜ் அனுப்பப்பட்டது!` }; 
    } catch(e) { 
        return { status: 'error', message: e.toString() }; 
    } 
}

function sendTemplateTestAction(templateName, toPhone) {
    try {
        let t = String(templateName || "").trim();
        let p = cleanPhoneNumber(toPhone || "");
        if (!t || !p) return { status: 'error', msg: 'template/phone required' };
        let cfg = getTemplateAndReminderConfig();
        let r = sendTemplateWithParamFallback(
            p, t,
            [
                [ "ரஞ்சித்", "கார் பயிற்சி", getISTDate(), "3500", "1000", "2500" ],
                [ "ரஞ்சித்", "2500" ],
                [ "ரஞ்சித்" ],
                [ "Demo announcement text" ],
                []
            ],
            null
        );
        if (!r || r.status === 'error') {
            sendWhatsAppMessage(p, `Template test fallback sent for: ${t}`);
        }
        return { status: 'success', msg: `Template test sent: ${t}`, cfg: cfg };
    } catch (e) {
        return { status: 'error', msg: e.toString() };
    }
}

// 🌟 FEATURE 1: CHIT PRE-ALERT LOGIC
function sendChitAdvanceAlert(groupName, dateText, note, rawDate) {
    try {
        let db = getChitData().data;
        let members = db.members.filter(m => m.group === groupName);
        let count = 0;
        let noteStr = note ? `\n\n📌 குறிப்பு: ${note}` : '';
        // Calculate next auction number for this group
        let pastAuctions = (db.auctions || []).filter(a => a.group === groupName);
        let nextAucNo = pastAuctions.length + 1;
        let aucNoStr = nextAucNo + 'வது';
        
        // Auto-save schedule for auto-reminder trigger
        if (rawDate) {
            try { saveChitAuctionSchedule(groupName, dateText, rawDate); } catch(e) {}
        }
        
        members.forEach((m, idx) => {
            if(m.phone) {
                try {
                    if(idx > 0) Utilities.sleep(3000);
                    
                    // Clean, simple message
                    let personalMsg = `வணக்கம் ${m.name}!\n\n*${groupName}* - ${aucNoStr} ஏலம் ${dateText} நடைபெறுகிறது.${noteStr}\n\n- நண்பன் சீட்டு`;
                    
                    // Try template first, fallback to personalized direct message
                    let res = sendTemplateWithParamFallback(
                        m.phone, 
                        "chit_auction_alert", 
                        [[m.name, groupName, dateText], [groupName, dateText], [groupName], []], 
                        null
                    );
                    
                    if (!res || res.status === 'error') {
                        sendWhatsAppMessage(m.phone, personalMsg);
                    }
                    count++;
                } catch(err){}
            }
        });
        
        return {status: 'success', msg: `${count} பேருக்கு அறிவிப்பு சென்றது!`};
    } catch(e) {
        return {status: 'error', msg: e.toString()};
    }
}

function triggerLiveChitBidding(groupName) { 
    try { 
        let db = getChitData().data; 
        let members = db.members.filter(m => m.group === groupName); 
        let count = 0; 
        let bidMsg = `📢 *சீட்டு ஏலம் ஆரம்பம்!*\n\nகுழு: ${groupName}\n\nஉங்களின் ஏலத் தொகையை (எ.கா: 15000) வாட்ஸ்அப்பில் ரிப்ளை செய்யவும். ஏலம் அரை மணி நேரத்தில் முடிவடையும்.`; 
        
        members.forEach((m, idx) => { 
            if(m.phone) { 
                if(idx > 0) Utilities.sleep(3000); 
                sendWhatsAppMessage(m.phone, bidMsg); 
                count++; 
            } 
        }); 
        return { status: 'success', msg: `${count} பேருக்கு ஏல அறிவிப்பு சென்றது!`}; 
    } catch(e) { 
        return { status: 'error' }; 
    } 
}

// ------------------------------------------------------------------------------
// 12. FILE UPLOAD LOGIC
// ------------------------------------------------------------------------------

function processFileUpload(fileData, fileName) { 
    return uploadFileToDrive(fileData, fileName); 
}

function uploadFileToDrive(base64Data, fileName) { 
    try { 
        let splitBase = base64Data.split(','); 
        let mimeType = splitBase[0].split(';')[0].replace('data:', ''); 
        let blob = Utilities.newBlob(Utilities.base64Decode(splitBase[1]), mimeType, fileName); 
        
        let folder; 
        let folders = DriveApp.getFoldersByName("Nanban_Uploads"); 
        
        if (folders.hasNext()) { 
            folder = folders.next(); 
        } else { 
            folder = DriveApp.createFolder("Nanban_Uploads"); 
        } 
        
        let file = folder.createFile(blob); 
        file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW); 
        
        let fileId = file.getId();
        return { url: getDrivePdfDownloadUrl(fileId), id: fileId }; 
    } catch(e) { 
        return { error: e.toString() }; 
    } 
}

/**
 * 🚧 Road Safety Educational Card Helper
 */
function sendRoadSafetyCard(senderPhone, cardId) {
    let cardData = {
        "CARD_ZEBRA": {
            title: "🛑 பாதசாரிகள் கடக்கும் இடம் (Zebra Crossing)",
            text: "இந்த வெள்ளை நிறக் கோடுகள் பாதசாரிகள் பாதுகாப்பாகச் சாலையைக் கடப்பதற்காகப் போடப்பட்டவை.\n\n✅ டிரைவர்கள் இந்தக் கோடுகளுக்கு முன்பே வாகனத்தை நிறுத்த வேண்டும்.",
            image: ""
        },
        "CARD_STOPLINE": {
             title: "🚦 நிறுத்தக் கோடு (Stop Line)",
             text: "சிக்னல்களில் போடப்பட்டுள்ள இந்த தடிமனான வெள்ளைக் கோடு 'நிறுத்தக் கோடு' எனப்படும்.\n\n✅ சிக்னலில் சிவப்பு விளக்கு எரியும் போது, உங்கள் வண்டியின் (TN 66 AE 8590) எந்தப் பகுதியும் இந்தக் கோட்டைத் தாண்டாதவாறு நிறுத்த வேண்டும்.",
             image: ""
        },
        "CARD_STOP": {
             title: "🛑 STOP - நில் சின்னம்",
             text: "இந்தச் சின்னம் இருந்தால் வாகனத்தை முழுமையாக நிறுத்த வேண்டும்.",
             image: ""
        },
        "CARD_NO_OVERTAKE": {
             title: "🚳 ஓவர்-டேக் செய்யக்கூடாது",
             text: "வளைவுகள் மற்றும் அபாயகரமான இடங்களில் இந்தச் சின்னம் இருக்கும். முன்னால் செல்லும் வாகனத்தை முந்தக் கூடாது.\n\n⚠️ இது விபத்துகளைத் தவிர்க்க உதவும் முக்கியமான விதி.",
             image: ""
        },
        "CARD_HORN": {
             title: "📣 ஹார்ன் அடிக்கக்கூடாது (No Horn)",
             text: "மருத்துவமனைகள் மற்றும் பள்ளி வளாகங்களுக்கு அருகில் இந்தச் சின்னம் இருக்கும்.\n\n✅ தேமையற்ற சத்தத்தைத் தவிர்த்து அமைதியைப் பாதுகாப்போம்.",
             image: ""
        }
    };

    let card = cardData[cardId];
    if (!card) return;

    let msg = `*${card.title}*\n\n${card.text}\n\nநண்பன் டிரைவிங் ஸ்கூல் - விபத்தில்லா தமிழ்நாடு! 🚦`;
    sendWhatsAppInteractiveButtons(senderPhone, msg, [{ id: "MENU_ROAD_SAFETY", title: "🔙 Back to Rules" }]);
}

/**
 * 📝 Log Bot Activity to Sheet (For Debugging)
 */
function logBotActivity(type, data) {
    try {
        let ss = getDB();
        let sh = ss.getSheetByName("Bot_Logs") || ss.insertSheet("Bot_Logs");
        if (sh.getLastRow() === 0) sh.appendRow(["Time", "Type", "Data"]);
        sh.appendRow([new Date().toLocaleString("en-US", { timeZone: "Asia/Kolkata" }), type, String(data).substring(0, 1000)]);
    } catch(e) {}
}


// ============================================================
-// 🔔 AUTO ENQUIRY FOLLOW-UP SYSTEM
-// Run setupFollowUpTrigger() ONCE to activate daily automation
-// ============================================================

/**
 * 🔔 Main Follow-up Runner — called daily by time trigger
 * Sends WhatsApp follow-up to Enquiry leads who haven't enrolled
 */
function runEnquiryFollowUp() {
    try {
        let db = getDatabaseData();
        let students = db.students || [];
        let now = new Date();
        let sent24h = 0, sent72h = 0, skipped = 0;

        students.forEach(function(s) {
            // Only process active Enquiry-type students
            if (s.type !== 'Enquiry' || s.status === 'Deleted') { skipped++; return; }
            if (!s.phone || !s.dateJoined) { skipped++; return; }

            // Parse joining date (DD-MM-YYYY)
            let dj = String(s.dateJoined).trim();
            let djDate = null;
            let parts = dj.split('-');
            if (parts.length === 3) {
                djDate = new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
            }
            if (!djDate || isNaN(djDate.getTime())) { skipped++; return; }

            let hoursSince = (now - djDate) / (1000 * 60 * 60);
            let followUpFlags = String(s.adminRemarks || '');
            let alreadyFollowed24 = followUpFlags.includes('FOLLOWUP_24H');
            let alreadyFollowed72 = followUpFlags.includes('FOLLOWUP_72H');

            // 24-hour follow-up (send between 20h and 36h)
            if (hoursSince >= 20 && hoursSince <= 36 && !alreadyFollowed24) {
                let msg24 = `வணக்கம் *${s.name}*! 🙏\n\n` +
                    `நேற்று நண்பன் டிரைவிங் ஸ்கூலில் உங்கள் enquiry பதிவு செய்தீர்கள்.\n\n` +
                    `🚗 *இன்னும் join ஆகலையா?*\n\n` +
                    `✅ Maruti S-Presso-ல் 100% practical பயிற்சி\n` +
                    `✅ Flexible morning/evening batches\n` +
                    `✅ RTO license guaranteed support\n\n` +
                    `👇 இப்பவே confirm பண்ண, கீழே reply பண்ணுங்கள்:\n` +
                    `*"CONFIRM"* என்று type பண்ணுங்கள் — slot block பண்ணிவிடுகிறோம்!`;
                let res = sendWhatsAppInteractiveButtons(
                    s.phone, msg24,
                    [{ id: 'ADM_DEMO', title: '✅ Confirm Admission' }, { id: 'GOTO_MENU', title: '❓ More Info' }]
                );
                if (res && res.status === 'success') {
                    markFollowUpDone(s, 'FOLLOWUP_24H');
                    sent24h++;
                }
                Utilities.sleep(500);
            }

            // 72-hour follow-up (send between 68h and 80h) — last chance message
            else if (hoursSince >= 68 && hoursSince <= 80 && alreadyFollowed24 && !alreadyFollowed72) {
                let msg72 = `வணக்கம் *${s.name}*! 🙏\n\n` +
                    `3 நாட்களுக்கு முன் நம்ம ஸ்கூல் பற்றி enquiry பண்ணீர்கள்.\n\n` +
                    `⚠️ *இந்த week-ல் சில slots மட்டுமே available.*\n\n` +
                    `இப்ப join பண்ணினால்:\n` +
                    `🎁 *Special: முதல் class FREE demo!*\n\n` +
                    `📞 ஒரு call பண்ணுங்கள் — 2 நிமிடத்தில் admission முடிய வைக்கிறோம்!`;
                let res = sendWhatsAppInteractiveButtons(
                    s.phone, msg72,
                    [{ id: 'ADM_DEMO', title: '🚗 Admission பண்ணலாம்' }, { id: 'ADM_HELP', title: '📞 Call Admin' }]
                );
                if (res && res.status === 'success') {
                    markFollowUpDone(s, 'FOLLOWUP_72H');
                    sent72h++;
                }
                Utilities.sleep(500);
            }
        });

        let summary = `✅ Follow-up done: 24h=${sent24h}, 72h=${sent72h}, skipped=${skipped}`;
        Logger.log(summary);
        notifyAdmins(`🔔 Auto Follow-up Report:\n📨 24h sent: ${sent24h}\n📨 72h sent: ${sent72h}\n⏭ Skipped: ${skipped}`);
        return summary;
    } catch(err) {
        Logger.log('Follow-up Error: ' + err.toString());
        return 'ERROR: ' + err.toString();
    }
}

/**
 * 🏷️ Mark a student as followed up in their sheet record
 */
function markFollowUpDone(student, flag) {
    try {
        let ss = getDB();
        let sh = ss.getSheetByName('Students') || ss.getSheets()[0];
        let data = sh.getDataRange().getValues();
        for (let i = 1; i < data.length; i++) {
            // Find by ID or phone in column 1 or 3
            let rowId = String(data[i][0]);
            let rowPhone = String(data[i][2]);
            if (rowId === student.id || cleanPhoneNumber(rowPhone) === cleanPhoneNumber(student.phone)) {
                // Update metadata JSON in last column
                let metaCol = data[i].length - 1;
                let metaStr = String(data[i][metaCol]);
                try {
                    let meta = JSON.parse(metaStr);
                    if (!meta.adminRemarks) meta.adminRemarks = [];
                    meta.adminRemarks.push({ date: getISTDate(), text: flag + ': Auto follow-up sent.' });
                    sh.getRange(i + 1, metaCol + 1).setValue(JSON.stringify(meta));
                } catch(je) {
                    // Fallback: append flag to a notes column if JSON parse fails
                    sh.getRange(i + 1, metaCol + 1).setValue(metaStr + ' | ' + flag);
                }
                break;
            }
        }
    } catch(e) {
        Logger.log('markFollowUpDone error: ' + e.toString());
    }
}

/**
 * ⏰ Setup daily trigger — Run this ONCE from Apps Script editor
 * Goes to: Run → setupFollowUpTrigger
 */
function setupFollowUpTrigger() {
    // Remove existing follow-up triggers to avoid duplicates
    ScriptApp.getProjectTriggers().forEach(function(t) {
        if (t.getHandlerFunction() === 'runEnquiryFollowUp') {
            ScriptApp.deleteTrigger(t);
        }
    });
    // Create new daily trigger at 10 AM IST (4:30 AM UTC)
    ScriptApp.newTrigger('runEnquiryFollowUp')
        .timeBased()
        .everyDays(1)
        .atHour(10)
        .create();
    Logger.log('✅ Follow-up trigger set: Daily at 10 AM');
    return 'Trigger created successfully!';
}

/**
 * 🧪 Test Function (User can run this in Editor)
 */
function testBotConnectivity() {
    let testNum = ADMINS[0];
    let res = sendWhatsAppMessage(testNum, "🔔 Bot Connectivity Test: OK.\nTime: " + new Date().toLocaleString());
    Logger.log("Result: " + JSON.stringify(res));
    return res;
}


// ===========================================================================
// 📒 MEMBER PASSBOOK
// ===========================================================================
/**
 * 🛠 UTILITY: Fixes bad auctionId in Chit_Payments sheet.
 * Run this if old auctions are still showing as "Pending".
 */
function fixAllHistoricalChitPayments() {
    try {
        let ss = getDB();
        let paySheet = ss.getSheetByName("Chit_Payments");
        let aucSheet = ss.getSheetByName("Chit_Auctions");
        if (!paySheet || !aucSheet) return { status: 'error', message: 'Sheets not found' };

        let payData = paySheet.getDataRange().getValues();
        let aucData = aucSheet.getDataRange().getValues();
        
        let fixes = 0;
        
        // Loop through payments starting from row 2
        for (let i = 1; i < payData.length; i++) {
            let row = payData[i];
            let currentAuctionId = String(row[1]); // Column B
            
            // Check if currentAuctionId is a group name (contains letters or spaces) instead of a numeric ID
            if (/[a-zA-Z\s]/.test(currentAuctionId)) {
                let groupName = currentAuctionId;
                let memberName = row[2];
                let monthNo = row[4]; // In historical format, month is at index 4
                
                // Try to find the correct auction ID from Chit_Auctions
                let correctAuction = aucData.find(a => String(a[1]) === groupName && String(a[2]) === String(monthNo));
                
                if (correctAuction) {
                    let correctId = correctAuction[0];
                    paySheet.getRange(i + 1, 2).setValue(correctId); // Update Auction ID
                    fixes++;
                }
            }
        }
        
        flushNoop_();
        return { status: 'success', message: `${fixes} payments were fixed and marked as Paid! ✅` };
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

function getMemberChitPassbook(memberName) {
    try {
        ensureChitSheets();
        let db = getChitData().data;
        
        // All groups this member belongs to
        let memberEntries = (db.members || []).filter(m => m.name === memberName);
        
        let groupSummaries = memberEntries.map(function(m) {
            let groupAuctions = (db.auctions || []).filter(a => a.group === m.group).sort((a,b) => parseInt(a.month) - parseInt(b.month));
            let winEntry = groupAuctions.find(a => a.winner === memberName);
            
            let payments = groupAuctions.map(function(a) {
                let pay = (db.payments || []).find(p => p.auctionId == a.id && p.memberName === memberName);
                return {
                    month: a.month,
                    date: a.date,
                    perHead: a.perHead,
                    paid: !!pay,
                    paidAmt: pay ? pay.amount : 0,
                    receiver: pay ? pay.receiver : ''
                };
            });
            
            let totalPaid = payments.reduce((s, p) => s + (parseInt(p.paidAmt) || 0), 0);
            let pendingMonths = payments.filter(p => !p.paid).length;
            
            return {
                group: m.group,
                joinedBy: m.joinedBy,
                joinDate: m.date,
                won: !!winEntry,
                wonMonth: winEntry ? winEntry.month : null,
                payments: payments,
                totalPaid: totalPaid,
                pendingMonths: pendingMonths
            };
        });
        
        return { status: 'success', name: memberName, groups: groupSummaries };
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

// ===========================================================================
// ⏰ AUTO REMINDER SCHEDULER
// ===========================================================================

function saveChitAuctionSchedule(groupName, displayDate, rawDate) {
    try {
        ensureChitSheets();
        let sheet = getDB().getSheetByName("Chit_Schedule");
        let data = sheet.getDataRange().getValues();
        // Update if group already exists, else append
        for (let i = 1; i < data.length; i++) {
            if (data[i][0] === groupName) {
                sheet.getRange(i+1, 2, 1, 3).setValues([[displayDate, rawDate, getISTDate()]]);
                flushNoop_();
                return { status: 'success' };
            }
        }
        sheet.appendRow([groupName, displayDate, rawDate, getISTDate()]);
        flushNoop_();
        return { status: 'success' };
    } catch(e) {
        return { status: 'error', message: e.toString() };
    }
}

function chitAutoReminderCheck() {
    try {
        ensureChitSheets();
        let sheet = getDB().getSheetByName("Chit_Schedule");
        if (!sheet || sheet.getLastRow() < 2) return;
        let data = sheet.getDataRange().getValues();
        let today = new Date(); today.setHours(0,0,0,0);
        let db = getChitData().data;
        
        for (let i = 1; i < data.length; i++) {
            let groupName = data[i][0];
            let displayDate = data[i][1];
            let rawDate = data[i][2]; // YYYY-MM-DD
            if (!rawDate) continue;
            
            let auctionDate = new Date(rawDate + 'T00:00:00');
            let diffDays = Math.round((auctionDate - today) / (1000*60*60*24));
            
            if (diffDays === 5 || diffDays === 1) {
                // Send reminder to all members of this group
                let members = (db.members || []).filter(m => m.group === groupName);
                let pastAuctions = (db.auctions || []).filter(a => a.group === groupName);
                let nextAucNo = pastAuctions.length + 1;
                let label = diffDays === 1 ? 'நாளை' : '5 நாட்களில்';
                
                members.forEach(function(m, idx) {
                    if (!m.phone) return;
                    try {
                        if (idx > 0) Utilities.sleep(3000);
                        let msg = `வணக்கம் ${m.name}!\n\n*${groupName}* - ${nextAucNo}வது ஏலம் ${label} (${displayDate}) நடைபெறுகிறது.\n\n- நண்பன் சீட்டு`;
                        sendWhatsAppMessage(m.phone, msg);
                    } catch(e) {}
                });
                
                Logger.log('Auto reminder sent for ' + groupName + ' — ' + diffDays + ' days away');
            }
        }
    } catch(e) {
        Logger.log('chitAutoReminderCheck error: ' + e.toString());
    }
}

function setupChitReminderTrigger() {
    try {
        // Remove existing chit reminder triggers to avoid duplicates
        let triggers = ScriptApp.getProjectTriggers();
        triggers.forEach(function(t) {
            if (t.getHandlerFunction() === 'chitAutoReminderCheck') {
                ScriptApp.deleteTrigger(t);
            }
        });
        // Create daily trigger at 9 AM
        ScriptApp.newTrigger('chitAutoReminderCheck')
            .timeBased()
            .everyDays(1)
            .atHour(9)
            .create();
        return { status: 'success', msg: 'ஒவ்வொரு நாளும் காலை 9 மணிக்கு Auto Reminder தயாராகிவிட்டது! ✅' };
    } catch(e) {
        return { status: 'error', msg: e.toString() };
    }
}

/**
 * 🎫 FEATURE: Digital Fee Receipt (Text/Fallback)
 */
function sendDigitalFeeReceiptForPayment(student, amount, trainer, receiver) {
    try {
        let d = getISTDate();
        let bal = (parseInt(student.totalFee) || 0) - (parseInt(student.advance) || 0) - (parseInt(student.discount) || 0);
        let msg = `💰 *கட்டண ரசீது (Receipt)*\n\n` +
            `மாணவர்: ${student.name}\n` +
            `தொகை: ₹${amount}\n` +
            `பெற்றவர்: ${receiver}\n` +
            `தேதி: ${d}\n` +
            `மீதம்: ₹${bal}\n\n` +
            `நண்பன் டிரைவிங் ஸ்கூல் - விபத்தில்லா தமிழ்நாடு! 🚦`;
        
        return sendWhatsAppMessage(student.phone, msg);
    } catch(e) {
        try { logBotActivity("RECEIPT_EXEC_FAIL", e.toString()); } catch(le){}
        return null;
    }
}

// Bridge/UI compatibility wrapper (used by frontend in multiple places)
function sendDigitalFeeReceiptAction(studentId, amount, receiver, loggedBy) {
    try {
        let db = getDatabaseData();
        let s = (db.students || []).find(function(x) { return String(x.id) === String(studentId); });
        if (!s) return { status: "error", message: "Student not found" };
        let amt = parseInt(amount) || 0;
        if (amt <= 0) return { status: "error", message: "Invalid amount" };
        let by = String(loggedBy || receiver || "System");
        let recv = String(receiver || loggedBy || "System");
        let res = sendDigitalFeeReceiptForPayment(s, amt, by, recv);
        return res || { status: "success" };
    } catch (e) {
        return { status: "error", message: String(e && e.message ? e.message : e) };
    }
}

/**
 * 🛠 Helper: Clean and normalize text for comparison
 * Removes emojis, extra spaces, and common prefixes like numbers.
 */
function cleanQuizText(txt) {
    if (!txt) return "";
    let s = String(txt).trim();
    
    // 🎯 NEW: Robust numeric emoji handling (1️⃣, 2️⃣, 3️⃣)
    if (s.includes("1️⃣") || s.indexOf("\u0031\uFE0F\u20E3") !== -1) return "1";
    if (s.includes("2️⃣") || s.indexOf("\u0032\uFE0F\u20E3") !== -1) return "2";
    if (s.includes("3️⃣") || s.indexOf("\u0033\uFE0F\u20E3") !== -1) return "3";
    
    // 🎯 NEW: Standardize numbers
    if (s === "1" || s === "2" || s === "3") return s;

    s = s.toLowerCase();
    // Remove emojis and special symbols
    s = s.replace(/[\u{1F300}-\u{1F9FF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}\u{1F1E6}-\u{1F1FF}]/gu, '');
    // Remove basic numeric prefixes like "1)", "1.", "1 "
    s = s.replace(/^[0-9]+[\s\.\)\-]+\s*/i, '');
    // Standardize whitespace
    s = s.replace(/\s+/g, ' ').trim();
    return s;
}

/**
 * 🛠 Helper: Get column indexes for QuizBank sheet by header names
 */
function getQuizBankConfig() {
    // HARDCODED FOR YOUR SHEET: A:Cat(0), B:Day(1), C:Question(2), D:Opt1(3), E:Opt2(4), F:Opt3(5), G:Ans(6), H:Img(7)
    return { cat: 0, day: 1, ques: 2, o1: 3, o2: 4, o3: 5, ansText: 6, ansNo: -1, img: 7 };
}

// ==============================================================================
// 🎯 15. RANJITH E-SEVAI MAIYAM - BACKEND MODULE
// ==============================================================================

function getDefaultESevaiStageTemplate_() {
    return "Enquiry, Documents, Processing, Completed";
}

function parseStageTemplate_(templateStr, autoDoneAll) {
    let arr = String(templateStr || getDefaultESevaiStageTemplate_())
        .split(",")
        .map(function(x) { return String(x || "").trim(); })
        .filter(function(x) { return !!x; });
    if (!arr.length) arr = ["Enquiry", "Documents", "Processing", "Completed"];
    return arr.map(function(name, idx) {
        let done = !!autoDoneAll || idx === 0;
        return { name: name, done: done, done_at: done ? getISTDate() : "" };
    });
}

function sendESevaiAppointmentReminderNow(workId, tenantId) {
    try {
        let data = getESevaiInitialData(tenantId);
        let works = data.works || [];
        let customers = data.customers || [];
        let w = works.find(function(x) { return String(x.id || x.ID) === String(workId); });
        if (!w) return { status: "error", message: "Work not found" };
        let c = customers.find(function(x) { return String(x.id || x.ID) === String(w.customer_id || w.Customer_ID); });
        if (!c || !(c.phone || c.Phone)) return { status: "error", message: "Customer phone not found" };

        let apptDate = w.appointment_date || w.Appointment_Date || "";
        let apptTime = w.appointment_time || w.Appointment_Time || "";
        let pending = w.pending_reason || w.Pending_Reason || "";
        let msg =
            `📣 *நினைவூட்டல் - ${w.service_name || w.Service_Name || 'E-Sevai Work'}*\n\n` +
            `👤 ${c.name || c.Name || 'Customer'}\n` +
            `🗓️ தேதி: ${apptDate || '-'}\n` +
            `⏰ நேரம்: ${apptTime || '-'}\n` +
            (pending ? `⏳ நிலை: ${pending}\n` : "") +
            `\nதயவுசெய்து நேரத்திற்கு வரவும். நன்றி 🙏`;
        let r = sendWhatsAppMessage(c.phone || c.Phone, msg);
        if (!r || r.status !== "success") return { status: "error", message: "WhatsApp send failed" };

        updateESevaiWorkAction(workId, { appointment_reminder_sent_at: new Date().toISOString() });
        return { status: "success" };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function runESevaiAppointmentReminderCron(tenantId) {
    try {
        let data = getESevaiInitialData(tenantId);
        let works = data.works || [];
        let todayYmd = normalizeDateValue(getISTDate());
        let nowTime = Utilities.formatDate(new Date(), "Asia/Kolkata", "HH:mm");
        let sent = 0;
        works.forEach(function(w) {
            let status = String(w.status || w.Status || "pending").toLowerCase();
            if (status === "finished") return;
            let apptYmd = normalizeDateValue(w.appointment_date || w.Appointment_Date || "");
            let apptTime = String(w.appointment_time || w.Appointment_Time || "");
            if (!apptYmd || !apptTime) return;
            if (apptYmd !== todayYmd) return;
            let already = String(w.appointment_reminder_sent_at || w.Appointment_Reminder_Sent_At || "");
            if (already && normalizeDateValue(already) === todayYmd) return;
            if (nowTime < apptTime) return;

            let res = sendESevaiAppointmentReminderNow(w.id || w.ID);
            if (res && res.status === "success") sent++;
        });
        return { status: "success", sent: sent };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

function setupESevaiReminderTrigger() {
    try {
        let triggers = ScriptApp.getProjectTriggers();
        triggers.forEach(function(t) {
            if (t.getHandlerFunction() === "runESevaiAppointmentReminderCron") ScriptApp.deleteTrigger(t);
        });
        ScriptApp.newTrigger("runESevaiAppointmentReminderCron")
            .timeBased()
            .everyHours(1)
            .inTimezone("Asia/Kolkata")
            .create();
        return { status: "success", message: "E-Sevai appointment reminder trigger created." };
    } catch (e) {
        return { status: "error", message: e.toString() };
    }
}

/**
 * 📊 Fetch all E-Sevai data for initialization
 */
function getESevaiInitialData(tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { error: true, message: "Firebase RTDB URL not configured." };
        tenantId = resolveTenantId_(tenantId);
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) {
            return { error: true, message: "Tenant access denied: " + String(tAccess.reason || "blocked"), tenant_id: tenantId };
        }
        let snap = tryRecoverESevaiDataIfEmpty_(tenantId);
        let data = (snap && typeof snap === 'object') ? snap : {};
        if (!data.balances) data.balances = { Cash: 0, SBI: 0, "Federal 1": 0, "Federal 2": 0, Paytm: 0 };
        if (!Array.isArray(data.services)) data.services = [];
        if (!Array.isArray(data.customers)) data.customers = [];
        if (!Array.isArray(data.agents)) data.agents = [];
        if (!Array.isArray(data.ledgerEntries)) data.ledgerEntries = [];
        if (!Array.isArray(data.enquiries)) data.enquiries = [];
        if (!Array.isArray(data.works)) data.works = [];
        if (!Array.isArray(data.transactions)) data.transactions = [];
        if (!Array.isArray(data.reminders)) data.reminders = [];
        if (!data.settings) data.settings = {};
        data.tenant_id = tenantId;
        data.subscription = tAccess.subscription || {};
        data.totalPending = (data.customers || []).reduce(function(sum, c) { return sum + (parseFloat(c.balance) || 0); }, 0);
        return data;
    } catch (e) {
        Logger.log("Error fetching E-Sevai data: " + e.stack);
        return { error: true, message: e.toString() + " | Stack: " + e.stack };
    }
}

/**
 * 📝 Save a new E-Sevai POS Transaction
 */
/**
 * 🏦 Helper: Sync Live Balance in ESEVAI_BALANCES_SHEET (Today's Row)
 * This ensures the dashboard reflects live spending immediately.
 */
function updateESevaiLiveBalance(accName, diff) {
    try {
        let db = getDB();
        let sheet = db.getSheetByName(ESEVAI_BALANCES_SHEET);
        let data = sheet.getDataRange().getValues();
        let todayStd = normalizeDateValue(getISTDate());
        
        let accIdx = -1;
        let lowAcc = accName.toLowerCase();
        if (lowAcc === "cash") accIdx = 1;
        else if (lowAcc.includes("sbi")) accIdx = 2;
        else if (lowAcc.includes("federal 1") || lowAcc.includes("federal1") || lowAcc.includes("fed 1")) accIdx = 3;
        else if (lowAcc.includes("federal 2") || lowAcc.includes("federal2") || lowAcc.includes("fed 2")) accIdx = 4;
        else if (lowAcc === "paytm") accIdx = 5;

        if (accIdx === -1) return; // Account not trackable

        for (let i = data.length - 1; i >= 1; i--) {
            // Standardize row date to YYYY-MM-DD for matching
            let rowDate = data[i][0];
            let rowDateStr = "";
            if (rowDate instanceof Date) {
                let d = rowDate.getDate(); let m = rowDate.getMonth() + 1; let y = rowDate.getFullYear();
                rowDateStr = `${y}-${(m<10?'0':'')+m}-${(d<10?'0':'')+d}`;
            } else {
                rowDateStr = normalizeDateValue(rowDate);
            }

            if (rowDateStr === todayStd) {
                let current = Number(data[i][accIdx]) || 0;
                sheet.getRange(i + 1, accIdx + 1).setValue(current + diff);
                flushNoop_();
                return true;
            }
        }
    } catch (e) { console.log('Sync Error', e); }
    return false;
}

function saveESevaiTransactionAction(tx, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId || (tx && (tx.tenant_id || tx.tenantId)));
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let firebaseWarn = "";
        if (useFirebaseRtdb_()) {
            try {
                let data = getESevaiSnapshot_(tenantId) || {};
                if (!Array.isArray(data.transactions)) data.transactions = [];
                if (!Array.isArray(data.customers)) data.customers = [];
                if (!Array.isArray(data.ledgerEntries)) data.ledgerEntries = [];
                if (!Array.isArray(data.works)) data.works = [];
                if (!data.balances) data.balances = { Cash: 0, SBI: 0, "Federal 1": 0, "Federal 2": 0, Paytm: 0 };

                let today = getISTDate();
                let txId = "ESTX" + Date.now();
                let totalAmt = Number(tx.totalAmount) || 0;
                let recvAmt = Number(tx.receivedAmount) || 0;
                let balDiff = recvAmt - totalAmt;
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
                    status: tx.status || 'finished',
                    date: today
                });

                for (let i = 0; i < data.customers.length; i++) {
                    if (String(data.customers[i].id) === String(tx.customerId)) {
                        customerType = String(data.customers[i].type || "");
                        data.customers[i].balance = (Number(data.customers[i].balance) || 0) + balDiff;
                        try {
                            let phone = String(data.customers[i].phone || "").trim();
                            if (phone) {
                                let cname = String(data.customers[i].name || "Customer");
                                let msg = `🙏 வணக்கம் ${cname}\n\n✅ E-Sevai Bill பதிவு செய்யப்பட்டது.\n🧾 Bill: ${txId}\n💰 Amount: ₹${totalAmt}\n💳 Received: ₹${recvAmt}\n📉 Balance Diff: ₹${balDiff}\n📅 தேதி: ${today}\n\nNanban Pro - Ranjith E-Sevai Maiyam`;
                                sendWhatsAppMessage(phone, msg);
                                let llrPdf = String(tx.llrCopyUrl || "").trim();
                                if (llrPdf) {
                                    sendWhatsAppDocumentMessage(phone, llrPdf, "LLR_" + txId + ".pdf", "📎 உங்கள் LLR PDF + Bill Reference: " + txId);
                                }
                            }
                        } catch (e) {}
                        break;
                    }
                }
                try {
                    notifyAdmins(`🧾 E-Sevai POS Bill\nBill: ${txId}\nAmount: ₹${totalAmt}\nReceived: ₹${recvAmt}\nMode: ${tx.paymentMode || 'Cash'}\nDate: ${today}`);
                } catch (e) {}

                if (tx.paymentMode !== 'Pending' && recvAmt > 0) {
                    data.balances[tx.paymentMode] = (Number(data.balances[tx.paymentMode]) || 0) + recvAmt;
                }
                let totalGovFee = (tx.items || []).reduce((sum, item) => sum + ((Number(item.gov_fee) || 0) * (item.qty || 1)), 0);
                if (totalGovFee > 0) {
                    data.balances[tx.govBank] = (Number(data.balances[tx.govBank]) || 0) - totalGovFee;
                    data.ledgerEntries.unshift({ date: today, type: 'expense', category: 'Gov Fee', description: `Gov Fee for Bill #${txId}`, amount: totalGovFee, account: tx.govBank });
                }
                let totalSrvFee = (tx.items || []).reduce((sum, item) => sum + ((Number(item.srv_fee) || 0) * (item.qty || 1)), 0);
                let netIncome = totalSrvFee - (Number(tx.otherExpenses) || 0);
                if (netIncome !== 0) {
                    let acc = tx.paymentMode === 'Pending' ? 'Cash' : tx.paymentMode;
                    data.ledgerEntries.unshift({ date: today, type: netIncome > 0 ? 'income' : 'expense', category: 'Service Fee', description: `Service Fee for Bill #${txId}`, amount: Math.abs(netIncome), account: acc });
                }

                (tx.items || []).forEach(function(item) {
                    data.works.unshift({
                        id: "ESWK" + Date.now() + Math.floor(Math.random() * 100),
                        transaction_id: txId,
                        customer_id: tx.customerId,
                        agent_id: tx.agentId || "",
                        agent_name: tx.agentName || "",
                        service_name: item.name,
                        status: tx.status || 'pending',
                        service_type: (Number(item.gov_fee) || 0) > 0 ? 'regular' : 'own',
                        stages: item.stages || [],
                        document_url: item.document_url || "",
                        llr_date: String(item.llr_date || tx.llrDate || "").trim(),
                        llr_copy_url: String(item.llr_copy_url || tx.llrCopyUrl || "").trim(),
                        llr_reminder_sent_at: "",
                        customer_type: customerType || "",
                        target_date: item.target_date || "",
                        delivery_status: item.delivery_status || "pending",
                        delivery_notified_at: "",
                        finished_date: (tx.status || 'pending') === 'finished' ? today : "",
                        created_at: today
                    });
                });

                saveESevaiSnapshot_(data, tenantId);
                return { status: 'success', id: txId, tenant_id: tenantId };
            } catch (fbErr) {
                return { status: 'error', message: String(fbErr || "") };
            }
        }

        let db = getDB();
        let today = getISTDate();
        let txId = "ESTX" + Date.now();
        
        // 1. Calculate Balance Diff
        let totalAmt = Number(tx.totalAmount) || 0;
        let recvAmt = Number(tx.receivedAmount) || 0;
        let balDiff = recvAmt - totalAmt; // +ve is advance, -ve is debt

        // 2. Save Transaction
        let txSheet = db.getSheetByName(ESEVAI_TRANSACTIONS_SHEET);
        txSheet.appendRow([
            txId,
            tx.customerId,
            JSON.stringify(tx.items),
            tx.govBank,
            tx.paymentMode,
            totalAmt,
            recvAmt,
            balDiff,
            tx.otherExpenses || 0,
            tx.status || 'finished',
            "'" + today // FORCE TEXT
        ]);

        // 3. Update Customer Balance (CUMULATIVE)
        let custSheet = db.getSheetByName(ESEVAI_CUSTOMERS_SHEET);
        let cData = custSheet.getDataRange().getValues();
        let customerType = "";
        for (let i = 1; i < cData.length; i++) {
            if (cData[i][0] == tx.customerId) {
                let currentBal = Number(cData[i][3]) || 0;
                let newBal = currentBal + balDiff;
                custSheet.getRange(i + 1, 4).setValue(newBal); // Balance is 4th col
                customerType = cData[i][4]; // Type is 5th col
                break;
            }
        }

        // 4. SYNC LIVE BALANCE (Real-time Bank/Cash update)
        if (tx.paymentMode !== 'Pending' && recvAmt > 0) {
            updateESevaiLiveBalance(tx.paymentMode, recvAmt); // Credit the received money
        }
        
        // 3. Record Ledger Entries
        let ledgerSheet = db.getSheetByName(ESEVAI_LEDGER_SHEET);
        
        // Expense: Gov Fee
        let totalGovFee = (tx.items || []).reduce((sum, item) => sum + ((Number(item.gov_fee) || 0) * (item.qty || 1)), 0);
        if (totalGovFee > 0) {
            ledgerSheet.appendRow([today, 'expense', 'Gov Fee', `Gov Fee for Bill #${txId}`, totalGovFee, tx.govBank]);
            updateESevaiLiveBalance(tx.govBank, -totalGovFee); // Debit the Gov Bank
        }

        // Income: Service Fee (Net)
        let totalSrvFee = (tx.items || []).reduce((sum, item) => sum + ((Number(item.srv_fee) || 0) * (item.qty || 1)), 0);
        let netIncome = totalSrvFee - (tx.otherExpenses || 0);
        if (netIncome !== 0) {
            let acc = tx.paymentMode === 'Pending' ? 'Cash' : tx.paymentMode;
            ledgerSheet.appendRow([today, netIncome > 0 ? 'income' : 'expense', 'Service Fee', `Service Fee for Bill #${txId}`, Math.abs(netIncome), acc]);
        }

        // 5. Create Work Entries
        let workSheet = db.getSheetByName(ESEVAI_WORKS_SHEET);
        (tx.items || []).forEach(item => {
            let govFee = Number(item.gov_fee) || 0;
            // 🎯 RULE: If Agent and NO Gov Fee, auto-finish. Else use transaction status.
            let isAutoFinished = (customerType === 'Agent' && govFee === 0);
            let workStatus = isAutoFinished ? 'finished' : (tx.status || 'pending');

            // 🎯 We record it if it has a Gov Fee OR if it's NOT finished OR if it's an auto-finished Agent work (for history)
            if (govFee > 0 || workStatus !== 'finished' || isAutoFinished) {
                workSheet.appendRow([
                    "ESWK" + Date.now() + Math.floor(Math.random()*100),
                    txId,
                    tx.customerId,
                    item.name,
                    workStatus,
                    govFee > 0 ? 'regular' : 'own',
                    JSON.stringify(item.stages || []),
                    item.document_url || "",
                    workStatus === 'finished' ? today : "",
                    today
                ]);
            }
        });

        return firebaseWarn
            ? { status: 'success', id: txId, warning: "Firebase sync failed. Saved to Sheets fallback.", firebaseError: firebaseWarn }
            : { status: 'success', id: txId };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 💰 Save Daily Opening Balance
 */
function saveESevaiOpeningBalanceAction(b, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId || (b && (b.tenant_id || b.tenantId)));
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let firebaseWarn = "";
        if (useFirebaseRtdb_()) {
            try {
                let data = getESevaiSnapshot_(tenantId) || {};
                let todayFb = getISTDate();
                data.openingBalance = { date: todayFb, cash: b.Cash || 0, sbi: b.SBI || 0, federal1: b["Federal 1"] || 0, federal2: b["Federal 2"] || 0, paytm: b.Paytm || 0 };
                data.balances = { Cash: b.Cash || 0, SBI: b.SBI || 0, "Federal 1": b["Federal 1"] || 0, "Federal 2": b["Federal 2"] || 0, Paytm: b.Paytm || 0 };
                if (!Array.isArray(data.ledgerEntries)) data.ledgerEntries = [];
                Object.entries(data.balances).forEach(function(entry) {
                    let acc = entry[0], amt = Number(entry[1]) || 0;
                    if (amt > 0) data.ledgerEntries.unshift({ date: todayFb, type: 'income', category: 'Opening Balance', description: `Opening Balance for ${todayFb}`, amount: amt, account: acc });
                });
                saveESevaiSnapshot_(data, tenantId);
                return { status: 'success' };
            } catch (fbErr) {
                return { status: 'error', message: String(fbErr || "") };
            }
        }

        let db = getDB();
        let today = getISTDate();
        let balSheet = db.getSheetByName(ESEVAI_BALANCES_SHEET);
        // Use apostrophe to prevent Google Sheets Date parsing corruption, and correct capitalization!
        balSheet.appendRow(["'" + today, b.Cash || 0, b.SBI || 0, b["Federal 1"] || 0, b["Federal 2"] || 0, b.Paytm || 0, "'" + today]);

        // Record in Ledger as Opening Balance
        let ledgerSheet = db.getSheetByName(ESEVAI_LEDGER_SHEET);
        let accounts = { 'Cash': b.Cash || 0, 'SBI': b.SBI || 0, 'Federal 1': b["Federal 1"] || 0, 'Federal 2': b["Federal 2"] || 0, 'Paytm': b.Paytm || 0 };
        Object.entries(accounts).forEach(([acc, amt]) => {
            if (amt > 0) {
                ledgerSheet.appendRow(["'" + today, 'income', 'Opening Balance', `Opening Balance for ${today}`, amt, acc]);
            }
        });

        return firebaseWarn
            ? { status: 'success', warning: "Firebase sync failed. Saved to Sheets fallback.", firebaseError: firebaseWarn }
            : { status: 'success' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🏁 Close E-Sevai Day & Record Discrepancies (Daily Settlement)
 */
function closeESevaiDayAction(actuals, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId || (actuals && (actuals.tenant_id || actuals.tenantId)));
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let data = getESevaiSnapshot_(tenantId) || {};
        let today = getISTDate();
        if (!data.balances) data.balances = { Cash: 0, SBI: 0, "Federal 1": 0, "Federal 2": 0, Paytm: 0 };
        if (!Array.isArray(data.ledgerEntries)) data.ledgerEntries = [];
        Object.keys(actuals || {}).forEach(function(acc) {
            let actualAmt = Number(actuals[acc]) || 0;
            let liveAmt = Number(data.balances[acc]) || 0;
            let diff = actualAmt - liveAmt;
            if (diff !== 0) {
                data.ledgerEntries.unshift({
                    date: today,
                    type: diff > 0 ? 'income' : 'expense',
                    category: 'Settlement Adjustment',
                    description: `Adjustment (Typo/Manual) for ${acc}`,
                    amount: Math.abs(diff),
                    account: acc
                });
            }
            data.balances[acc] = actualAmt;
        });
        data.day_closed_at = new Date().toISOString();
        data.day_closed_date = today;
        saveESevaiSnapshot_(data, tenantId);
        return { status: 'success' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🏗️ Helper to get Sheet Data as Objects
 */
function getSheetData(db, sheetName, limit = 0) {
    let sheet = db.getSheetByName(sheetName);
    if (!sheet) return [];
    let data = sheet.getDataRange().getValues();
    if (data.length <= 1) return [];
    
    let headers = data[0];
    let result = [];
    let startRow = 1;
    if (limit > 0 && data.length > limit) startRow = data.length - limit;

    for (let i = startRow; i < data.length; i++) {
        let obj = {};
        for (let j = 0; j < headers.length; j++) {
            let key = headers[j].toString()
                .replace(/([a-z])([A-Z])/g, '$1_$2') // camelCase to snake_case
                .toLowerCase()
                .replace(/\s+/g, '_')                // spaces to underscores
                .replace(/_+/g, '_');                // multiple underscores to single
            let val = data[i][j];
            
            // SECURITY FIX: Convert Google Sheet Date objects to safe strings 
            // to prevent google.script.run silent serialization failures (returning null)
            if (val instanceof Date) {
                val = isNaN(val.getTime()) ? "" : val.toISOString();
            }
            
            // Try parse JSON strings back to objects
            if (typeof val === 'string' && (val.startsWith('{') || val.startsWith('['))) {
                try { val = JSON.parse(val); } catch(err) {}
            }
            obj[key] = val;
        }
        result.push(obj);
    }
    return result.reverse(); 
}

/**
 * 🛠️ Update E-Sevai Work Status/Stages
 */
function updateESevaiWorkAction(id, update, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        if (useFirebaseRtdb_()) {
            tenantId = resolveTenantId_(tenantId || (update && (update.tenant_id || update.tenantId)));
            let tAccess = buildTenantAccessContext_(tenantId, "", "");
            if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
            let data = getESevaiSnapshot_(tenantId) || {};
            if (!Array.isArray(data.works)) data.works = [];
            let idx = data.works.findIndex(function(w) { return String(w.id) === String(id); });
            if (idx === -1) return { status: 'error', message: 'Work ID not found' };
            data.works[idx] = Object.assign({}, data.works[idx], update || {});
            if (String(data.works[idx].status || '').toLowerCase() === 'finished' && !data.works[idx].completed_at) {
                data.works[idx].completed_at = getISTDate();
            }
            saveESevaiSnapshot_(data, tenantId);
            return { status: 'success' };
        }
        return { status: 'error', message: 'Work ID not found' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 📝 Update E-Sevai Enquiry Stages
 */
function updateESevaiStageAction(id, stages, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId);
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let data = getESevaiSnapshot_(tenantId) || {};
        if (!Array.isArray(data.enquiries)) data.enquiries = [];
        let idx = data.enquiries.findIndex(function(e) { return String(e.id) === String(id); });
        if (idx >= 0) {
            data.enquiries[idx].stages = stages || [];
            data.enquiries[idx].stages_json = JSON.stringify(stages || []);
            saveESevaiSnapshot_(data, tenantId);
            return { status: 'success' };
        }
        return { status: 'error', message: 'Enquiry not found' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 📝 Save a new E-Sevai Enquiry
 */
function saveESevaiEnquiryAction(enquiry, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId || (enquiry && (enquiry.tenant_id || enquiry.tenantId)));
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let firebaseWarn = "";
        if (useFirebaseRtdb_()) {
            try {
                let data = getESevaiSnapshot_(tenantId) || {};
                if (!Array.isArray(data.enquiries)) data.enquiries = [];
                if (!Array.isArray(data.works)) data.works = [];
                let enqId = "ESENQ" + Date.now();
                let quoted = Number(enquiry.quoted_amount) || 0;
                let adv = Number(enquiry.advance || enquiry.advance_received) || 0;
                let govSpent = Number(enquiry.gov_spent) || 0;
                let serviceObj = (data.services || []).find(function(s) {
                    return String(s.id || "").trim() === String(enquiry.service_id || "").trim();
                });
                if (!serviceObj) {
                    serviceObj = (data.services || []).find(function(s) {
                        return String(s.name || "").trim() === String(enquiry.service_name || "").trim();
                    }) || {};
                }
                let custObj = (data.customers || []).find(function(c) {
                    return String(c.id || "").trim() === String(enquiry.customer_id || "").trim();
                }) || {};
                let cType = String(custObj.type || "Direct").trim().toLowerCase();
                let isAgent = (cType === "agent" || cType === "broker");
                let stageTemplate = String(serviceObj.stage_template || getDefaultESevaiStageTemplate_());
                let directFollowOnly = (serviceObj.direct_follow_only !== false);
                let autoFinish = isAgent && directFollowOnly;
                let stages = parseStageTemplate_(stageTemplate, autoFinish);
                data.enquiries.unshift({
                    id: enqId,
                    customer_id: enquiry.customer_id,
                    service_id: enquiry.service_id || "",
                    service_name: enquiry.service_name,
                    agent_id: enquiry.agent_id || "",
                    agent_name: enquiry.agent_name || "",
                    llr_date: enquiry.llr_date || "",
                    llr_copy_url: enquiry.llr_copy_url || "",
                    llr_reminder_sent_at: "",
                    customer_type: enquiry.customer_type || (custObj.type || "Direct"),
                    quoted_amount: quoted,
                    advance: adv,
                    gov_spent: govSpent,
                    appointment_date: enquiry.appointment_date || "",
                    appointment_time: enquiry.appointment_time || "",
                    pending_reason: enquiry.pending_reason || "",
                    stage_template: stageTemplate,
                    direct_follow_only: directFollowOnly,
                    notes: enquiry.notes || "",
                    status: enquiry.status || "pending",
                    created_at: getISTDate()
                });
                data.works.unshift({
                    id: "ESWK" + Date.now(),
                    enquiry_id: enqId,
                    customer_id: enquiry.customer_id,
                    customer_name: custObj.name || "",
                    customer_type: enquiry.customer_type || (custObj.type || "Direct"),
                    agent_id: enquiry.agent_id || "",
                    agent_name: enquiry.agent_name || "",
                    llr_date: enquiry.llr_date || "",
                    llr_copy_url: enquiry.llr_copy_url || "",
                    llr_reminder_sent_at: "",
                    service_name: enquiry.service_name,
                    status: autoFinish ? "finished" : (enquiry.status || "pending"),
                    stage_name: autoFinish ? "Completed (Agent)" : (stages[0] ? stages[0].name : "Enquiry"),
                    stage_template: stageTemplate,
                    stages: stages,
                    quoted_amount: quoted,
                    received_amount: adv,
                    gov_spent: govSpent,
                    balance_due: Math.max(0, quoted - adv),
                    appointment_date: enquiry.appointment_date || "",
                    appointment_time: enquiry.appointment_time || "",
                    pending_reason: autoFinish ? "Agent வேலை - self follow" : (enquiry.pending_reason || ""),
                    last_note: enquiry.notes || "",
                    target_date: enquiry.target_date || "",
                    delivery_status: enquiry.delivery_status || "pending",
                    delivery_notified_at: "",
                    created_at: getISTDate()
                });
                if (!Array.isArray(data.ledgerEntries)) data.ledgerEntries = [];
                if (adv > 0) {
                    data.ledgerEntries.unshift({ date: getISTDate(), type: 'income', category: 'Advance Received', description: `Advance for ${enquiry.service_name}`, amount: adv, account: 'Cash' });
                }
                saveESevaiSnapshot_(data, tenantId);
                try {
                    let msgCust = `✅ உங்கள் Enquiry பதிவு செய்யப்பட்டது.\nService: ${enquiry.service_name || '-'}\nEnquiry ID: ${enqId}\n\nNanban Pro E-Sevai`;
                    if (String(custObj.phone || "").trim()) sendWhatsAppMessage(String(custObj.phone), msgCust);
                    if (isAgent && String(enquiry.agent_id || "").trim()) {
                        let agentObj = (data.agents || []).find(function(a) { return String(a.id || "") === String(enquiry.agent_id || ""); }) || {};
                        let llrDateTxt = String(enquiry.llr_date || "").trim() || "-";
                        let llrCopyTxt = String(enquiry.llr_copy_url || "").trim() || "-";
                        let msgAgent = `📌 Agent Update\nLLR enquiry பதிவு வெற்றிகரமாக சேமிக்கப்பட்டது.\nCustomer: ${custObj.name || enquiry.customer_id || '-'}\nService: ${enquiry.service_name || '-'}\nLLR Date: ${llrDateTxt}\nLLR Copy: ${llrCopyTxt}\nEnquiry ID: ${enqId}`;
                        if (String(agentObj.phone || "").trim()) sendWhatsAppMessage(String(agentObj.phone), msgAgent);
                    }
                } catch (waErr) {}
                return { status: 'success', id: enqId, tenant_id: tenantId };
            } catch (fbErr) {
                return { status: 'error', message: String(fbErr || "") };
            }
        }

        let db = getDB();
        let enqId = "ESENQ" + Date.now();
        let quoted = Number(enquiry.quoted_amount) || 0;
        let adv = Number(enquiry.advance || enquiry.advance_received) || 0;
        let sheet = db.getSheetByName(ESEVAI_ENQUIRIES_SHEET);
        sheet.appendRow([
            enqId,
            enquiry.customer_id,
            enquiry.service_name,
            quoted,
            enquiry.stages_json || "[]",
            'pending',
            enquiry.notes || '',
            getISTDate(),
            adv
        ]);

        let workSheet = db.getSheetByName(ESEVAI_WORKS_SHEET);
        if (workSheet) {
            let stageTemplate2 = String(enquiry.stage_template || getDefaultESevaiStageTemplate_());
            let isAgent2 = String(enquiry.customer_type || "Direct") === "Agent";
            let autoFinish2 = isAgent2 && enquiry.direct_follow_only !== false;
            let stages2 = parseStageTemplate_(stageTemplate2, autoFinish2);
            workSheet.appendRow([
                "ESWK" + Date.now() + Math.floor(Math.random()*100),
                enqId,
                enquiry.customer_id,
                enquiry.service_name,
                autoFinish2 ? 'finished' : 'pending',
                'regular',
                JSON.stringify(stages2),
                '',
                '',
                getISTDate()
            ]);
        }

        // If advance is received, record in wallet immediately
        if (adv > 0) {
            saveESevaiLedgerAction({
                type: 'income',
                account: 'Cash', // Default for enquiries
                particulars: `Advance for Enquiry #${enqId}`,
                amount: adv,
                customer_id: enquiry.customer_id
            });
        }
        return firebaseWarn
            ? { status: 'success', id: enqId, warning: "Firebase sync failed. Saved to Sheets fallback.", firebaseError: firebaseWarn }
            : { status: 'success', id: enqId };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 📝 Update E-Sevai Enquiry Status
 */
function updateESevaiEnquiryStatusAction(id, status, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId);
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let data = getESevaiSnapshot_(tenantId) || {};
        if (!Array.isArray(data.enquiries)) data.enquiries = [];
        let idx = data.enquiries.findIndex(function(e) { return String(e.id) === String(id); });
        if (idx >= 0) {
            data.enquiries[idx].status = status;
            saveESevaiSnapshot_(data, tenantId);
            return { status: 'success' };
        }
        return { status: 'error', message: 'Enquiry not found' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 👥 Save a new E-Sevai Customer
 */
function saveESevaiCustomerAction(c, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId || (c && (c.tenant_id || c.tenantId)));
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let firebaseWarn = "";
        if (useFirebaseRtdb_()) {
            try {
                let data = getESevaiSnapshot_(tenantId) || {};
                if (!Array.isArray(data.customers)) data.customers = [];
                let id = "ESC" + Date.now();
                data.customers.unshift({
                    id: id,
                    name: c.name,
                    phone: c.phone,
                    balance: Number(c.oldBalance) || 0,
                    type: c.type || "Direct",
                    created_at: getISTDate()
                });
                saveESevaiSnapshot_(data, tenantId);
                return { status: 'success', id: id, tenant_id: tenantId };
            } catch (fbErr) {
                return { status: 'error', message: String(fbErr || "") };
            }
        }

        let db = getDB();
        let sheet = db.getSheetByName(ESEVAI_CUSTOMERS_SHEET);
        let id = "ESC" + Date.now();
        sheet.appendRow([id, c.name, c.phone, Number(c.oldBalance) || 0, c.type]);
        return firebaseWarn
            ? { status: 'success', id: id, warning: "Firebase sync failed. Saved to Sheets fallback.", firebaseError: firebaseWarn }
            : { status: 'success', id: id };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function saveESevaiAgentAction(a, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId || (a && (a.tenant_id || a.tenantId)));
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let data = getESevaiSnapshot_(tenantId) || {};
        if (!Array.isArray(data.agents)) data.agents = [];
        let id = a.id || ("ESAG" + Date.now());
        let idx = data.agents.findIndex(function(x) { return String(x.id) === String(id); });
        let payload = {
            id: id,
            name: a.name || "",
            phone: a.phone || "",
            area: a.area || "",
            active: (a.active !== false),
            created_at: a.created_at || getISTDate()
        };
        if (idx >= 0) data.agents[idx] = payload;
        else data.agents.unshift(payload);
        saveESevaiSnapshot_(data, tenantId);
        return { status: 'success', id: id, tenant_id: tenantId };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function notifyESevaiDeliveryNowAction(workId, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId);
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let data = getESevaiSnapshot_(tenantId) || {};
        if (!Array.isArray(data.works)) data.works = [];
        if (!Array.isArray(data.customers)) data.customers = [];
        let idx = data.works.findIndex(function(w) { return String(w.id) === String(workId); });
        if (idx < 0) return { status: 'error', message: 'Work not found' };
        let w = data.works[idx];
        let c = data.customers.find(function(x) { return String(x.id) === String(w.customer_id); });
        let msg = `✅ உங்கள் விண்ணப்ப வேலை முடிந்தது.\nService: ${w.service_name || '-'}\nதேதி: ${getISTDate()}\nNanban Ranjith E-Sevai Maiyam.`;
        if (c && c.phone) {
            sendWhatsAppMessage(c.phone, msg);
        }
        w.delivery_status = 'delivered';
        w.delivered_at = getISTDate();
        w.delivery_notified_at = getISTDate();
        data.works[idx] = w;
        saveESevaiSnapshot_(data, tenantId);
        return { status: 'success' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function runESevaiDeliveryReminderCron(tenantId) {
    try {
        let data = getESevaiInitialData(tenantId);
        if (!data || data.error) return { status: 'error', message: data && data.message ? data.message : 'No data' };
        let works = Array.isArray(data.works) ? data.works : [];
        let customers = Array.isArray(data.customers) ? data.customers : [];
        let today = normalizeDateValue(getISTDate());
        let sent = 0;
        works.forEach(function(w) {
            let status = String(w.status || '').toLowerCase();
            if (status === 'finished' || String(w.delivery_status || '').toLowerCase() === 'delivered') return;
            let td = normalizeDateValue(w.target_date || "");
            if (!td) return;
            let nearDue = td <= today;
            if (!nearDue) return;
            if (w.delivery_notified_at && String(w.delivery_notified_at) === getISTDate()) return;
            let c = customers.find(function(x) { return String(x.id) === String(w.customer_id); });
            if (!c || !c.phone) return;
            let note = `⏰ உங்கள் சேவை டெலிவரி தேதி நெருங்கியுள்ளது.\nService: ${w.service_name || '-'}\nTarget: ${w.target_date || '-'}\nNanban Ranjith E-Sevai Maiyam.`;
            sendWhatsAppMessage(c.phone, note);
            w.delivery_notified_at = getISTDate();
            sent++;
        });
        saveESevaiSnapshot_(data, data.tenant_id || tenantId);
        return { status: 'success', sent: sent };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function runESevaiAgentLlrReminderCron(tenantId) {
    try {
        let data = getESevaiInitialData(tenantId);
        if (!data || data.error) return { status: 'error', message: data && data.message ? data.message : 'No data' };
        let works = Array.isArray(data.works) ? data.works : [];
        let agents = Array.isArray(data.agents) ? data.agents : [];
        let now = new Date();
        let sent = 0;
        works.forEach(function(w) {
            let llrDate = String(w.llr_date || "").trim();
            if (!llrDate) return;
            if (String(w.llr_reminder_sent_at || "").trim()) return;
            let aid = String(w.agent_id || "").trim();
            if (!aid) return;
            let agent = agents.find(function(a) { return String(a.id || "") === aid; });
            if (!agent || !agent.phone) return;
            let dt = new Date(llrDate);
            if (!(dt instanceof Date) || isNaN(dt.getTime())) return;
            let diffDays = Math.floor((now.getTime() - dt.getTime()) / 86400000);
            if (diffDays !== 30) return;
            let msg = `📣 Agent Reminder\n\nYour customer ${w.customer_name || w.customer_id || '-'}'s LLR has completed 30 days.\nLLR Date: ${llrDate}\nService: ${w.service_name || 'LLR'}\nதயவு செய்து follow-up call செய்யவும்.\n\nNanban Pro E-Sevai`;
            sendWhatsAppMessage(String(agent.phone), msg);
            w.llr_reminder_sent_at = getISTDate();
            sent++;
        });
        saveESevaiSnapshot_(data, data.tenant_id || tenantId);
        return { status: 'success', sent: sent };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function setupESevaiAgentLlrReminderTrigger() {
    try {
        ScriptApp.getProjectTriggers().forEach(function(t) {
            if (t.getHandlerFunction() === "runESevaiAgentLlrReminderCron") ScriptApp.deleteTrigger(t);
        });
        ScriptApp.newTrigger("runESevaiAgentLlrReminderCron")
            .timeBased()
            .everyHours(6)
            .create();
        return { status: 'success' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

function setupESevaiDeliveryReminderTrigger() {
    try {
        ScriptApp.getProjectTriggers().forEach(function(t) {
            if (t.getHandlerFunction() === "runESevaiDeliveryReminderCron") ScriptApp.deleteTrigger(t);
        });
        ScriptApp.newTrigger("runESevaiDeliveryReminderCron")
            .timeBased()
            .everyHours(2)
            .create();
        return { status: 'success' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 💸 Save E-Sevai Manual Ledger Entry
 */
function saveESevaiLedgerAction(l, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId || (l && (l.tenant_id || l.tenantId)));
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let firebaseWarn = "";
        if (useFirebaseRtdb_()) {
            try {
                let data = getESevaiSnapshot_(tenantId) || {};
                if (!Array.isArray(data.ledgerEntries)) data.ledgerEntries = [];
                if (!Array.isArray(data.customers)) data.customers = [];
                if (!data.balances) data.balances = { Cash: 0, SBI: 0, "Federal 1": 0, "Federal 2": 0, Paytm: 0 };
                let today = getISTDate();
                let amt = Number(l.amount) || 0;
                let type = String(l.type || '').toLowerCase();
                data.ledgerEntries.unshift({ date: today, type: type === 'in' ? 'income' : (type === 'out' ? 'expense' : type), category: 'Manual', description: l.particulars, amount: amt, account: l.account });
                let isIncome = (type === 'income' || type === 'in');
                let diff = isIncome ? amt : -amt;
                data.balances[l.account] = (Number(data.balances[l.account]) || 0) + diff;
                if (l.customer_id) {
                    for (let i = 0; i < data.customers.length; i++) {
                        if (String(data.customers[i].id) === String(l.customer_id)) {
                            data.customers[i].balance = (Number(data.customers[i].balance) || 0) + diff;
                            break;
                        }
                    }
                }
                saveESevaiSnapshot_(data, tenantId);
                return { status: 'success' };
            } catch (fbErr) {
                return { status: 'error', message: String(fbErr || "") };
            }
        }

        let db = getDB();
        let today = getISTDate();
        let sheet = db.getSheetByName(ESEVAI_LEDGER_SHEET);
        let amt = Number(l.amount) || 0;
        
        sheet.appendRow(["'" + today, l.type.toLowerCase(), 'Manual', l.particulars, amt, l.account]);
        
        // 🏦 Sync Live Balance
        updateESevaiLiveBalance(l.account, l.type.toLowerCase() === 'income' ? amt : -amt);

        // 🏦 SYNC WALLET (If customer_id provided)
        if (l.customer_id) {
            let custSheet = db.getSheetByName(ESEVAI_CUSTOMERS_SHEET);
            let cData = custSheet.getDataRange().getValues();
            for (let i = 1; i < cData.length; i++) {
                if (cData[i][0] == l.customer_id) {
                    let currentBal = Number(cData[i][3]) || 0;
                    // income increases balance, expense decreases balance
                    let diff = l.type.toLowerCase() === 'income' ? amt : -amt;
                    custSheet.getRange(i + 1, 4).setValue(currentBal + diff);
                    break;
                }
            }
        }
        return firebaseWarn
            ? { status: 'success', warning: "Firebase sync failed. Saved to Sheets fallback.", firebaseError: firebaseWarn }
            : { status: 'success' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🛠️ Save/Update E-Sevai Service Catalog Item
 */
function saveESevaiServiceAction(s, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId || (s && (s.tenant_id || s.tenantId)));
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let firebaseWarn = "";
        if (useFirebaseRtdb_()) {
            try {
                let data = getESevaiSnapshot_(tenantId) || {};
                if (!Array.isArray(data.services)) data.services = [];
                let id = s.id || ("ESS" + Date.now());
                let idx = data.services.findIndex(function(x) { return String(x.id) === String(id); });
                let payload = {
                    id: id,
                    name: s.name,
                    category: s.category,
                    type: s.type,
                    gov_fee: Number(s.gov_fee) || 0,
                    direct_charge: Number(s.direct_charge) || 0,
                    agent_charge: Number(s.agent_charge) || 0,
                    required_documents: s.docs || "",
                    icon: s.icon || "",
                    stage_template: s.stage_template || getDefaultESevaiStageTemplate_(),
                    direct_follow_only: (s.direct_follow_only !== false)
                };
                if (idx >= 0) data.services[idx] = payload;
                else data.services.unshift(payload);
                saveESevaiSnapshot_(data, tenantId);
                return { status: 'success', id: id, tenant_id: tenantId };
            } catch (fbErr) {
                return { status: 'error', message: String(fbErr || "") };
            }
        }

        let db = getDB();
        let sheet = db.getSheetByName(ESEVAI_SERVICES_SHEET);
        let data = sheet.getDataRange().getValues();
        let id = s.id || ("ESS" + Date.now());
        
        // Update if exists, else append
        for (let i = 1; i < data.length; i++) {
            if (data[i][0] == id) {
                sheet.getRange(i + 1, 1, 1, 11).setValues([[
                    id, s.name, s.category, s.type, s.gov_fee, s.direct_charge, s.agent_charge, s.docs || "", s.icon || "",
                    s.stage_template || getDefaultESevaiStageTemplate_(),
                    (s.direct_follow_only !== false)
                ]]);
                return { status: 'success', id: id };
            }
        }
        sheet.appendRow([
            id, s.name, s.category, s.type, s.gov_fee, s.direct_charge, s.agent_charge, s.docs || "", s.icon || "",
            s.stage_template || getDefaultESevaiStageTemplate_(),
            (s.direct_follow_only !== false)
        ]);
        return firebaseWarn
            ? { status: 'success', id: id, warning: "Firebase sync failed. Saved to Sheets fallback.", firebaseError: firebaseWarn }
            : { status: 'success', id: id };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * ⚙️ Save E-Sevai Settings
 */
function saveESevaiSettingsAction(settingsObj, tenantId) {
    try {
        if (!useFirebaseRtdb_()) return { status: 'error', message: 'Firebase RTDB URL not configured' };
        tenantId = resolveTenantId_(tenantId || (settingsObj && (settingsObj.tenant_id || settingsObj.tenantId)));
        let tAccess = buildTenantAccessContext_(tenantId, "", "");
        if (!tAccess.allowed) return { status: 'error', message: 'Subscription inactive: ' + String(tAccess.reason || 'blocked'), tenant_id: tenantId };
        let firebaseWarn = "";
        if (useFirebaseRtdb_()) {
            try {
                let data = getESevaiSnapshot_(tenantId) || {};
                data.settings = data.settings || {};
                for (const [k, v] of Object.entries(settingsObj || {})) data.settings[k] = v;
                saveESevaiSnapshot_(data, tenantId);
                return { status: 'success' };
            } catch (fbErr) {
                return { status: 'error', message: String(fbErr || "") };
            }
        }

        let db = getDB();
        let sheet = db.getSheetByName(ESEVAI_SETTINGS_SHEET);
        sheet.clear();
        sheet.appendRow(["Key", "Value"]);
        sheet.getRange(1, 1, 1, 2).setFontWeight("bold").setBackground("#f1f5f9");
        for (const [k, v] of Object.entries(settingsObj)) {
            sheet.appendRow([k, v]);
        }
        return firebaseWarn
            ? { status: 'success', warning: "Firebase sync failed. Saved to Sheets fallback.", firebaseError: firebaseWarn }
            : { status: 'success' };
    } catch (e) {
        return { status: 'error', message: e.toString() };
    }
}

/**
 * 🛠️ Initialize all E-Sevai Sheets with Headers and Seed Data
 */
function initESevaiSheets() {
    let db = getDB();
    let sheets = [
        { name: ESEVAI_SERVICES_SHEET, headers: ["ID", "Name", "Category", "Type", "Gov_Fee", "Direct_Charge", "Agent_Charge", "Required_Documents", "Icon"] },
        { name: ESEVAI_CUSTOMERS_SHEET, headers: ["ID", "Name", "Phone", "Balance", "Type"] },
        { name: ESEVAI_TRANSACTIONS_SHEET, headers: ["ID", "Customer_ID", "Services_JSON", "Gov_Bank", "Payment_Mode", "Total_Amount", "Received_Amount", "Balance_Diff", "Other_Expenses", "Status", "Created_At"] },
        { name: ESEVAI_LEDGER_SHEET, headers: ["Date", "Type", "Category", "Description", "Amount", "Account"] },
        { name: ESEVAI_ENQUIRIES_SHEET, headers: ["ID", "Customer_ID", "Service_Name", "Quoted_Amount", "Stages_JSON", "Status", "Notes", "Created_At", "Advance_Received"] },
        { name: ESEVAI_WORKS_SHEET, headers: ["ID", "Transaction_ID", "Customer_ID", "Service_Name", "Status", "Type", "Stages_Json", "Document_Url", "Completed_At", "Created_At"] },
        { name: ESEVAI_BALANCES_SHEET, headers: ["Date", "Cash", "SBI", "Federal 1", "Federal 2", "Paytm", "LastSync", "Status"] },
        { name: ESEVAI_SETTINGS_SHEET, headers: ["Key", "Value"] },
        { name: "ES_Service_Stages", headers: ["Service_ID", "Stages_Json"] }
    ];

    sheets.forEach(s => {
        let sheet = db.getSheetByName(s.name);
        if (!sheet) {
            sheet = db.insertSheet(s.name);
            sheet.appendRow(s.headers);
            sheet.getRange(1, 1, 1, s.headers.length).setFontWeight("bold").setBackground("#f1f5f9");
        }
    });

    // Seed Initial Settings if empty
    let settingsSheet = db.getSheetByName(ESEVAI_SETTINGS_SHEET);
    if (settingsSheet.getLastRow() === 1) {
        settingsSheet.appendRow(["business_name", "Ranjith E Sevai Maiyam"]);
        settingsSheet.appendRow(["business_address", "RTO Office OPP Mettur Main Road, Urachikottai, Bhavani"]);
        settingsSheet.appendRow(["business_phone", "9942391870"]);
        settingsSheet.appendRow(["business_gstin", "33BJVPR5841P1Z6"]);
        settingsSheet.appendRow(["gst_rate", "18"]);
    }

    // Seed Services if empty
    let srvSheet = db.getSheetByName(ESEVAI_SERVICES_SHEET);
    if (srvSheet.getLastRow() === 1) {
        let services = [
            // ID, Name, Cat, Type, Gov, Direct, Agent, Docs, Icon
            [1, "Issue of Learners Licence (LLR)", "Licence Services", "Service", 230, 270, 120, "Aadhaar Card, Photo, Blood Group", ""],
            [2, "Issue of Duplicate LL", "Licence Services", "Service", 200, 200, 100, "Aadhaar Card", ""],
            [3, "Renewal of DL", "Licence Services", "Service", 450, 350, 150, "Original DL, Medical Certificate", ""],
            [4, "Transfer of Ownership (Seller)", "Vehicle Services", "Service", 300, 300, 150, "Original RC, Aadhaar", ""],
            [5, "Hypothecation Termination (HPT)", "Vehicle Services", "Service", 275, 225, 75, "Original RC, Bank NOC, Form 35", ""],
            [6, "Xerox (BW)", "General", "Product", 0, 2, 1, "None", ""],
            [7, "Xerox (Color)", "General", "Product", 0, 10, 5, "None", ""]
        ];
        services.forEach(row => srvSheet.appendRow(row));
    }

    // Seed Initial Agents if empty
    let custSheet = db.getSheetByName(ESEVAI_CUSTOMERS_SHEET);
    if (custSheet.getLastRow() === 1) {
        custSheet.appendRow([1, "Walk-in Customer", "0000000000", 0, "Direct"]);
        custSheet.appendRow([2, "General Agent 1", "9000000001", 0, "Agent"]);
        custSheet.appendRow([3, "Speed Agent Bhavani", "9000000002", 0, "Agent"]);
    }

    return "E-Sevai Initialized Successfully!";
}

/**
 * 📅 Safe Normalize Date for Matching (Standardize to YYYY-MM-DD)
 */
function normalizeDateValue(val) {
    if (!val) return "";
    let d = null;
    if (val instanceof Date) {
        d = val;
    } else {
        let str = String(val).trim();
        if (str.startsWith("'")) str = str.substring(1);
        let parts = str.split(/[\/\-]/);
        if (parts.length === 3) {
            // Assume DD/MM/YYYY
            if (parts[2].length === 4) d = new Date(parts[2], parts[1]-1, parts[0]);
            // Assume YYYY/MM/DD
            else if (parts[0].length === 4) d = new Date(parts[0], parts[1]-1, parts[2]);
        }
    }
    if (d && !isNaN(d.getTime())) {
        let day = String(d.getDate()).padStart(2, '0');
        let mon = String(d.getMonth() + 1).padStart(2, '0');
        let year = d.getFullYear();
        return `${year}-${mon}-${day}`;
    }
    return String(val).trim();
}

/**
 * ✅ No-argument wrappers for non-coders (Apps Script Run dropdown)
 */
function runHealthCheckNow() {
    return runDiagnosticHealthReport(false, "ரஞ்சித்");
}

function runHealthCheckWithMsg() {
    return runDiagnosticHealthReport(true, "ரஞ்சித்");
}

function runQuizDayFixDryRun() {
    return fixQuizDayMappingFromJoinDate(true, "ரஞ்சித்");
}

function runQuizDayFixApply() {
    return fixQuizDayMappingFromJoinDate(false, "ரஞ்சித்");
}

// Alternate short names (in case function list cache issues in Apps Script UI)
function zzQuizFixDryRun() {
    return runQuizDayFixDryRun();
}

function zzQuizFixApply() {
    return runQuizDayFixApply();
}

function runWebhookDebugNow() {
    let out = getWebhookDebugStatus();
    Logger.log(JSON.stringify(out, null, 2));
    return out;
}

function runWebhookDebugNowWithMsg() {
    let out = runWebhookDebugNow();
    try {
        let txt = "Webhook Debug:\n" + JSON.stringify(out, null, 2);
        uiToast_(txt, "Webhook Status");
    } catch (e) {}
    return out;
}

