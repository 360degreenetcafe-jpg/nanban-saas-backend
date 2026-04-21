# Nanban / NANBAN PRO — Product roadmap

**Vision:** Hosted-first, single codebase: driving school ERP, E‑Sevai POS, WhatsApp automation, and accounting-grade reports — stable on mobile, tenant-safe, and audit-friendly. Legacy Google Apps Script remains an optional bridge until feature parity.

**Last updated:** April 2026

**How to read this file:** Everything below is **in scope for the product** unless explicitly marked optional. Phases are **delivery order and focus**, not “we will not build” the rest. Use this as the single place to see *all futures* in one map.

---

## Full product surface (feature map)

These are the major capability areas. Items ship across phases; many foundations already exist in `functions/` and the hosted UI.

| Area | What “done” looks like | Primary code / entry (indicative) |
|------|------------------------|-------------------------------------|
| **Driving school ERP** | Students, fees/expenses, RTO services, fleet (vehicle, fuel, service logs), chits, daily triggers — usable on mobile and hosted. | `erpRpcDispatch.js`, `gode.gs` parity reference |
| **Identity & tenant** | Invited staff/partners: Google login resolves to tenant; soft-deactivate respected; formal owner link where needed. | `resolveHostedGoogleLoginUserAction`, `linkInvitedUserToAuthUidAction`, team actions |
| **Team & access** | Add/update PIN, deactivate/reactivate, role expectations documented for sensitive exports (roadmap: RBAC hardening). | `addTeamMemberAction`, `deactivateTeamMemberAction`, … |
| **Contacts** | Post-OAuth contact sync for supported flows; predictable errors and retries. | `googleContactsOAuth.js`, `contactSyncService.js` |
| **Hosted reports & PDF** | Audit, cashbook, monthly pack, filing index — print + download; Tamil-safe output; filing metadata persisted for hosted. | `nanbanHostedReportsPdf.js`, `erpRpcDispatch.js` PDF RPCs |
| **E‑Sevai** | Customers, agents, ledger, services, enquiries, transactions, day close, openings, WA bill notify — My Bill Book–style parity over time. | E‑Sevai `save*` / `closeESevaiDayAction` RPCs |
| **GST / tax (E‑Sevai)** | GSTIN, line GST, period summaries; full GSTR automation optional later program. | Settings + report builders (phased) |
| **WhatsApp core** | Templates sync, outbound queue, idempotency, native job processing, tenant branding. | `waTemplateConfig`, `waOutboundQueue`, `waNativeJobProcessor`, `tenantMessagingBrand` |
| **WhatsApp rich flows** | Interactive menus, student assistant config, quiz inbound, smart replies, PDF outbound + storage where applicable. | `whatsappInteractiveMenus`, `studentWaAssistant`, `nanbanQuizInbound`, `waPdfOutbound` |
| **RTO / training aids** | Checklist definitions, quiz banks (incl. heavy vehicle topics), optional LLR capture helpers — integrated where product needs them. | `rtoChecklistDef`, `heavyVehicleQuizBank`, `llrVisionExtract` |
| **SaaS & commercial** | Tenant billing updates, payment recording, activity/session signals for ops. | `updateSaaSTenantBillingAction`, `recordSaaSPaymentAction`, `dynamicPricingEngine` |
| **Admin & reliability** | Admin notify, phone resolve, DLQ/ops routes, crons, structured logging. | `adminNotify`, `scheduledCrons`, DLQ routes |
| **PWA / static shell** | Manifest, SW, SEO basics, hosted `index.html` alignment with native integration. | `public/*`, `index.html` |

Optional or **explicitly later** programs (still on the map, not cancelled): super-admin multi-tenant console, full GSTR filing automation, deep inventory beyond current E‑Sevai scope.

---

## Phase 0 — Foundation (2–4 weeks, ongoing)

| Item | Outcome |
|------|---------|
| Deploy discipline | Functions + static hosting deployed together after RPC or UI changes that depend on each other. |
| Auth & tenant | Invited staff/partners: Google login lands on correct tenant; soft-deactivate respected everywhere. |
| Observability | Structured logs for RPC failures, PDF generation, and WhatsApp sends; a short internal health checklist. |
| Security | Secrets only in environment / Secret Manager; document RPC auth expectations. |

**Exit criteria:** No silent “not implemented” stubs on critical hosted paths; support can follow a short runbook.

---

## Phase 1 — Reports & accounting (Nanban driving school) (4–8 weeks)

| Item | Outcome |
|------|---------|
| PDF quality | Tamil-safe PDFs (e.g. embed Noto Sans Tamil or vetted HTML→PDF); consistent filenames and optional retention policy in Storage. |
| Filing index | Persist export metadata in Firestore (replace Drive-only filing log for hosted mode); surface in Reports UI. |
| Cashbook | Standalone cashbook PDF + optional CSV; unify date handling (ISO `YYYY-MM-DD` vs `DD/MM/YYYY`) across UI, RPC, and PDF builders. |
| Reconciliation | Monthly opening/closing with drill-down to source lines; consider account/bank tags on expenses in a later iteration. |

**Exit criteria:** Month-end close possible using **hosted only**: on-screen report, print, PDF download, saved openings, and a minimal filing trail.

---

## Phase 2 — E‑Sevai (“My Bill Book” style) (8–16 weeks)

| Item | Outcome |
|------|---------|
| Ledger & bills | Customer balance, transaction history, printable views; PDF per bill / period summary using the same server pattern as Nanban reports. |
| GST | GSTIN on tenant/customer profile; line-level GST %; monthly GST **summary** report (full GSTR filing automation is a separate, optional program). |
| Catalog | Service catalog hardening; optional inventory only if product scope expands. |

**Exit criteria:** Daily E‑Sevai operations and month-end summaries without dependence on Sheets for core flows.

---

## Phase 3 — WhatsApp & automation (6–12 weeks, can overlap)

| Item | Outcome |
|------|---------|
| Reliability | Template sync, retries, idempotency keys for outbound jobs. |
| Student / trainer flows | Tenant-branded messages; clear failure reasons for admins. |
| Alerts | Payment due, enquiry follow-up, RTO/test milestones tied to configurable rules. |

**Exit criteria:** Measurable reduction in manual follow-ups and failed sends.

---

## Phase 4 — SaaS & white-label (12–24 weeks)

| Item | Outcome |
|------|---------|
| Onboarding | Trial → payment → tenant provisioning UX; self-serve where safe. |
| Multi-tenant ops | Per-tenant branding, isolated data, minimal super-admin tools. |
| Billing | Pricing rules visible in admin UI with audit trail (align with `dynamicPricingEngine` direction). |

**Exit criteria:** New driving school tenants onboard with bounded support load.

---

## Phase 5 — Quality & scale (continuous)

| Track | Examples |
|--------|-----------|
| Performance | Snapshot pagination, lazy-loaded tabs, large-tenant testing. |
| Mobile | PWA polish, safe handling of auth redirect and cache. |
| Accessibility & i18n | Consistent Tamil/English; focus order and labels on key flows. |
| Compliance | WhatsApp / Google consent notes; per-tenant data export for portability. |

---

## Next 90 days — suggested priority

1. **PDF:** Tamil font embedding (or equivalent) + filing log persistence in Firestore for hosted mode.  
2. **E‑Sevai:** Ledger / bill **PDF + print** parity with Nanban report pipeline.  
3. **Governance:** Role matrix for financial export and opening-balance edits (Admin/Owner vs Partner/Staff).  
4. **Operations:** Monitoring hooks + internal runbook (deploy, rollback, common Tamil user-facing errors).

---

## Related implementation notes (repo)

- Hosted PDFs: `functions/src/services/nanbanHostedReportsPdf.js` and RPC cases in `functions/src/services/erpRpcDispatch.js` (`generateFullAuditPdfAction`, `generateMonthlyCashbookPdfAction`, `generateMonthlyPdfPackAction`, `generateFilingIndexPdfAction`).  
- Legacy parity: `gode.gs` (Apps Script) remains reference for behaviour until fully retired.  
- UI entry: Reports tab in `index.html` / `public/index.html` (hosted path uses `apiCall_` when native integration is configured).

---

## How to use this document

- Treat phases as **planning horizons**, not fixed deadlines.  
- The **feature map** is the catalogue of *everything we intend*; phases only sequence *when we stress-test and harden* each stream.  
- Re-prioritise after each production release based on customer pain (support tickets, revenue, compliance).  
- Keep **one source of truth** for dates: update the “Last updated” line when this file changes.
