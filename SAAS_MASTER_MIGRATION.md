# Nanban SaaS Master Migration (Direct Firebase)

## Goal

Move from:
- Firebase Hosting + Apps Script bridge

To:
- Firebase Hosting + Firebase Auth (Google Login)
- Firebase backend (Cloud Functions + RTDB/Firestore)
- Multi-tenant SaaS model (many driving schools)
- Zero dependency on Google Apps Script

---

## Final Architecture

- Frontend: Firebase Hosting
- Auth: Firebase Authentication (Google Sign-In)
- API: Firebase Cloud Functions (HTTPS + callable)
- Database: Firebase RTDB (current data) with tenant-wise paths
- Storage: Firebase Storage (for docs/images/receipts)
- Jobs: Cloud Scheduler + Functions (quiz, reminders, daily summary)
- WhatsApp webhook: Cloud Function endpoint

---

## Tenant Model (SaaS)

- One org = one driving school tenant
- Suggested paths:
  - `tenants/{tenantId}/students`
  - `tenants/{tenantId}/expenses`
  - `tenants/{tenantId}/settings`
  - `tenants/{tenantId}/esevai/*`
  - `tenants/{tenantId}/users`
- Global map:
  - `userTenants/{uid}` -> role + tenant access

Roles:
- SuperAdmin (platform)
- Owner/Admin (tenant)
- Partner
- Staff
- Trainer

---

## Migration Phases

## Phase 1 - Auth & Tenant Foundation
- [ ] Enable Firebase Auth (Google)
- [ ] Add login screen Google mode
- [ ] Add tenant membership resolver
- [ ] Store role/tenant claims mapping

## Phase 2 - API Replacement
- [ ] Move `getAppUsers/getDatabaseData/getESevaiInitialData` to Cloud Functions
- [ ] Move save/update/delete APIs to Cloud Functions
- [ ] Add centralized API client in frontend
- [ ] Remove bridge-only dependency from critical flows

## Phase 3 - Background Jobs & Integrations
- [ ] Move WhatsApp webhook to Cloud Functions
- [ ] Move quiz/reminder cron jobs to Scheduler
- [ ] Move PDF/report generation workflow
- [ ] Move file upload from Drive to Firebase Storage

## Phase 4 - Cutover & Cleanup
- [ ] Data parity validation (students, expenses, esevai)
- [ ] Disable Apps Script bridge in production
- [ ] Remove legacy script hooks from frontend
- [ ] Final hardening (rules, rate-limit, audit logs)

---

## Deployment Strategy

- Keep current production stable during migration
- Introduce new SaaS APIs in parallel
- Shift feature by feature (no big-bang outage)
- Final cutover after parity report

---

## Rollback Plan

- If issue found, keep bridge mode fallback until fixed
- Release flags per feature (auth/api/module level)

---

## Current Status

- [x] Custom domain live on Firebase Hosting
- [x] Bridge mode operational baseline
- [ ] SaaS direct auth live
- [ ] SaaS direct APIs live
- [ ] Full Apps Script removal
