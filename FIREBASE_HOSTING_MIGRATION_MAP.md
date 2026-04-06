# Nanban Pro - Firebase Hosting Migration Map

## 1) Current dependency checklist (`index.html`)

- Frontend currently uses `google.script.run` heavily for CRUD and operations.
- Runtime assumptions tied to Apps Script:
  - `google.script.run.withSuccessHandler(...).withFailureHandler(...).<serverFunction>()`
  - `google.script.url.getLocation(...)`
- Backend functions used by UI include (sample high-impact groups):
  - **Auth/App boot:** `getAppUsers`, `getAppSettings`, `getDatabaseData`, `getESevaiInitialData`
  - **Nanban CRUD:** `saveStudentData`, `updateStudentData`, `deleteStudent`, `saveExpenseData`, `updateExpenseDataAction`
  - **Trainer/POS:** `processTrainerEntry`, `processDayCloseHandover`, `markTestResultActionEx`
  - **E-Sevai CRUD:** `saveESevai*`, `updateESevai*`, `closeESevaiDayAction`
  - **Messaging/automation:** `sendWelcomeMessageAction`, `sendPaymentReminderAction`, diagnostics and cron helpers

## 2) What is now implemented

- Added a **compatibility bridge** in `index.html`:
  - If native Apps Script runtime exists, app behaves exactly the same.
  - If hosted externally (Firebase Hosting), a shim provides:
    - `google.script.run`
    - `google.script.url.getLocation`
  - Calls are forwarded to Apps Script Web App through `fetch` with payload:
    - `action: "api_bridge"`
    - `fn`
    - `args`
    - `key` (shared secret)
- Added login UX for hosting mode:
  - `Configure API Bridge` button
  - bridge status handling through login status area
  - settings stored in browser localStorage (`nanban_bridge_url`, `nanban_bridge_secret`)

## 3) Backend bridge contract (Apps Script)

- `doPost(e)` now needs to support:
  - `action = "api_bridge"`
  - validate shared secret (`WEB_BRIDGE_KEY`)
  - dispatch to safe callable functions
  - return JSON: `{ status: "success", result }` or `{ status: "error", message }`

## 4) Security model

- Mandatory shared secret (`WEB_BRIDGE_KEY`) for external bridge calls.
- Keep WhatsApp webhook and backup import endpoints unchanged.
- Do not expose private helper functions ending with `_`.
- Recommended production hardening:
  - maintain strict allowlist for callable function names
  - rotate `WEB_BRIDGE_KEY` periodically
  - keep Firebase RTDB rules server-only for write paths

## 5) Deploy flow

1. Deploy Apps Script Web App (new version) and copy `/exec` URL.
2. Set Script Property: `WEB_BRIDGE_KEY=<strong-random-secret>`.
3. Firebase Hosting deploy with `public/index.html`.
4. Open site and set bridge via `Configure API Bridge`.
5. Smoke test:
   - login
   - student create/update
   - expense create/update
   - E-Sevai POS save
   - trainer day close

## 6) Next enhancement (optional, SaaS scale)

- Replace localStorage bridge config with admin-managed Firebase config endpoint.
- Add tenant key (`x-tenant-id`) for multi-school domain routing.
- Add signed JWT-based request auth instead of static secret.
