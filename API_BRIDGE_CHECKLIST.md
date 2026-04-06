# API Bridge Dependency Checklist

## Runtime Modes

- **Native mode**: App runs inside Apps Script HTMLService (`google.script.host` available).
- **Bridge mode**: App runs on Firebase Hosting and forwards server calls to Apps Script `doPost`.

## Implemented in Frontend (`index.html`)

- Unified run client wraps all existing `google.script.run` calls.
- Auto runtime detection:
  - native -> uses original Apps Script transport
  - hosting -> uses fetch bridge transport with retry
- Added helper:
  - `apiCall_(fnName, ...args)` for Promise-based migration
- Bridge config persistence:
  - `nanban_bridge_url`
  - `nanban_bridge_secret`
- Live network indicator includes bridge latency in hosting mode.

## Migrated to Promise API (`apiCall_`)

- `loadAdminSystem` boot sequence:
  - `getAppUsers`
  - `getAppSettings`
- `loadDatabaseLegacy`:
  - `getDatabaseData`
  - `getChitData`
- `loadDatabaseSilent`:
  - `getDatabaseData`
  - `getChitData`
- `loadESevaiInitialDataLegacy`:
  - `getESevaiInitialData`

## Implemented in Backend (`gode.gs`)

- `doPost` route: `action = "api_bridge"`
- Bridge auth with `WEB_BRIDGE_KEY`
- Common JSON output helper
- Call guard for non-callable/private functions
- Added compatibility API:
  - `sendDigitalFeeReceiptAction`
- Added health API:
  - `bridgePingAction`

## Required Script Properties

- `WEB_BRIDGE_KEY` (required for hosting mode)
- Existing keys remain as-is:
  - `FIREBASE_DB_SECRET` (if used)
  - `IMPORT_BACKUP_KEY` (backup endpoint)

## Quick Smoke Tests

1. Login load (`getAppUsers`) works.
2. Dashboard loads students/expenses.
3. E-Sevai dashboard loads services/customers.
4. Student fee receipt send action does not error.
5. Network pill shows latency (`xxms`) in hosting mode.
