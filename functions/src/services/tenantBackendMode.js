/**
 * Legacy cutover flag — GAS bridge removed; always native Firebase.
 */
async function getTenantBackendMode(_tenantId) {
  return "firebase";
}

module.exports = { getTenantBackendMode };
