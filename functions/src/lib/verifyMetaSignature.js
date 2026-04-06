const crypto = require("crypto");

function verifyMetaSignature(rawBodyBuffer, xHubSignature256, appSecret) {
  if (!rawBodyBuffer || !Buffer.isBuffer(rawBodyBuffer)) return false;
  if (!xHubSignature256 || typeof xHubSignature256 !== "string") return false;
  if (!appSecret || typeof appSecret !== "string") return false;

  const parts = xHubSignature256.split("=");
  if (parts.length !== 2 || parts[0] !== "sha256") return false;

  const receivedHex = parts[1];
  const expectedHex = crypto.createHmac("sha256", appSecret).update(rawBodyBuffer).digest("hex");

  const expectedBuf = Buffer.from(expectedHex, "hex");
  const receivedBuf = Buffer.from(receivedHex, "hex");
  if (expectedBuf.length !== receivedBuf.length) return false;

  return crypto.timingSafeEqual(expectedBuf, receivedBuf);
}

module.exports = { verifyMetaSignature };
