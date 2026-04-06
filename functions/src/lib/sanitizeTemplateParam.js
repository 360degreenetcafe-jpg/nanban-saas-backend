/**
 * WhatsApp Cloud API template body variables cannot contain raw newlines/tabs.
 * Mirrors gode.gs sanitizeTemplateParamText_.
 */
function sanitizeTemplateParamText(txt) {
  try {
    let s = String(txt || "");
    s = s.replace(/[\r\n\t]+/g, " | ");
    s = s.replace(/\s{5,}/g, "    ");
    s = s.replace(/\s+\|\s+/g, " | ");
    return s.trim();
  } catch (e) {
    return String(txt || "");
  }
}

module.exports = { sanitizeTemplateParamText };
