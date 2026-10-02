// ════════════════════════════════════════════════════════════════
// Account name matching — ONE rule used everywhere (quote → account matching,
// duplicate-account review, quote contact lists):
//   • case, punctuation and spacing don't matter
//   • a trailing legal suffix doesn't matter: Inc, LLC, Co, Company
//     (also "Incorporated" and "Branch", e.g. "CRS - Kennesaw Branch")
//   • anything else — including a city after the name — is part of the name,
//     so "CRS-Savannah" stays separate from "CRS Inc"
// Examples → key:
//   "Mid-Atlantic Roofing Supply Company"  → "mid atlantic roofing supply"
//   "CRS, Inc."                            → "crs"
//   "CRS-Savannah"                         → "crs savannah"
// ════════════════════════════════════════════════════════════════

const SUFFIX_RX = /\s+(inc|incorporated|llc|co|company|branch)$/;

function accountKey(name) {
  let s = String(name || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')   // punctuation / dashes / commas → space
    .trim()
    .replace(/\s+/g, ' ');
  // Strip trailing suffixes repeatedly ("Company, Inc." → both go).
  let prev;
  do { prev = s; s = s.replace(SUFFIX_RX, '').trim(); } while (s !== prev && s);
  return s || String(name || '').toLowerCase().trim();
}

// Postgres expression computing the same key for a column, so matching can run
// in SQL. Mirrors accountKey(); keep the two in step.
function accountKeySql(col) {
  return `TRIM(REGEXP_REPLACE(
            TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(REPLACE(COALESCE(${col},''), '&', ' and ')), '[^a-z0-9]+', ' ', 'g'), '\\s+', ' ', 'g')),
            '(\\s+(inc|incorporated|llc|co|company|branch))+$', '', 'g'))`;
}

module.exports = { accountKey, accountKeySql };
