'use strict';

// ── Levenshtein + fuzzy matching ─────────────────────────────────
// Shared account-name fuzzy helpers. Extracted verbatim from routes/voice.js
// so the Voice Logger account matcher and the Commission import account matcher
// use ONE implementation. Do not fork these — both modules depend on identical
// behavior.

function levenshtein(a, b) {
  const m = a.length, n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const dp = [];
  for (let i = 0; i <= m; i++) { dp[i] = [i]; }
  for (let j = 0; j <= n; j++) { dp[0][j] = j; }
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = a[i-1] === b[j-1]
        ? dp[i-1][j-1]
        : 1 + Math.min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]);
    }
  }
  return dp[m][n];
}

function normalize(s) {
  return (s || '').toLowerCase()
    .replace(/[-_&,\.]/g, ' ')
    .replace(/\bllc\b|\binc\b|\bcorp\b|\bco\b|\bltd\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function similarity(a, b) {
  const na = normalize(a), nb = normalize(b);
  if (!na || !nb) return 0;
  const longer = Math.max(na.length, nb.length);
  if (longer === 0) return 1;
  return (longer - levenshtein(na, nb)) / longer;
}

module.exports = { levenshtein, normalize, similarity };
