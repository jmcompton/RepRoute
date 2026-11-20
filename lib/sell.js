// "What you sell": the lines the team actively sells and who buys them.
// One source for every agent: the lead hunt searches the buyer segments, next
// steps and shelf photos spot the lines by name, documents file under them.
const { pool } = require('../db');

let cache = null, cachedAt = 0;
const TTL = 60 * 1000;

async function getLines(force) {
  if (!force && cache && Date.now() - cachedAt < TTL) return cache;
  const r = await pool.query(
    `SELECT id, name, what, COALESCE(buyer_segments, '{}') AS buyer_segments, competitors, match_words, sort_order
       FROM lines WHERE featured ORDER BY sort_order, name`);
  cache = r.rows; cachedAt = Date.now();
  return cache;
}
function clearCache() { cache = null; }

function escapeRx(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }

// [[name, RegExp]] for spotting lines in visit notes. Each word or phrase
// matches whole words, any capitalization, with flexible spaces and dashes.
function matchers(lines) {
  return lines.map(l => {
    const words = String(l.match_words || '').split(',').map(w => w.trim()).filter(Boolean);
    if (!words.includes(l.name.toLowerCase())) words.unshift(l.name.toLowerCase());
    const parts = words.map(w => escapeRx(w.toLowerCase()).replace(/[-\s]+/g, '[\\s-]?'));
    return [l.name, new RegExp('\\b(' + parts.join('|') + ')\\b', 'i')];
  });
}

// Every buyer segment across the lines, with the lines each one buys.
function segments(lines) {
  const m = new Map();
  for (const l of lines) for (const s of l.buyer_segments || []) {
    if (!m.has(s)) m.set(s, []);
    m.get(s).push(l.name);
  }
  return m;
}

module.exports = { getLines, clearCache, matchers, segments };
