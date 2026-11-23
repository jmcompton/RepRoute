// Where a rep works. One answer for every feature that needs it (overnight
// leads, lead search, day plan), so they can't disagree.
//   1. The Territory the rep picked (bottom left of the menu). It's what they
//      see and control, so it wins.
//   2. Their home base, when it sits inside that territory (it's more exact),
//      or when no territory is set.
//   3. The middle of their mapped accounts, using the median so a few
//      accounts mapped to the wrong state can't drag it away.
const { pool } = require('../db');
const { geocodeAddress, milesBetween } = require('./geocode');

const cache = new Map();
async function placeCenter(text) {
  const k = String(text || '').trim().toLowerCase();
  if (!k) return null;
  const hit = cache.get(k);
  if (hit && Date.now() - hit.at < 864e5) return hit.c;
  let c = null;
  try { const g = await geocodeAddress(text); if (g && Number.isFinite(g.lat) && Number.isFinite(g.lng)) c = { lat: g.lat, lng: g.lng }; } catch (e) {}
  // Keep a miss only briefly, so one failed lookup can't stick for a day.
  cache.set(k, { c, at: c ? Date.now() : Date.now() - 864e5 + 6e5 });
  return c;
}
function median(xs) { const s = xs.slice().sort((a, b) => a - b); const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; }

async function workCenter(userId) {
  const u = (await pool.query('SELECT territory, home_base_lat::float AS hlat, home_base_lng::float AS hlng, home_base_city FROM users WHERE id=$1', [userId])).rows[0] || {};
  const home = u.hlat != null && u.hlng != null ? { lat: u.hlat, lng: u.hlng } : null;
  const terr = u.territory && String(u.territory).trim();
  const tc = terr ? await placeCenter(terr) : null;
  if (tc) {
    if (home && milesBetween(home, tc) <= 40) return Object.assign({ label: terr, source: 'home base' }, home);
    return Object.assign({ label: terr, source: 'territory' }, tc);
  }
  if (home) return Object.assign({ label: terr || u.home_base_city || 'your home base', source: 'home base' }, home);
  const a = (await pool.query('SELECT lat::float lat, lng::float lng FROM prospects WHERE user_id=$1 AND lat IS NOT NULL AND lng IS NOT NULL', [userId])).rows;
  if (a.length >= 3) return { lat: median(a.map(x => x.lat)), lng: median(a.map(x => x.lng)), label: 'your accounts', source: 'accounts' };
  return null;
}

module.exports = { workCenter, placeCenter };
