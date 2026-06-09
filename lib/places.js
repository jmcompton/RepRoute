'use strict';

// ── Google Places enrichment ─────────────────────────────────────────────────
// Reusable Places lookup + blanks-only account enrichment. The lookup logic is
// lifted verbatim from routes/quotes.js upsert-prospect (the proven path):
//   findplacefromtext → place_id → details (phone, website, address, components)
// Uses GOOGLE_PLACES_API_KEY. Returns null gracefully when the key is missing or
// no candidate is found, so callers can no-op safely.

const fetch = require('node-fetch');

// lookupPlace(query) → { phone, website, address, city, state } | null
async function lookupPlace(query) {
  const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;
  const q = String(query || '').trim();
  if (!PLACES_KEY || !q) return null;

  try {
    // Step 1: find the place
    const findUrl = `https://maps.googleapis.com/maps/api/place/findplacefromtext/json` +
      `?input=${encodeURIComponent(q)}` +
      `&inputtype=textquery` +
      `&fields=place_id,name` +
      `&key=${PLACES_KEY}`;

    const findRes = await fetch(findUrl);
    const findData = await findRes.json();

    if (!findData.candidates || findData.candidates.length === 0) return null;
    const placeId = findData.candidates[0].place_id;

    // Step 2: get full details including address_components, phone, website
    const detailUrl = `https://maps.googleapis.com/maps/api/place/details/json` +
      `?place_id=${placeId}` +
      `&fields=name,formatted_phone_number,website,formatted_address,address_components` +
      `&key=${PLACES_KEY}`;

    const detailRes = await fetch(detailUrl);
    const detailData = await detailRes.json();

    if (!detailData.result) return null;
    const r = detailData.result;

    let phone = null, website = null, address = null, city = null, state = null;
    if (r.formatted_phone_number) phone = r.formatted_phone_number;
    if (r.website) website = r.website;
    if (r.formatted_address) address = r.formatted_address;
    if (r.address_components) {
      for (const comp of r.address_components) {
        if (comp.types.includes('locality')) city = comp.long_name;
        if (comp.types.includes('administrative_area_level_1')) state = comp.short_name;
      }
    }

    return { phone, website, address, city, state };
  } catch (plErr) {
    console.error('Places lookup error:', plErr.message);
    return null;
  }
}

// enrichAccount(pool, accountId) → { skipped } | { enriched, changed:[...] } | { error }
// Blanks-only: fills phone/address/website/city/state only where currently blank.
// Skips entirely if the account already has a phone. Never touches contact.
async function enrichAccount(pool, accountId) {
  const id = parseInt(accountId);
  if (!Number.isFinite(id)) return { error: 'invalid id' };

  const cur = await pool.query(
    `SELECT id, company, phone, address, website, city, state FROM prospects WHERE id=$1`,
    [id]
  );
  if (!cur.rows.length) return { error: 'not found' };
  const p = cur.rows[0];

  // Already has a phone → skip (idempotent, cost-capped).
  if (p.phone && String(p.phone).trim() !== '') return { skipped: true };

  // Build query with city/state for match accuracy.
  const query = [p.company, p.city, p.state].filter(function (v) {
    return v && String(v).trim() !== '';
  }).join(', ');

  const place = await lookupPlace(query);
  if (!place) return { enriched: false, changed: [] };

  // Fill ONLY blank columns — COALESCE(NULLIF(TRIM(col),''),$new) never overwrites.
  const cols = ['phone', 'address', 'website', 'city', 'state'];
  const sets = [];
  const vals = [];
  const changed = [];
  cols.forEach(function (col) {
    const v = place[col];
    if (v !== null && v !== undefined && String(v).trim() !== '') {
      vals.push(String(v).trim());
      sets.push(`${col} = COALESCE(NULLIF(TRIM(${col}),''), $${vals.length})`);
      changed.push(col);
    }
  });

  if (sets.length === 0) return { enriched: false, changed: [] };

  vals.push(id);
  await pool.query(
    `UPDATE prospects SET ${sets.join(', ')}, last_activity_at=NOW() WHERE id=$${vals.length}`,
    vals
  );

  return { enriched: true, changed: changed };
}

module.exports = { lookupPlace, enrichAccount };
