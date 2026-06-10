'use strict';

// ── Google Places enrichment ─────────────────────────────────────────────────
// Reusable Places lookup + blanks-only account enrichment. The lookup logic is
// lifted verbatim from routes/quotes.js upsert-prospect (the proven path):
//   findplacefromtext → place_id → details (phone, website, address, components)
// Uses GOOGLE_PLACES_API_KEY. Returns null gracefully when the key is missing or
// no candidate is found, so callers can no-op safely.

const fetch = require('node-fetch');
const { splitLocation } = require('./commission-matcher');

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

// enrichAccount(pool, accountId) → { skipped } | { enriched, changed:[...], attempted }
//                                  | { error }
// Blanks-only: fills phone/address/website/city/state only where currently blank.
// Skips entirely if the account already has a phone. Never touches contact.
// ALWAYS stamps enrich_attempted_at = NOW() (success OR miss) so unmatchable
// accounts drop out of the "missing phone" queue and aren't retried forever.
async function enrichAccount(pool, accountId) {
  const id = parseInt(accountId);
  if (!Number.isFinite(id)) return { error: 'invalid id' };

  const cur = await pool.query(
    `SELECT id, company, phone, address, website, city, state FROM prospects WHERE id=$1`,
    [id]
  );
  if (!cur.rows.length) return { error: 'not found' };
  const p = cur.rows[0];

  // Already has a phone → skip (idempotent, cost-capped). Not stamped: nothing
  // was attempted, and it isn't in the missing-phone queue anyway.
  if (p.phone && String(p.phone).trim() !== '') return { skipped: true };

  // Build a CLEAN query. Commission-imported names often bake the location into
  // the company field ("Heritage Insulation, Auburn, AL"), which pollutes the
  // Places query. Strip the trailing ", City, ST" off the name and prefer the
  // dedicated city/state columns (falling back to whatever the name yielded).
  const split = splitLocation(p.company);
  const cleanName = (split.core || String(p.company || '')).trim();
  const city = (p.city && String(p.city).trim()) || split.city || '';
  const state = (p.state && String(p.state).trim()) || split.state || '';
  const query = [cleanName, city, state].filter(function (v) {
    return v && String(v).trim() !== '';
  }).join(', ');

  const place = query ? await lookupPlace(query) : null;

  // Fill ONLY blank columns — COALESCE(NULLIF(TRIM(col),''),$new) never overwrites.
  const sets = [];
  const vals = [];
  const changed = [];
  if (place) {
    ['phone', 'address', 'website', 'city', 'state'].forEach(function (col) {
      const v = place[col];
      if (v !== null && v !== undefined && String(v).trim() !== '') {
        vals.push(String(v).trim());
        sets.push(`${col} = COALESCE(NULLIF(TRIM(${col}),''), $${vals.length})`);
        changed.push(col);
      }
    });
  }

  // Always stamp the attempt marker so the account leaves the queue regardless
  // of whether Places found anything.
  sets.push('enrich_attempted_at = NOW()');
  sets.push('last_activity_at = NOW()');
  vals.push(id);
  await pool.query(
    `UPDATE prospects SET ${sets.join(', ')} WHERE id=$${vals.length}`,
    vals
  );

  return { enriched: changed.length > 0, changed: changed, attempted: true };
}

module.exports = { lookupPlace, enrichAccount };
