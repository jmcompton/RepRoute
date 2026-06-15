'use strict';

// ── Google Places enrichment ─────────────────────────────────────────────────
// Reusable Places lookup + blanks-only account enrichment. The lookup logic is
// lifted verbatim from routes/quotes.js upsert-prospect (the proven path):
//   findplacefromtext → place_id → details (phone, website, address, components)
// Uses GOOGLE_PLACES_API_KEY. Returns null gracefully when the key is missing or
// no candidate is found, so callers can no-op safely.

const fetch = require('node-fetch');
const https = require('https');
const { splitLocation } = require('./commission-matcher');

// ── Google Places Text Search (v1) ───────────────────────────────────────────
// SAME integration the Lead Finder uses (routes/places.js → places:searchText with
// X-Goog-Api-Key = GOOGLE_PLACES_API_KEY). Factored here so the commission
// "Find missing info" enrichment reuses the exact endpoint + key + field mask.
// Resolves to [] on any error / missing key so callers can no-op safely.
function placesTextSearch(textQuery, locationBias) {
  const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;
  const q = String(textQuery || '').trim();
  if (!PLACES_KEY || !q) return Promise.resolve([]);
  const body = { textQuery: q, maxResultCount: 5, rankPreference: 'RELEVANCE' };
  if (locationBias) body.locationBias = locationBias;
  return new Promise((resolve) => {
    const data = JSON.stringify(body);
    const opts = {
      hostname: 'places.googleapis.com',
      path: '/v1/places:searchText',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': PLACES_KEY,
        'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.addressComponents,places.businessStatus',
        'Content-Length': Buffer.byteLength(data),
      },
    };
    const req = https.request(opts, (res) => {
      let raw = '';
      res.on('data', (c) => raw += c);
      res.on('end', () => { try { resolve(JSON.parse(raw).places || []); } catch (e) { resolve([]); } });
    });
    req.on('error', () => resolve([]));
    req.write(data);
    req.end();
  });
}

// Southeastern-US bias rectangle (GA, AL, TN, the Carolinas, FL, plus neighbors).
// SW corner → NE corner. Keeps results in-territory without hard-excluding edges.
const SOUTHEAST_BIAS = {
  rectangle: {
    low:  { latitude: 24.4, longitude: -91.7 },
    high: { latitude: 37.2, longitude: -75.0 },
  },
};

// Infer a business-type hint from the manufacturer/line we represent, so the
// Places query reads like a real business search ("Acme closet and storage
// dealer Atlanta GA") instead of a bare company name.
function businessTypeHint(manufacturer) {
  const m = String(manufacturer || '').toLowerCase();
  if (/closet|maid|shelv|storage/.test(m))       return 'closet and storage products dealer';
  if (/boss|soudal|sealant|adhesive|seal/.test(m)) return 'building products and sealant supplier';
  if (/fortress|deck|rail/.test(m))              return 'decking and railing distributor';
  if (/mirror|glass/.test(m))                    return 'glass and mirror supplier';
  if (/citadel|fortified|roof/.test(m))          return 'roofing and building products supplier';
  if (/burn|fire|safe/.test(m))                  return 'building materials supplier';
  if (/quadrant|concept|hardware|zalcow/.test(m)) return 'building hardware supplier';
  return 'building products supplier contractor';
}

// City of a Places result: prefer the locality address component, else parse the
// formatted address. Used to detect "plausible results in DIFFERENT cities".
function placeCity(place) {
  const comps = place.addressComponents || [];
  for (const c of comps) {
    if ((c.types || []).includes('locality')) return (c.longText || c.shortText || '').trim();
  }
  const segs = String(place.formattedAddress || '').split(',').map(s => s.trim());
  return segs.length >= 2 ? segs[segs.length - 3] || segs[1] || '' : '';
}

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

// ── "Find missing info" enrichment via Places Text Search ────────────────────
// For a single needs_info account: query Google Places (business-type hint from
// the manufacturer, biased to the Southeast) and resolve to one of three states:
//   single clear result  → fill BLANK phone/address, contact_status='ai_found'
//   different cities      → store top candidates, contact_status='needs_review'
//   no result             → leave contact_status='needs_info' (manual entry)
// Never overwrites an existing phone/address. Never fabricates. Always stamps
// enrich_attempted_at so the account leaves the queue and isn't retried forever.
async function enrichByPlacesTextSearch(pool, accountId) {
  const id = parseInt(accountId);
  if (!Number.isFinite(id)) return { result: 'error', error: 'invalid id' };

  // Pull the account + the manufacturer/line we represent for it (latest non-
  // adjustment commission line). The manufacturer drives the business-type hint.
  const cur = await pool.query(
    `SELECT p.id, p.company, p.phone, p.address, p.city, p.state,
            (SELECT cl.manufacturer FROM commission_lines cl
              WHERE cl.account_id = p.id AND cl.is_adjustment = FALSE
              ORDER BY cl.id DESC LIMIT 1) AS manufacturer
       FROM prospects p WHERE p.id=$1`,
    [id]);
  if (!cur.rows.length) return { result: 'error', error: 'not found' };
  const p = cur.rows[0];

  // Build a clean, business-like query. Strip any baked-in ", City, ST" from the
  // company name and prefer the dedicated city/state columns; bias to GA when no
  // location is known so the search stays in-territory.
  const split = splitLocation(p.company);
  const cleanName = (split.core || String(p.company || '')).trim();
  const city = (p.city && String(p.city).trim()) || split.city || '';
  const state = (p.state && String(p.state).trim()) || split.state || '';
  const hint = businessTypeHint(p.manufacturer);
  const geo = [city, state].filter(Boolean).join(' ') || 'Southeast US';
  const textQuery = [cleanName, hint, geo].filter(Boolean).join(' ');

  const raw = cleanName ? await placesTextSearch(textQuery, SOUTHEAST_BIAS) : [];

  // Keep only plausible, open businesses.
  const candidates = raw.filter(pl =>
    pl && pl.displayName && pl.businessStatus !== 'CLOSED_PERMANENTLY'
  ).slice(0, 3);

  // No result → leave needs_info, just stamp the attempt.
  if (!candidates.length) {
    await pool.query('UPDATE prospects SET enrich_attempted_at = NOW() WHERE id=$1', [id]);
    return { result: 'none' };
  }

  const distinctCities = new Set(
    candidates.map(c => placeCity(c).toLowerCase()).filter(Boolean));

  // Multiple plausible results in DIFFERENT cities → don't guess. Stash the top
  // candidates and flag needs_review so the user picks the right one.
  if (candidates.length > 1 && distinctCities.size > 1) {
    const stash = candidates.map(c => ({
      place_id: c.id || null,
      name: (c.displayName && c.displayName.text) || '',
      phone: c.nationalPhoneNumber || null,
      address: c.formattedAddress || null,
      city: placeCity(c) || null,
    }));
    await pool.query(
      `UPDATE prospects
          SET contact_status='needs_review', enrich_candidates=$1::jsonb,
              enrich_attempted_at=NOW()
        WHERE id=$2`,
      [JSON.stringify(stash), id]);
    return { result: 'needs_review', candidates: stash.length };
  }

  // Single clear result → fill BLANK phone/address only, mark ai_found ("verify").
  const top = candidates[0];
  const phone = top.nationalPhoneNumber || null;
  const address = top.formattedAddress || null;
  const placeId = top.id || null;

  const sets = ['contact_status = \'ai_found\'', 'enrich_attempted_at = NOW()', 'last_activity_at = NOW()'];
  const vals = [];
  const changed = [];
  if (phone)   { vals.push(phone);   sets.push(`phone = COALESCE(NULLIF(TRIM(phone),''), $${vals.length})`); changed.push('phone'); }
  if (address) { vals.push(address); sets.push(`address = COALESCE(NULLIF(TRIM(address),''), $${vals.length})`); changed.push('address'); }
  if (placeId) { vals.push(placeId); sets.push(`google_place_id = COALESCE(NULLIF(TRIM(google_place_id),''), $${vals.length})`); }
  vals.push(id);
  await pool.query(`UPDATE prospects SET ${sets.join(', ')} WHERE id=$${vals.length}`, vals);

  return { result: 'ai_found', changed };
}

module.exports = { lookupPlace, enrichAccount, placesTextSearch, businessTypeHint, enrichByPlacesTextSearch };
