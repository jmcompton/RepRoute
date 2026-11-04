// Geocode a full street address to {lat, lng} via the Google Geocoding API.
// Pinned to the US so a bare "Birmingham" is Birmingham AL, not England.
// (same GOOGLE_PLACES_API_KEY the Lead Finder uses). Returns null on any
// failure — never throws — so callers can fall back to the city centroid.
const fetch = require('node-fetch');

const TIMEOUT_MS = 6000;

// geocodeLookup → { coords } when found, { notFound: true } when Google has no
// match, { error } when Google couldn't be reached or refused the request.
// Callers must not treat an outage as "this address doesn't exist".
async function geocodeLookup(address) {
  const key = process.env.GOOGLE_PLACES_API_KEY;
  const q = String(address || '').trim();
  if (!q) return { notFound: true };
  if (!key) return { error: 'No Google key configured' };
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), TIMEOUT_MS);
  try {
    const r = await fetch(
      `${(process.env.GEOCODE_BASE_URL || 'https://maps.googleapis.com').replace(/\/$/, '')}/maps/api/geocode/json?address=${encodeURIComponent(q)}&components=country:US&key=${key}`,
      { signal: ctl.signal }
    );
    const d = await r.json();
    const loc = d && d.results && d.results[0] && d.results[0].geometry && d.results[0].geometry.location;
    if (loc) return { coords: { lat: loc.lat, lng: loc.lng } };
    if (!r.ok || (d && d.status && d.status !== 'ZERO_RESULTS' && d.status !== 'OK')) {
      return { error: (d && (d.error_message || d.status)) || ('HTTP ' + r.status) };
    }
    return { notFound: true };
  } catch (e) {
    console.error('[geocode]', e.message);
    return { error: e.message };
  } finally {
    clearTimeout(timer);
  }
}

async function geocodeAddress(address) {
  const g = await geocodeLookup(address);
  return g.coords || null;
}

// Geocode an account's address and store lat/lng (null when the address is
// empty or won't geocode). Fire-and-forget safe: logs, never throws.
async function geocodeProspect(pool, prospectId, address) {
  try {
    let coords = null;
    if (address) {
      const g = await geocodeLookup(address);
      if (g.error) return; // Google down: keep whatever location we had, retry next save
      coords = g.coords || null;
    }
    await pool.query(
      'UPDATE prospects SET lat=$1, lng=$2, geocoded_at=NOW() WHERE id=$3',
      [coords ? coords.lat : null, coords ? coords.lng : null, prospectId]
    );
  } catch (e) {
    console.error('[geocodeProspect]', prospectId, e.message);
  }
}

module.exports = { geocodeAddress, geocodeLookup, geocodeProspect };
