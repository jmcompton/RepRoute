// Geocode a full street address to {lat, lng} via the Google Geocoding API
// (same GOOGLE_PLACES_API_KEY the Lead Finder uses). Returns null on any
// failure — never throws — so callers can fall back to the city centroid.
const fetch = require('node-fetch');

const TIMEOUT_MS = 6000;

async function geocodeAddress(address) {
  const key = process.env.GOOGLE_PLACES_API_KEY;
  const q = String(address || '').trim();
  if (!key || !q) return null;
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), TIMEOUT_MS);
  try {
    const r = await fetch(
      `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(q)}&key=${key}`,
      { signal: ctl.signal }
    );
    const d = await r.json();
    const loc = d && d.results && d.results[0] && d.results[0].geometry && d.results[0].geometry.location;
    return loc ? { lat: loc.lat, lng: loc.lng } : null;
  } catch (e) {
    console.error('[geocode]', e.message);
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// Geocode an account's address and store lat/lng (null when the address is
// empty or won't geocode). Fire-and-forget safe: logs, never throws.
async function geocodeProspect(pool, prospectId, address) {
  try {
    const coords = address ? await geocodeAddress(address) : null;
    await pool.query(
      'UPDATE prospects SET lat=$1, lng=$2, geocoded_at=NOW() WHERE id=$3',
      [coords ? coords.lat : null, coords ? coords.lng : null, prospectId]
    );
  } catch (e) {
    console.error('[geocodeProspect]', prospectId, e.message);
  }
}

module.exports = { geocodeAddress, geocodeProspect };
