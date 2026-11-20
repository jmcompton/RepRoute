// The overnight lead hunt. For each rep, search their part of the map for the
// business types that buy the team's lines ("What you sell"), skip anyone
// already in RepRoute, and keep the best as New leads for the morning.
// Uses the same Lead Finder the reps use by hand (routes/morning.js), so the
// searches and the junk filters Kody tuned apply here too.
const { pool } = require('../db');
const sell = require('./sell');
const { milesBetween } = require('./geocode');

const TZ = 'America/New_York';
const MAX_NEW_PER_RUN = 15;
const RADIUS_MILES = 25;

// Lead Finder segment → the account type it's saved as.
const SEGMENT_CATEGORY = {
  'Lumber / Building Supply': 'Building Materials Distributor',
  'Window/Door Installer': 'Window & Door Installer',
  'Roofing Contractor': 'Roofing Contractor',
  'Roofing Distributor': 'Roofing Distributor',
  'Siding Contractor': 'Siding Contractor',
  'Siding Distributor': 'Siding Distributor',
  'Deck Contractor': 'Decking Contractor',
  'Deck Distributor/Dealer': 'Decking Distributor',
  'Construction Fasteners': 'Construction Fasteners',
  'Cornice Contractor': 'Cornice Contractor',
  'Spray Foam Contractor': 'Spray Foam Contractor',
  'Glass Shop': 'Glass Shop',
  'Division 10 Dealer': 'Building Materials Distributor',
  'Rebar / Concrete Supply': 'Building Materials Distributor'
};
const SEGMENT_LABEL = {
  'Lumber / Building Supply': 'Lumber and building supply',
  'Window/Door Installer': 'Window and door dealer or installer',
  'Deck Distributor/Dealer': 'Deck supply dealer',
  'Construction Fasteners': 'Contractor tool and fastener supply',
  'Rebar / Concrete Supply': 'Rebar and concrete supply'
};
const label = s => SEGMENT_LABEL[s] || s;

async function today() {
  return (await pool.query(`SELECT (NOW() AT TIME ZONE '${TZ}')::date::text d`)).rows[0].d;
}

// Where a rep works: their home base, else the middle of their located accounts.
async function centerFor(userId) {
  const u = (await pool.query('SELECT home_base_lat, home_base_lng FROM users WHERE id=$1', [userId])).rows[0] || {};
  if (u.home_base_lat != null && u.home_base_lng != null) return { lat: Number(u.home_base_lat), lng: Number(u.home_base_lng) };
  const a = (await pool.query(`SELECT AVG(lat)::float lat, AVG(lng)::float lng, COUNT(*)::int n FROM prospects WHERE user_id=$1 AND lat IS NOT NULL AND lng IS NOT NULL`, [userId])).rows[0];
  return a && a.n >= 3 ? { lat: a.lat, lng: a.lng } : null;
}

// Rotate through the buyer segments a night at a time, so every type gets
// searched over a few nights without a big Google bill on any one of them.
function pickSegments(all, day, userId, n) {
  if (!all.length) return [];
  const dayNum = Math.floor(Date.parse(day + 'T12:00:00Z') / 864e5);
  const start = (dayNum + userId) % all.length;
  const out = [];
  for (let i = 0; i < Math.min(n, all.length); i++) out.push(all[(start + i) % all.length]);
  return out;
}

async function huntForRep(user, opts = {}) {
  const { runDailyLeads } = require('../routes/morning');
  const day = opts.day || await today();
  const lines = await sell.getLines();
  const segMap = sell.segments(lines);
  const segs = opts.segments || pickSegments([...segMap.keys()].sort(), day, user.id, opts.perRun || 1);
  const center = await centerFor(user.id);
  const record = async (found, note) => pool.query(
    `INSERT INTO lead_hunt_runs (user_id, run_day, found, note) VALUES ($1,$2,$3,$4)
     ON CONFLICT (user_id, run_day) DO UPDATE SET found = lead_hunt_runs.found + EXCLUDED.found, note = EXCLUDED.note, ran_at = NOW()`,
    [user.id, day, found, note]);
  if (!segs.length) { await record(0, 'No lines with buyers set on What you sell'); return { found: 0, note: 'no segments' }; }
  if (!center) { await record(0, 'No home base or located accounts'); return { found: 0, note: 'no location' }; }

  const mine = (await pool.query(`SELECT company, lat::float lat, lng::float lng FROM prospects WHERE user_id=$1 AND lat IS NOT NULL AND lng IS NOT NULL`, [user.id])).rows;
  let found = 0; const notes = [];
  for (const seg of segs) {
    let r;
    try {
      r = await runDailyLeads(user, { brands: segMap.get(seg) || [], channel: seg, lat: center.lat, lng: center.lng, radius_miles: RADIUS_MILES });
    } catch (e) { notes.push(seg + ': ' + e.message); continue; }
    if (r.status !== 200 || !r.body || !Array.isArray(r.body.leads)) { notes.push(seg + ': ' + ((r.body && r.body.error) || r.status)); continue; }
    for (const l of r.body.leads) {
      if (found >= MAX_NEW_PER_RUN) break;
      if (l.already_contacted || l.possible_match || !l.place_id || l.homeBased) continue;
      if (l.business_status && l.business_status !== 'OPERATIONAL') continue;
      let near = null;
      if (l.lat != null && l.lng != null) {
        for (const a of mine) {
          const d = milesBetween({ lat: l.lat, lng: l.lng }, a);
          if (d != null && (!near || d < near.d)) near = { d, name: a.company };
        }
      }
      const why = label(seg) + '.' +
        (near && near.d <= 10 ? ` ${near.d < 1 ? 'Under a mile' : Math.round(near.d) + ' mi'} from ${near.name}.` :
         l.distance_miles != null ? ` ${Math.round(l.distance_miles)} mi from where you usually work.` : '') +
        (l.rating ? ` ${l.rating} stars on Google (${l.reviews}).` : '');
      const ins = await pool.query(
        `INSERT INTO leads (user_id, place_id, company, segment, category, address, city, state, phone, website, lat, lng, score, why, pitch)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT (user_id, place_id) DO NOTHING RETURNING id`,
        [user.id, l.place_id, l.company, seg, SEGMENT_CATEGORY[seg] || null, l.address || null, l.city || null, l.state || null,
         l.phone || null, l.website || null, l.lat ?? null, l.lng ?? null, l.opportunity_score || 0, why,
         (segMap.get(seg) || []).join(', ')]);
      if (ins.rows.length) found++;
    }
  }
  await record(found, notes.join('; ') || null);
  return { found, segments: segs, note: notes.join('; ') || null };
}

let running = false;
// Everyone who sells and has a place on the map, once a day.
async function huntAll() {
  if (running) return { skipped: 'already running' };
  if (String(process.env.LEAD_HUNT || '').toLowerCase() === 'off') return { skipped: 'turned off' };
  if (!process.env.GOOGLE_PLACES_API_KEY) return { skipped: 'no Google key' };
  running = true;
  try {
    const day = await today();
    const users = (await pool.query(
      `SELECT u.id, u.name, u.email, u.role FROM users u
        WHERE u.disabled_at IS NULL AND u.role IN ('rep','manager')
          -- only people out selling (a visit or call in the last 3 weeks), so
          -- nobody pays for Google searches on an account no one uses
          AND EXISTS (SELECT 1 FROM calls c WHERE c.user_id = u.id AND c.call_date > CURRENT_DATE - 21)
          AND NOT EXISTS (SELECT 1 FROM lead_hunt_runs r WHERE r.user_id=u.id AND r.run_day=$1)`, [day])).rows;
    let total = 0;
    for (const u of users) {
      try { total += (await huntForRep(u, { day })).found; }
      catch (e) { console.error('[lead-hunt]', u.id, e.message); }
    }
    if (users.length) console.log(`[lead-hunt] ${day}: ${total} new leads for ${users.length} people`);
    return { day, people: users.length, found: total };
  } finally { running = false; }
}

module.exports = { huntForRep, huntAll, pickSegments, SEGMENT_CATEGORY, label };
