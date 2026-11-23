// /api/leads: the New leads the overnight hunt found for this rep.
const express = require('express');
const { pool } = require('../db');
const hunt = require('../lib/lead-hunt');
const router = express.Router();

router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const r = await pool.query(
    `SELECT id, company, segment, category, address, city, state, phone, website, score, why, pitch, found_at
       FROM leads WHERE user_id=$1 AND status='new' ORDER BY score DESC, found_at DESC, id LIMIT 100`, [uid]);
  const last = (await pool.query(`SELECT run_day::text, found, note FROM lead_hunt_runs WHERE user_id=$1 ORDER BY run_day DESC LIMIT 1`, [uid])).rows[0] || null;
  const area = await require('../lib/work-area').workCenter(uid).catch(() => null);
  res.json({ leads: r.rows.map(l => Object.assign(l, { segment_label: hunt.label(l.segment) })), last_run: last,
    area: area ? { label: area.label, source: area.source } : null });
});

router.get('/count', async (req, res) => {
  const r = await pool.query(`SELECT COUNT(*)::int n FROM leads WHERE user_id=$1 AND status='new'`, [req.session.user.id]);
  res.json({ count: r.rows[0].n });
});

async function myLead(req) {
  return (await pool.query('SELECT * FROM leads WHERE id=$1 AND user_id=$2', [parseInt(req.params.id) || 0, req.session.user.id])).rows[0];
}

// Make it an account (once). Returns the account id.
async function saveLead(lead, uid) {
  if (lead.prospect_id) {
    const still = await pool.query('SELECT id FROM prospects WHERE id=$1', [lead.prospect_id]);
    if (still.rows.length) return lead.prospect_id;
  }
  const { insertProspect } = require('./prospects');
  const p = await insertProspect(uid, {
    company: lead.company, category: lead.category || 'Building Materials Distributor', city: lead.city, state: lead.state,
    address: lead.address, phone: lead.phone, website: lead.website, google_place_id: lead.place_id,
    products: lead.pitch, notes: lead.why, source: 'Lead Hunt', lat: lead.lat, lng: lead.lng
  });
  await pool.query(`UPDATE leads SET status='saved', prospect_id=$2, decided_at=NOW() WHERE id=$1`, [lead.id, p.id]);
  return p.id;
}

router.post('/:id/save', async (req, res) => {
  const lead = await myLead(req);
  if (!lead) return res.status(404).json({ error: 'Lead not found' });
  res.json({ prospect_id: await saveLead(lead, req.session.user.id) });
});

// Save it and put it on the next workday's plan (or the date given).
router.post('/:id/route', async (req, res) => {
  const uid = req.session.user.id;
  const lead = await myLead(req);
  if (!lead) return res.status(404).json({ error: 'Lead not found' });
  const pid = await saveLead(lead, uid);
  let day = (req.body || {}).date;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day || '')) {
    day = (await pool.query(`SELECT (d + CASE EXTRACT(DOW FROM d) WHEN 5 THEN 3 WHEN 6 THEN 2 ELSE 1 END)::date::text AS d
                               FROM (SELECT (NOW() AT TIME ZONE 'America/New_York')::date AS d) x`)).rows[0].d;
  }
  const dup = await pool.query('SELECT 1 FROM planner_items WHERE rep_id=$1 AND planned_date=$2 AND account_id=$3', [uid, day, pid]);
  if (!dup.rows.length) {
    const n = (await pool.query('SELECT COALESCE(MAX(sort_order), -1)::int + 1 AS n FROM planner_items WHERE rep_id=$1 AND planned_date=$2', [uid, day])).rows[0].n;
    await pool.query(`INSERT INTO planner_items (rep_id, planned_date, item_type, account_id, title, note, sort_order, source)
                      VALUES ($1,$2,'stop',$3,$4,$5,$6,'ai')`, [uid, day, pid, lead.company, 'New lead: ' + (lead.why || ''), n]);
  }
  await pool.query(`UPDATE leads SET status='routed' WHERE id=$1`, [lead.id]);
  res.json({ prospect_id: pid, date: day });
});

router.post('/:id/skip', async (req, res) => {
  const r = await pool.query(`UPDATE leads SET status='skipped', decided_at=NOW() WHERE id=$1 AND user_id=$2 AND status='new' RETURNING id`,
    [parseInt(req.params.id) || 0, req.session.user.id]);
  if (!r.rows.length) return res.status(404).json({ error: 'Lead not found' });
  res.json({ success: true });
});

// Search now instead of waiting for tonight. Every search is a Google
// charge, so reps get two of these a day; managers aren't limited.
const manualRuns = new Map();
router.post('/hunt', async (req, res) => {
  const me = req.session.user;
  if (!process.env.GOOGLE_PLACES_API_KEY) return res.status(503).json({ error: 'Lead search is not set up yet' });
  const seg = (req.body || {}).segment;
  const known = Object.keys(require('./morning').SEGMENT_SEARCH_CONFIG);
  if (seg && !known.includes(seg)) return res.status(400).json({ error: 'Unknown business type' });
  const day = new Date().toISOString().slice(0, 10);
  const used = manualRuns.get(me.id);
  const count = used && used.day === day ? used.n : 0;
  if (me.role !== 'manager' && count >= 2) return res.status(429).json({ error: 'That is today\'s searches. More leads come in tonight.' });
  manualRuns.set(me.id, { day, n: count + 1 });
  let routeDate = null;
  if ((req.body || {}).mode === 'route') {
    routeDate = (await pool.query(`SELECT (NOW() AT TIME ZONE 'America/New_York')::date::text d`)).rows[0].d;
    const stops = await pool.query('SELECT 1 FROM planner_items WHERE rep_id=$1 AND planned_date=$2 AND account_id IS NOT NULL LIMIT 1', [me.id, routeDate]);
    if (!stops.rows.length) return res.status(400).json({ error: 'Nothing on today\'s route yet. Plan your day first, or search near you.' });
  }
  let gps = null;
  if ((req.body || {}).mode === 'gps') {
    const lat = parseFloat(req.body.lat), lng = parseFloat(req.body.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return res.status(400).json({ error: 'Couldn\'t get your location. Allow location for RepRoute, or search your territory.' });
    gps = { lat, lng, label: 'where you are' };
  }
  res.json(await hunt.huntForRep(me, { segments: seg ? [seg] : undefined, perRun: 1, routeDate, gps, max: 15 }));
});

module.exports = router;
