// /api/today: the new Today screen. The day's plan, what got done overnight,
// what's waiting on the rep, and who's due for a visit.
const express = require('express');
const { pool } = require('../db');
const plan = require('../lib/day-plan');
const router = express.Router();

async function dayOf(req) {
  const d = (req.query && req.query.date) || (req.body && req.body.date);
  if (/^\d{4}-\d{2}-\d{2}$/.test(d || '')) return d;
  return (await pool.query(`SELECT (NOW() AT TIME ZONE 'America/New_York')::date::text d`)).rows[0].d;
}
function gps(b) {
  const lat = parseFloat(b && b.lat), lng = parseFloat(b && b.lng);
  return Number.isFinite(lat) && Number.isFinite(lng) && Math.abs(lat) <= 90 && Math.abs(lng) <= 180 ? { lat, lng } : null;
}

router.get('/summary', async (req, res) => {
  const uid = req.session.user.id;
  const day = await dayOf(req);
  const onRoute = new Set((await pool.query(`SELECT account_id FROM planner_items WHERE rep_id=$1 AND planned_date=$2 AND account_id IS NOT NULL`, [uid, day])).rows.map(r => r.account_id));
  const cands = await plan.candidates(uid, day);
  const waiting = cands.filter(c => ['followup', 'quote', 'sample'].includes(c.kind)).slice(0, 12).map(c => Object.assign({ on_route: onRoute.has(c.account_id) }, c));
  const due = cands.filter(c => c.kind === 'touch' && !onRoute.has(c.account_id)).slice(0, 8);
  const [aiStops, leads, run, week] = await Promise.all([
    pool.query(`SELECT COUNT(*)::int n FROM planner_items WHERE rep_id=$1 AND planned_date=$2 AND source='ai'`, [uid, day]),
    pool.query(`SELECT COUNT(*)::int n, COUNT(*) FILTER (WHERE found_at > NOW() - INTERVAL '24 hours')::int fresh FROM leads WHERE user_id=$1 AND status='new'`, [uid]),
    pool.query(`SELECT miles::float FROM day_plan_runs WHERE user_id=$1 AND run_day=$2`, [uid, day]),
    pool.query(`SELECT
        (SELECT COUNT(*)::int FROM calls WHERE user_id=$1 AND call_date >= date_trunc('week', $2::date) AND call_date <= $2::date AND COALESCE(call_type,'') <> 'Note') AS visits,
        (SELECT COUNT(*)::int FROM samples WHERE user_id=$1 AND closed_at IS NULL AND LOWER(COALESCE(status,'pending')) NOT IN ('converted','no_interest','no interest')) AS samples_out,
        (SELECT COALESCE(SUM(amount),0)::float FROM quotes WHERE (user_id=$1 OR rep_id=$1) AND status NOT IN ('Won','Lost')) AS open_quotes`, [uid, day])
  ]);
  const name = (req.session.user.name || '').split(' ')[0] || '';
  res.json({
    day, name,
    overnight: { stops: aiStops.rows[0].n, leads: leads.rows[0].n, fresh_leads: leads.rows[0].fresh, due: cands.filter(c => c.kind === 'touch').length, miles: run.rows[0] ? run.rows[0].miles : null },
    waiting, due, week: week.rows[0]
  });
});

// Build or top up the day, then put it in driving order.
router.post('/plan', async (req, res) => {
  const day = await dayOf(req);
  res.json(await plan.planDay(req.session.user.id, day, { goal: (req.body || {}).goal, gps: gps(req.body) }));
});

// Just reorder the day's open stops for the shortest drive.
router.post('/optimize', async (req, res) => {
  const day = await dayOf(req);
  const start = gps(req.body) ? Object.assign({ label: 'your location' }, gps(req.body)) : null;
  res.json(await plan.optimizeDay(req.session.user.id, day, start));
});

// Put one account on the day (from "Due for a visit" or "Waiting on you").
router.post('/add', async (req, res) => {
  const uid = req.session.user.id;
  const day = await dayOf(req);
  const aid = parseInt((req.body || {}).account_id) || 0;
  const a = (await pool.query(`SELECT p.id, p.company FROM prospects p JOIN users u ON u.id=p.user_id WHERE p.id=$1 AND u.disabled_at IS NULL`, [aid])).rows[0];
  if (!a) return res.status(404).json({ error: 'Account not found' });
  const dup = await pool.query('SELECT id FROM planner_items WHERE rep_id=$1 AND planned_date=$2 AND account_id=$3', [uid, day, aid]);
  if (!dup.rows.length) {
    const n = (await pool.query('SELECT COALESCE(MAX(sort_order), -1)::int + 1 AS n FROM planner_items WHERE rep_id=$1 AND planned_date=$2', [uid, day])).rows[0].n;
    await pool.query(`INSERT INTO planner_items (rep_id, planned_date, item_type, account_id, title, sort_order, source, ai_reason) VALUES ($1,$2,'stop',$3,$4,$5,'manual',$6)`,
      [uid, day, aid, a.company, n, String((req.body || {}).reason || '').slice(0, 200) || null]);
  }
  res.json(await plan.optimizeDay(uid, day, gps(req.body) ? Object.assign({ label: 'your location' }, gps(req.body)) : null));
});

module.exports = router;
