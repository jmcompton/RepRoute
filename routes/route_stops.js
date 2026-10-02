const express = require('express');
const { pool } = require('../db');
const router = express.Router();
const { findMatchingAccount } = require('./prospects');

// ════════════════════════════════════════════════════════════════
// ROUTE STOPS — the rep's working "Today's Route" list (+ Route).
// Stored server-side, one row per user, so a route built on desktop
// shows up on the phone (and vice versa). Previously this lived only
// in the browser's localStorage, which never leaves the device.
// ════════════════════════════════════════════════════════════════

const MAX_STOPS = 20;

// ── GET /api/route-stops ─────────────────────────────────────────
router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  try {
    const r = await pool.query('SELECT stops, updated_at FROM route_stops WHERE user_id=$1', [uid]);
    res.set('Cache-Control', 'no-store');
    if (!r.rows.length) return res.json({ stops: [], updated_at: null });
    res.json({ stops: r.rows[0].stops || [], updated_at: r.rows[0].updated_at });
  } catch (err) {
    console.error('[route-stops] GET error:', err.message);
    res.status(500).json({ error: 'Failed to load route' });
  }
});

// ── Today's Route → Weekly Planner ───────────────────────────────
// Every route stop is mirrored as a planner stop on the rep's local "today"
// (source='route', route_key = the stop's identity). A stop already planned that
// day (any source) is skipped. Route-added planner stops whose route stop is gone
// are removed — manual / AI planner stops are never touched.
function normName(s) { return String(s || '').toLowerCase().replace(/[^a-z0-9]/g, ''); }

async function resolveStopAccount(uid, stop) {
  const id = parseInt(stop.id);
  if (Number.isFinite(id) && id > 0) {
    const r = await pool.query('SELECT id FROM prospects WHERE id=$1 AND user_id=$2', [id, uid]);
    if (r.rows.length) return id;
  }
  const m = await findMatchingAccount(uid, { phone: stop.phone, company: stop.company, city: stop.city });
  return m ? m.account.id : null;
}

async function syncRouteToPlanner(uid, date, stops) {
  const existing = (await pool.query(
    `SELECT id, account_id, title, source, route_key, sort_order FROM planner_items
      WHERE rep_id=$1 AND planned_date=$2 AND item_type='stop'`,
    [uid, date]
  )).rows;
  const keep = new Set();
  let order = existing.reduce((mx, r) => Math.max(mx, r.sort_order || 0), 0);
  let added = 0, removed = 0;

  for (const stop of stops) {
    if (!stop || !String(stop.company || '').trim()) continue;
    const accountId = await resolveStopAccount(uid, stop);
    const key = accountId ? 'a:' + accountId : (stop.place_id ? 'p:' + stop.place_id : 'n:' + normName(stop.company));
    keep.add(key);
    const hit = existing.find(r =>
      r.route_key === key ||
      (accountId && r.account_id === accountId) ||
      (!r.account_id && normName(r.title) === normName(stop.company)));
    if (hit) {
      // Same stop, new identity (e.g. it gained a CRM account after a call was
      // logged) — re-key the route item so it isn't treated as removed below.
      if (hit.id && hit.source === 'route' && hit.route_key !== key) {
        await pool.query(
          'UPDATE planner_items SET route_key=$1, account_id=COALESCE(account_id, $2) WHERE id=$3',
          [key, accountId, hit.id]);
        hit.route_key = key;
        if (accountId && !hit.account_id) hit.account_id = accountId;
      }
      continue;
    }
    await pool.query(
      `INSERT INTO planner_items (rep_id, planned_date, item_type, account_id, title, note, sort_order, source, route_key)
       VALUES ($1,$2,'stop',$3,$4,$5,$6,'route',$7)`,
      [uid, date, accountId, accountId ? null : String(stop.company).trim().slice(0, 200),
       stop.address ? String(stop.address).slice(0, 300) : null, ++order, key]
    );
    existing.push({ account_id: accountId, title: stop.company, route_key: key, source: 'route' });
    added++;
  }

  const stale = existing.filter(r => r.id && r.source === 'route' && r.route_key && !keep.has(r.route_key));
  if (stale.length) {
    const del = await pool.query(
      `DELETE FROM planner_items WHERE rep_id=$1 AND id = ANY($2::int[]) AND source='route'`,
      [uid, stale.map(r => r.id)]
    );
    removed = del.rowCount;
  }
  return { added, removed };
}

// ── PUT /api/route-stops ─────────────────────────────────────────
// Replaces the whole list (last write wins). Body: { stops: [...] }
router.put('/', async (req, res) => {
  const uid = req.session.user.id;
  const stops = req.body && req.body.stops;
  if (!Array.isArray(stops)) return res.status(400).json({ error: 'stops must be an array' });
  if (stops.length > MAX_STOPS) return res.status(400).json({ error: 'Route full (' + MAX_STOPS + ' stops max)' });
  try {
    const r = await pool.query(
      `INSERT INTO route_stops (user_id, stops, updated_at)
       VALUES ($1, $2::jsonb, NOW())
       ON CONFLICT (user_id) DO UPDATE SET stops = EXCLUDED.stops, updated_at = NOW()
       RETURNING updated_at`,
      [uid, JSON.stringify(stops)]
    );
    // planner_date = the device's local today; only then is the planner synced
    // (an old client that doesn't send it just saves the route).
    let planner = null;
    const pd = req.body && req.body.planner_date;
    if (/^\d{4}-\d{2}-\d{2}$/.test(pd || '')) {
      try { planner = await syncRouteToPlanner(uid, pd, stops); }
      catch (e) { console.error('[route-stops] planner sync error:', e.message); }
    }
    res.json({ ok: true, updated_at: r.rows[0].updated_at, planner });
  } catch (err) {
    console.error('[route-stops] PUT error:', err.message);
    res.status(500).json({ error: 'Failed to save route' });
  }
});

module.exports = router;
