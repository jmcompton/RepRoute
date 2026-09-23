const express = require('express');
const { pool } = require('../db');
const router = express.Router();

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
    res.json({ ok: true, updated_at: r.rows[0].updated_at });
  } catch (err) {
    console.error('[route-stops] PUT error:', err.message);
    res.status(500).json({ error: 'Failed to save route' });
  }
});

module.exports = router;
