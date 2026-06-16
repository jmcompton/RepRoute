'use strict';

// ── Fortress Railing Promo ───────────────────────────────────────────────────
// A self-contained, pre-routed dealer-visit campaign (separate from Accounts).
// Stops are seeded from fortress_promo_routes.csv (see db.js). This route only
// reads stops and records visits — it never re-optimizes or re-orders.

const express = require('express');
const router = express.Router();
const { pool } = require('../db');

// GET /api/fortress/stops[?rep=Kody]
// Returns campaign stops, ordered rep → day → stop_order. Optional rep filter.
router.get('/stops', async (req, res) => {
  try {
    const rep = (req.query.rep || '').trim();
    const params = [];
    let where = '';
    if (rep && rep.toLowerCase() !== 'all') { params.push(rep); where = 'WHERE rep = $1'; }
    const r = await pool.query(
      `SELECT id, rep, day, stop_order, company, address, city, zip, phone, status, source,
              lat, lng, visited_at, outcome, notes
         FROM fortress_promo_stops
         ${where}
        ORDER BY rep ASC, day ASC, stop_order ASC, id ASC`,
      params);
    res.json(r.rows);
  } catch (e) {
    console.error('[fortress/stops]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/fortress/stops/:id/visit  { outcome, notes, visited? }
// Log a visit on a stop. Sets visited_at = NOW() (or clears it when visited:false),
// plus outcome and notes. Returns the updated row.
router.post('/stops/:id/visit', async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'invalid id' });
    const body = req.body || {};
    const outcome = body.outcome != null ? String(body.outcome).trim() : null;
    const notes = body.notes != null ? String(body.notes).trim() : null;
    const unvisit = body.visited === false; // allow undo
    const r = await pool.query(
      `UPDATE fortress_promo_stops
          SET visited_at = ${unvisit ? 'NULL' : 'NOW()'},
              outcome    = $1,
              notes      = $2
        WHERE id = $3
        RETURNING id, rep, day, stop_order, company, address, city, zip, phone, status, source,
                  lat, lng, visited_at, outcome, notes`,
      [unvisit ? null : (outcome || null), notes || null, id]);
    if (!r.rows.length) return res.status(404).json({ error: 'Stop not found' });
    res.json(r.rows[0]);
  } catch (e) {
    console.error('[fortress/visit]', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
