const express = require('express');
const { pool } = require('../db');
const router = express.Router();

// ── JohnMark-only guard ──────────────────────────────────────────
const JM_EMAIL = 'johnmarkcompton@gmail.com';
const HOURLY_RATE = 50; // USD per hour — billed to Compton Sales

function requireJM(req, res, next) {
  if (!req.session.user || req.session.user.email !== JM_EMAIL) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  next();
}

// ── GET /api/time/sessions?month=&year= ──────────────────────────
router.get('/sessions', requireJM, async (req, res) => {
  const uid = req.session.user.id;
  const month = parseInt(req.query.month) || new Date().getMonth() + 1;
  const year  = parseInt(req.query.year)  || new Date().getFullYear();
  try {
    const result = await pool.query(
      `SELECT * FROM time_sessions
       WHERE user_id = $1
         AND EXTRACT(MONTH FROM start_time) = $2
         AND EXTRACT(YEAR  FROM start_time) = $3
       ORDER BY start_time DESC`,
      [uid, month, year]
    );
    res.json(result.rows);
  } catch (e) {
    console.error('[time/sessions]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/time/active ─────────────────────────────────────────
// Returns any open session (end_time IS NULL)
router.get('/active', requireJM, async (req, res) => {
  const uid = req.session.user.id;
  try {
    const result = await pool.query(
      `SELECT * FROM time_sessions
       WHERE user_id = $1 AND end_time IS NULL
       ORDER BY start_time DESC LIMIT 1`,
      [uid]
    );
    res.json(result.rows[0] || null);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/time/sessions/start ────────────────────────────────
router.post('/sessions/start', requireJM, async (req, res) => {
  const uid = req.session.user.id;
  try {
    // Close any orphaned open sessions before starting a new one
    await pool.query(
      `UPDATE time_sessions
       SET end_time = NOW(),
           duration_minutes = GREATEST(1, ROUND(EXTRACT(EPOCH FROM (NOW() - start_time))/60)::integer)
       WHERE user_id = $1 AND end_time IS NULL`,
      [uid]
    );
    const result = await pool.query(
      `INSERT INTO time_sessions (user_id, start_time)
       VALUES ($1, NOW()) RETURNING *`,
      [uid]
    );
    res.json(result.rows[0]);
  } catch (e) {
    console.error('[time/start]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── PUT /api/time/sessions/:id/stop ─────────────────────────────
router.put('/sessions/:id/stop', requireJM, async (req, res) => {
  const uid = req.session.user.id;
  const id  = req.params.id;
  const { description } = req.body;
  try {
    const result = await pool.query(
      `UPDATE time_sessions
       SET end_time = NOW(),
           duration_minutes = GREATEST(1, ROUND(EXTRACT(EPOCH FROM (NOW() - start_time))/60)::integer),
           description = $1
       WHERE id = $2 AND user_id = $3 AND end_time IS NULL
       RETURNING *`,
      [description || '', id, uid]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Session not found or already stopped' });
    res.json(result.rows[0]);
  } catch (e) {
    console.error('[time/stop]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── DELETE /api/time/sessions/:id ───────────────────────────────
router.delete('/sessions/:id', requireJM, async (req, res) => {
  const uid = req.session.user.id;
  try {
    await pool.query(
      'DELETE FROM time_sessions WHERE id = $1 AND user_id = $2',
      [req.params.id, uid]
    );
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/time/summary?month=&year= ──────────────────────────
router.get('/summary', requireJM, async (req, res) => {
  const uid = req.session.user.id;
  const month = parseInt(req.query.month) || new Date().getMonth() + 1;
  const year  = parseInt(req.query.year)  || new Date().getFullYear();
  try {
    const result = await pool.query(
      `SELECT
         COUNT(*) FILTER (WHERE end_time IS NOT NULL) as session_count,
         COALESCE(SUM(duration_minutes) FILTER (WHERE end_time IS NOT NULL), 0) as total_minutes
       FROM time_sessions
       WHERE user_id = $1
         AND EXTRACT(MONTH FROM start_time) = $2
         AND EXTRACT(YEAR  FROM start_time) = $3`,
      [uid, month, year]
    );
    const row = result.rows[0];
    const totalMinutes = parseInt(row.total_minutes) || 0;
    const totalHours   = totalMinutes / 60;
    const billable     = totalHours * HOURLY_RATE;
    res.json({
      session_count:  parseInt(row.session_count) || 0,
      total_minutes:  totalMinutes,
      total_hours:    Math.round(totalHours * 10) / 10,
      billable_amount: Math.round(billable * 100) / 100,
      hourly_rate:    HOURLY_RATE
    });
  } catch (e) {
    console.error('[time/summary]', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
