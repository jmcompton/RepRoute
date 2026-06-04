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

// ── ensureSeedSession ────────────────────────────────────────────
// Called from server.js AFTER initDB() resolves so the table is
// guaranteed to exist. Inserts the June 4 2026 seed session if
// missing, then logs a COUNT(*) verification.
async function ensureSeedSession() {
  console.log('[time] ensureSeedSession starting…');
  try {
    // 1. Resolve JohnMark's user_id
    const userRow = await pool.query(
      'SELECT id FROM users WHERE email = $1', [JM_EMAIL]
    );
    if (!userRow.rows.length) {
      console.log('[time] Seed skip — user not found for email:', JM_EMAIL);
      return;
    }
    const uid = userRow.rows[0].id;
    console.log('[time] JohnMark user_id =', uid);

    // 2. Insert seed session; unique constraint prevents duplicates
    const ins = await pool.query(
      `INSERT INTO time_sessions
         (user_id, start_time, end_time, duration_minutes, description)
       VALUES ($1,
               '2026-06-04 09:00:00+00'::timestamptz,
               '2026-06-04 13:00:00+00'::timestamptz,
               240,
               $2)
       ON CONFLICT (user_id, start_time) DO NOTHING
       RETURNING id, duration_minutes, start_time`,
      [uid, 'Voice call logger, business card scanner, contact detail page, mobile optimization, invoice generation, board meeting PDF']
    );
    if (ins.rows.length) {
      console.log('[time] Seed session inserted — id=' + ins.rows[0].id +
                  ', start=' + ins.rows[0].start_time);
    } else {
      console.log('[time] Seed session already existed (ON CONFLICT DO NOTHING)');
    }

    // 3. Startup verification — count all sessions and log the June 2026 total
    const countAll = await pool.query(
      'SELECT COUNT(*) as total FROM time_sessions WHERE user_id = $1', [uid]
    );
    console.log('[time] time_sessions total for user_id=' + uid + ':', countAll.rows[0].total);

    const countJune = await pool.query(
      `SELECT COUNT(*) as cnt, COALESCE(SUM(duration_minutes),0) as mins
       FROM time_sessions
       WHERE user_id = $1
         AND EXTRACT(YEAR  FROM start_time AT TIME ZONE 'UTC') = 2026
         AND EXTRACT(MONTH FROM start_time AT TIME ZONE 'UTC') = 6
         AND end_time IS NOT NULL`,
      [uid]
    );
    console.log('[time] June 2026 sessions:', countJune.rows[0].cnt,
                '| total_minutes:', countJune.rows[0].mins);
  } catch (e) {
    console.error('[time] ensureSeedSession error:', e.message);
  }
}

// ── GET /api/time/sessions?month=&year= ──────────────────────────
router.get('/sessions', requireJM, async (req, res) => {
  const uid   = req.session.user.id;
  const month = parseInt(req.query.month) || new Date().getMonth() + 1;
  const year  = parseInt(req.query.year)  || new Date().getFullYear();
  console.log('[time/sessions] uid=%d month=%d year=%d', uid, month, year);
  try {
    const result = await pool.query(
      `SELECT * FROM time_sessions
       WHERE user_id = $1
         AND EXTRACT(YEAR  FROM start_time AT TIME ZONE 'UTC') = $3
         AND EXTRACT(MONTH FROM start_time AT TIME ZONE 'UTC') = $2
         AND end_time IS NOT NULL
       ORDER BY start_time DESC`,
      [uid, month, year]
    );
    console.log('[time/sessions] rows returned:', result.rows.length);
    res.json(result.rows);
  } catch (e) {
    console.error('[time/sessions] error:', e.message);
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
  const uid   = req.session.user.id;
  const month = parseInt(req.query.month) || new Date().getMonth() + 1;
  const year  = parseInt(req.query.year)  || new Date().getFullYear();
  console.log('[time/summary] uid=%d month=%d year=%d', uid, month, year);
  try {
    const result = await pool.query(
      `SELECT
         COUNT(*) FILTER (WHERE end_time IS NOT NULL) AS session_count,
         COALESCE(SUM(duration_minutes) FILTER (WHERE end_time IS NOT NULL), 0) AS total_minutes
       FROM time_sessions
       WHERE user_id = $1
         AND EXTRACT(YEAR  FROM start_time AT TIME ZONE 'UTC') = $3
         AND EXTRACT(MONTH FROM start_time AT TIME ZONE 'UTC') = $2`,
      [uid, month, year]
    );
    const row = result.rows[0];
    console.log('[time/summary] raw row:', JSON.stringify(row));
    const totalMinutes = parseInt(row.total_minutes) || 0;
    const totalHours   = totalMinutes / 60;
    const billable     = totalHours * HOURLY_RATE;
    const response = {
      session_count:   parseInt(row.session_count) || 0,
      total_minutes:   totalMinutes,
      total_hours:     Math.round(totalHours * 10) / 10,
      billable_amount: Math.round(billable * 100) / 100,
      hourly_rate:     HOURLY_RATE
    };
    console.log('[time/summary] response:', JSON.stringify(response));
    res.json(response);
  } catch (e) {
    console.error('[time/summary] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = { router, ensureSeedSession };
