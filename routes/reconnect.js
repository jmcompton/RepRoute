'use strict';

// ── Reconnect API (requireAuthAPI) ───────────────────────────────────────────
// GET  /api/reconnect                  → flagged, value-ranked lapsed accounts.
// POST /api/reconnect/:id/touch         → mark contacted now (resets last_activity,
//                                         drops it off the list).
// POST /api/reconnect/:id/snooze        → hide for config.snoozeDays, then it returns.
// POST /api/reconnect/:id/dismiss        → stop tracking (not a real account).
// Rep scoping: a rep only sees/acts on their own accounts; a manager sees firm-wide
// and may filter by rep. Snooze/dismiss are honored in the read query.

const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const { getReconnect, RECONNECT_CONFIG } = require('../lib/reconnect-store');

function isMgr(req) { return req.session.user.role === 'manager'; }

// GET /api/reconnect?filter=customers|leads|all&rep_id=123
router.get('/', async (req, res) => {
  try {
    const out = await getReconnect(pool, {
      uid: req.session.user.id,
      scope: isMgr(req) ? 'manager' : 'rep',
      repId: req.query.rep_id,
      filter: req.query.filter,
    });
    res.json(out);
  } catch (e) {
    console.error('[reconnect]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Resolve the account, enforcing that a rep can only act on their own.
async function loadOwned(req, id) {
  const r = await pool.query('SELECT id, user_id FROM prospects WHERE id=$1', [id]);
  if (!r.rows.length) return { error: 404 };
  if (!isMgr(req) && r.rows[0].user_id !== req.session.user.id) return { error: 403 };
  return { row: r.rows[0] };
}

// POST /api/reconnect/:id/touch — stamp last_activity_at = now; also un-snooze.
router.post('/:id/touch', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id.' });
    const owned = await loadOwned(req, id);
    if (owned.error) return res.status(owned.error).json({ error: owned.error === 404 ? 'Account not found.' : 'Not your account.' });
    await pool.query(
      'UPDATE prospects SET last_activity_at = NOW(), reconnect_snoozed_until = NULL WHERE id=$1', [id]);
    res.json({ ok: true, id, last_activity: new Date().toISOString() });
  } catch (e) {
    console.error('[reconnect/touch]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/reconnect/:id/snooze — hide for config.snoozeDays.
router.post('/:id/snooze', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id.' });
    const owned = await loadOwned(req, id);
    if (owned.error) return res.status(owned.error).json({ error: owned.error === 404 ? 'Account not found.' : 'Not your account.' });
    const days = RECONNECT_CONFIG.snoozeDays;
    const r = await pool.query(
      `UPDATE prospects SET reconnect_snoozed_until = NOW() + ($2 || ' days')::interval
         WHERE id=$1 RETURNING reconnect_snoozed_until`, [id, String(days)]);
    res.json({ ok: true, id, snoozed_until: r.rows[0].reconnect_snoozed_until, days });
  } catch (e) {
    console.error('[reconnect/snooze]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/reconnect/:id/dismiss — stop tracking entirely.
router.post('/:id/dismiss', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id.' });
    const owned = await loadOwned(req, id);
    if (owned.error) return res.status(owned.error).json({ error: owned.error === 404 ? 'Account not found.' : 'Not your account.' });
    await pool.query('UPDATE prospects SET reconnect_dismissed = TRUE WHERE id=$1', [id]);
    res.json({ ok: true, id });
  } catch (e) {
    console.error('[reconnect/dismiss]', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
