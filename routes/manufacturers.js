'use strict';

// ── Manufacturers view API ───────────────────────────────────────────────────
// Read-only, derived from lines + account_lines + prospects. Powers Keith's
// "accounts by manufacturer (line) → territory" mental model. Scope: a manager
// sees every account firm-wide; a rep sees only their own (prospects.user_id).
// No schema change — everything is rolled up from tables the commission import
// already populates.

const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const linesStore = require('../lib/lines-store');

// scopeUid(req) → user_id when the caller is a rep, null when a manager (= all).
function scopeUid(req) {
  return req.session.user.role === 'manager' ? null : req.session.user.id;
}

// GET /api/manufacturers — summary cards, sorted by sales.
router.get('/', async (req, res) => {
  try {
    res.json(await linesStore.manufacturersSummary(pool, scopeUid(req)));
  } catch (e) {
    console.error('[manufacturers]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/manufacturers/:id — one manufacturer's accounts grouped by territory.
router.get('/:id', async (req, res) => {
  try {
    const lineId = parseInt(req.params.id);
    if (!Number.isFinite(lineId)) return res.status(400).json({ error: 'Invalid manufacturer id.' });
    const detail = await linesStore.manufacturerDetail(pool, lineId, scopeUid(req));
    if (!detail) return res.status(404).json({ error: 'Manufacturer not found.' });
    res.json(detail);
  } catch (e) {
    console.error('[manufacturers/:id]', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
