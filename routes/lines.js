'use strict';

// ── Manufacturer "lines" API (manager-only) ──────────────────────────────────
// Thin HTTP layer over lib/lines-store. Mounted in server.js behind
// requireAuth + requireManager. account_id = prospects(id).

const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const linesStore = require('../lib/lines-store');

// GET /api/lines — all lines with revenue rollup (sorted by sales desc).
router.get('/', async (req, res) => {
  try {
    res.json(await linesStore.linesWithRollup(pool));
  } catch (e) {
    console.error('[lines]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/lines/account/:id — the lines a given account buys (account_lines).
router.get('/account/:id', async (req, res) => {
  try {
    const accountId = parseInt(req.params.id);
    if (!Number.isFinite(accountId)) return res.status(400).json({ error: 'Invalid account id.' });
    res.json(await linesStore.linesForAccount(pool, accountId));
  } catch (e) {
    console.error('[lines/account]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/lines/backfill — (re)resolve + rebuild across all confirmed lines.
router.post('/backfill', async (req, res) => {
  try {
    res.json(await linesStore.backfillAllLines(pool));
  } catch (e) {
    console.error('[lines/backfill]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/lines/merge { from_line_id, into_line_id } — roll one line into another.
router.post('/merge', async (req, res) => {
  try {
    const { from_line_id, into_line_id } = req.body || {};
    res.json(await linesStore.mergeLines(pool, from_line_id, into_line_id));
  } catch (e) {
    console.error('[lines/merge]', e.message);
    res.status(400).json({ error: e.message });
  }
});

module.exports = router;
