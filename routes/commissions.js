'use strict';

// ── Commission Import API (manager-only) ─────────────────────────────────────
// Thin HTTP layer over lib/commission-store + parser/matcher. Mounted in
// server.js behind requireAuth + requireManager. Upload mirrors zoho.js:
// the browser sends a base64 workbook as JSON (no multer).

const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const { parseCommissionReport } = require('../lib/commission-parser');
const store = require('../lib/commission-store');

// GET /api/commissions/reps — rep list for the picker when rep can't auto-resolve
router.get('/reps', async (req, res) => {
  try {
    const r = await pool.query('SELECT id, name, email FROM users ORDER BY name');
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/commissions/imports — recent imports (history)
router.get('/imports', async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT ci.*, u.name AS rep_resolved_name
         FROM commission_imports ci
         LEFT JOIN users u ON u.id = ci.rep_id
        ORDER BY ci.created_at DESC LIMIT 25`);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /api/commissions/import — parse + persist (pending_review) + return review
router.post('/import', async (req, res) => {
  try {
    const { file_b64, filename, format } = req.body || {};
    if (!file_b64) return res.status(400).json({ error: 'A commission file (file_b64) is required.' });

    let parsed;
    try {
      parsed = parseCommissionReport(file_b64, format || 'trilogy');
    } catch (e) {
      return res.status(400).json({ error: 'Could not parse the file: ' + e.message });
    }
    if (!parsed.lines.length) {
      return res.status(400).json({ error: 'No commission lines found. Is this a Trilogy Payment Detail Report?' });
    }

    const { import_id } = await store.insertImport(pool, {
      parsed, filename: filename || null, createdBy: req.session.user.id,
    });
    const review = await store.buildReview(pool, import_id);
    res.json({ import_id, review });
  } catch (e) {
    console.error('[commissions/import]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/commissions/imports/:id/review
router.get('/imports/:id/review', async (req, res) => {
  try {
    const review = await store.buildReview(pool, parseInt(req.params.id));
    res.json(review);
  } catch (e) {
    console.error('[commissions/review]', e.message);
    res.status(e.message === 'Import not found' ? 404 : 500).json({ error: e.message });
  }
});

// POST /api/commissions/imports/:id/confirm  { rep_id, decisions:[...] }
router.post('/imports/:id/confirm', async (req, res) => {
  try {
    const { rep_id, decisions } = req.body || {};
    const result = await store.confirmImport(pool, parseInt(req.params.id), { rep_id, decisions });
    const review = await store.buildReview(pool, parseInt(req.params.id));
    res.json({ ...result, review });
  } catch (e) {
    console.error('[commissions/confirm]', e.message);
    res.status(400).json({ error: e.message });
  }
});

// POST /api/commissions/imports/:id/discard
router.post('/imports/:id/discard', async (req, res) => {
  try {
    const result = await store.discardImport(pool, parseInt(req.params.id));
    res.json(result);
  } catch (e) {
    console.error('[commissions/discard]', e.message);
    res.status(e.message === 'Import not found' ? 404 : 500).json({ error: e.message });
  }
});

module.exports = router;
