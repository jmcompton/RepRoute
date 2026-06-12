'use strict';

// ── Commission PDF importer API (requireAuthAPI) ─────────────────────────────
// Two endpoints over the Trilogy PDF parser:
//   POST /api/commission-pdf/preview  — parse only, return summary + reconcile
//                                        result. Writes NOTHING.
//   POST /api/commission-pdf/commit   — re-parse the uploaded PDF and write it.
// Both take a multipart upload (field name "file"). A manager may import for any
// rep (rep_id override); a rep imports their own (the report names the rep, and
// resolveReportRep matches it to a user).

const express = require('express');
const multer = require('multer');
const router = express.Router();
const { pool } = require('../db');
const { parsePdfCommissionReport } = require('../lib/commission-pdf-parser');
const { commitPdfImport } = require('../lib/commission-pdf-store');

// In-memory upload, 25 MB cap, PDFs only.
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = /pdf$/i.test(file.mimetype) || /\.pdf$/i.test(file.originalname || '');
    cb(ok ? null : new Error('Please upload a PDF (.pdf) commission report.'), ok);
  },
});

// Shared summary shape so preview and commit responses agree.
function summarize(parsed, filename) {
  return {
    filename: filename || null,
    rep_name: parsed.rep_name,
    period: parsed.period,
    period_start: parsed.period_start,
    period_end: parsed.period_end,
    date_range: parsed.date_range,
    manufacturers: parsed.manufacturers,
    counts: parsed.counts,
    grand_totals: parsed.grand_totals,
    reconciliation: parsed.reconciliation,
  };
}

// POST /api/commission-pdf/preview
router.post('/preview', upload.single('file'), async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) return res.status(400).json({ error: 'No PDF uploaded (field "file").' });
    let parsed;
    try {
      parsed = await parsePdfCommissionReport(req.file.buffer);
    } catch (e) {
      return res.status(400).json({ error: 'Could not parse the PDF: ' + e.message });
    }
    res.json({ preview: summarize(parsed, req.file.originalname) });
  } catch (e) {
    console.error('[commission-pdf/preview]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/commission-pdf/commit  (optional rep_id override in body, multipart)
router.post('/commit', upload.single('file'), async (req, res) => {
  try {
    if (!req.file || !req.file.buffer) return res.status(400).json({ error: 'No PDF uploaded (field "file").' });

    let parsed;
    try {
      parsed = await parsePdfCommissionReport(req.file.buffer);
    } catch (e) {
      return res.status(400).json({ error: 'Could not parse the PDF: ' + e.message });
    }
    if (!parsed.reconciliation.reconciles) {
      return res.status(400).json({
        error: 'The report did not reconcile to its printed totals — import blocked.',
        mismatches: parsed.reconciliation.mismatches,
      });
    }

    // Only a manager may import on behalf of another rep.
    const isMgr = req.session.user.role === 'manager';
    const repId = (isMgr && req.body && req.body.rep_id) ? parseInt(req.body.rep_id) : null;

    let result;
    try {
      result = await commitPdfImport(pool, {
        parsed,
        filename: req.file.originalname,
        createdBy: req.session.user.id,
        repId,
      });
    } catch (e) {
      return res.status(400).json({ error: e.message });
    }
    res.json(result);
  } catch (e) {
    console.error('[commission-pdf/commit]', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
