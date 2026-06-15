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
const { enrichByPlacesTextSearch } = require('../lib/places');
const { rebuildAccountLines } = require('../lib/lines-store');

// Resolve which rep's accounts an action targets: a manager may pass rep_id to
// act on another rep's accounts; everyone else acts on their own.
function targetRepId(req) {
  const isMgr = req.session.user.role === 'manager';
  return (isMgr && req.body && req.body.rep_id) ? parseInt(req.body.rep_id) : req.session.user.id;
}

// Can the current user act on this account? Owner or any manager.
async function ownsAccount(req, accountId) {
  const r = await pool.query('SELECT user_id FROM prospects WHERE id=$1', [accountId]);
  if (!r.rows.length) return false;
  return req.session.user.role === 'manager' || r.rows[0].user_id === req.session.user.id;
}

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
        reasons: parsed.reconciliation.reasons,
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

// POST /api/commission-pdf/enrich-missing
// "Find missing info": drain the needs_info queue for the rep, looking up each
// account via Google Places Text Search. Batched (15/call) + idempotent — each
// account is stamped enrich_attempted_at and leaves the queue, so the frontend
// loop terminates. Re-eligible after 30 days. Returns per-batch outcome counts.
const ENRICH_QUEUE_WHERE =
  `contact_status='needs_info'
   AND (enrich_attempted_at IS NULL OR enrich_attempted_at < NOW() - INTERVAL '30 days')`;

router.post('/enrich-missing', async (req, res) => {
  const targetUid = targetRepId(req);
  try {
    const batch = await pool.query(
      `SELECT id FROM prospects WHERE user_id=$1 AND ${ENRICH_QUEUE_WHERE} ORDER BY id ASC LIMIT 15`,
      [targetUid]);

    let ai_found = 0, needs_review = 0, none = 0;
    for (const row of batch.rows) {
      const r = await enrichByPlacesTextSearch(pool, row.id);
      if (r.result === 'ai_found') ai_found++;
      else if (r.result === 'needs_review') needs_review++;
      else if (r.result === 'none') none++;
    }

    const remR = await pool.query(
      `SELECT COUNT(*)::int AS c FROM prospects WHERE user_id=$1 AND ${ENRICH_QUEUE_WHERE}`,
      [targetUid]);

    res.json({ processed: batch.rows.length, ai_found, needs_review, none, remaining: remR.rows[0].c });
  } catch (e) {
    console.error('[commission-pdf/enrich-missing]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/commission-pdf/review-match  { account_id, action: 'confirm'|'reject' }
// Resolve a medium-confidence "review match":
//   confirm – the new account IS the candidate: move its commission lines onto the
//             candidate, rebuild both rollups, delete the now-empty new account.
//   reject  – keep the new account as its own; clear the review flag.
router.post('/review-match', async (req, res) => {
  const accountId = parseInt(req.body && req.body.account_id);
  const action = req.body && req.body.action;
  if (!Number.isFinite(accountId)) return res.status(400).json({ error: 'account_id required' });
  if (action !== 'confirm' && action !== 'reject') return res.status(400).json({ error: 'action must be confirm or reject' });
  if (!(await ownsAccount(req, accountId))) return res.status(403).json({ error: 'Not your account.' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const a = await client.query(
      'SELECT id, match_review_candidate_id FROM prospects WHERE id=$1 FOR UPDATE', [accountId]);
    if (!a.rows.length) throw new Error('Account not found');
    const candidateId = a.rows[0].match_review_candidate_id;

    if (action === 'reject') {
      await client.query(
        `UPDATE prospects SET match_review_candidate=NULL, match_review_candidate_id=NULL WHERE id=$1`,
        [accountId]);
      await client.query(
        `UPDATE commission_lines SET match_status='new' WHERE account_id=$1 AND match_status='review'`,
        [accountId]);
      await client.query('COMMIT');
      return res.json({ ok: true, action: 'reject', account_id: accountId });
    }

    // confirm → merge into the candidate existing account.
    if (!Number.isFinite(candidateId)) throw new Error('No candidate account to merge into.');
    await client.query(
      `UPDATE commission_lines SET account_id=$1, match_status='matched' WHERE account_id=$2`,
      [candidateId, accountId]);
    await rebuildAccountLines(client, candidateId);
    await rebuildAccountLines(client, accountId); // now empty → rollup cleared
    // Remove the now-empty duplicate account.
    await client.query('DELETE FROM prospects WHERE id=$1', [accountId]);
    await client.query('COMMIT');
    res.json({ ok: true, action: 'confirm', account_id: accountId, merged_into: candidateId });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[commission-pdf/review-match]', e.message);
    res.status(400).json({ error: e.message });
  } finally {
    client.release();
  }
});

// POST /api/commission-pdf/verify-contact  { account_id }
// A human confirmed the AI-found phone/address → flip the amber "verify" badge to
// green "Verified".
router.post('/verify-contact', async (req, res) => {
  const accountId = parseInt(req.body && req.body.account_id);
  if (!Number.isFinite(accountId)) return res.status(400).json({ error: 'account_id required' });
  if (!(await ownsAccount(req, accountId))) return res.status(403).json({ error: 'Not your account.' });
  try {
    await pool.query(`UPDATE prospects SET contact_status='verified' WHERE id=$1`, [accountId]);
    res.json({ ok: true, account_id: accountId });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
