'use strict';

const express = require('express');
const { pool } = require('../db');
const { buildCooccurrence, recommendForAccount } = require('../lib/crosssell');
const router = express.Router();

function isManager(u) { return !!u && (u.role === 'manager' || u.role === 'admin'); }

// Agency-wide account_lines feed the co-occurrence model (the more confirmed
// commission data, the better the signal — visibility is enforced per-rep below).
async function loadAllAccountLines() {
  const r = await pool.query(
    `SELECT al.account_id, al.line_id, l.name, al.total_sales
       FROM account_lines al JOIN lines l ON l.id = al.line_id`);
  return r.rows;
}

// ── GET /api/crosssell/account/:id ────────────────────────────────────────────
// Opportunities for one account. A rep may only view accounts they own.
router.get('/account/:id', async (req, res) => {
  try {
    const accountId = parseInt(req.params.id);
    if (!Number.isFinite(accountId)) return res.status(400).json({ error: 'Invalid account id.' });

    const acct = await pool.query('SELECT id, company, user_id FROM prospects WHERE id=$1', [accountId]);
    if (!acct.rows.length) return res.status(404).json({ error: 'Account not found.' });
    if (!isManager(req.session.user) && acct.rows[0].user_id !== req.session.user.id) {
      return res.status(403).json({ error: 'Not your account.' });
    }

    const allRows = await loadAllAccountLines();
    const coocc = buildCooccurrence(allRows);
    const accountLines = allRows.filter(r => r.account_id === accountId);
    const recs = recommendForAccount(accountId, coocc, accountLines);
    res.json(recs);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/crosssell/opportunities ──────────────────────────────────────────
// The viewer's ranked opportunities. Rep = own accounts (prospects.user_id);
// manager = all accounts. Ranked by est_sales * confidence desc.
router.get('/opportunities', async (req, res) => {
  try {
    const allRows = await loadAllAccountLines();
    const coocc = buildCooccurrence(allRows);

    // Which accounts is the viewer allowed to see?
    const acctSql = isManager(req.session.user)
      ? `SELECT p.id, p.company, u.name AS rep FROM prospects p LEFT JOIN users u ON u.id = p.user_id`
      : `SELECT p.id, p.company, u.name AS rep FROM prospects p LEFT JOIN users u ON u.id = p.user_id WHERE p.user_id = $1`;
    const acctParams = isManager(req.session.user) ? [] : [req.session.user.id];
    const accts = await pool.query(acctSql, acctParams);

    const out = [];
    for (const a of accts.rows) {
      const accountLines = allRows.filter(r => r.account_id === a.id);
      if (!accountLines.length) continue;
      const recs = recommendForAccount(a.id, coocc, accountLines);
      for (const rec of recs) {
        out.push({
          account_id: a.id, account_name: a.company, rep: a.rep || null,
          line_id: rec.line_id, line_name: rec.line_name,
          est_sales: rec.est_sales, confidence: rec.confidence,
          reason: rec.reason, supporting_line: rec.supporting_line,
        });
      }
    }
    out.sort((x, y) => (y.est_sales * y.confidence) - (x.est_sales * x.confidence));
    res.json(out);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

module.exports = router;
