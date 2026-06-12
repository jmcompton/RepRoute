'use strict';

// ── Clear Demo Data (manager-only, scoped) ───────────────────────────────────
// Removes ONLY the sample/demo accounts that came in from the sample commission
// report: prospects WHERE source='Commission Import' AND user_id = the caller.
// NEVER touches any other source and NEVER touches another user's data (the
// user_id scope is the hard guard). Mounted behind requireAuthAPI +
// requireManagerAPI in server.js.

const express = require('express');
const router = express.Router();
const { pool } = require('../db');

// The single scoped predicate used by BOTH preview and clear, so they can never
// drift apart. $1 = the caller's user_id.
const SCOPE_WHERE = `source = 'Commission Import' AND user_id = $1`;

// GET /api/admin/demo-data/preview — exactly what clear would delete.
router.get('/preview', async (req, res) => {
  try {
    const uid = req.session.user.id;
    const r = await pool.query(
      `SELECT company AS name, city, source
         FROM prospects
        WHERE ${SCOPE_WHERE}
        ORDER BY company ASC`, [uid]);
    res.json({ count: r.rows.length, accounts: r.rows });
  } catch (e) {
    console.error('[demo-data/preview]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/admin/demo-data/clear — delete the scoped set in one transaction.
router.post('/clear', async (req, res) => {
  const uid = req.session.user.id;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // The exact account ids in scope (source + owner). Everything keys off this.
    const idsR = await client.query(
      `SELECT id FROM prospects WHERE ${SCOPE_WHERE}`, [uid]);
    const accountIds = idsR.rows.map(x => x.id);

    if (!accountIds.length) {
      await client.query('COMMIT');
      return res.json({ deleted_accounts: 0, deleted_lines: 0, deleted_opportunities: 0 });
    }

    // Lines these accounts touch — candidates to become orphaned once the
    // accounts (and their account_lines) are gone.
    const candR = await client.query(
      'SELECT DISTINCT line_id FROM account_lines WHERE account_id = ANY($1::int[])',
      [accountIds]);
    const candidateLineIds = candR.rows.map(x => x.line_id).filter(v => v != null);

    // (1) Cross-sell opportunities: derived live from account_lines (no persisted
    //     table), so deleting the account_lines below removes them implicitly.
    //     Count = the rollup rows we're about to drop for these accounts.
    const oppR = await client.query(
      'SELECT COUNT(*)::int AS c FROM account_lines WHERE account_id = ANY($1::int[])',
      [accountIds]);
    const deletedOpportunities = oppR.rows[0].c;

    // (2) Delete the per-account × per-line rollup rows.
    await client.query(
      'DELETE FROM account_lines WHERE account_id = ANY($1::int[])', [accountIds]);

    // (3) Delete the demo accounts themselves. (commission_customer_map cascades;
    //     commission_lines.account_id is SET NULL — raw facts are preserved.)
    const delAcct = await client.query(
      `DELETE FROM prospects WHERE ${SCOPE_WHERE}`, [uid]);

    // (4) Delete any line that is now orphaned (no remaining account_lines).
    let deletedLines = 0;
    if (candidateLineIds.length) {
      const delLines = await client.query(
        `DELETE FROM lines
           WHERE id = ANY($1::int[])
             AND NOT EXISTS (SELECT 1 FROM account_lines al WHERE al.line_id = lines.id)`,
        [candidateLineIds]);
      deletedLines = delLines.rowCount;
    }

    await client.query('COMMIT');
    res.json({
      deleted_accounts: delAcct.rowCount,
      deleted_lines: deletedLines,
      deleted_opportunities: deletedOpportunities,
    });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[demo-data/clear]', e.message);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

module.exports = router;
