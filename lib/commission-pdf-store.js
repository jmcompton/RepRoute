'use strict';

// ── Commission PDF import → DB ───────────────────────────────────────────────
// Commits a parsed Trilogy Payment Detail Report into the EXISTING commission
// tables (commission_imports, commission_lines, prospects, lines, account_lines)
// so the Manufacturers view + cross-sell (both derived from account_lines) light
// up automatically. Reuses the proven helpers: findOrCreateAccount (fuzzy dedup),
// resolveLine, rebuildAccountLines.
//
// Idempotent re-import: deleting the prior import(s) for the SAME rep + SAME
// period cascade-deletes their commission_lines; account_lines is then rebuilt
// from the surviving commission_lines for every affected account, so re-uploading
// April can never double-count. (account_lines isn't period-keyed in the schema;
// rebuilding from the period-keyed commission_lines is the source-of-truth path.)

const { resolveRep, findOrCreateAccount } = require('./commission-store');
const { resolveLine, rebuildAccountLines } = require('./lines-store');

// Resolve the rep named in the report to a real user. FAIL (throw) rather than
// guess — the task requires an explicit match. An override repId (manager picking
// a rep) is honored if provided.
async function resolveReportRep(pool, repName, overrideRepId) {
  if (overrideRepId != null && Number.isFinite(parseInt(overrideRepId))) {
    const r = await pool.query('SELECT id, name FROM users WHERE id=$1', [parseInt(overrideRepId)]);
    if (!r.rows.length) throw new Error('Selected rep (id ' + overrideRepId + ') does not exist.');
    return { rep_id: r.rows[0].id, rep_name: r.rows[0].name };
  }
  const res = await resolveRep(pool, repName);
  if (!res.rep_id) {
    throw new Error('Could not match the report\'s rep "' + repName + '" to a RepRoute user. ' +
      'Create that user first or pick the rep explicitly.');
  }
  return { rep_id: res.rep_id, rep_name: res.rep_name };
}

// commitPdfImport(pool, { parsed, filename, createdBy, repId }) →
//   { accounts, lines, period, rep, import_id }
async function commitPdfImport(pool, { parsed, filename, createdBy, repId }) {
  if (!parsed || !Array.isArray(parsed.rows)) throw new Error('No parsed rows to commit.');
  if (!parsed.reconciliation || !parsed.reconciliation.reconciles) {
    throw new Error('Refusing to commit: the report did not reconcile to its printed totals.');
  }

  const rep = await resolveReportRep(pool, parsed.rep_name, repId);
  const period_start = parsed.period_start;
  const period_end = parsed.period_end;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // (1) Idempotency: capture accounts touched by any prior import for this rep +
    //     period, then delete those imports (cascade removes their commission_lines).
    const priorImpR = await client.query(
      `SELECT id FROM commission_imports
        WHERE rep_id=$1 AND period_start=$2 AND period_end=$3`,
      [rep.rep_id, period_start, period_end]);
    const priorIds = priorImpR.rows.map(r => r.id);
    const affected = new Set();
    if (priorIds.length) {
      const oldAcctR = await client.query(
        'SELECT DISTINCT account_id FROM commission_lines WHERE import_id = ANY($1::int[]) AND account_id IS NOT NULL',
        [priorIds]);
      oldAcctR.rows.forEach(r => affected.add(r.account_id));
      await client.query('DELETE FROM commission_imports WHERE id = ANY($1::int[])', [priorIds]);
    }

    // (2) Fresh import header (confirmed — the PDF path reconciles before writing).
    const impR = await client.query(
      `INSERT INTO commission_imports
         (rep_name, rep_id, period_start, period_end, source_filename, row_count,
          total_sales, total_commission, status, created_by)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'confirmed',$9) RETURNING id`,
      [rep.rep_name, rep.rep_id, period_start, period_end, filename || null,
       parsed.rows.length, parsed.grand_totals.parsed_sales, parsed.grand_totals.parsed_commission,
       createdBy || null]);
    const importId = impR.rows[0].id;

    // (3) Rows → accounts + lines + commission_lines. Caches keep one resolve per
    //     distinct manufacturer / customer within this import.
    const lineCache = new Map();   // manufacturer → line_id
    const claimed = new Set();
    const distinctLines = new Set();

    for (const row of parsed.rows) {
      // Resolve (and cache) the manufacturer line.
      let lineId = lineCache.get(row.manufacturer);
      if (lineId === undefined) { lineId = await resolveLine(client, row.manufacturer); lineCache.set(row.manufacturer, lineId); }
      if (lineId != null) distinctLines.add(lineId);

      let accountId = null;
      if (!row.is_adjustment && row.customer_raw) {
        accountId = await findOrCreateAccount(client, rep.rep_id, row.customer_raw, null, null, claimed);
        if (Number.isFinite(accountId)) affected.add(accountId);
      }

      await client.query(
        `INSERT INTO commission_lines
           (import_id, rep_name, manufacturer, customer_raw, customer_normalized,
            account_id, line_id, sales_amount, commission_amount,
            period_start, period_end, is_adjustment)
         VALUES ($1,$2,$3,$4,NULL,$5,$6,$7,$8,$9,$10,$11)`,
        [importId, rep.rep_name, row.manufacturer, row.customer_raw,
         accountId, lineId, row.total_sales, row.total_commission,
         period_start, period_end, !!row.is_adjustment]);
    }

    // (4) Rebuild the per-account × line rollup for every affected account. This
    //     IS the cross-sell recompute — opportunities are derived live from
    //     account_lines (no persisted table to update).
    let accountsTouched = 0;
    for (const acctId of affected) { await rebuildAccountLines(client, acctId); accountsTouched++; }

    await client.query('COMMIT');
    return {
      accounts: accountsTouched,
      lines: distinctLines.size,
      period: parsed.period,
      rep: rep.rep_name,
      import_id: importId,
    };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

module.exports = { commitPdfImport, resolveReportRep };
