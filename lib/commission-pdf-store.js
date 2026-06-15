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
const { normalizeCustomer, pairScore, entityCity, cityCompatible } = require('./commission-matcher');

// Confidence tiers for matching an imported company to an EXISTING account.
// Both use the SHARED fuzzy/Levenshtein helper (pairScore → similarity →
// levenshtein in lib/fuzzy.js) — no separate matching algorithm.
const HIGH_CONF = 0.85;   // very close → attach commission to the existing account
const MEDIUM_CONF = 0.70; // plausible → create new, flag "review match" (never merge silently)

// Best fuzzy match of a raw customer name against a snapshot of the rep's existing
// accounts, honoring the same city guard the rest of the engine uses (a Huntsville
// customer never matches a Birmingham branch). Returns { account, score } | null.
function bestExistingMatch(customerRaw, existingAccounts) {
  const nc = normalizeCustomer(customerRaw);
  const norm = nc.normalized;
  if (!norm) return null;
  const custCity = nc.city;
  let best = null, bestScore = 0;
  for (const a of existingAccounts) {
    if (!cityCompatible(custCity, entityCity(a.company, a.city))) continue;
    const an = normalizeCustomer(a.company).normalized;
    const s = pairScore(norm, an);
    if (s > bestScore) { bestScore = s; best = a; }
  }
  return best ? { account: best, score: bestScore } : null;
}

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

    // Snapshot of the rep's EXISTING accounts, taken BEFORE this import creates
    // any, so tier matching is strictly "imported company vs. accounts that
    // already existed". Accounts created mid-import still dedup to each other via
    // findOrCreateAccount (it re-queries), just never count as a high-confidence
    // "existing" match here.
    const snapR = await client.query(
      'SELECT id, company, city, state FROM prospects WHERE user_id=$1', [rep.rep_id]);
    const existingAccounts = snapR.rows;

    // (3) Rows → accounts + lines + commission_lines. Caches keep one resolve per
    //     distinct manufacturer / customer within this import.
    const lineCache = new Map();   // manufacturer → line_id
    const claimed = new Set();
    const distinctLines = new Set();
    const matchedAccountIds = new Set();   // attached to an existing account (complete)
    const newAccountIds = new Set();       // freshly created, need contact info
    const reviewAccountIds = new Set();    // medium-confidence, flagged "review match"

    for (const row of parsed.rows) {
      // Resolve (and cache) the manufacturer line.
      let lineId = lineCache.get(row.manufacturer);
      if (lineId === undefined) { lineId = await resolveLine(client, row.manufacturer); lineCache.set(row.manufacturer, lineId); }
      if (lineId != null) distinctLines.add(lineId);

      let accountId = null;
      let matchStatus = null;        // 'matched' | 'review' | 'new'
      let matchCandidateName = null; // existing account this row matched / might match

      if (!row.is_adjustment && row.customer_raw) {
        const m = bestExistingMatch(row.customer_raw, existingAccounts);
        if (m && m.score >= HIGH_CONF) {
          // High confidence → attach the commission to the EXISTING account.
          // Do NOT create a new account; leave its phone/address untouched.
          accountId = m.account.id;
          matchStatus = 'matched';
          matchCandidateName = m.account.company;
          matchedAccountIds.add(accountId);
        } else if (m && m.score >= MEDIUM_CONF) {
          // Medium confidence → create a NEW account (needs_info) but flag the
          // line "review match" with the candidate name. Never silently merge.
          accountId = await findOrCreateAccount(client, rep.rep_id, row.customer_raw, null, null, claimed);
          matchStatus = 'review';
          matchCandidateName = m.account.company;
          if (Number.isFinite(accountId)) {
            newAccountIds.add(accountId);
            reviewAccountIds.add(accountId);
            // Stamp the review candidate on the new account (first one wins).
            await client.query(
              `UPDATE prospects
                  SET match_review_candidate = $1, match_review_candidate_id = $2
                WHERE id = $3 AND match_review_candidate IS NULL`,
              [m.account.company, m.account.id, accountId]);
          }
        } else {
          // No confident match → brand-new account (needs_info).
          accountId = await findOrCreateAccount(client, rep.rep_id, row.customer_raw, null, null, claimed);
          matchStatus = 'new';
          if (Number.isFinite(accountId)) newAccountIds.add(accountId);
        }
        if (Number.isFinite(accountId)) affected.add(accountId);
      }

      await client.query(
        `INSERT INTO commission_lines
           (import_id, rep_name, manufacturer, customer_raw, customer_normalized,
            account_id, line_id, sales_amount, commission_amount,
            period_start, period_end, is_adjustment, match_status, match_candidate_name)
         VALUES ($1,$2,$3,$4,NULL,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
        [importId, rep.rep_name, row.manufacturer, row.customer_raw,
         accountId, lineId, row.total_sales, row.total_commission,
         period_start, period_end, !!row.is_adjustment, matchStatus, matchCandidateName]);
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
      rep_id: rep.rep_id,
      import_id: importId,
      // Match/enrich summary for the import UI.
      matched: matchedAccountIds.size,   // attached to an existing account (complete)
      needs_info: newAccountIds.size,    // new accounts missing phone/address
      review: reviewAccountIds.size,     // medium-confidence "review match" flags
    };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

module.exports = { commitPdfImport, resolveReportRep };
