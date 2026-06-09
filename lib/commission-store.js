'use strict';

// ── Commission import store ──────────────────────────────────────────────────
// All DB logic for the commission engine. Every function takes a pg pool/client
// so it is fully testable headlessly (pg-mem) and reused by routes/commissions.js
// with the real pool. Account = prospects row (RepRoute's account store).

const { parseCommissionReport } = require('./commission-parser');
const { clusterCommissionLines, matchClusterToAccount, normalizeCustomer, pairScore } = require('./commission-matcher');
const { similarity } = require('./fuzzy');
const { resolveCompanyType } = require('../routes/prospects');

// ── Rep resolution: exact → fuzzy → picker (never hard-fail) ──────────────────
async function resolveRep(pool, repName) {
  const name = String(repName || '').trim();
  const usersR = await pool.query('SELECT id, name, email FROM users');
  const users = usersR.rows;

  // (a) exact (case-insensitive) name match
  const exact = users.filter(u => (u.name || '').trim().toLowerCase() === name.toLowerCase());
  if (exact.length === 1) {
    return { rep_id: exact[0].id, rep_name: exact[0].name, confidence: 1, needs_picker: false, candidates: [] };
  }

  // (b) fuzzy match
  const scored = users
    .map(u => ({ id: u.id, name: u.name, email: u.email, sim: similarity(name, u.name || '') }))
    .sort((a, b) => b.sim - a.sim);
  const best = scored[0];
  if (best && best.sim >= 0.8 && (!scored[1] || best.sim - scored[1].sim >= 0.1)) {
    return { rep_id: best.id, rep_name: best.name, confidence: Math.round(best.sim * 100) / 100, needs_picker: false, candidates: scored.slice(0, 5) };
  }

  // (c) ambiguous / none → require a picker. Never throw.
  return { rep_id: null, rep_name: name, confidence: best ? Math.round(best.sim * 100) / 100 : 0, needs_picker: true, candidates: scored.slice(0, 8) };
}

// ── Insert a parsed report (status pending_review, lines unlinked) ────────────
async function insertImport(pool, { parsed, filename, createdBy }) {
  const rep = await resolveRep(pool, parsed.rep_name);
  const imp = await pool.query(
    `INSERT INTO commission_imports
       (rep_name, rep_id, period_start, period_end, source_filename, row_count,
        total_sales, total_commission, status, created_by)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending_review',$9)
     RETURNING id`,
    [parsed.rep_name, rep.rep_id, parsed.period_start, parsed.period_end,
     filename || null, parsed.lines.length, parsed.totals.sales, parsed.totals.commission,
     createdBy || null]
  );
  const importId = imp.rows[0].id;

  for (const l of parsed.lines) {
    const nc = normalizeCustomer(l.customer_raw);
    await pool.query(
      `INSERT INTO commission_lines
         (import_id, rep_name, manufacturer, customer_raw, customer_normalized,
          account_id, sales_amount, commission_amount, period_start, period_end, is_adjustment)
       VALUES ($1,$2,$3,$4,$5,NULL,$6,$7,$8,$9,$10)`,
      [importId, parsed.rep_name, l.manufacturer, l.customer_raw,
       l.is_adjustment ? null : nc.normalized,
       l.sales, l.commission, parsed.period_start, parsed.period_end, !!l.is_adjustment]
    );
  }
  return { import_id: importId, rep };
}

// ── Build the review for an import ───────────────────────────────────────────
async function buildReview(pool, importId) {
  const impR = await pool.query('SELECT * FROM commission_imports WHERE id=$1', [importId]);
  if (!impR.rows.length) throw new Error('Import not found');
  const imp = impR.rows[0];

  const linesR = await pool.query(
    `SELECT id, manufacturer, customer_raw, customer_normalized, sales_amount, commission_amount, is_adjustment
       FROM commission_lines WHERE import_id=$1 ORDER BY id`, [importId]);
  const lines = linesR.rows.map(r => ({
    id: r.id,
    manufacturer: r.manufacturer,
    customer_raw: r.customer_raw,
    sales: Number(r.sales_amount) || 0,
    commission: Number(r.commission_amount) || 0,
    is_adjustment: r.is_adjustment,
  }));

  const rep = await resolveRep(pool, imp.rep_name);
  const repId = imp.rep_id || rep.rep_id;

  // Rep's existing accounts (prospects) for matching.
  let accounts = [];
  if (repId) {
    const aR = await pool.query(
      `SELECT id, company, city, state FROM prospects WHERE user_id=$1`, [repId]);
    accounts = aR.rows;
  }

  // Learned per-rep map.
  const mapByNorm = new Map();
  if (repId) {
    const mR = await pool.query(
      `SELECT m.customer_normalized, m.account_id, m.confidence, p.company
         FROM commission_customer_map m
         LEFT JOIN prospects p ON p.id = m.account_id
        WHERE m.user_id=$1`, [repId]);
    for (const row of mR.rows) {
      mapByNorm.set(row.customer_normalized, { account_id: row.account_id, confidence: row.confidence, account_name: row.company });
    }
  }

  // Cluster + match. Map line_index → line_id for confirm.
  const { clusters, adjustments } = clusterCommissionLines(lines);
  const groups = clusters.map(c => {
    const match = matchClusterToAccount(c, accounts, mapByNorm);
    return {
      representative: c.representative,
      members: c.members,
      member_count: c.member_count,
      normalized_keys: c.normalized_keys,
      line_ids: c.line_indexes.map(i => lines[i].id),
      total_sales: c.total_sales,
      total_commission: c.total_commission,
      cities: c.cities,
      proposed_account: match.proposed_account,
      suggest_new: match.suggest_new,
      confidence: match.confidence,
      matched_by: match.matched_by,
    };
  }).sort((a, b) => b.total_sales - a.total_sales);

  const matchedSales = round2(groups.filter(g => g.proposed_account && !g.suggest_new)
    .reduce((s, g) => s + g.total_sales, 0));

  return {
    import_id: imp.id,
    status: imp.status,
    rep: { id: repId, name: imp.rep_name, resolution: rep },
    period: { start: imp.period_start, end: imp.period_end },
    source_filename: imp.source_filename,
    totals: { sales: Number(imp.total_sales), commission: Number(imp.total_commission) },
    reconciliation: {
      file_sales: Number(imp.total_sales),
      matched_sales: matchedSales,
      unmatched_sales: round2(Number(imp.total_sales) - matchedSales),
    },
    groups,
    adjustments: adjustments.map(a => ({
      ...a,
      line_id: lines[a.line_index].id,
    })),
  };
}

// ── Find-or-create an account (prospect) for a rep, with dedup ────────────────
// Reuses fuzzy matching to avoid creating a duplicate of an existing account.
async function findOrCreateAccount(client, repUserId, name, city) {
  const norm = normalizeCustomer(name).normalized;
  const existing = await client.query(
    `SELECT id, company FROM prospects WHERE user_id=$1`, [repUserId]);
  let best = null, bestScore = 0;
  for (const a of existing.rows) {
    const an = normalizeCustomer(a.company).normalized;
    const s = pairScore(norm, an);
    if (s > bestScore) { bestScore = s; best = a; }
  }
  if (best && bestScore >= 0.85) return best.id; // dedup: reuse existing account

  const company = String(name || '').trim();
  const company_type = resolveCompanyType('');
  const ins = await client.query(
    `INSERT INTO prospects
       (user_id, company, category, company_type, city, state, source, data_status, last_activity_at)
     VALUES ($1,$2,$3,$4,$5,$6,'Commission Import','Verified CRM Data',NOW())
     RETURNING id`,
    [repUserId, company, 'Commission Import', company_type, city || null, 'AL']
  );
  return ins.rows[0].id;
}

// ── Confirm: link lines to accounts, learn the map, mark confirmed ────────────
// decisions: [{ line_ids:[], normalized_keys:[], action:'existing'|'new'|'skip',
//               account_id?, new_name?, new_city?, confidence? }]
// repId required (from picker if rep couldn't be auto-resolved).
async function confirmImport(pool, importId, { rep_id, decisions }) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const impR = await client.query('SELECT * FROM commission_imports WHERE id=$1 FOR UPDATE', [importId]);
    if (!impR.rows.length) throw new Error('Import not found');
    const imp = impR.rows[0];
    const repUserId = rep_id || imp.rep_id;
    if (!repUserId) throw new Error('A rep must be selected before confirming this import.');

    for (const d of (decisions || [])) {
      if (!d || d.action === 'skip') continue;
      let accountId = null;
      if (d.action === 'existing') {
        accountId = parseInt(d.account_id);
        if (!Number.isFinite(accountId)) throw new Error('existing decision missing account_id');
      } else if (d.action === 'new') {
        accountId = await findOrCreateAccount(client, repUserId, d.new_name, d.new_city);
      } else {
        throw new Error('Unknown decision action: ' + d.action);
      }

      const lineIds = (d.line_ids || []).map(n => parseInt(n)).filter(Number.isFinite);
      for (const lid of lineIds) {
        await client.query(
          `UPDATE commission_lines SET account_id=$1 WHERE id=$2 AND import_id=$3`,
          [accountId, lid, importId]);
      }

      for (const key of (d.normalized_keys || [])) {
        if (!key) continue;
        await client.query(
          `INSERT INTO commission_customer_map (user_id, customer_normalized, account_id, confidence)
           VALUES ($1,$2,$3,$4)
           ON CONFLICT (user_id, customer_normalized)
           DO UPDATE SET account_id=EXCLUDED.account_id, confidence=EXCLUDED.confidence`,
          [repUserId, key, accountId, d.confidence != null ? d.confidence : 1]);
      }
    }

    await client.query(
      `UPDATE commission_imports SET status='confirmed', rep_id=$1 WHERE id=$2`,
      [repUserId, importId]);

    await client.query('COMMIT');
    return { ok: true, import_id: importId, rep_id: repUserId };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

async function discardImport(pool, importId) {
  const r = await pool.query(
    `UPDATE commission_imports SET status='discarded' WHERE id=$1 RETURNING id`, [importId]);
  if (!r.rows.length) throw new Error('Import not found');
  return { ok: true, import_id: importId };
}

function round2(n) { return Math.round((Number(n) || 0) * 100) / 100; }

module.exports = {
  resolveRep, insertImport, buildReview, confirmImport, discardImport, findOrCreateAccount,
  parseCommissionReport,
};
