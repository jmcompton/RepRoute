'use strict';

// ── Manufacturer "lines" store ───────────────────────────────────────────────
// A "line" is a deduped manufacturer/principal built from the commission data.
// Every confirmed commission_line is resolved to a line_id; account_lines holds
// the per-account × per-line revenue rollup. All functions take a pg pool/client
// so they're testable headlessly (pg-mem) and reusable from routes with the real
// pool. Account = prospects row. Manufacturer dedup reuses lib/fuzzy.js — no new
// fuzzy logic here. Engine is generalizable: NO hardcoded principal aliases.

const { normalize, similarity } = require('./fuzzy');

// Fuzzy merge threshold. High on purpose: "Boss"/"BOSS"/"Soudal BOSS" collapse,
// but distinct principals (e.g. "Fortress" vs "Fortress/Fortified") stay split.
// Cleanup of any over-split is handled by mergeLines / POST /api/lines/merge.
const LINE_MATCH_THRESHOLD = 0.88;

// ── resolveLine(client, manufacturer) → line_id ──────────────────────────────
// Normalize via lib/fuzzy; find an existing line by exact normalized_name, else
// by fuzzy similarity >= threshold, else create a new line. Idempotent.
async function resolveLine(client, manufacturer) {
  const name = String(manufacturer || '').trim();
  const norm = normalize(name);
  if (!norm) return null;

  // (a) durable alias match — a prior manual merge pinned this manufacturer to a
  //     line. Checked BEFORE fuzzy so merges never re-split on re-resolution.
  const alias = await client.query(
    'SELECT line_id FROM line_aliases WHERE normalized_manufacturer=$1', [norm]);
  if (alias.rows.length && alias.rows[0].line_id != null) return alias.rows[0].line_id;

  // (b) exact normalized match.
  const exact = await client.query('SELECT id FROM lines WHERE normalized_name=$1', [norm]);
  if (exact.rows.length) return exact.rows[0].id;

  // (c) fuzzy match against existing lines.
  const all = await client.query('SELECT id, name, normalized_name FROM lines');
  let best = null, bestScore = 0;
  for (const l of all.rows) {
    const s = similarity(norm, l.normalized_name || '');
    if (s > bestScore) { bestScore = s; best = l; }
  }
  if (best && bestScore >= LINE_MATCH_THRESHOLD) return best.id;

  // (d) create. Guard the UNIQUE(normalized_name) race with ON CONFLICT.
  const ins = await client.query(
    `INSERT INTO lines (name, normalized_name)
     VALUES ($1,$2)
     ON CONFLICT (normalized_name) DO UPDATE SET name = lines.name
     RETURNING id`,
    [name, norm]);
  return ins.rows[0].id;
}

// ── rebuildAccountLines(client, accountId) ───────────────────────────────────
// Rebuild account_lines rows for ONE account from its confirmed commission_lines
// (account_id IS NOT NULL, line_id set). Delete-then-insert → fully idempotent.
async function rebuildAccountLines(client, accountId) {
  await client.query('DELETE FROM account_lines WHERE account_id=$1', [accountId]);
  await client.query(
    `INSERT INTO account_lines
       (account_id, line_id, total_sales, total_commission, line_count, first_period, last_period, updated_at)
     SELECT account_id, line_id,
            COALESCE(SUM(sales_amount),0), COALESCE(SUM(commission_amount),0),
            COUNT(*), MIN(period_start), MAX(period_end), NOW()
       FROM commission_lines
      WHERE account_id=$1 AND line_id IS NOT NULL AND is_adjustment = FALSE
      GROUP BY account_id, line_id`,
    [accountId]);
}

// ── rebuildLinesFor(client, accountIds[]) ────────────────────────────────────
// (1) Resolve line_id for every confirmed commission_line of the given accounts
//     that doesn't have one yet; (2) rebuild account_lines for those accounts.
// Idempotent: re-running yields identical account_lines totals.
async function rebuildLinesFor(client, accountIds) {
  const ids = Array.from(new Set((accountIds || []).map(n => parseInt(n)).filter(Number.isFinite)));
  if (!ids.length) return { accounts: 0, lines_resolved: 0 };

  let resolved = 0;
  for (const accountId of ids) {
    // Resolve lines for this account's commission_lines. Cache by manufacturer
    // within the loop so each distinct manufacturer hits resolveLine once.
    const rows = await client.query(
      `SELECT id, manufacturer FROM commission_lines
        WHERE account_id=$1 AND is_adjustment = FALSE`, [accountId]);
    const cache = new Map();
    for (const r of rows.rows) {
      const key = normalize(r.manufacturer);
      let lineId = cache.get(key);
      if (lineId === undefined) { lineId = await resolveLine(client, r.manufacturer); cache.set(key, lineId); }
      await client.query('UPDATE commission_lines SET line_id=$1 WHERE id=$2', [lineId, r.id]);
      resolved++;
    }
    await rebuildAccountLines(client, accountId);
  }
  return { accounts: ids.length, lines_resolved: resolved };
}

// ── backfillAllLines(pool) ───────────────────────────────────────────────────
// Standalone: resolve line_id across ALL confirmed commission_lines and rebuild
// account_lines for every account that has any. Idempotent.
async function backfillAllLines(pool) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const acctR = await client.query(
      `SELECT DISTINCT account_id FROM commission_lines
        WHERE account_id IS NOT NULL AND is_adjustment = FALSE`);
    const ids = acctR.rows.map(r => r.account_id);
    const result = await rebuildLinesFor(client, ids);
    await client.query('COMMIT');
    return result;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

// ── mergeLines(pool, fromLineId, intoLineId) ─────────────────────────────────
// Repoint commission_lines + account_lines from one line to another, delete the
// now-empty source line, and rebuild the affected accounts. Fixes mis-splits
// (e.g. folding "Fortress/Fortified" into "Fortress").
async function mergeLines(pool, fromLineId, intoLineId) {
  const from = parseInt(fromLineId), into = parseInt(intoLineId);
  if (!Number.isFinite(from) || !Number.isFinite(into)) throw new Error('from_line_id and into_line_id are required.');
  if (from === into) throw new Error('Cannot merge a line into itself.');

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const exists = await client.query('SELECT id FROM lines WHERE id IN ($1,$2)', [from, into]);
    if (exists.rows.length < 2) throw new Error('Both lines must exist.');

    // Accounts touched by either line — need their rollups rebuilt afterward.
    const acctR = await client.query(
      `SELECT DISTINCT account_id FROM commission_lines
        WHERE line_id IN ($1,$2) AND account_id IS NOT NULL`, [from, into]);
    const accountIds = acctR.rows.map(r => r.account_id);

    // Durable merge: pin every distinct normalized manufacturer currently under
    // the from-line to the into-line, so future resolveLine / backfill never
    // re-creates the merged-away line. Upsert in case an alias already exists.
    const mans = await client.query(
      'SELECT DISTINCT manufacturer FROM commission_lines WHERE line_id=$1', [from]);
    for (const m of mans.rows) {
      const norm = normalize(m.manufacturer);
      if (!norm) continue;
      await client.query(
        `INSERT INTO line_aliases (normalized_manufacturer, line_id)
         VALUES ($1,$2)
         ON CONFLICT (normalized_manufacturer) DO UPDATE SET line_id = EXCLUDED.line_id`,
        [norm, into]);
    }

    // Repoint the raw facts, then drop the source line (account_lines for it
    // cascade-delete; we rebuild from commission_lines below for correctness).
    await client.query('UPDATE commission_lines SET line_id=$1 WHERE line_id=$2', [into, from]);
    await client.query('DELETE FROM account_lines WHERE line_id=$1', [from]);
    await client.query('DELETE FROM lines WHERE id=$1', [from]);

    // Rebuild the rollup ONLY (rebuildAccountLines) — NOT rebuildLinesFor, which
    // would re-resolve line_id from the manufacturer text and recreate the line
    // we just merged away. The manual repoint above is authoritative.
    for (const accountId of accountIds) {
      await rebuildAccountLines(client, accountId);
    }

    await client.query('COMMIT');
    return { ok: true, merged_from: from, merged_into: into, accounts_rebuilt: accountIds.length };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

// ── linesWithRollup(pool) → [{ id, name, total_sales, account_count }] ────────
async function linesWithRollup(pool) {
  const r = await pool.query(
    `SELECT l.id, l.name, l.status, l.category_hint,
            COALESCE(SUM(al.total_sales),0)      AS total_sales,
            COALESCE(SUM(al.total_commission),0) AS total_commission,
            COUNT(al.account_id)                 AS account_count
       FROM lines l
       LEFT JOIN account_lines al ON al.line_id = l.id
      GROUP BY l.id, l.name, l.status, l.category_hint
      ORDER BY total_sales DESC, l.name ASC`);
  return r.rows.map(x => ({
    id: x.id, name: x.name, status: x.status, category_hint: x.category_hint,
    total_sales: Number(x.total_sales), total_commission: Number(x.total_commission),
    account_count: Number(x.account_count),
  }));
}

// ── linesForAccount(pool, accountId) → [{ ...account_lines, name }] ───────────
async function linesForAccount(pool, accountId) {
  const r = await pool.query(
    `SELECT al.line_id, l.name, al.total_sales, al.total_commission,
            al.line_count, al.first_period, al.last_period
       FROM account_lines al
       JOIN lines l ON l.id = al.line_id
      WHERE al.account_id=$1
      ORDER BY al.total_sales DESC, l.name ASC`, [accountId]);
  return r.rows.map(x => ({
    line_id: x.line_id, name: x.name,
    total_sales: Number(x.total_sales), total_commission: Number(x.total_commission),
    line_count: Number(x.line_count), first_period: x.first_period, last_period: x.last_period,
  }));
}

// ── repForTerritory(lineId, state) → rep | null ──────────────────────────────
// SEAM ONLY. Keith's "which rep covers which (line, territory)" mapping doesn't
// exist yet. This is the single place that lookup will live. Returns null today
// so every territory renders "Rep: —" / Unassigned. When the mapping arrives,
// join it here (a line_territory_reps table or similar) — callers never change.
function repForTerritory(/* lineId, state */) {
  return null;
}

// ── manufacturersSummary(pool, userId) ───────────────────────────────────────
// [{ id, name, total_sales, total_commission, account_count, territory_count }]
// territory_count = COUNT(DISTINCT state) among the line's accounts. Scope: when
// userId is a finite number (a rep), only that rep's accounts count and empty
// lines drop out; when null (manager), every line is included firm-wide.
async function manufacturersSummary(pool, userId) {
  const scoped = Number.isFinite(userId);
  const params = scoped ? [userId] : [];
  const sql =
    `SELECT l.id, l.name,
            COALESCE(SUM(al.total_sales),0)      AS total_sales,
            COALESCE(SUM(al.total_commission),0) AS total_commission,
            COUNT(DISTINCT al.account_id)        AS account_count,
            COUNT(DISTINCT NULLIF(TRIM(p.state),'')) AS territory_count
       FROM lines l
       ${scoped ? 'JOIN' : 'LEFT JOIN'} account_lines al ON al.line_id = l.id
       ${scoped ? 'JOIN' : 'LEFT JOIN'} prospects p ON p.id = al.account_id
       ${scoped ? 'AND p.user_id = $1' : ''}
      GROUP BY l.id, l.name
      ORDER BY total_sales DESC, l.name ASC`;
  const r = await pool.query(sql, params);
  return r.rows.map(x => ({
    id: x.id, name: x.name,
    total_sales: Number(x.total_sales), total_commission: Number(x.total_commission),
    account_count: Number(x.account_count), territory_count: Number(x.territory_count),
  }));
}

// ── manufacturerDetail(pool, lineId, userId) ─────────────────────────────────
// { manufacturer:{id,name,total_sales,total_commission,account_count},
//   territories:[ { state, account_count, total_sales, total_commission, rep,
//                   accounts:[ {account_id, company, city, state,
//                              total_sales, total_commission, last_period} ] } ] }
// Territories sorted by sales desc; accounts within a territory by sales desc.
async function manufacturerDetail(pool, lineId, userId) {
  const id = parseInt(lineId);
  if (!Number.isFinite(id)) return null;
  const lr = await pool.query('SELECT id, name FROM lines WHERE id=$1', [id]);
  if (!lr.rows.length) return null;
  const line = lr.rows[0];

  const scoped = Number.isFinite(userId);
  const params = scoped ? [id, userId] : [id];
  const ar = await pool.query(
    `SELECT al.account_id, p.company, p.city, p.state,
            al.total_sales, al.total_commission, al.last_period
       FROM account_lines al
       JOIN prospects p ON p.id = al.account_id
      WHERE al.line_id = $1 ${scoped ? 'AND p.user_id = $2' : ''}
      ORDER BY al.total_sales DESC, p.company ASC`, params);

  // Group accounts by state (territory). Unknown state → "Unassigned" bucket.
  const byState = new Map();
  let grandSales = 0, grandComm = 0;
  for (const x of ar.rows) {
    const acct = {
      account_id: x.account_id, company: x.company,
      city: x.city || null, state: x.state || null,
      total_sales: Number(x.total_sales), total_commission: Number(x.total_commission),
      last_period: x.last_period,
    };
    grandSales += acct.total_sales; grandComm += acct.total_commission;
    const key = (x.state && String(x.state).trim()) ? String(x.state).trim().toUpperCase() : 'Unassigned';
    if (!byState.has(key)) byState.set(key, []);
    byState.get(key).push(acct);
  }

  const territories = Array.from(byState.entries()).map(([state, accounts]) => {
    const ts = accounts.reduce((s, a) => s + a.total_sales, 0);
    const tc = accounts.reduce((s, a) => s + a.total_commission, 0);
    return {
      state,
      account_count: accounts.length,
      total_sales: Math.round(ts * 100) / 100,
      total_commission: Math.round(tc * 100) / 100,
      rep: repForTerritory(id, state),
      accounts,
    };
  }).sort((a, b) => b.total_sales - a.total_sales);

  return {
    manufacturer: {
      id: line.id, name: line.name,
      total_sales: Math.round(grandSales * 100) / 100,
      total_commission: Math.round(grandComm * 100) / 100,
      account_count: ar.rows.length,
    },
    territories,
  };
}

module.exports = {
  resolveLine, rebuildAccountLines, rebuildLinesFor, backfillAllLines,
  mergeLines, linesWithRollup, linesForAccount, LINE_MATCH_THRESHOLD,
  manufacturersSummary, manufacturerDetail, repForTerritory,
};
