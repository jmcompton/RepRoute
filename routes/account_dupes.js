const express = require('express');
const { pool } = require('../db');
const { accountKey, accountKeySql } = require('../lib/account-key');
const { rebuildAccountLines } = require('../lib/lines-store');
const router = express.Router();

// ════════════════════════════════════════════════════════════════
// DUPLICATE ACCOUNTS — review + merge (never automatic).
// Two of a rep's accounts are proposed as duplicates when their names match
// under lib/account-key.js (Inc/LLC/Co/Company/punctuation ignored; a city after
// the name keeps it a separate location). The rep confirms or skips each group.
// Merging moves everything that points at the duplicates onto the kept account,
// then deletes the duplicates — all in one transaction.
// ════════════════════════════════════════════════════════════════

// Tables whose rows point at an account and move with a merge.
const MOVE = [
  ['calls', 'prospect_id'],
  ['contacts', 'prospect_id'],
  ['email_logs', 'prospect_id'],
  ['notifications', 'prospect_id'],
  ['samples', 'prospect_id'],
  ['planner_items', 'account_id'],
  ['commission_lines', 'account_id'],
  ['commission_customer_map', 'account_id'],
];
// Kept account's blank fields are filled from the merged ones.
const FILL = ['phone', 'email', 'contact', 'title', 'mobile', 'website', 'address', 'city', 'state',
  'zip', 'lat', 'lng', 'google_place_id', 'category', 'manufacturer_assoc'];

// ── GET /api/account-dupes → { groups: [{ key, suggested_keep_id, accounts:[...] }] }
router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  try {
    const rows = (await pool.query(
      `WITH p AS (
         SELECT id, company, city, state, phone, address, created_at, last_activity_at,
                ${accountKeySql('company')} AS k
           FROM prospects WHERE user_id = $1
       ), dup AS (
         SELECT k FROM p GROUP BY k HAVING COUNT(*) > 1
       ), q AS (
         SELECT ${accountKeySql('account_name')} AS k, COUNT(*) AS n
           FROM quotes WHERE user_id = $1 OR rep_id = $1 GROUP BY 1
       )
       SELECT p.*,
              (SELECT COUNT(*) FROM calls c    WHERE c.prospect_id = p.id) AS calls,
              (SELECT COUNT(*) FROM contacts ct WHERE ct.prospect_id = p.id) AS contacts,
              (SELECT COUNT(*) FROM commission_lines cl WHERE cl.account_id = p.id) AS commission_lines,
              COALESCE((SELECT n FROM q WHERE q.k = p.k), 0) AS group_quotes
         FROM p JOIN dup USING (k)
        WHERE NOT EXISTS (SELECT 1 FROM account_dupe_skips s WHERE s.user_id = $1 AND s.key = p.k)
        ORDER BY p.k, p.id`,
      [uid]
    )).rows;

    const byKey = new Map();
    for (const r of rows) {
      if (!byKey.has(r.k)) byKey.set(r.k, []);
      byKey.get(r.k).push({
        id: r.id, company: r.company, city: r.city, state: r.state, phone: r.phone, address: r.address,
        created_at: r.created_at, last_activity_at: r.last_activity_at,
        calls: Number(r.calls), contacts: Number(r.contacts), commission_lines: Number(r.commission_lines),
      });
    }
    const groups = [];
    for (const [key, accounts] of byKey) {
      // Suggest keeping the most-used record (calls + contacts + commission), oldest on ties.
      const score = a => a.calls * 3 + a.commission_lines * 3 + a.contacts;
      const keep = accounts.slice().sort((a, b) => (score(b) - score(a)) || (a.id - b.id))[0];
      const quotes = rows.find(r => r.k === key);
      groups.push({ key, suggested_keep_id: keep.id, quotes: Number(quotes ? quotes.group_quotes : 0), accounts });
    }
    groups.sort((a, b) => (b.accounts.length - a.accounts.length) || a.key.localeCompare(b.key));
    res.set('Cache-Control', 'no-store');
    res.json({ groups });
  } catch (e) {
    console.error('[account-dupes] GET error:', e.message);
    res.status(500).json({ error: 'Failed to find duplicate accounts' });
  }
});

// ── POST /api/account-dupes/skip { key } — don't propose this group again.
router.post('/skip', async (req, res) => {
  const key = String((req.body && req.body.key) || '').trim();
  if (!key) return res.status(400).json({ error: 'key required' });
  try {
    await pool.query(
      `INSERT INTO account_dupe_skips (user_id, key) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
      [req.session.user.id, key]);
    res.json({ ok: true });
  } catch (e) {
    console.error('[account-dupes] skip error:', e.message);
    res.status(500).json({ error: 'Failed to skip' });
  }
});

// ── POST /api/account-dupes/merge { keep_id, merge_ids: [] }
router.post('/merge', async (req, res) => {
  const uid = req.session.user.id;
  const keepId = parseInt(req.body && req.body.keep_id);
  const mergeIds = ((req.body && req.body.merge_ids) || []).map(Number).filter(n => Number.isFinite(n) && n !== keepId);
  if (!keepId || !mergeIds.length) return res.status(400).json({ error: 'Pick an account to keep and at least one to merge' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const all = (await client.query(
      'SELECT * FROM prospects WHERE user_id = $1 AND id = ANY($2::int[]) FOR UPDATE',
      [uid, [keepId].concat(mergeIds)]
    )).rows;
    const keep = all.find(p => p.id === keepId);
    const merged = all.filter(p => p.id !== keepId);
    if (!keep || merged.length !== mergeIds.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Account not found' });
    }
    // Safety: only accounts that really match by the duplicate rules.
    const key = accountKey(keep.company);
    if (merged.some(p => accountKey(p.company) !== key)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Those accounts don\'t match by name — not merged' });
    }

    const moved = {};
    for (const [table, col] of MOVE) {
      const r = await client.query(
        `UPDATE ${table} SET ${col} = $1 WHERE ${col} = ANY($2::int[])`, [keepId, mergeIds]);
      moved[table] = r.rowCount;
    }
    // Contacts: one primary per account; drop exact-name repeats (keep the oldest).
    await client.query(
      `UPDATE contacts SET is_primary = FALSE
        WHERE prospect_id = $1 AND is_primary = TRUE
          AND id <> (SELECT MIN(id) FROM contacts WHERE prospect_id = $1 AND is_primary = TRUE)`, [keepId]);
    await client.query(
      `DELETE FROM contacts c USING contacts o
        WHERE c.prospect_id = $1 AND o.prospect_id = $1
          AND LOWER(TRIM(c.name)) = LOWER(TRIM(o.name)) AND c.id > o.id`, [keepId]);
    // Commission rollups are derived — rebuild for the kept account.
    await client.query('DELETE FROM account_lines WHERE account_id = ANY($1::int[])', [mergeIds]);
    await rebuildAccountLines(client, keepId);

    // Fill the kept account's blanks; append merged notes; keep latest activity.
    const sets = [], vals = [];
    for (const f of FILL) {
      if (keep[f] != null && String(keep[f]).trim() !== '') continue;
      const src = merged.find(p => p[f] != null && String(p[f]).trim() !== '');
      if (src) { vals.push(src[f]); sets.push(`${f} = $${vals.length}`); }
    }
    const extraNotes = merged.map(p => (p.notes || '').trim()).filter(n => n && n !== (keep.notes || '').trim());
    if (extraNotes.length) {
      vals.push([(keep.notes || '').trim()].concat(extraNotes).filter(Boolean).join('\n'));
      sets.push(`notes = $${vals.length}`);
    }
    sets.push(`last_activity_at = GREATEST(last_activity_at, (SELECT MAX(last_activity_at) FROM prospects WHERE id = ANY($${vals.length + 1}::int[])))`);
    vals.push(mergeIds);
    vals.push(keepId);
    await client.query(`UPDATE prospects SET ${sets.join(', ')} WHERE id = $${vals.length}`, vals);

    // Quotes reference the account by name — point this rep's quotes for any
    // spelling of this account at the kept name.
    const q = await client.query(
      `UPDATE quotes SET account_name = $1, updated_at = NOW()
        WHERE (user_id = $2 OR rep_id = $2) AND ${accountKeySql('account_name')} = $3 AND account_name <> $1`,
      [keep.company, uid, key]);
    moved.quotes = q.rowCount;

    await client.query('DELETE FROM prospects WHERE user_id = $1 AND id = ANY($2::int[])', [uid, mergeIds]);
    await client.query('COMMIT');
    console.log(`[account-dupes] uid=${uid} merged ${mergeIds.join(',')} → ${keepId} (${keep.company})`, moved);
    res.json({ ok: true, kept_id: keepId, merged: mergeIds.length, moved });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[account-dupes] merge error:', e.message);
    res.status(500).json({ error: 'Merge failed — nothing was changed' });
  } finally {
    client.release();
  }
});

module.exports = router;
