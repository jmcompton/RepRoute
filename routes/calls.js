const express = require('express');
const { pool } = require('../db');
const pipeline = require('../lib/pipeline');
const router = express.Router();

router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const result = await pool.query(
    `SELECT c.*, p.company, p.category, p.city, p.phone as prospect_phone
     FROM calls c JOIN prospects p ON c.prospect_id=p.id
     WHERE c.user_id=$1 ORDER BY c.call_date DESC`, [uid]);
  res.json(result.rows);
});

// GET /api/calls/search?q=MoistureShield — every visit note on the team that
// mentions a word, newest first, with the account it was at. (Kody, Oct 27.)
router.get('/search', async (req, res) => {
  const q = String(req.query.q || '').trim();
  if (q.length < 2) return res.json({ q, results: [] });
  const like = '%' + q.replace(/[\\%_]/g, m => '\\' + m) + '%';
  const r = await pool.query(
    `SELECT c.id, c.call_date::text AS call_date, c.call_type, c.outcome, c.prospect_id,
            p.company, p.city, COALESCE(NULLIF(TRIM(u.name),''), u.email) AS rep,
            c.notes, c.next_step, c.products_discussed
       FROM calls c JOIN prospects p ON p.id = c.prospect_id LEFT JOIN users u ON u.id = c.user_id
      WHERE c.notes ILIKE $1 OR c.next_step ILIKE $1 OR c.products_discussed ILIKE $1
      ORDER BY c.call_date DESC NULLS LAST, c.id DESC LIMIT 100`, [like]);
  const accounts = new Set(r.rows.map(x => x.prospect_id)).size;
  res.json({ q, accounts, results: r.rows });
});

router.post('/', async (req, res) => {
  const uid = req.session.user.id;
  const { prospect_id, call_date, call_type, outcome, products_discussed, next_step, next_step_date, notes } = req.body;
  // client_ref: optional idempotency key from the offline call queue. A retry of a
  // call the server already saved returns the saved row instead of logging it twice.
  const client_ref = typeof req.body.client_ref === 'string' && req.body.client_ref ? req.body.client_ref.slice(0, 100) : null;
  // Every visit belongs to an account (reports, the call list and Reconnect all key on it).
  if (!parseInt(prospect_id)) return res.status(400).json({ error: 'Pick the account this call was with' });
  try {
  if (client_ref) {
    const dup = await pool.query('SELECT * FROM calls WHERE user_id=$1 AND client_ref=$2', [uid, client_ref]);
    if (dup.rows.length) return res.json(dup.rows[0]);
  }
  const result = await pool.query(
    `INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, products_discussed, next_step, next_step_date, notes, client_ref)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
    [uid, prospect_id, call_date, call_type, outcome, products_discussed, next_step, next_step_date || null, notes, client_ref]
  );
  // Update prospect status if outcome provided
  if (outcome) {
    const statusMap = { 'Not Interested': 'Cold', 'Spoke - Not Interested': 'Cold', 'Interested': 'Warm', 'Spoke - Interested': 'Warm', 'Ready to Buy': 'Hot', 'Order Placed': 'Hot' };
    const status = statusMap[outcome] || (outcome.includes('Not Interest') ? 'Cold' : outcome.includes('Interest') ? 'Warm' : null);
    if (status) await pool.query('UPDATE prospects SET status=$1 WHERE id=$2 AND user_id=$3', [status, prospect_id, uid]);
  }
  // If notes provided, append them to the prospect notes field so they show on contact detail
  if (notes && notes.trim()) {
    const today = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'America/New_York' });
    const callType = call_type || 'Call';
    const noteEntry = '[' + today + ' — ' + callType + (outcome ? ' / ' + outcome : '') + ']: ' + notes.trim();
    await pool.query(
      `UPDATE prospects
       SET notes = CASE
         WHEN notes IS NULL OR notes = '' THEN $1
         ELSE notes || E'\n' || $1
       END
       WHERE id=$2 AND user_id=$3`,
      [noteEntry, prospect_id, uid]
    );
  }
  // Auto-mark prospect as Contacted and update last_activity_at whenever a call is logged
  await pool.query(
    `UPDATE prospects
     SET data_status    = CASE WHEN data_status = 'Unvetted' THEN 'Contacted' ELSE data_status END,
         last_activity_at = NOW()
     WHERE id = $1 AND user_id = $2`,
    [prospect_id, uid]
  );
  // "Left Sample" means a sample is out: track it so the 7-day check-in,
  // the morning brief and the Samples page all see it.
  if (/left sample/i.test(outcome || '')) {
    try {
      await pool.query(
        `INSERT INTO samples (user_id, prospect_id, product_line, quantity, notes, sent_date, status, follow_up_date)
         VALUES ($1, $2, $3, 1, $4, COALESCE($5::date, CURRENT_DATE), 'pending', COALESCE($5::date, CURRENT_DATE) + INTERVAL '7 days')`,
        [uid, prospect_id, String(products_discussed || '').trim() || 'Sample', 'From visit log', call_date || null]);
    } catch (e) { console.error('[calls] sample from visit:', e.message); }
  }
  await pipeline.advance(pool, prospect_id, pipeline.stageForOutcome(outcome));
  res.json(result.rows[0]);
  } catch (err) {
    console.error('[calls] POST error:', err.message);
    res.status(500).json({ error: 'Failed to save call' });
  }
});

// PUT /api/calls/:id — fix a visit you logged (Kody: voice logs sometimes get
// the account or names wrong). You can move it to the right account and fix
// the notes, outcome and follow-up. If the visit had created a throwaway
// account (a misheard name) that now has nothing else on it, that account is
// archived so it doesn't linger as a duplicate.
router.put('/:id', async (req, res) => {
  const id = parseInt(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'Bad id' });
  const me = req.session.user;
  const cur = (await pool.query('SELECT * FROM calls WHERE id=$1', [id])).rows[0];
  if (!cur) return res.status(404).json({ error: 'Not found' });
  if (cur.user_id !== me.id) return res.status(403).json({ error: 'You can only edit visits you logged' });
  const b = req.body || {};
  const clip = (v, n) => (v == null ? null : String(v).slice(0, n));
  let pid = cur.prospect_id;
  if (b.prospect_id != null && !Number.isFinite(parseInt(b.prospect_id))) return res.status(400).json({ error: 'Pick an account' });
  if (b.prospect_id != null && parseInt(b.prospect_id) !== cur.prospect_id) {
    const t = await pool.query(
      `SELECT p.id FROM prospects p JOIN users u ON u.id = p.user_id WHERE p.id=$1 AND u.disabled_at IS NULL`, [parseInt(b.prospect_id)]);
    if (!t.rows.length) return res.status(400).json({ error: 'That account was not found' });
    pid = t.rows[0].id;
  }
  const nsd = b.next_step_date === '' ? null : (b.next_step_date !== undefined ? b.next_step_date : cur.next_step_date);
  if (nsd && !/^\d{4}-\d{2}-\d{2}$/.test(String(nsd).slice(0, 10))) return res.status(400).json({ error: 'Bad follow-up date' });
  const r = await pool.query(
    `UPDATE calls SET prospect_id=$2, outcome=$3, notes=$4, next_step=$5, next_step_date=$6
      WHERE id=$1 RETURNING *`,
    [id, pid,
     b.outcome !== undefined ? clip(b.outcome, 100) : cur.outcome,
     b.notes !== undefined ? clip(b.notes, 8000) : cur.notes,
     b.next_step !== undefined ? clip(b.next_step, 500) : cur.next_step,
     nsd ? String(nsd).slice(0, 10) : null]);
  let archived = null;
  if (pid !== cur.prospect_id) {
    await pipeline.advance(pool, pid, pipeline.stageForOutcome(r.rows[0].outcome));
    try {
      const old = (await pool.query(
        `SELECT p.* FROM prospects p WHERE p.id=$1 AND p.user_id=$2 AND p.source='Voice Logger'
            AND p.created_at > NOW() - INTERVAL '30 days'
            AND NOT EXISTS (SELECT 1 FROM calls WHERE prospect_id=p.id)
            AND NOT EXISTS (SELECT 1 FROM quotes WHERE prospect_id=p.id)
            AND NOT EXISTS (SELECT 1 FROM samples WHERE prospect_id=p.id)
            AND NOT EXISTS (SELECT 1 FROM contacts WHERE prospect_id=p.id)
            AND NOT EXISTS (SELECT 1 FROM shelf_photos WHERE prospect_id=p.id)
            AND NOT EXISTS (SELECT 1 FROM account_documents WHERE prospect_id=p.id)
            AND NOT EXISTS (SELECT 1 FROM prospects k WHERE k.parent_id=p.id)`, [cur.prospect_id, me.id])).rows[0];
      if (old) {
        // Copy first, then remove, in one transaction so nothing is lost either way.
        const c = await pool.connect();
        try {
          await c.query('BEGIN');
          await c.query(`INSERT INTO archived_records (table_name, original_id, data, reason) VALUES ('prospects',$1,$2,'emptied by a recap fix')`, [old.id, JSON.stringify(old)]);
          await c.query('DELETE FROM prospects WHERE id=$1', [old.id]);
          await c.query('COMMIT');
          archived = old.company;
        } catch (e) { await c.query('ROLLBACK').catch(() => {}); throw e; } finally { c.release(); }
      }
    } catch (e) { console.error('[calls] tidy old account:', e.message); }
  }
  res.json({ call: r.rows[0], archived_account: archived });
});

// Delete a note the rep added. Logged visits and calls stay: they're the
// team's history for reports and commissions.
router.delete('/:id', async (req, res) => {
  const id = parseInt(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'Bad id' });
  const r = await pool.query('SELECT user_id, call_type FROM calls WHERE id=$1', [id]);
  if (!r.rows.length) return res.status(404).json({ error: 'Not found' });
  if (r.rows[0].user_id !== req.session.user.id) return res.status(403).json({ error: 'You can only delete your own notes' });
  if (r.rows[0].call_type !== 'Note') return res.status(400).json({ error: "Logged visits and calls can't be deleted, only notes." });
  await pool.query('DELETE FROM calls WHERE id=$1', [id]);
  res.json({ success: true });
});

router.get('/today', async (req, res) => {
  const uid = req.session.user.id;
  const result = await pool.query(
    `SELECT c.*, p.company, p.city, p.phone as prospect_phone FROM calls c
     JOIN prospects p ON c.prospect_id=p.id
     WHERE c.user_id=$1 AND c.call_date=timezone($$America/New_York$$, NOW())::date`, [uid]);
  res.json(result.rows);
});

module.exports = router;
