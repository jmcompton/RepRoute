const express = require('express');
const { pool } = require('../db');
const router = express.Router();

router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const result = await pool.query(
    `SELECT c.*, p.company, p.category, p.city, p.phone as prospect_phone
     FROM calls c JOIN prospects p ON c.prospect_id=p.id
     WHERE c.user_id=$1 ORDER BY c.call_date DESC`, [uid]);
  res.json(result.rows);
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
  res.json(result.rows[0]);
  } catch (err) {
    console.error('[calls] POST error:', err.message);
    res.status(500).json({ error: 'Failed to save call' });
  }
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
