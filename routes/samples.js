const express = require('express');
const { pool } = require('../db');
const router = express.Router();

// Reps see and change their own samples; managers see the team.
function scope(req, alias, params) {
  const me = req.session.user;
  if (me.role === 'manager') return 'TRUE';
  params.push(me.id);
  return `${alias}.user_id = $${params.length}`;
}

router.get('/', async (req, res) => {
  try {
    const params = [];
    const where = scope(req, 's', params);
    const result = await pool.query(`
      SELECT s.*, u.name as rep_name, p.company, p.city
      FROM samples s
      JOIN users u ON s.user_id = u.id
      JOIN prospects p ON s.prospect_id = p.id
      WHERE ${where}
      ORDER BY s.sent_date DESC
    `, params);
    res.json(result.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.post('/', async (req, res) => {
  const { prospect_id, product_line, quantity, notes, sent_date } = req.body;
  const userId = req.session.user.id;
  if (!parseInt(prospect_id) || !String(product_line || '').trim()) return res.status(400).json({ error: 'Pick an account and a product' });
  try {
    const result = await pool.query(`
      INSERT INTO samples (user_id, prospect_id, product_line, quantity, notes, sent_date, status, follow_up_date)
      VALUES ($1,$2,$3,$4,$5,COALESCE($6::date, CURRENT_DATE),'pending', COALESCE($6::date, CURRENT_DATE) + INTERVAL '7 days')
      RETURNING *`,
      [userId, prospect_id, product_line, quantity || 1, notes || '', sent_date || null]
    );
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.put('/:id/status', async (req, res) => {
  const { status, outcome_notes } = req.body;
  try {
    const result = await pool.query(`
      UPDATE samples SET status=$1, outcome_notes=$2, closed_at=NOW()
      WHERE id=$3 AND (user_id=$4 OR $5) RETURNING *`,
      [status, outcome_notes || '', parseInt(req.params.id) || 0, req.session.user.id, req.session.user.role === 'manager']
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Sample not found' });
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.delete('/:id', async (req, res) => {
  try {
    const r = await pool.query('DELETE FROM samples WHERE id=$1 AND (user_id=$2 OR $3)',
      [parseInt(req.params.id) || 0, req.session.user.id, req.session.user.role === 'manager']);
    if (!r.rowCount) return res.status(404).json({ error: 'Sample not found' });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.get('/overdue', async (req, res) => {
  try {
    const params = [];
    const where = scope(req, 's', params);
    const result = await pool.query(`
      SELECT s.*, u.name as rep_name, p.company, p.city
      FROM samples s
      JOIN users u ON s.user_id = u.id
      JOIN prospects p ON s.prospect_id = p.id
      WHERE s.status = 'pending' AND s.follow_up_date <= CURRENT_DATE AND ${where}
      ORDER BY s.follow_up_date ASC
    `, params);
    res.json(result.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

module.exports = router;
