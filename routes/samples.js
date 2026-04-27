const express = require('express');
const { pool } = require('../db');
const router = express.Router();

router.get('/', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT s.*, u.name as rep_name, p.company, p.city
      FROM samples s
      JOIN users u ON s.user_id = u.id
      JOIN prospects p ON s.prospect_id = p.id
      ORDER BY s.sent_date DESC
    `);
    res.json(result.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.post('/', async (req, res) => {
  const { prospect_id, product_line, quantity, notes, sent_date } = req.body;
  const userId = req.session.user.id;
  try {
    const result = await pool.query(`
      INSERT INTO samples (user_id, prospect_id, product_line, quantity, notes, sent_date, status, follow_up_date)
      VALUES ($1,$2,$3,$4,$5,$6,'pending', $6::date + INTERVAL '7 days')
      RETURNING *`,
      [userId, prospect_id, product_line, quantity || 1, notes || '', sent_date || new Date().toISOString().split('T')[0]]
    );
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.put('/:id/status', async (req, res) => {
  const { status, outcome_notes } = req.body;
  try {
    const result = await pool.query(`
      UPDATE samples SET status=$1, outcome_notes=$2, closed_at=NOW()
      WHERE id=$3 RETURNING *`,
      [status, outcome_notes || '', req.params.id]
    );
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.delete('/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM samples WHERE id=$1', [req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.get('/overdue', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT s.*, u.name as rep_name, p.company, p.city
      FROM samples s
      JOIN users u ON s.user_id = u.id
      JOIN prospects p ON s.prospect_id = p.id
      WHERE s.status = 'pending' AND s.follow_up_date <= NOW()
      ORDER BY s.follow_up_date ASC
    `);
    res.json(result.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

module.exports = router;
