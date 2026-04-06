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
  const result = await pool.query(
    `INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, products_discussed, next_step, next_step_date, notes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
    [uid, prospect_id, call_date, call_type, outcome, products_discussed, next_step, next_step_date || null, notes]
  );
  // Update prospect status if outcome provided
  if (outcome) {
    const status = outcome === 'Not Interested' ? 'Cold' : outcome === 'Interested' ? 'Warm' : outcome === 'Ready to Buy' ? 'Hot' : null;
    if (status) await pool.query('UPDATE prospects SET status=$1 WHERE id=$2', [status, prospect_id]);
  }
  res.json(result.rows[0]);
});

router.get('/today', async (req, res) => {
  const uid = req.session.user.id;
  const result = await pool.query(
    `SELECT c.*, p.company, p.city, p.phone as prospect_phone FROM calls c
     JOIN prospects p ON c.prospect_id=p.id
     WHERE c.user_id=$1 AND c.call_date=CURRENT_DATE`, [uid]);
  res.json(result.rows);
});

module.exports = router;
