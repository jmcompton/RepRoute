const express = require('express');
const { pool } = require('../db');
const router = express.Router();

router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const { category, status, search } = req.query;
  let query = 'SELECT * FROM prospects WHERE user_id=$1';
  const params = [uid];
  if (category && category !== 'All') { params.push(category); query += ` AND category=$${params.length}`; }
  if (status && status !== 'All') { params.push(status); query += ` AND status=$${params.length}`; }
  if (search) { params.push(`%${search}%`); query += ` AND (company ILIKE $${params.length} OR city ILIKE $${params.length} OR contact ILIKE $${params.length})`; }
  query += ' ORDER BY created_at DESC';
  const result = await pool.query(query, params);
  res.json(result.rows);
});

router.post('/', async (req, res) => {
  const uid = req.session.user.id;
  const { company, category, city, state, phone, contact, website, products, status, priority, notes, source } = req.body;
  const result = await pool.query(
    `INSERT INTO prospects (user_id, company, category, city, state, phone, contact, website, products, status, priority, notes, source)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *`,
    [uid, company, category, city || 'Atlanta', state || 'GA', phone, contact, website, products, status || 'New', priority || 'Medium', notes, source || 'Manual']
  );
  res.json(result.rows[0]);
});

router.put('/:id', async (req, res) => {
  const { status, priority, notes, contact, phone } = req.body;
  const result = await pool.query(
    'UPDATE prospects SET status=$1, priority=$2, notes=$3, contact=$4, phone=$5 WHERE id=$6 AND user_id=$7 RETURNING *',
    [status, priority, notes, contact, phone, req.params.id, req.session.user.id]
  );
  res.json(result.rows[0]);
});

router.delete('/:id', async (req, res) => {
  await pool.query('DELETE FROM prospects WHERE id=$1 AND user_id=$2', [req.params.id, req.session.user.id]);
  res.json({ success: true });
});

module.exports = router;
