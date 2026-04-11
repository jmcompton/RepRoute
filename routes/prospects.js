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
  if (search) { params.push('%' + search + '%'); query += ` AND (company ILIKE $${params.length} OR city ILIKE $${params.length} OR contact ILIKE $${params.length})`; }
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
  const uid = req.session.user.id;
  const id = req.params.id;
  const { company, category, city, state, phone, email, contact, website, products, status, priority, notes, pipeline_stage } = req.body;

  // Build dynamic update — only update fields that were sent
  const fields = [];
  const vals = [];
  const add = (col, val) => { if (val !== undefined) { vals.push(val); fields.push(col + '=$' + vals.length); } };

  add('company', company);
  add('category', category);
  add('city', city);
  add('state', state);
  add('phone', phone);
  add('email', email);
  add('contact', contact);
  add('website', website);
  add('products', products);
  add('status', status);
  add('priority', priority);
  add('notes', notes);
  add('pipeline_stage', pipeline_stage);

  if (fields.length === 0) return res.json({ error: 'Nothing to update' });

  vals.push(id);
  vals.push(uid);
  const result = await pool.query(
    'UPDATE prospects SET ' + fields.join(', ') + ' WHERE id=$' + (vals.length - 1) + ' AND user_id=$' + vals.length + ' RETURNING *',
    vals
  );
  res.json(result.rows[0] || { error: 'Not found' });
});

router.delete('/:id', async (req, res) => {
  await pool.query('DELETE FROM prospects WHERE id=$1 AND user_id=$2', [req.params.id, req.session.user.id]);
  res.json({ success: true });
});

module.exports = router;
