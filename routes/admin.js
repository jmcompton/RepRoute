const express = require('express');
const bcrypt = require('bcryptjs');
const { pool } = require('../db');
const router = express.Router();

router.get('/requests', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM access_requests ORDER BY created_at DESC');
    res.json(r.rows);
  } catch(e) { res.json([]); }
});

router.get('/users', async (req, res) => {
  try {
    const r = await pool.query('SELECT id, name, email, role, territory, created_at FROM users ORDER BY created_at ASC');
    res.json(r.rows);
  } catch(e) { res.json([]); }
});

router.patch('/requests/:id', async (req, res) => {
  const { status } = req.body;
  try {
    await pool.query('UPDATE access_requests SET status=$1 WHERE id=$2', [status, req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.json({ error: e.message }); }
});

router.post('/create-user', async (req, res) => {
  const { name, email, password, role, territory } = req.body;
  try {
    const hash = await bcrypt.hash(password, 10);
    await pool.query(
      'INSERT INTO users (name, email, password, role, territory) VALUES ($1,$2,$3,$4,$5)',
      [name, email, hash, role || 'rep', territory || 'Atlanta Metro']
    );
    res.json({ ok: true });
  } catch(e) {
    if (e.code === '23505') return res.json({ error: 'Email already exists' });
    res.json({ error: e.message });
  }
});

module.exports = router;