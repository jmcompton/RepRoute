const express = require('express');
const { pool } = require('../db');
const router = express.Router();

router.get('/reps', async (req, res) => {
  const result = await pool.query(
    `SELECT u.id, u.name, u.email, u.territory, u.created_at,
      (SELECT COUNT(*) FROM prospects WHERE user_id=u.id) as prospect_count,
      (SELECT COUNT(*) FROM calls WHERE user_id=u.id) as call_count,
      (SELECT COUNT(*) FROM calls WHERE user_id=u.id AND call_date >= CURRENT_DATE - INTERVAL '7 days') as calls_this_week,
      (SELECT COUNT(*) FROM prospects WHERE user_id=u.id AND status='Hot') as hot_count
     FROM users u WHERE u.role='rep' ORDER BY u.created_at DESC`);
  res.json(result.rows);
});

router.get('/activity', async (req, res) => {
  const result = await pool.query(
    `SELECT c.*, u.name as rep_name, p.company, p.category, p.city
     FROM calls c JOIN users u ON c.user_id=u.id JOIN prospects p ON c.prospect_id=p.id
     ORDER BY c.created_at DESC LIMIT 20`);
  res.json(result.rows);
});

router.post('/add-rep', async (req, res) => {
  const bcrypt = require('bcryptjs');
  const { name, email, password, territory } = req.body;
  const hash = await bcrypt.hash(password || 'reproute2025', 10);
  try {
    const result = await pool.query(
      'INSERT INTO users (name, email, password, role, territory) VALUES ($1,$2,$3,$4,$5) RETURNING id, name, email, territory',
      [name, email, hash, 'rep', territory || 'Atlanta Metro']
    );
    res.json({ success: true, rep: result.rows[0] });
  } catch (e) {
    res.json({ error: e.code === '23505' ? 'Email already exists' : e.message });
  }
});

module.exports = router;
