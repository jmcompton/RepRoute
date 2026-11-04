const express = require('express');
const { pool } = require('../db');
const router = express.Router();

router.get('/reps', async (req, res) => {
  const result = await pool.query(
    `SELECT u.id, u.name, u.email, u.territory, u.created_at, u.disabled_at,
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
  const { name, email, territory } = req.body;
  // No shared default password: use the one given (8+ chars) or make one up.
  let password = String(req.body.password || '');
  let generated = false;
  if (password.length < 8) { password = require('crypto').randomBytes(6).toString('base64url'); generated = true; }
  const hash = await bcrypt.hash(password, 10);
  try {
    const result = await pool.query(
      'INSERT INTO users (name, email, password, role, territory) VALUES ($1,$2,$3,$4,$5) RETURNING id, name, email, territory',
      [name, email, hash, 'rep', territory || 'Atlanta Metro']
    );
    res.json({ success: true, rep: result.rows[0], temp_password: generated ? password : undefined });
  } catch (e) {
    res.json({ error: e.code === '23505' ? 'Email already exists' : e.message });
  }
});

// Turn a rep's login off (they left) or back on. History stays in place.
router.post('/reps/:id/access', async (req, res) => {
  const id = parseInt(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'Bad rep id' });
  if (id === req.session.user.id) return res.status(400).json({ error: "You can't turn off your own login" });
  const on = req.body && req.body.enabled !== false;
  const r = await pool.query('UPDATE users SET disabled_at = ' + (on ? 'NULL' : 'NOW()') + ' WHERE id=$1 RETURNING id, name, disabled_at', [id]);
  if (!r.rows.length) return res.status(404).json({ error: 'Rep not found' });
  if (req.app.locals.refreshDisabled) req.app.locals.refreshDisabled();
  res.json({ success: true, rep: r.rows[0] });
});

// Set a new password for a rep who's locked out. Returns it once so the manager can text it.
router.post('/reps/:id/password', async (req, res) => {
  const bcrypt = require('bcryptjs');
  const id = parseInt(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'Bad rep id' });
  let password = String((req.body && req.body.password) || '');
  if (password.length < 8) password = require('crypto').randomBytes(6).toString('base64url');
  const r = await pool.query('UPDATE users SET password=$1 WHERE id=$2 RETURNING id, name', [await bcrypt.hash(password, 10), id]);
  if (!r.rows.length) return res.status(404).json({ error: 'Rep not found' });
  res.json({ success: true, rep: r.rows[0], password });
});

module.exports = router;
