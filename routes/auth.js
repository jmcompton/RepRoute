const express = require('express');
const bcrypt = require('bcryptjs');
const { pool } = require('../db');
const router = express.Router();

router.get('/', (req, res) => {
  if (req.session.user) return res.redirect('/app');
  res.sendFile(__dirname + '/../views/login.html');
});

router.post('/login', async (req, res) => {
  const { email, password } = req.body;
  try {
    const result = await pool.query('SELECT * FROM users WHERE email=$1', [email]);
    const user = result.rows[0];
    if (!user || !await bcrypt.compare(password, user.password))
      return res.json({ error: 'Invalid email or password' });
    req.session.user = { id: user.id, name: user.name, email: user.email, role: user.role, territory: user.territory };
    res.json({ success: true, role: user.role });
  } catch (e) {
    res.json({ error: 'Login failed' });
  }
});

router.post('/register', async (req, res) => {
  const { name, email, password, role, territory } = req.body;
  try {
    const hash = await bcrypt.hash(password, 10);
    const result = await pool.query(
      'INSERT INTO users (name, email, password, role, territory) VALUES ($1,$2,$3,$4,$5) RETURNING *',
      [name, email, hash, role || 'rep', territory || 'Atlanta Metro']
    );
    const user = result.rows[0];
    req.session.user = { id: user.id, name: user.name, email: user.email, role: user.role, territory: user.territory };
    res.json({ success: true, role: user.role });
  } catch (e) {
    if (e.code === '23505') return res.json({ error: 'Email already registered' });
    res.json({ error: 'Registration failed' });
  }
});

router.post('/logout', (req, res) => {
  req.session.destroy(() => res.json({ success: true }));
});

module.exports = router;
