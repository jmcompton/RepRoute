const express = require('express');
const bcrypt = require('bcryptjs');
const { pool } = require('../db');
const router = express.Router();

router.get('/', (req, res) => {
  if (req.session.user) return res.redirect('/app');
  res.sendFile(__dirname + '/../views/login.html');
});

// Simple in-memory brake on password guessing: 8 misses per email+IP in 15
// minutes locks that pair for the rest of the window. One server, so memory is fine.
const FAILS = new Map();
const WINDOW_MS = 15 * 60 * 1000, MAX_FAILS = 8;
function failKey(req, email) { return (req.ip || '') + '|' + String(email || '').toLowerCase().trim(); }
function locked(key) {
  const f = FAILS.get(key);
  if (!f) return false;
  if (Date.now() - f.first > WINDOW_MS) { FAILS.delete(key); return false; }
  return f.count >= MAX_FAILS;
}
function noteFail(key) {
  const f = FAILS.get(key);
  if (!f || Date.now() - f.first > WINDOW_MS) FAILS.set(key, { first: Date.now(), count: 1 });
  else f.count++;
}

router.post('/login', async (req, res) => {
  const { email, password } = req.body;
  const key = failKey(req, email);
  if (locked(key)) return res.status(429).json({ error: 'Too many tries. Wait 15 minutes or ask Sean to reset your password.' });
  try {
    const result = await pool.query('SELECT * FROM users WHERE LOWER(email)=LOWER($1)', [String(email || '').trim()]);
    const user = result.rows[0];
    if (!user || !password || !await bcrypt.compare(password, user.password)) {
      noteFail(key);
      return res.json({ error: 'Invalid email or password' });
    }
    if (user.disabled_at) return res.json({ error: 'This login has been turned off. Talk to your manager.' });
    FAILS.delete(key);
    // New session id on login so a pre-login cookie can't be reused.
    req.session.regenerate(err => {
      if (err) return res.json({ error: 'Login failed' });
      req.session.user = { id: user.id, name: user.name, email: user.email, role: user.role, territory: user.territory };
      req.session.save(() => res.json({ success: true, role: user.role }));
    });
  } catch (e) {
    res.json({ error: 'Login failed' });
  }
});

// Public sign-up is closed: anyone could create a rep login and read the
// team's accounts. Managers add reps from the app (Team > Add rep).
router.post('/register', (req, res) => {
  res.status(403).json({ error: 'Sign-up is closed. Ask your manager to add you.' });
});

router.post('/logout', (req, res) => {
  req.session.destroy(() => res.json({ success: true }));
});

module.exports = router;
