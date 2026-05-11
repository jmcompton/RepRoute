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

router.delete('/delete-user/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM users WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.json({ error: e.message }); }
});


// ONE-TIME: Safe data wipe for a specific user by email
// Deletes prospects + calls for that user only, touches nothing else
router.post('/wipe-demo-data', async (req, res) => {
  const { target_email, confirm } = req.body;
  if (confirm !== 'YES_DELETE_MY_DATA') {
    return res.json({ error: 'Must pass confirm: YES_DELETE_MY_DATA' });
  }
  try {
    // Find the user
    const uRes = await pool.query('SELECT id, name, email FROM users WHERE email=$1', [target_email]);
    if (!uRes.rows.length) return res.json({ error: 'User not found: ' + target_email });
    const uid = uRes.rows[0].id;
    const uname = uRes.rows[0].name;

    // Count before delete
    const pc = await pool.query('SELECT COUNT(*) FROM prospects WHERE user_id=$1', [uid]);
    const cc = await pool.query('SELECT COUNT(*) FROM calls WHERE user_id=$1', [uid]);
    const nc = await pool.query('SELECT COUNT(*) FROM notifications WHERE user_id=$1', [uid]);

    // Delete in dependency order
    await pool.query('DELETE FROM notifications WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM samples WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM email_logs WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM weekly_plans WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM onboarding_plans WHERE rep_id=$1', [uid]);
    await pool.query('DELETE FROM calendar_events WHERE user_id=$1', [uid]);
    // Delete calls first (FK to prospects)
    await pool.query('DELETE FROM calls WHERE user_id=$1', [uid]);
    // Delete prospects (FK from calls already gone)
    await pool.query('DELETE FROM prospects WHERE user_id=$1', [uid]);

    res.json({
      ok: true,
      user: uname,
      email: target_email,
      deleted: {
        prospects: parseInt(pc.rows[0].count),
        calls: parseInt(cc.rows[0].count),
        notifications: parseInt(nc.rows[0].count)
      }
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});


// Self-service: wipe YOUR OWN demo data (uses session auth, only deletes your records)
router.post('/wipe-my-data', async (req, res) => {
  if (!req.session || !req.session.user) return res.status(401).json({ error: 'Not logged in' });
  const uid = req.session.user.id;
  const uname = req.session.user.name;
  const { confirm } = req.body;
  if (confirm !== 'YES_DELETE_MY_DATA') {
    return res.json({ error: 'Pass confirm: YES_DELETE_MY_DATA' });
  }
  try {
    const pc = await pool.query('SELECT COUNT(*) FROM prospects WHERE user_id=$1', [uid]);
    const cc = await pool.query('SELECT COUNT(*) FROM calls WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM notifications WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM samples WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM email_logs WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM weekly_plans WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM onboarding_plans WHERE rep_id=$1', [uid]);
    await pool.query('DELETE FROM calendar_events WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM calls WHERE user_id=$1', [uid]);
    await pool.query('DELETE FROM prospects WHERE user_id=$1', [uid]);
    console.log(`Data wiped for user ${uname} (id=${uid}): ${pc.rows[0].count} prospects, ${cc.rows[0].count} calls`);
    res.json({ ok: true, deleted: { prospects: parseInt(pc.rows[0].count), calls: parseInt(cc.rows[0].count) } });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;