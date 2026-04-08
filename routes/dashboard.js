const express = require('express');
const { pool } = require('../db');
const router = express.Router();

router.get('/', async (req, res) => {
  res.sendFile(__dirname + '/../views/app.html');
});

router.get('/data', async (req, res) => {
  const uid = req.session.user.id;
  try {
    const prospects = await pool.query('SELECT COUNT(*) FROM prospects WHERE user_id=$1', [uid]);
    const hot = await pool.query("SELECT COUNT(*) FROM prospects WHERE user_id=$1 AND status='Hot'", [uid]);
    const calls = await pool.query('SELECT COUNT(*) FROM calls WHERE user_id=$1', [uid]);
    const recentCalls = await pool.query(
      `SELECT c.*, p.company, p.category, p.city FROM calls c
       JOIN prospects p ON c.prospect_id=p.id
       WHERE c.user_id=$1 ORDER BY c.created_at DESC LIMIT 5`, [uid]);
    const hotProspects = await pool.query(
      "SELECT * FROM prospects WHERE user_id=$1 AND status='Hot' ORDER BY created_at DESC LIMIT 6", [uid]);
    res.json({ user: req.session.user,
      stats: {
        prospects: prospects.rows[0].count,
        hot: hot.rows[0].count,
        calls: calls.rows[0].count
      },
      recentCalls: recentCalls.rows,
      hotProspects: hotProspects.rows
    });
  } catch (e) {
    res.json({ error: e.message });
  }
});

module.exports = router;
