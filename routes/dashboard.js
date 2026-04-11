const express = require('express');
const path = require('path');
const { pool } = require('../db');
const router = express.Router();

router.get('/', async (req, res) => {
  res.sendFile(path.join(__dirname, '/../views/app.html'));
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

    // Follow-up reminders
    const followUps = await pool.query(
      `SELECT c.id, c.next_step, c.next_step_date, c.prospect_id,
              p.company, p.category, p.city, p.phone,
              CASE
                WHEN c.next_step_date < CURRENT_DATE THEN 'overdue'
                WHEN c.next_step_date = CURRENT_DATE THEN 'today'
                ELSE 'upcoming'
              END as urgency,
              (CURRENT_DATE - c.next_step_date) as days_overdue
       FROM calls c
       JOIN prospects p ON c.prospect_id = p.id
       WHERE c.user_id = $1
         AND c.next_step_date IS NOT NULL
         AND c.next_step_date <= CURRENT_DATE + 1
         AND c.next_step IS NOT NULL
         AND c.next_step != ''
       ORDER BY c.next_step_date ASC
       LIMIT 20`, [uid]);

    res.json({
      user: req.session.user,
      stats: {
        prospects: prospects.rows[0].count,
        hot: hot.rows[0].count,
        calls: calls.rows[0].count,
        followUps: followUps.rows.filter(f => f.urgency !== 'upcoming').length
      },
      recentCalls: recentCalls.rows,
      hotProspects: hotProspects.rows,
      followUps: followUps.rows
    });
  } catch (e) {
    res.json({ error: e.message });
  }
});

module.exports = router;
