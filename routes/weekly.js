const express = require('express');
const { pool } = require('../db');
const router = express.Router();

router.get('/latest', async (req, res) => {
  const uid = req.session.user.id;
  const result = await pool.query(
    'SELECT * FROM weekly_plans WHERE user_id=$1 ORDER BY created_at DESC LIMIT 1', [uid]);
  if (!result.rows[0]) return res.json({ plan: null });
  res.json({ plan: JSON.parse(result.rows[0].plan_json) });
});

module.exports = router;
