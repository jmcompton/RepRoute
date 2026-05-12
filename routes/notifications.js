const express = require('express');
const { pool } = require('../db');
const router = express.Router();

// Core evaluation engine — scans database and creates notifications
async function evaluateForUser(userId) {
  const created = [];

  // RULE 1: Follow-ups due today (today urgency)
  const dueToday = await pool.query(`
    SELECT DISTINCT ON (p.id) p.id, p.company, c.next_step, c.next_step_date
    FROM calls c
    JOIN prospects p ON c.prospect_id = p.id
    WHERE c.user_id = $1
      AND c.next_step_date = CURRENT_DATE
      AND c.next_step IS NOT NULL AND c.next_step != ''
    ORDER BY p.id, c.next_step_date DESC
  `, [userId]);

  for (const row of dueToday.rows) {
    const key = `followup_today_${row.id}_${new Date().toISOString().split('T')[0]}`;
    try {
      const r = await pool.query(`
        INSERT INTO notifications (user_id, prospect_id, type, urgency, title, body, action_url, unique_key)
        VALUES ($1, $2, 'followup_today', 'today', $3, $4, $5, $6)
        ON CONFLICT (unique_key) DO NOTHING
        RETURNING id
      `, [userId, row.id, `Follow-up due today: ${row.company}`, row.next_step, `/app#prospect/${row.id}`, key]);
      if (r.rows.length) created.push({ type: 'followup_today', company: row.company });
    } catch(e) { console.error('notif insert error:', e.message); }
  }

  // RULE 2: Follow-ups overdue 2+ days (urgent)
  const overdue = await pool.query(`
    SELECT DISTINCT ON (p.id) p.id, p.company, c.next_step, c.next_step_date,
           (CURRENT_DATE - c.next_step_date) as days_overdue
    FROM calls c
    JOIN prospects p ON c.prospect_id = p.id
    WHERE c.user_id = $1
      AND c.next_step_date < CURRENT_DATE - INTERVAL '1 day'
      AND c.next_step IS NOT NULL AND c.next_step != ''
      AND NOT EXISTS (
        SELECT 1 FROM calls c2
        WHERE c2.prospect_id = p.id
          AND c2.user_id = $1
          AND c2.call_date > c.next_step_date
      )
    ORDER BY p.id, c.next_step_date DESC
  `, [userId]);

  for (const row of overdue.rows) {
    const key = `followup_overdue_${row.id}_${row.next_step_date.toISOString().split('T')[0]}`;
    try {
      const r = await pool.query(`
        INSERT INTO notifications (user_id, prospect_id, type, urgency, title, body, action_url, unique_key)
        VALUES ($1, $2, 'followup_overdue', 'urgent', $3, $4, $5, $6)
        ON CONFLICT (unique_key) DO NOTHING
        RETURNING id
      `, [userId, row.id,
          `URGENT: ${row.company} — ${row.days_overdue} days overdue`,
          row.next_step,
          `/app#prospect/${row.id}`,
          key]);
      if (r.rows.length) created.push({ type: 'followup_overdue', company: row.company });
    } catch(e) { console.error(e.message); }
  }

  // RULE 3: Hot leads going cold (no contact 7+ days, status=Hot)
  const goingCold = await pool.query(`
    SELECT p.id, p.company,
           COALESCE(MAX(c.call_date), p.created_at::date) as last_touch,
           CURRENT_DATE - COALESCE(MAX(c.call_date), p.created_at::date) as days_silent
    FROM prospects p
    LEFT JOIN calls c ON c.prospect_id = p.id AND c.user_id = $1
    WHERE p.user_id = $1 AND p.status = 'Hot'
    GROUP BY p.id, p.company, p.created_at
    HAVING CURRENT_DATE - COALESCE(MAX(c.call_date), p.created_at::date) >= 7
  `, [userId]);

  for (const row of goingCold.rows) {
    const key = `hot_cold_${row.id}_${new Date().toISOString().split('T')[0].slice(0,7)}`;
    try {
      const r = await pool.query(`
        INSERT INTO notifications (user_id, prospect_id, type, urgency, title, body, action_url, unique_key)
        VALUES ($1, $2, 'hot_cold', 'urgent', $3, $4, $5, $6)
        ON CONFLICT (unique_key) DO NOTHING
        RETURNING id
      `, [userId, row.id,
          `Hot lead going cold: ${row.company}`,
          `No contact in ${row.days_silent} days. Don't lose this one.`,
          `/app#prospect/${row.id}`,
          key]);
      if (r.rows.length) created.push({ type: 'hot_cold', company: row.company });
    } catch(e) { console.error(e.message); }
  }

  // RULE 4: Long-silent prospects (30+ days, FYI only)
  const longSilent = await pool.query(`
    SELECT p.id, p.company,
           CURRENT_DATE - COALESCE(MAX(c.call_date), p.created_at::date) as days_silent
    FROM prospects p
    LEFT JOIN calls c ON c.prospect_id = p.id AND c.user_id = $1
    WHERE p.user_id = $1 AND p.status != 'Cold'
    GROUP BY p.id, p.company, p.created_at
    HAVING CURRENT_DATE - COALESCE(MAX(c.call_date), p.created_at::date) >= 30
       AND CURRENT_DATE - COALESCE(MAX(c.call_date), p.created_at::date) < 60
    LIMIT 5
  `, [userId]);

  for (const row of longSilent.rows) {
    const key = `long_silent_${row.id}_${new Date().toISOString().split('T')[0].slice(0,7)}`;
    try {
      const r = await pool.query(`
        INSERT INTO notifications (user_id, prospect_id, type, urgency, title, body, action_url, unique_key)
        VALUES ($1, $2, 'long_silent', 'fyi', $3, $4, $5, $6)
        ON CONFLICT (unique_key) DO NOTHING
        RETURNING id
      `, [userId, row.id,
          `${row.company} hasn't been touched in ${row.days_silent} days`,
          `Worth a check-in?`,
          `/app#prospect/${row.id}`,
          key]);
      if (r.rows.length) created.push({ type: 'long_silent', company: row.company });
    } catch(e) { console.error(e.message); }
  }


  // RULE 5: Quote follow-ups due today or overdue
  const quoteDue = await pool.query(`
    SELECT id, account_name, follow_up_date, status, quote_number,
           (CURRENT_DATE - follow_up_date::date) as days_overdue
    FROM quotes
    WHERE user_id = $1
      AND follow_up_date IS NOT NULL
      AND follow_up_date::date <= CURRENT_DATE
      AND status NOT IN ('Won', 'Lost')
  `, [userId]);

  for (const row of quoteDue.rows) {
    const dateStr = row.follow_up_date.toISOString ? row.follow_up_date.toISOString().split('T')[0] : String(row.follow_up_date).split('T')[0];
    const overdue = parseInt(row.days_overdue) || 0;
    const urgency = overdue > 0 ? 'urgent' : 'today';
    const qnum = row.quote_number || ('QT-' + String(row.id).padStart(3, '0'));
    const key = `quote_followup_${row.id}_${dateStr}`;
    const title = overdue > 0
      ? `Quote follow-up ${overdue} day${overdue !== 1 ? 's' : ''} overdue: ${row.account_name}`
      : `Quote follow-up due today: ${row.account_name}`;
    try {
      const r = await pool.query(`
        INSERT INTO notifications (user_id, type, urgency, title, body, action_url, unique_key)
        VALUES ($1, 'quote_followup', $2, $3, $4, '/app#quotes', $5)
        ON CONFLICT (unique_key) DO NOTHING
        RETURNING id
      `, [userId, urgency, title, `${qnum} — Status: ${row.status}`, key]);
      if (r.rows.length) created.push({ type: 'quote_followup', company: row.account_name });
    } catch(e) { console.error('quote notif error:', e.message); }
  }

  return created;
}

// Run evaluation for current user
router.post('/evaluate', async (req, res) => {
  try {
    const created = await evaluateForUser(req.session.user.id);
    res.json({ ok: true, created: created.length, items: created });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Get all unread for current user
router.get('/', async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT id, prospect_id, type, urgency, title, body, action_url, status, created_at
      FROM notifications
      WHERE user_id = $1 AND status = 'unread'
      ORDER BY 
        CASE urgency WHEN 'urgent' THEN 1 WHEN 'today' THEN 2 ELSE 3 END,
        created_at DESC
      LIMIT 50
    `, [req.session.user.id]);

    const counts = await pool.query(`
      SELECT urgency, COUNT(*) as count
      FROM notifications
      WHERE user_id = $1 AND status = 'unread'
      GROUP BY urgency
    `, [req.session.user.id]);

    const countMap = { urgent: 0, today: 0, fyi: 0 };
    counts.rows.forEach(c => countMap[c.urgency] = parseInt(c.count));

    res.json({ ok: true, notifications: r.rows, counts: countMap, total: r.rows.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Mark single notification as read
router.post('/:id/read', async (req, res) => {
  try {
    await pool.query(
      `UPDATE notifications SET status='read', read_at=NOW() WHERE id=$1 AND user_id=$2`,
      [req.params.id, req.session.user.id]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Dismiss notification
router.post('/:id/dismiss', async (req, res) => {
  try {
    await pool.query(
      `UPDATE notifications SET status='dismissed', dismissed_at=NOW() WHERE id=$1 AND user_id=$2`,
      [req.params.id, req.session.user.id]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Mark all as read
router.post('/read-all', async (req, res) => {
  try {
    await pool.query(
      `UPDATE notifications SET status='read', read_at=NOW() WHERE user_id=$1 AND status='unread'`,
      [req.session.user.id]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

module.exports = { router, evaluateForUser };
