const express = require('express');
const { pool } = require('../db');
const router = express.Router();


// Follow-ups set on logged visits ("come back early December") show on the
// rep's calendar that day. One per account: a later visit replaces it.
async function followUpEvents(userId) {
  const r = await pool.query(
    `SELECT DISTINCT ON (c.prospect_id) c.id, c.prospect_id, c.next_step, c.next_step_date::text AS d, p.company, p.city
       FROM calls c JOIN prospects p ON p.id = c.prospect_id
      WHERE c.user_id = $1 AND c.next_step_date IS NOT NULL
        AND c.next_step_date >= (timezone('America/New_York', NOW())::date - 30)
      ORDER BY c.prospect_id, c.call_date DESC, c.id DESC`, [userId]);
  return r.rows.filter(x => x.d).map(x => ({
    id: 'fu-' + x.id, user_id: userId, title: 'Follow up: ' + x.company,
    description: x.next_step || '', location: x.city || '',
    start_time: x.d, end_time: null, all_day: true, event_type: 'followup',
    creator_name: 'Visit log', prospect_id: x.prospect_id, readonly: true
  }));
}

router.get('/events', async (req, res) => {
  const { filter } = req.query;
  const userId = req.session.user.id;
  try {
    let result;
    if (filter === 'mine') {
      result = await pool.query(
        `SELECT e.*, u.name as creator_name FROM calendar_events e JOIN users u ON e.user_id = u.id WHERE e.user_id = $1 ORDER BY e.start_time ASC`,
        [userId]
      );
    } else {
      result = await pool.query(
        `SELECT e.*, u.name as creator_name FROM calendar_events e JOIN users u ON e.user_id = u.id ORDER BY e.start_time ASC`
      );
    }
    let followUps = [];
    try { followUps = await followUpEvents(userId); } catch (e) { console.error('[calendar] follow-ups:', e.message); }
    res.json(result.rows.concat(followUps));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.post('/events', async (req, res) => {
  const { title, description, start_time, end_time, event_type, location } = req.body;
  const userId = req.session.user.id;
  try {
    const result = await pool.query(
      `INSERT INTO calendar_events (user_id, title, description, start_time, end_time, event_type, location) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [userId, title, description || '', start_time, end_time, event_type || 'general', location || '']
    );
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.put('/events/:id', async (req, res) => {
  const { title, description, start_time, end_time, event_type, location } = req.body;
  const userId = req.session.user.id;
  const role = req.session.user.role;
  try {
    const check = await pool.query('SELECT * FROM calendar_events WHERE id=$1', [req.params.id]);
    if (!check.rows[0]) return res.status(404).json({ error: 'Event not found' });
    if (check.rows[0].user_id !== userId && role !== 'manager') return res.status(403).json({ error: 'Not authorized' });
    const result = await pool.query(
      `UPDATE calendar_events SET title=$1, description=$2, start_time=$3, end_time=$4, event_type=$5, location=$6 WHERE id=$7 RETURNING *`,
      [title, description, start_time, end_time, event_type, location, req.params.id]
    );
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.delete('/events/:id', async (req, res) => {
  const userId = req.session.user.id;
  const role = req.session.user.role;
  try {
    const check = await pool.query('SELECT * FROM calendar_events WHERE id=$1', [req.params.id]);
    if (!check.rows[0]) return res.status(404).json({ error: 'Event not found' });
    if (check.rows[0].user_id !== userId && role !== 'manager') return res.status(403).json({ error: 'Not authorized' });
    await pool.query('DELETE FROM calendar_events WHERE id=$1', [req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

router.get('/feed/:token', async (req, res) => {
  try {
    const userResult = await pool.query('SELECT * FROM users WHERE ical_token=$1', [req.params.token]);
    if (!userResult.rows[0]) return res.status(404).send('Feed not found');
    const eventsResult = await pool.query(
      `SELECT e.*, u.name as creator_name FROM calendar_events e JOIN users u ON e.user_id = u.id ORDER BY e.start_time ASC`
    );
    const events = eventsResult.rows;
    let ical = ['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//Compton Sales//RepRoute//EN','X-WR-CALNAME:Compton Sales Calendar','X-WR-TIMEZONE:America/Chicago','CALSCALE:GREGORIAN','METHOD:PUBLISH'];
    for (const event of events) {
      ical.push('BEGIN:VEVENT');
      ical.push(`UID:event-${event.id}@comptongroupllc.com`);
      ical.push(`DTSTAMP:${formatICalDate(event.created_at)}`);
      ical.push(`DTSTART:${formatICalDate(event.start_time)}`);
      ical.push(`DTEND:${formatICalDate(event.end_time)}`);
      ical.push(`SUMMARY:${escapeIcal(event.title)}`);
      if (event.description) ical.push(`DESCRIPTION:${escapeIcal('Created by: ' + event.creator_name + '. ' + event.description)}`);
      if (event.location) ical.push(`LOCATION:${escapeIcal(event.location)}`);
      ical.push('END:VEVENT');
    }
    // This rep's own visit follow-ups, as all-day events.
    try {
      for (const f of await followUpEvents(userResult.rows[0].id)) {
        const day = f.start_time.replace(/-/g, '');
        ical.push('BEGIN:VEVENT');
        ical.push(`UID:followup-${f.id}@comptongroupllc.com`);
        ical.push(`DTSTAMP:${formatICalDate(new Date())}`);
        ical.push(`DTSTART;VALUE=DATE:${day}`);
        ical.push(`SUMMARY:${escapeIcal(f.title)}`);
        if (f.description) ical.push(`DESCRIPTION:${escapeIcal(f.description)}`);
        ical.push('END:VEVENT');
      }
    } catch (e) { console.error('[calendar] feed follow-ups:', e.message); }
    ical.push('END:VCALENDAR');
    res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
    res.send(ical.join('\r\n'));
  } catch (e) { res.status(500).send('Error generating feed'); }
});

router.get('/my-feed-url', async (req, res) => {
  const userId = req.session.user.id;
  try {
    let result = await pool.query('SELECT ical_token FROM users WHERE id=$1', [userId]);
    let token = result.rows[0]?.ical_token;
    if (!token) {
      token = Math.random().toString(36).substring(2) + Math.random().toString(36).substring(2) + Date.now().toString(36);
      await pool.query('UPDATE users SET ical_token=$1 WHERE id=$2', [token, userId]);
    }
    const feedUrl = `${process.env.APP_URL || 'https://comptongroupllc.com'}/api/calendar/feed/${token}`;
    res.json({ feedUrl });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

function formatICalDate(dateStr) {
  return new Date(dateStr).toISOString().replace(/[-:]/g, '').replace('.000', '');
}
function escapeIcal(str) {
  return (str || '').replace(/\\/g, '\\\\').replace(/;/g, '\\;').replace(/,/g, '\\,').replace(/\n/g, '\\n');
}

module.exports = router;
