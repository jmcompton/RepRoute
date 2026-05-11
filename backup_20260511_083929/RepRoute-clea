const express = require('express');
const { pool } = require('../db');
const router = express.Router();

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
    res.json(result.rows);
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
