const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';

router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const userName = req.session.user.name || 'Rep';
  try {
    const followUps = await pool.query(`
      SELECT p.id, p.company, p.category, p.city, p.phone, p.priority,
             c.next_step, c.next_step_date, 'follow_up' as call_reason,
             (CURRENT_DATE - c.next_step_date) as days_overdue
      FROM calls c JOIN prospects p ON c.prospect_id = p.id
      WHERE c.user_id = $1 AND c.next_step_date IS NOT NULL
        AND c.next_step_date <= CURRENT_DATE
        AND c.next_step IS NOT NULL AND c.next_step != ''
      ORDER BY c.next_step_date ASC LIMIT 10
    `, [uid]);

    const neverCalled = await pool.query(`
      SELECT p.id, p.company, p.category, p.city, p.phone, p.priority,
             'new_prospect' as call_reason, p.created_at
      FROM prospects p
      WHERE p.user_id = $1 AND p.priority IN ('High', 'Medium')
        AND p.id NOT IN (SELECT DISTINCT prospect_id FROM calls WHERE user_id = $1)
      ORDER BY CASE p.priority WHEN 'High' THEN 1 ELSE 2 END, p.created_at ASC LIMIT 10
    `, [uid]);

    const todayCalls = await pool.query(
      'SELECT COUNT(*) FROM calls WHERE user_id=$1 AND call_date=CURRENT_DATE', [uid]);
    const stats = await pool.query(
      'SELECT COUNT(*) as total, COUNT(CASE WHEN priority=\'High\' THEN 1 END) as hot FROM prospects WHERE user_id=$1', [uid]);
    const userRow = await pool.query('SELECT daily_call_goal FROM users WHERE id=$1', [uid]);
    const dailyGoal = userRow.rows[0]?.daily_call_goal || 10;
    const callsMadeToday = parseInt(todayCalls.rows[0].count);

    const followUpList = followUps.rows;
    const followUpIds = new Set(followUpList.map(r => r.id));
    const freshList = neverCalled.rows.filter(r => !followUpIds.has(r.id));
    const combinedList = [...followUpList, ...freshList].slice(0, 15);

    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });
    const dayOfWeek = new Date().toLocaleDateString('en-US', { weekday: 'long' });
    const dayFocus = {
      Monday: 'Dealers and distributors today. Start the week with your biggest accounts.',
      Tuesday: 'Deck contractors. Follow up on any samples you dropped last week.',
      Wednesday: 'Window and siding contractors. Mid-week push.',
      Thursday: 'Roofing contractors. Close out your active deals.',
      Friday: 'Wrap up follow-ups and prep next week.'
    };

    const prompt = `You are a sales coach for Compton Group LLC, a manufacturer's rep firm in Atlanta selling Alum-A-Pole, Soudal/Boss sealants, ShurTape, Fortress Steel Framing, and Fortress Railing.

Today is ${today}. Write a short morning message for ${userName}, a sales rep.

Situation: ${callsMadeToday} calls made today out of ${dailyGoal} goal. ${followUpList.length} overdue follow-ups. ${freshList.length} fresh high-priority prospects. Today's focus: ${dayFocus[dayOfWeek] || 'Stay on your hottest prospects.'}

Write 3-4 sentences max. Casual and direct like a coach, not a robot. End with one specific action they should do first. No bullet points, no em dashes.`;

    let aiMessage = '';
    try {
      const aiRes = await fetch(CLAUDE_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({ model: MODEL, max_tokens: 250, messages: [{ role: 'user', content: prompt }] })
      });
      const aiData = await aiRes.json();
      aiMessage = aiData.content?.filter(b => b.type === 'text').map(b => b.text).join('') || '';
    } catch(e) {
      aiMessage = `Good morning ${userName}. You've got ${combinedList.length} prospects to hit today. Start with your overdue follow-ups first, then work the fresh leads.`;
    }

    res.json({
      user: { name: userName }, date: today, dayOfWeek, aiMessage,
      stats: { callsMadeToday, dailyGoal, overdueFollowUps: followUpList.length, freshProspects: freshList.length, hotProspects: parseInt(stats.rows[0].hot) },
      callList: combinedList
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

router.post('/log-call', async (req, res) => {
  const uid = req.session.user.id;
  const { prospect_id, outcome, next_step, next_step_date } = req.body;
  if (!prospect_id) return res.status(400).json({ error: 'prospect_id required' });
  try {
    const today = new Date().toISOString().split('T')[0];
    await pool.query(
      'INSERT INTO calls (user_id, prospect_id, call_date, outcome, next_step, next_step_date) VALUES ($1,$2,$3,$4,$5,$6)',
      [uid, prospect_id, today, outcome || '', next_step || '', next_step_date || null]);
    if (outcome === 'Interested') await pool.query("UPDATE prospects SET status='Warm' WHERE id=$1", [prospect_id]);
    else if (outcome === 'Ready to Buy') await pool.query("UPDATE prospects SET status='Hot' WHERE id=$1", [prospect_id]);
    else if (outcome === 'Not Interested') await pool.query("UPDATE prospects SET status='Cold' WHERE id=$1", [prospect_id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

module.exports = router;
