const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

async function callClaude(prompt) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 3000, messages: [{ role: 'user', content: prompt }] })
  });
  const data = await res.json();
  return data.content?.map(b => b.text || '').join('') || '';
}

// Generate onboarding plan for a rep
router.post('/generate', async (req, res) => {
  const { rep_name, rep_territory, rep_id, focus_products, start_date } = req.body;
  const manager = req.session.user;

  const prompt = `You are a sales training manager for Compton Group LLC, a manufacturer's rep company in the building products industry.

Create a detailed 30-day onboarding plan for a new sales rep named ${rep_name}.
Territory: ${rep_territory || 'Atlanta Metro, Georgia'}
Start date: ${start_date || 'Monday'}
Product lines to learn: ${focus_products || 'Soudal Adhesives & Sealants, ShurTape Flashing & Deck Tape, Fortress Evolution Steel Framing, Fortress Railing, Alum-A-Pole Equipment'}
Target customers: Deck Contractors, Window & Door Installers, Commercial Roofers, Dealers & Distributors

Return ONLY a JSON object (no markdown, no explanation):
{
  "rep_name": "${rep_name}",
  "territory": "${rep_territory || 'Atlanta Metro'}",
  "start_date": "${start_date || 'TBD'}",
  "overview": "2-3 sentence plan summary",
  "weeks": [
    {
      "week": 1,
      "title": "Week title",
      "focus": "Main focus area",
      "days": [
        {
          "day": "Day 1",
          "title": "Session title",
          "content": "What to learn/do",
          "assignment": "Specific task to complete",
          "duration": "e.g. 2 hours"
        }
      ],
      "weekly_goal": "Goal for the week"
    }
  ],
  "daily_expectations": {
    "calls": 10,
    "in_person": 5,
    "new_prospects": 2,
    "crm_updates": true
  },
  "weekly_rhythm": {
    "monday": "Focus description",
    "tuesday": "Focus description",
    "wednesday": "Focus description",
    "thursday": "Focus description",
    "friday": "Focus description"
  },
  "success_metrics": ["metric 1", "metric 2", "metric 3"]
}`;

  try {
    const text = await callClaude(prompt);
    const clean = text.replace(/```json|```/g, '').trim();
    const plan = JSON.parse(clean);

    // Save plan
    const targetRep = rep_id || req.session.user.id;
    await pool.query(
      'INSERT INTO onboarding_plans (rep_id, plan_json, created_by) VALUES ($1, $2, $3)',
      [targetRep, JSON.stringify(plan), manager.id]
    );
    res.json({ plan });
  } catch (e) {
    res.json({ error: 'Could not generate plan', raw: e.message });
  }
});

// Get onboarding plan for current user
router.get('/my-plan', async (req, res) => {
  const uid = req.session.user.id;
  const result = await pool.query(
    'SELECT * FROM onboarding_plans WHERE rep_id=$1 ORDER BY created_at DESC LIMIT 1', [uid]);
  if (!result.rows[0]) return res.json({ plan: null });
  res.json({ plan: JSON.parse(result.rows[0].plan_json) });
});

// Get all onboarding plans (manager)
router.get('/all', async (req, res) => {
  const result = await pool.query(
    `SELECT o.*, u.name as rep_name, u.territory FROM onboarding_plans o
     JOIN users u ON o.rep_id=u.id ORDER BY o.created_at DESC`);
  res.json(result.rows);
});

module.exports = router;
