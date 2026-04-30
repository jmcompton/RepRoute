const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

async function callClaude(prompt) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: 'claude-sonnet-4-5', max_tokens: 3000, messages: [{ role: 'user', content: prompt }] })
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
Product lines to focus on: ${focus_products || 'Soudal Adhesives & Sealants, ShurTape Flashing & Deck Tape, Fortress Evolution Steel Framing, Fortress Railing, Alum-A-Pole Equipment'}
Target customers in the Southeast: Deck Contractors, Window & Door Installers, Commercial Roofers, General Contractors, Building Material Dealers & Distributors
Company: Compton Group LLC — manufacturer's rep for the Southeast region
Key selling points per product:
- Soudal: #1 global sealant brand, superior adhesion, paintable, waterproof
- ShurTape: premium flashing and deck tape, code compliant, easy application
- Fortress Steel Framing: rot-proof, termite-proof, stronger than wood
- Fortress Railing: aluminum and steel, low maintenance, code compliant
- Alum-A-Pole: lightweight scaffolding, OSHA compliant, contractor favorite
Make each day's content SPECIFIC to these products and the Southeast market. Include real objections they will hear and how to handle them. Include specific talking points for each product.

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
    console.log('Generating onboarding plan for:', rep_name);
    const text = await callClaude(prompt);
    console.log('Claude response length:', text.length);
    console.log('Claude response preview:', text.slice(0, 200));
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) {
      console.error('No JSON found in response:', text.slice(0, 500));
      return res.json({ error: 'Could not generate plan', raw: 'No JSON found' });
    }
    const plan = JSON.parse(match[0]);
    const targetRep = rep_id || req.session.user.id;
    await pool.query(
      'INSERT INTO onboarding_plans (rep_id, plan_json, created_by) VALUES ($1, $2, $3)',
      [targetRep, JSON.stringify(plan), manager.id]
    );
    res.json({ plan });
  } catch (e) {
    console.error('Onboarding error:', e.message, e.stack);
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

// Save progress on onboarding plan
router.post('/progress', async (req, res) => {
  const uid = req.session.user.id;
  const { day_key, completed } = req.body;
  try {
    const result = await pool.query(
      'SELECT * FROM onboarding_plans WHERE rep_id=$1 ORDER BY created_at DESC LIMIT 1', [uid]);
    if (!result.rows[0]) return res.json({ error: 'No plan found' });
    let plan = JSON.parse(result.rows[0].plan_json);
    if (!plan.progress) plan.progress = {};
    plan.progress[day_key] = completed;
    await pool.query('UPDATE onboarding_plans SET plan_json=$1 WHERE id=$2',
      [JSON.stringify(plan), result.rows[0].id]);
    res.json({ ok: true, progress: plan.progress });
  } catch(e) { res.json({ error: e.message }); }
});

module.exports = router;
