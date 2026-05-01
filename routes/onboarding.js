const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

async function callClaude(prompt) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: 'claude-sonnet-4-5', max_tokens: 8000, messages: [{ role: 'user', content: prompt }] })
  });
  const data = await res.json();
  if (data.error) throw new Error(data.error.message);
  return data.content?.map(b => b.text || '').join('') || '';
}

router.post('/generate', async (req, res) => {
  const { rep_name, rep_territory, rep_id, focus_products, start_date, focus_level } = req.body;
  const manager = req.session.user;

  const products = focus_products || 'Soudal Sealants & Adhesives, ShurTape Flashing & Deck Tape, Alum-A-Pole Pump Jack Scaffolding';
  const territory = rep_territory || 'Southeast';

  const prompt = `You are a sales training manager for Compton Group LLC, a building products manufacturer rep in the Southeast US.

Create a 30-day onboarding plan for ${rep_name}, a new sales rep covering ${territory}.
Products to sell: ${products}
Focus style: ${focus_level || 'balanced'}
Start date: ${start_date || 'TBD'}

Key product info:
- Soudal BOSS: #1 global sealant brand, used by window/door installers, waterproofers, commercial contractors
- ShurTape: premium flashing & deck tape, code compliant, used by roofers, window installers, deck builders  
- Fortress Steel Framing: rot-proof, termite-proof deck framing, used by deck contractors
- Fortress Railing: aluminum/steel railing, used by deck contractors and fence companies
- Alum-A-Pole: OSHA-compliant pump jack scaffolding, used by siding contractors

Return ONLY valid JSON, no markdown, no backticks, starting with { and ending with }:
{
  "rep_name": "${rep_name}",
  "territory": "${territory}",
  "start_date": "${start_date || 'TBD'}",
  "overview": "2 sentence summary of the plan",
  "weeks": [
    {
      "week": 1,
      "title": "Foundation Week",
      "focus": "Product knowledge and CRM setup",
      "weekly_goal": "Know all products cold, first 10 prospects in CRM",
      "days": [
        {"day": "Day 1", "title": "Orientation", "content": "Learn company overview and product lines", "assignment": "Set up RepRoute CRM and add 5 contacts", "duration": "4 hours"},
        {"day": "Day 2", "title": "Soudal Deep Dive", "content": "Learn Soudal BOSS sealants and adhesives", "assignment": "Practice pitch to manager", "duration": "3 hours"},
        {"day": "Day 3", "title": "ShurTape Training", "content": "Learn ShurTape flashing and deck tape applications", "assignment": "Visit 2 roofing supply stores", "duration": "3 hours"},
        {"day": "Day 4", "title": "Fortress Products", "content": "Learn Fortress steel framing and railing systems", "assignment": "Find 3 deck contractor prospects", "duration": "3 hours"},
        {"day": "Day 5", "title": "Alum-A-Pole Training", "content": "Learn pump jack scaffolding for siding contractors", "assignment": "Find 5 siding contractor prospects", "duration": "3 hours"}
      ]
    },
    {
      "week": 2,
      "title": "Market Exposure",
      "focus": "First customer visits and cold calls",
      "weekly_goal": "10 cold calls made, 3 in-person visits completed",
      "days": [
        {"day": "Day 6", "title": "Cold Call Blitz", "content": "Make first 10 cold calls to siding contractors", "assignment": "Log all calls in RepRoute", "duration": "Full day"},
        {"day": "Day 7", "title": "In-Person Visits", "content": "Visit 2 building material dealers", "assignment": "Leave samples and follow up plan", "duration": "Full day"},
        {"day": "Day 8", "title": "Objection Handling", "content": "Learn top objections: price, brand loyalty, switching cost", "assignment": "Role play objections with manager", "duration": "3 hours"},
        {"day": "Day 9", "title": "Deck Contractor Outreach", "content": "Call deck contractors about Fortress framing", "assignment": "Set 1 demo appointment", "duration": "Full day"},
        {"day": "Day 10", "title": "Week 2 Review", "content": "Review calls, wins, losses with manager", "assignment": "Update all prospects in CRM", "duration": "2 hours"}
      ]
    },
    {
      "week": 3,
      "title": "Pipeline Building",
      "focus": "Build a real prospect pipeline",
      "weekly_goal": "20 prospects in pipeline, first sample placed",
      "days": [
        {"day": "Day 11", "title": "Sample Strategy", "content": "Learn how to use samples to close", "assignment": "Place first sample with a prospect", "duration": "Full day"},
        {"day": "Day 12", "title": "Roofing Contractor Blitz", "content": "Focus on ShurTape with roofers", "assignment": "Visit 3 roofing companies", "duration": "Full day"},
        {"day": "Day 13", "title": "Window Installer Outreach", "content": "Pitch Soudal to window and door installers", "assignment": "10 calls to window installers", "duration": "Full day"},
        {"day": "Day 14", "title": "Follow Up Day", "content": "Follow up on all week 1-2 contacts", "assignment": "Move 5 prospects to next pipeline stage", "duration": "Full day"},
        {"day": "Day 15", "title": "Mid-Point Review", "content": "Full pipeline and performance review", "assignment": "Set goals for final 2 weeks", "duration": "2 hours"}
      ]
    },
    {
      "week": 4,
      "title": "Closing Week",
      "focus": "Close first deals and establish rhythm",
      "weekly_goal": "First order placed, weekly routine established",
      "days": [
        {"day": "Day 16", "title": "Close Attempts", "content": "Push hardest prospects toward first order", "assignment": "Ask for the order on 3 accounts", "duration": "Full day"},
        {"day": "Day 17", "title": "Dealer Strategy", "content": "Get products on dealer shelves", "assignment": "Present to 1 building material dealer", "duration": "Full day"},
        {"day": "Day 18", "title": "Referral Outreach", "content": "Ask happy contacts for referrals", "assignment": "Get 3 referrals from existing contacts", "duration": "Full day"},
        {"day": "Day 19", "title": "Route Planning", "content": "Build weekly drive route for territory", "assignment": "Plan month 2 call schedule", "duration": "2 hours"},
        {"day": "Day 20", "title": "30-Day Graduation", "content": "Final review and month 2 goal setting", "assignment": "Present pipeline to manager", "duration": "2 hours"}
      ]
    }
  ],
  "daily_expectations": {
    "calls": 15,
    "in_person": 3,
    "new_prospects": 2,
    "crm_updates": true
  },
  "weekly_rhythm": {
    "monday": "Plan the week, make calls, update CRM",
    "tuesday": "In-person visits and demos",
    "wednesday": "Cold calls and new prospect outreach",
    "thursday": "Follow ups and sample placements",
    "friday": "Pipeline review and next week prep"
  },
  "success_metrics": [
    "25 prospects added to CRM by day 30",
    "First order placed by week 4",
    "10+ in-person visits completed",
    "All products demoed at least once",
    "Weekly call rhythm established"
  ]
}`;

  try {
    console.log('Generating plan for:', rep_name, '| Products:', products);
    const text = await callClaude(prompt);
    console.log('Response length:', text.length);

    let clean = text.replace(/```json\n?/g, '').replace(/```/g, '').trim();
    const startIdx = clean.indexOf('{');
    const endIdx = clean.lastIndexOf('}');
    if (startIdx === -1 || endIdx === -1) {
      console.error('No JSON in response. Preview:', text.slice(0, 300));
      return res.json({ error: 'Could not generate plan', raw: 'No JSON found' });
    }
    clean = clean.substring(startIdx, endIdx + 1);

    let plan;
    try {
      plan = JSON.parse(clean);
    } catch(parseErr) {
      console.error('Parse error:', parseErr.message, '| End of JSON:', clean.slice(-100));
      return res.json({ error: 'Could not generate plan', raw: 'Parse error - try again' });
    }

    const targetRep = rep_id || req.session.user.id;
    await pool.query(
      'INSERT INTO onboarding_plans (rep_id, plan_json, created_by) VALUES ($1, $2, $3)',
      [targetRep, JSON.stringify(plan), manager.id]
    );
    console.log('Plan saved successfully for:', rep_name);
    res.json({ plan });
  } catch (e) {
    console.error('Onboarding error:', e.message);
    res.json({ error: 'Could not generate plan', raw: e.message });
  }
});

router.get('/my-plan', async (req, res) => {
  const uid = req.session.user.id;
  const result = await pool.query(
    'SELECT * FROM onboarding_plans WHERE rep_id=$1 ORDER BY created_at DESC LIMIT 1', [uid]);
  if (!result.rows[0]) return res.json({ plan: null });
  res.json({ plan: JSON.parse(result.rows[0].plan_json) });
});

router.get('/all', async (req, res) => {
  const result = await pool.query(
    'SELECT o.*, u.name as rep_name, u.territory FROM onboarding_plans o JOIN users u ON o.rep_id=u.id ORDER BY o.created_at DESC');
  res.json(result.rows);
});

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