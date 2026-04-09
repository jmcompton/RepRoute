const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';

async function callClaude(prompt, useWebSearch = false) {
  const body = {
    model: MODEL,
    max_tokens: 2000,
    messages: [{ role: 'user', content: prompt }]
  };
  if (useWebSearch) {
    body.tools = [{ type: 'web_search_20250305', name: 'web_search' }];
  }
  const res = await fetch(CLAUDE_API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', ...(useWebSearch ? { 'anthropic-beta': 'web-search-2025-03-05' } : {}) },
    body: JSON.stringify(body)
  });
  const data = await res.json();
  return data.content?.map(b => b.text || '').filter(Boolean).join('') || '';
}

// AI Lead Finder — searches real businesses
router.post('/leads', async (req, res) => {
  const { category, territory } = req.body;
  const user = req.session.user;
  const loc = territory || user.territory || 'Atlanta Metro, Georgia';

  const prompt = `You are a field sales intelligence AI for a manufacturer's rep company called Compton Group LLC. 
  
Search the web and find 10 REAL, specific businesses in the "${loc}" area that match this category: "${category}".

Product lines we sell: Soudal Adhesives & Sealants, ShurTape Flashing & Deck Tape, Fortress Evolution Steel Framing, Fortress Railing, Alum-A-Pole Equipment.

For each business return ONLY a JSON array (no other text, no markdown) like this:
[
  {
    "company": "Exact Business Name",
    "category": "${category}",
    "city": "City Name",
    "state": "GA",
    "phone": "xxx-xxx-xxxx or null",
    "website": "website.com or null",
    "contact": "Owner/Manager name if known or null",
    "products": "which of our products they would use",
    "notes": "why they are a good prospect, 1 sentence",
    "priority": "High or Medium or Low"
  }
]

Rules:
- Only include REAL businesses that actually exist
- Focus on ${loc} specifically
- Prioritize businesses most likely to buy our products
- Return valid JSON only, nothing else`;

  try {
    const text = await callClaude(prompt, true);
    // Extract JSON array from anywhere in the response
    const match = text.match(/\[[\s\S]*\]/);
    if (!match) {
      return res.json({ error: 'Could not parse leads. Try again.', raw: 'No JSON array found in response' });
    }
    const leads = JSON.parse(match[0]);
    res.json({ leads });
  } catch (e) {
    res.json({ error: 'Could not parse leads. Try again.', raw: e.message });
  }
});

// Save AI leads to database
router.post('/leads/save', async (req, res) => {
  const uid = req.session.user.id;
  const { leads } = req.body;
  const saved = [];
  for (const l of leads) {
    try {
      const result = await pool.query(
        `INSERT INTO prospects (user_id, company, category, city, state, phone, contact, website, products, notes, priority, source)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'AI') 
         ON CONFLICT DO NOTHING RETURNING *`,
        [uid, l.company, l.category, l.city, l.state || 'GA', l.phone, l.contact, l.website, l.products, l.notes, l.priority || 'Medium']
      );
      if (result.rows[0]) saved.push(result.rows[0]);
    } catch (e) {}
  }
  res.json({ saved: saved.length });
});

// AI Command — general Q&A
router.post('/command', async (req, res) => {
  const { prompt } = req.body;
  const user = req.session.user;
  const full = `You are a field sales intelligence assistant for RepRoute, built by Compton Group LLC. 
The rep's name is ${user.name}, territory: ${user.territory || 'Atlanta Metro'}.
Product lines: Soudal Adhesives & Sealants, ShurTape Flashing & Deck Tape, Fortress Evolution Steel Framing, Fortress Railing, Alum-A-Pole Equipment.
Target customers: Deck Contractors, Window & Door Installers, Commercial Roofers, Dealers & Distributors.

Question: ${prompt}

Be specific, practical, and Atlanta/Southeast market focused.`;
  try {
    const text = await callClaude(full, true);
    res.json({ response: text });
  } catch (e) {
    res.json({ error: 'AI error' });
  }
});

// AI Outreach writer
router.post('/outreach', async (req, res) => {
  const { company, category, products, contact } = req.body;
  const user = req.session.user;
  const prompt = `Write 3 outreach messages for a manufacturer's rep at Compton Group LLC in ${user.territory || 'Atlanta Metro'}.

Target: ${company || 'a ' + category} (${category})
Products to pitch: ${products}
Contact name: ${contact || 'the owner/manager'}

Write:
1. EMAIL — subject line + body (professional, 5-7 sentences)
2. LINKEDIN MESSAGE — concise, 3-4 sentences  
3. FOLLOW-UP TEXT — after a first call, 2-3 sentences

Label each clearly. Be specific to building products industry.`;
  try {
    const text = await callClaude(prompt);
    res.json({ response: text });
  } catch (e) {
    res.json({ error: 'AI error' });
  }
});

// AI Weekly Plan generator
router.post('/weekly-plan', async (req, res) => {
  const uid = req.session.user.id;
  const user = req.session.user;

  // Get prospects and recent calls
  const prospects = await pool.query(
    "SELECT * FROM prospects WHERE user_id=$1 ORDER BY CASE priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END, CASE status WHEN 'Hot' THEN 1 WHEN 'Warm' THEN 2 ELSE 3 END LIMIT 30", [uid]);
  const recentCalls = await pool.query(
    "SELECT c.*, p.company FROM calls c JOIN prospects p ON c.prospect_id=p.id WHERE c.user_id=$1 ORDER BY c.call_date DESC LIMIT 10", [uid]);

  const prompt = `You are a sales manager AI for RepRoute. Build a 5-day weekly call plan for ${user.name}.

Territory: ${user.territory || 'Atlanta Metro'}
Products: Soudal Adhesives & Sealants, ShurTape Flashing & Deck Tape, Fortress Evolution Steel Framing, Fortress Railing, Alum-A-Pole Equipment

Available prospects (prioritized):
${prospects.rows.map(p => `- ${p.company} (${p.category}, ${p.city}, Status: ${p.status}, Priority: ${p.priority})`).join('\n')}

Recent call history:
${recentCalls.rows.map(c => `- ${c.company}: ${c.outcome} on ${c.call_date}, next step: ${c.next_step}`).join('\n') || 'No recent calls'}

Weekly rhythm rules:
- Monday: Dealer/Distributor focus + planning
- Tuesday: Deck Contractor calls
- Wednesday: Window & Door Installer calls
- Thursday: Jobsite visits + Commercial Roofers
- Friday: Follow-ups + prospects needing next step

Return ONLY a JSON object (no markdown):
{
  "week_of": "Apr 7, 2025",
  "days": [
    {
      "day": "Monday",
      "focus": "Focus description",
      "calls": [
        { "company": "name", "category": "type", "city": "city", "action": "what to do/say", "priority": "High/Medium" }
      ],
      "tip": "Manager coaching tip for the day"
    }
  ],
  "weekly_goal": "Overall goal statement for the week"
}`;

  try {
    const text = await callClaude(prompt);
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return res.json({ error: 'Could not generate plan', raw: 'No JSON found' });
    const plan = JSON.parse(match[0]);
    await pool.query(
      'INSERT INTO weekly_plans (user_id, week_start, plan_json) VALUES ($1, CURRENT_DATE, $2)',
      [uid, JSON.stringify(plan)]
    );
    res.json({ plan });
  } catch (e) {
    res.json({ error: 'Could not generate plan', raw: e.message });
  }
});

module.exports = router;
