const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';
const SEARCH_MODEL = 'claude-haiku-4-5-20251001';

// Standard Claude call (no web search)
async function callClaude(prompt) {
  const res = await fetch(CLAUDE_API, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: 4000,
      messages: [{ role: 'user', content: prompt }]
    })
  });
  const data = await res.json();
  if (!data.content) return '';
  return data.content.filter(b => b.type === 'text').map(b => b.text || '').join('');
}

// Web search call — Anthropic handles the search internally, we just poll until done
async function callClaudeWithSearch(prompt) {
  const headers = {
    'Content-Type': 'application/json',
    'x-api-key': process.env.ANTHROPIC_API_KEY,
    'anthropic-version': '2023-06-01',
    'anthropic-beta': 'web-search-2025-03-05'
  };
  const tools = [{ type: 'web_search_20250305', name: 'web_search' }];
  let messages = [{ role: 'user', content: prompt }];
  let fullText = '';

  // Loop up to 5 turns to handle tool use
  for (let turn = 0; turn < 5; turn++) {
    const res = await fetch(CLAUDE_API, {
      method: 'POST', headers,
      body: JSON.stringify({ model: SEARCH_MODEL, max_tokens: 2000, tools, messages })
    });
    const data = await res.json();
    if (data.error) {
      if (data.error.type === 'rate_limit_error') throw new Error('Rate limit hit — please wait 60 seconds and try again.');
      throw new Error(data.error.message || 'API error');
    }
    if (!data.content) break;

    // Collect any text from this turn
    const text = data.content.filter(b => b.type === 'text').map(b => b.text).join('');
    if (text) fullText += text;

    // If done, stop
    if (data.stop_reason === 'end_turn' || data.stop_reason !== 'tool_use') break;

    // Continue the conversation with the tool result
    messages.push({ role: 'assistant', content: data.content });
    const toolResults = data.content
      .filter(b => b.type === 'tool_use')
      .map(b => ({ type: 'tool_result', tool_use_id: b.id, content: '' }));
    if (toolResults.length === 0) break;
    messages.push({ role: 'user', content: toolResults });
  }

  return fullText;
}

// Extract JSON array from any text
function extractJSON(text) {
  const start = text.indexOf('[');
  const end = text.lastIndexOf(']');
  if (start === -1 || end === -1 || end < start) return null;
  try {
    return JSON.parse(text.substring(start, end + 1));
  } catch(e) {
    // Try to fix truncated JSON
    const lastBrace = text.lastIndexOf('}');
    if (lastBrace > start) {
      try {
        return JSON.parse(text.substring(start, lastBrace + 1) + ']');
      } catch(e2) { return null; }
    }
    return null;
  }
}

// Territory city lookup
function getTerritoryContext(territory) {
  const t = (territory || "").toLowerCase();
  if (t.includes("atlanta") || t.includes("georgia") || t.includes(" ga") || t === "ga")
    return { state: "GA", cities: "Atlanta, Marietta, Kennesaw, Alpharetta, Roswell, Smyrna, Sandy Springs, Dunwoody, Decatur, Norcross, Duluth, Lawrenceville, Buford, Cumming, Woodstock, Canton, Peachtree City, Newnan" };
  if (t.includes("birmingham") || t.includes("alabama") || t.includes(" al") || t === "al")
    return { state: "AL", cities: "Birmingham, Hoover, Vestavia Hills, Homewood, Bessemer, Tuscaloosa, Huntsville, Montgomery, Auburn, Decatur, Florence, Dothan, Gadsden" };
  if (t.includes("charlotte") || t.includes("north carolina") || t.includes(" nc") || t === "nc")
    return { state: "NC", cities: "Charlotte, Raleigh, Durham, Greensboro, Winston-Salem, Cary, High Point, Wilmington, Concord, Gastonia, Fayetteville, Asheville" };
  if (t.includes("nashville") || t.includes("tennessee") || t.includes(" tn") || t === "tn")
    return { state: "TN", cities: "Nashville, Memphis, Knoxville, Chattanooga, Clarksville, Murfreesboro, Franklin, Brentwood, Hendersonville" };
  if (t.includes("dallas") || t.includes("fort worth") || t.includes("dfw"))
    return { state: "TX", cities: "Dallas, Fort Worth, Arlington, Plano, Frisco, McKinney, Irving, Garland, Grand Prairie, Carrollton, Richardson, Lewisville, Denton" };
  if (t.includes("houston"))
    return { state: "TX", cities: "Houston, Sugar Land, Pearland, Pasadena, The Woodlands, Katy, Baytown, League City, Friendswood, Missouri City, Conroe" };
  if (t.includes("florida") || t.includes(" fl") || t === "fl" || t.includes("tampa") || t.includes("orlando") || t.includes("miami"))
    return { state: "FL", cities: "Miami, Orlando, Tampa, Jacksonville, Fort Lauderdale, St. Petersburg, Tallahassee, Cape Coral, Pembroke Pines" };
  return { state: "", cities: territory };
}

// AI Lead Finder
router.post('/leads', async (req, res) => {
  const { category, territory } = req.body;
  const user = req.session.user;
  const loc = territory || user.territory || 'Atlanta Metro, Georgia';
  const { state, cities } = getTerritoryContext(loc);
  const product = req.body.product || category;

  const prompt = `Find 20 real ${category} businesses in ${cities} that would buy ${product}. Search Google Maps and company websites. For each, find their phone, email, and owner name. Return ONLY a JSON array:
[{"company":"Name","category":"Type","address":"addr or null","city":"city","state":"${state}","phone":"number or null","email":"email or null","website":"url or null","contact":"name or null","products":"${product}","priority":"High or Medium or Low"}]
Start with [ immediately. No other text.`;


  try {
    const text = await callClaudeWithSearch(prompt);
    const leads = extractJSON(text);
    if (!leads) {
      // Wait 3 seconds before retry to avoid rate limit
      await new Promise(resolve => setTimeout(resolve, 3000));
      const retry = await callClaudeWithSearch(prompt + ' Return ONLY the JSON array starting with [. No other text.');
      const leads2 = extractJSON(retry);
      if (!leads2) return res.json({ error: 'Could not parse leads. Try again.', raw: retry.substring(0, 200) });
      return res.json({ leads: Array.isArray(leads2) ? leads2 : [] });
    }
    res.json({ leads: Array.isArray(leads) ? leads : [] });
  } catch (e) {
    // Check for rate limit error
    if (e.message && (e.message.includes('529') || e.message.includes('rate') || e.message.includes('overloaded'))) {
      return res.json({ error: 'AI is busy — please wait 30 seconds and try again.', raw: e.message });
    }
    res.json({ error: 'Could not parse leads. Try again.', raw: e.message });
  }
});

// Save leads to CRM
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
        [uid, l.company, l.category, l.city, l.state || 'GA', l.phone, l.contact, l.website, l.products, l.notes || '', l.priority || 'Medium']
      );
      if (result.rows[0]) saved.push(result.rows[0]);
    } catch (e) {}
  }
  res.json({ saved: saved.length });
});

// AI Command Center
router.post('/command', async (req, res) => {
  const { prompt } = req.body;
  const user = req.session.user;
  const full = `You are an expert sales coach for Compton Group LLC, a manufacturer's rep in Atlanta, GA.
Products: Soudal Adhesives & Sealants, ShurTape Flashing & Deck Tape, Fortress Evolution Steel Framing, Fortress Railing, Alum-A-Pole Equipment.
Customers: Deck Contractors, Window & Door Installers, Commercial Roofers, Building Material Dealers, Distributors.
Rep: ${user.name}, Territory: ${user.territory || 'Atlanta Metro'}
Question: ${prompt}
Give a specific, actionable, concise response.`;
  try {
    const text = await callClaude(full);
    res.json({ response: text });
  } catch (e) {
    res.json({ error: 'AI error' });
  }
});

// Outreach Writer
router.post('/outreach', async (req, res) => {
  const { company, category, products } = req.body;
  const user = req.session.user;
  const prompt = `Write 3 outreach messages for a manufacturer's rep selling ${products} to ${company || 'a ' + category} in Atlanta.
Rep: ${user.name} from Compton Group LLC.
1. EMAIL - subject + body (under 150 words)
2. LINKEDIN - connection message (under 300 chars)
3. TEXT - follow up text (under 160 chars)
Keep it specific, relationship-based, not pushy.`;
  try {
    const text = await callClaude(prompt);
    res.json({ response: text });
  } catch (e) {
    res.json({ error: 'AI error' });
  }
});

// Weekly Plan
router.post('/weekly-plan', async (req, res) => {
  const uid = req.session.user.id;
  const user = req.session.user;
  const prospects = await pool.query(
    "SELECT * FROM prospects WHERE user_id=$1 ORDER BY CASE priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END LIMIT 30", [uid]);
  const recentCalls = await pool.query(
    "SELECT c.*, p.company FROM calls c JOIN prospects p ON c.prospect_id=p.id WHERE c.user_id=$1 ORDER BY c.call_date DESC LIMIT 10", [uid]);

  const prompt = `Build a 5-day weekly call plan as JSON for ${user.name}, a manufacturer's rep in ${user.territory || 'Atlanta Metro'}.
Products: Soudal, ShurTape, Fortress, Alum-A-Pole.
Prospects: ${prospects.rows.map(p => p.company + ' (' + p.category + ', ' + p.city + ', ' + p.priority + ')').join('; ') || 'none yet'}
Recent calls: ${recentCalls.rows.map(c => c.company + ': ' + c.outcome).join('; ') || 'none'}
Return ONLY this JSON (start with { end with }):
{"week_of":"April 2025","weekly_goal":"goal","days":[{"day":"Monday","focus":"focus","calls":[{"company":"name","category":"type","city":"city","action":"what to do","priority":"High"}],"tip":"tip"}]}`;

  try {
    const text = await callClaude(prompt);
    const start = text.indexOf('{');
    const end = text.lastIndexOf('}');
    if (start === -1) return res.json({ error: 'Could not generate plan' });
    const plan = JSON.parse(text.substring(start, end + 1));
    await pool.query('INSERT INTO weekly_plans (user_id, week_start, plan_json) VALUES ($1, CURRENT_DATE, $2)', [uid, JSON.stringify(plan)]);
    res.json({ plan });
  } catch (e) {
    res.json({ error: 'Could not generate plan', raw: e.message });
  }
});

module.exports = router;
