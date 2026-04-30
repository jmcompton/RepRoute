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
  for (let turn = 0; turn < 2; turn++) {
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

// Territory city lookup — Compton Group Southeast coverage
function getTerritoryContext(territory) {
  const t = (territory || "").toLowerCase();

  // Georgia
  if (t.includes("atlanta") || t === "ga" || t.includes(" ga") || t.includes("georgia"))
    return { state: "GA", cities: "Atlanta, Marietta, Kennesaw, Alpharetta, Roswell, Smyrna, Sandy Springs, Dunwoody, Decatur, Norcross, Duluth, Lawrenceville, Buford, Cumming, Woodstock, Canton, Peachtree City, Newnan, Augusta, Savannah, Columbus, Macon, Athens, Warner Robins, Valdosta, Gainesville, Albany, Rome" };
  if (t.includes("savannah"))
    return { state: "GA", cities: "Savannah, Brunswick, Statesboro, Hinesville, Valdosta, Waycross, Jesup, Pooler, Richmond Hill" };
  if (t.includes("augusta"))
    return { state: "GA", cities: "Augusta, Evans, Martinez, Grovetown, Aiken, North Augusta, Thomson, Waynesboro, Harlem" };

  // Alabama
  if (t === "al" || t.includes(" al") || t.includes("alabama") || t.includes("birmingham") || t.includes("huntsville") || t.includes("montgomery") || t.includes("tuscaloosa") || t.includes("mobile"))
    return { state: "AL", cities: "Birmingham, Hoover, Vestavia Hills, Homewood, Bessemer, Tuscaloosa, Huntsville, Madison, Decatur, Montgomery, Mobile, Auburn, Opelika, Dothan, Florence, Muscle Shoals, Sheffield, Gadsden, Anniston, Talladega, Selma, Phenix City, Northport, Prattville, Enterprise" };
  if (t.includes("jackson") && t.includes("al"))
    return { state: "AL", cities: "Jackson, Chatom, Grove Hill, Evergreen, Brewton, Atmore, Bay Minette, Daphne, Fairhope, Foley" };

  // Mississippi
  if (t === "ms" || t.includes(" ms") || t.includes("mississippi") || t.includes("jackson ms") || t.includes("jackson, ms"))
    return { state: "MS", cities: "Jackson, Ridgeland, Madison, Brandon, Pearl, Flowood, Hattiesburg, Gulfport, Biloxi, Southaven, Olive Branch, Tupelo, Meridian, Greenville, Vicksburg, Columbus, Starkville, Natchez, Pascagoula, Ocean Springs, Laurel, Clarksdale, Corinth, Brookhaven" };
  if (t.includes("gulfport") || t.includes("biloxi") || t.includes("gulf coast"))
    return { state: "MS", cities: "Gulfport, Biloxi, Ocean Springs, Pascagoula, Gautier, D'Iberville, Long Beach, Pass Christian, Bay St. Louis, Waveland" };

  // Tennessee
  if (t === "tn" || t.includes(" tn") || t.includes("tennessee") || t.includes("nashville") || t.includes("memphis") || t.includes("knoxville") || t.includes("chattanooga"))
    return { state: "TN", cities: "Nashville, Brentwood, Franklin, Murfreesboro, Smyrna, La Vergne, Hendersonville, Gallatin, Clarksville, Memphis, Germantown, Collierville, Bartlett, Cordova, Knoxville, Maryville, Oak Ridge, Chattanooga, Cleveland, Cookeville, Jackson, Kingsport, Bristol, Johnson City" };

  // North Carolina
  if (t === "nc" || t.includes(" nc") || t.includes("north carolina") || t.includes("charlotte") || t.includes("raleigh"))
    return { state: "NC", cities: "Charlotte, Concord, Gastonia, Rock Hill, Mooresville, Huntersville, Matthews, Raleigh, Durham, Cary, Chapel Hill, Apex, Greensboro, Winston-Salem, High Point, Burlington, Wilmington, Fayetteville, Asheville, Hickory, Greenville, Jacksonville, Rocky Mount, Wilson" };

  // South Carolina
  if (t === "sc" || t.includes(" sc") || t.includes("south carolina") || t.includes("columbia sc") || t.includes("charleston") || t.includes("greenville sc") || t.includes("spartanburg"))
    return { state: "SC", cities: "Columbia, Lexington, Irmo, West Columbia, Cayce, Greenville, Spartanburg, Greer, Mauldin, Simpsonville, Taylors, Anderson, Charleston, North Charleston, Mount Pleasant, Summerville, Goose Creek, Myrtle Beach, Conway, Florence, Rock Hill, Aiken, Hilton Head" };

  // Southeast region-wide
  if (t.includes("southeast") || t.includes("south east"))
    return { state: "", cities: "Atlanta GA, Birmingham AL, Jackson MS, Nashville TN, Charlotte NC, Columbia SC, Greenville SC, Chattanooga TN, Knoxville TN, Memphis TN, Savannah GA, Augusta GA, Huntsville AL, Montgomery AL, Tupelo MS, Hattiesburg MS, Asheville NC, Raleigh NC, Charleston SC" };

  return { state: "", cities: territory };
}

// AI Lead Finder
router.post('/leads', async (req, res) => {
  const { category, territory, count, customer_type } = req.body;
  const user = req.session.user;
  const loc = territory || user.territory || 'Atlanta Metro, Georgia';
  const { state, cities } = getTerritoryContext(loc);
  const product = req.body.product || category;
  const numLeads = parseInt(count) || 20;
  const custType = customer_type || 'any building products buyer';

  // Split city list into batches for multiple searches
  const cityList = cities.split(',').map(c => c.trim());
  const batchSize = Math.ceil(numLeads / 2);
  const mid = Math.floor(cityList.length / 2);
  const cities1 = cityList.slice(0, mid).join(', ') || cities;
  const cities2 = cityList.slice(mid).join(', ') || cities;

  function buildPrompt(citySet, batchCount, offset) {
    return `You are a B2B sales researcher for Compton Group LLC, a manufacturer's rep selling building products across the Southeast US.

Find exactly ${batchCount} REAL ${custType} businesses in these cities: ${citySet}
These businesses should be strong prospects for: ${product}

Search Google, company websites, and directories. For EACH business return:
- Real verified company name (must exist)
- Exact business type
- City and state
- Phone number from their Google listing or website
- Email from their contact page
- Owner or purchasing manager name if findable
- Website URL
- One specific reason they need ${product} (reference their actual work)
- Priority: High (large active jobs, growing company), Medium (steady established), Low (small or unclear)

Return ONLY a JSON array starting with [ — no markdown, no explanation, no preamble:
[{"company":"Name","category":"Type","city":"City","state":"${state}","phone":"number or null","email":"email or null","website":"url or null","contact":"name or null","products":"${product}","why":"specific reason","priority":"High or Medium or Low"}]

IMPORTANT: Return exactly ${batchCount} businesses. Real businesses only. No duplicates.`;
  }

  try {
    // Run two searches in parallel across different city sets
    const [text1, text2] = await Promise.all([
      callClaudeWithSearch(buildPrompt(cities1, batchSize, 0)),
      callClaudeWithSearch(buildPrompt(cities2, numLeads - batchSize, batchSize))
    ]);

    let leads1 = extractJSON(text1) || [];
    let leads2 = extractJSON(text2) || [];

    // Deduplicate by company name
    const seen = new Set(leads1.map(l => l.company?.toLowerCase()));
    const unique2 = leads2.filter(l => !seen.has(l.company?.toLowerCase()));
    let leads = [...leads1, ...unique2];

    // If still short, do a third search for remaining
    if (leads.length < numLeads) {
      const remaining = numLeads - leads.length;
      const existingNames = leads.map(l => l.company).join(', ');
      const fill = await callClaudeWithSearch(
        buildPrompt(cities, remaining, 0) +
        ` Do NOT include these already found: ${existingNames}`
      );
      const leads3 = extractJSON(fill) || [];
      const seen2 = new Set(leads.map(l => l.company?.toLowerCase()));
      leads = [...leads, ...leads3.filter(l => !seen2.has(l.company?.toLowerCase()))];
    }

    if (leads.length === 0) return res.json({ error: 'Could not find leads. Try a different category or territory.' });
    res.json({ leads: leads.slice(0, numLeads) });
  } catch (e) {
    if (e.message && e.message.includes('rate limit')) return res.json({ error: e.message });
    res.json({ error: 'Could not find leads. Try again.', raw: e.message });
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
