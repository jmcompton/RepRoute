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

// Territory city lookup — tight radius lists for driveable territories
function getTerritoryContext(territory) {
  const t = (territory || "").toLowerCase();

  // Atlanta Metro — 45 min radius only
  if (t.includes("atlanta metro") || t === "atlanta" || t === "atl")
    return { state: "GA", cities: "Atlanta, Marietta, Kennesaw, Alpharetta, Roswell, Smyrna, Sandy Springs, Dunwoody, Decatur, Norcross, Duluth, Lawrenceville, Buford, Cumming, Woodstock, Canton, Peachtree City, Newnan, Douglasville, Acworth, Powder Springs, Stockbridge, McDonough, Fayetteville, Cartersville" };

  // Georgia statewide
  if (t === "georgia" || t === "ga" || (t.includes("georgia") && !t.includes("atlanta")))
    return { state: "GA", cities: "Atlanta, Marietta, Alpharetta, Roswell, Kennesaw, Smyrna, Decatur, Lawrenceville, Augusta, Savannah, Columbus, Macon, Athens, Warner Robins, Valdosta, Gainesville, Albany, Rome, Dalton, Brunswick, Statesboro, Newnan, Peachtree City, Douglasville, Cartersville" };

  // Savannah metro — 45 min radius
  if (t.includes("savannah"))
    return { state: "GA", cities: "Savannah, Pooler, Richmond Hill, Hinesville, Statesboro, Brunswick, Jesup, Rincon, Bluffton SC, Hilton Head SC" };

  // Augusta metro — 45 min radius
  if (t.includes("augusta"))
    return { state: "GA", cities: "Augusta, Evans, Martinez, Grovetown, Aiken SC, North Augusta SC, Thomson, Harlem, Waynesboro, Edgefield SC" };

  // Macon / Middle Georgia
  if (t.includes("macon") || t.includes("middle georgia"))
    return { state: "GA", cities: "Macon, Warner Robins, Byron, Perry, Forsyth, Milledgeville, Dublin, Cochran, Gray, Kathleen" };

  // Birmingham metro — 45 min radius
  if (t.includes("birmingham"))
    return { state: "AL", cities: "Birmingham, Hoover, Vestavia Hills, Homewood, Bessemer, Pelham, Alabaster, Helena, Trussville, Gardendale, Moody, Leeds, Pell City, Calera, Clanton, Anniston, Talladega, Northport" };

  // Alabama statewide
  if (t === "al" || t === "alabama" || t.includes("alabama") || t.includes("huntsville") || t.includes("montgomery") || t.includes("tuscaloosa") || t.includes("mobile"))
    return { state: "AL", cities: "Birmingham, Hoover, Huntsville, Madison, Decatur, Montgomery, Mobile, Tuscaloosa, Northport, Auburn, Opelika, Dothan, Florence, Muscle Shoals, Gadsden, Anniston, Phenix City, Prattville, Enterprise, Daphne, Fairhope, Foley" };

  // Nashville metro — 45 min radius
  if (t.includes("nashville"))
    return { state: "TN", cities: "Nashville, Brentwood, Franklin, Murfreesboro, Smyrna, La Vergne, Hendersonville, Gallatin, Mount Juliet, Nolensville, Spring Hill, Columbia, Dickson, Clarksville, Lebanon" };

  // Memphis metro — 45 min radius
  if (t.includes("memphis"))
    return { state: "TN", cities: "Memphis, Germantown, Collierville, Bartlett, Cordova, Southaven MS, Olive Branch MS, Horn Lake MS, Hernando MS, Millington, Arlington, Lakeland" };

  // Chattanooga metro — 45 min radius
  if (t.includes("chattanooga"))
    return { state: "TN", cities: "Chattanooga, Cleveland, East Ridge, Soddy-Daisy, Red Bank, Signal Mountain, Ringgold GA, Dalton GA, Fort Oglethorpe GA, Rossville GA, LaFayette GA" };

  // Knoxville metro — 45 min radius
  if (t.includes("knoxville"))
    return { state: "TN", cities: "Knoxville, Maryville, Oak Ridge, Alcoa, Farragut, Powell, Lenoir City, Sevierville, Gatlinburg, Clinton, Morristown, Jefferson City" };

  // Tennessee statewide
  if (t === "tn" || t === "tennessee" || t.includes("tennessee"))
    return { state: "TN", cities: "Nashville, Brentwood, Franklin, Murfreesboro, Memphis, Germantown, Knoxville, Maryville, Chattanooga, Cleveland, Clarksville, Cookeville, Jackson, Kingsport, Johnson City, Bristol" };

  // Charlotte metro — 45 min radius
  if (t.includes("charlotte"))
    return { state: "NC", cities: "Charlotte, Concord, Kannapolis, Gastonia, Belmont, Mount Holly, Huntersville, Cornelius, Davidson, Mooresville, Matthews, Mint Hill, Monroe, Waxhaw, Rock Hill SC, Fort Mill SC, Tega Cay SC" };

  // Raleigh / Triangle metro — 45 min radius
  if (t.includes("raleigh") || t.includes("triangle") || t.includes("durham") || t.includes("chapel hill"))
    return { state: "NC", cities: "Raleigh, Durham, Cary, Chapel Hill, Apex, Holly Springs, Fuquay-Varina, Morrisville, Wake Forest, Garner, Clayton, Smithfield, Burlington, Mebane, Hillsborough" };

  // North Carolina statewide
  if (t === "nc" || t === "north carolina" || t.includes("north carolina"))
    return { state: "NC", cities: "Charlotte, Concord, Gastonia, Raleigh, Durham, Cary, Greensboro, Winston-Salem, High Point, Wilmington, Fayetteville, Asheville, Hickory, Greenville, Jacksonville, Rocky Mount, Wilson, Burlington, Mooresville, Huntersville" };

  // Columbia SC metro — 45 min radius
  if (t.includes("columbia sc") || t.includes("columbia, sc"))
    return { state: "SC", cities: "Columbia, Lexington, Irmo, West Columbia, Cayce, Chapin, Blythewood, Lugoff, Elgin, Sumter, Orangeburg, Newberry, Camden" };

  // Greenville SC / Upstate — 45 min radius
  if (t.includes("greenville sc") || t.includes("greenville, sc") || t.includes("spartanburg") || t.includes("upstate sc"))
    return { state: "SC", cities: "Greenville, Spartanburg, Greer, Mauldin, Simpsonville, Taylors, Anderson, Easley, Seneca, Duncan, Boiling Springs, Gaffney, Union, Laurens" };

  // Charleston SC metro — 45 min radius
  if (t.includes("charleston"))
    return { state: "SC", cities: "Charleston, North Charleston, Mount Pleasant, Summerville, Goose Creek, Hanahan, Ladson, Moncks Corner, Walterboro, Beaufort, Bluffton, Hilton Head" };

  // South Carolina statewide
  if (t === "sc" || t === "south carolina" || t.includes("south carolina"))
    return { state: "SC", cities: "Columbia, Lexington, Greenville, Spartanburg, Greer, Mauldin, Simpsonville, Anderson, Charleston, North Charleston, Mount Pleasant, Summerville, Myrtle Beach, Conway, Florence, Rock Hill, Aiken, Hilton Head, Beaufort" };

  // Mississippi statewide
  if (t === "ms" || t === "mississippi" || t.includes("mississippi") || t.includes("jackson ms"))
    return { state: "MS", cities: "Jackson, Ridgeland, Madison, Brandon, Pearl, Flowood, Hattiesburg, Laurel, Gulfport, Biloxi, Ocean Springs, Southaven, Olive Branch, Tupelo, Meridian, Vicksburg, Natchez, Pascagoula, Columbus, Starkville" };

  // Gulf Coast MS
  if (t.includes("gulf coast") || t.includes("gulfport") || t.includes("biloxi"))
    return { state: "MS", cities: "Gulfport, Biloxi, Ocean Springs, Pascagoula, Gautier, D'Iberville, Long Beach, Pass Christian, Bay St. Louis, Waveland, Slidell LA" };

  // Southeast region-wide
  if (t.includes("southeast") || t.includes("south east"))
    return { state: "", cities: "Atlanta GA, Marietta GA, Alpharetta GA, Birmingham AL, Hoover AL, Huntsville AL, Nashville TN, Brentwood TN, Franklin TN, Charlotte NC, Concord NC, Gastonia NC, Columbia SC, Greenville SC, Spartanburg SC, Chattanooga TN, Knoxville TN, Memphis TN, Jackson MS, Hattiesburg MS, Savannah GA, Augusta GA, Montgomery AL, Raleigh NC, Charleston SC" };

  // Default — just use what they typed
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

  function getProductContext(prod) {
    const p = (prod || '').toLowerCase();
    if (p.includes('soudal') || p.includes('boss') || p.includes('sealant') || p.includes('adhesive')) return {
      who: 'window and door installers, insulation contractors, commercial contractors, after-paint installers, glazing contractors, waterproofing contractors',
      why: 'Soudal BOSS sealants and adhesives are used for window and door installation, weatherproofing, firestopping, bonding, and gap sealing on commercial and residential jobs.',
      signals: 'Companies that install windows, doors, siding, roofing, or do commercial construction. They buy sealants in bulk.'
    };
    if (p.includes('shurtape') || p.includes('flashing') || p.includes('deck tape')) return {
      who: 'window and door installers, roofing contractors, deck contractors, home builders, remodelers',
      why: 'ShurTape flashing and deck tape is used for window rough openings, door flashing, deck waterproofing, and moisture barriers.',
      signals: 'Target companies installing windows, doors, or decks. They use flashing tape on every job.'
    };
    if (p.includes('alum') || p.includes('scaffolding') || p.includes('scaffold')) return {
      who: 'siding contractors, James Hardie installers, fiber cement siding contractors, exterior painters, stucco contractors, soffit and fascia contractors',
      why: 'Alum-A-Pole pump jack scaffolding is used by siding pros working at heights up to 50 feet. OSHA-compliant, lightweight aluminum, made in USA.',
      signals: 'Siding companies, exterior renovation contractors, James Hardie preferred installers, painting contractors on multi-story homes.'
    };
    if (p.includes('fortress') && (p.includes('framing') || p.includes('steel frame') || p.includes('evolution'))) return {
      who: 'deck builders, deck contractors, remodelers, general contractors, custom home builders',
      why: 'Fortress Evolution steel deck framing is rot-proof, termite-proof, stronger than wood. Eliminates callbacks from rot and termite damage.',
      signals: 'Dedicated deck builders and remodelers who build multiple decks per year.'
    };
    if (p.includes('fortress') && p.includes('railing')) return {
      who: 'deck contractors, fence contractors, builders, remodelers, commercial contractors',
      why: 'Fortress Railing aluminum and steel systems are low maintenance, code-compliant, and aesthetically superior to wood.',
      signals: 'Any company building decks, porches, balconies, or commercial walkways.'
    };
    return { who: custType, why: 'They regularly purchase building products.', signals: 'Active construction company.' };
  }

  const ctx = getProductContext(product);
  const targetType = custType !== 'any building products buyer' ? custType : ctx.who;

  // Split cities into chunks of ~4 cities each for reliable 10-lead batches
  const cityArr = cities.split(',').map(c => c.trim()).filter(Boolean);
  const CHUNK = 4;
  const BATCH_SIZE = 10;
  const numBatches = Math.ceil(numLeads / BATCH_SIZE);

  const batches = [];
  for (let i = 0; i < numBatches; i++) {
    const start = (i * CHUNK) % cityArr.length;
    const citySlice = [...cityArr.slice(start, start + CHUNK), ...cityArr.slice(0, Math.max(0, (start + CHUNK) - cityArr.length))];
    batches.push(citySlice.join(', ') || cities);
  }

  function makePrompt(citySet, n, exclude = []) {
    const excludeStr = exclude.length > 0 ? `\nDo NOT include these companies: ${exclude.slice(0,20).join(', ')}` : '';
    return `You are a B2B sales researcher for Compton Group LLC, a building products manufacturer's rep in the Southeast US.

Find exactly ${n} REAL ${targetType} businesses currently operating in: ${citySet}

Product to sell: ${product}
Why they need it: ${ctx.why}
What to look for: ${ctx.signals}

Search Google Maps, Houzz, Angi, BuildZoom, and company websites. For each return verified:
- Company name (must be a real operating business)
- Specific business type
- City, state
- Phone from Google listing
- Email from their website contact page  
- Owner or decision maker name
- Website URL
- Why this specific company needs ${product} (1 sentence based on their actual work)
- Priority: High / Medium / Low${excludeStr}

Return ONLY a JSON array starting with [ with exactly ${n} entries:
[{"company":"Name","category":"Type","city":"City","state":"${state}","phone":"number or null","email":"email or null","website":"url or null","contact":"name or null","products":"${product}","why":"specific reason","priority":"High or Medium or Low"}]`;
  }

  try {
    // Run all batches in parallel
    const results = await Promise.all(
      batches.map((citySet, i) => {
        const n = Math.min(BATCH_SIZE, numLeads - i * BATCH_SIZE);
        return n > 0
          ? callClaudeWithSearch(makePrompt(citySet, n)).catch(() => '[]')
          : Promise.resolve('[]');
      })
    );

    // Merge + dedupe
    const seen = new Set();
    let leads = [];
    for (const text of results) {
      for (const l of (extractJSON(text) || [])) {
        const key = (l.company || '').toLowerCase().trim();
        if (key && !seen.has(key) && leads.length < numLeads) {
          seen.add(key);
          leads.push(l);
        }
      }
    }

    // Fill gap with extra searches if needed
    let attempts = 0;
    while (leads.length < numLeads && attempts < 3) {
      attempts++;
      const need = Math.min(BATCH_SIZE, numLeads - leads.length);
      const exclude = leads.map(l => l.company);
      try {
        const fill = await callClaudeWithSearch(makePrompt(cities, need, exclude));
        for (const l of (extractJSON(fill) || [])) {
          const key = (l.company || '').toLowerCase().trim();
          if (key && !seen.has(key) && leads.length < numLeads) {
            seen.add(key);
            leads.push(l);
          }
        }
      } catch(e) { break; }
    }

    if (leads.length === 0) return res.json({ error: 'Could not find leads. Try a different category or territory.' });
    res.json({ leads });
  } catch (e) {
    if (e.message?.includes('rate limit')) return res.json({ error: e.message });
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

// Morning Briefing
router.post('/morning-briefing', async (req, res) => {
  const uid = req.session.user.id;
  const user = req.session.user;
  try {
    const [prospects, calls, followups] = await Promise.all([
      pool.query("SELECT company, pipeline_stage, priority, products FROM prospects WHERE user_id=$1 ORDER BY CASE priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END LIMIT 20", [uid]),
      pool.query("SELECT c.*, p.company FROM calls c JOIN prospects p ON c.prospect_id=p.id WHERE c.user_id=$1 ORDER BY c.call_date DESC LIMIT 5", [uid]),
      pool.query("SELECT c.next_step, c.next_step_date, p.company, p.phone FROM calls c JOIN prospects p ON c.prospect_id=p.id WHERE c.user_id=$1 AND c.next_step_date <= CURRENT_DATE AND c.next_step IS NOT NULL ORDER BY c.next_step_date ASC LIMIT 5", [uid])
    ]);

    const prompt = `You are a sales coach for ${user.name}, a manufacturer's rep at Compton Group LLC in the Southeast.

Their CRM data:
- Prospects: ${prospects.rows.map(p => p.company + ' (' + p.pipeline_stage + ', ' + p.priority + ' priority)').join('; ') || 'none yet'}
- Recent calls: ${calls.rows.map(c => c.company + ': ' + c.outcome).join('; ') || 'none'}
- Follow-ups due: ${followups.rows.map(f => f.company + ' - ' + f.next_step).join('; ') || 'none'}

Write exactly 4 short morning briefing bullets (1 sentence each) telling them what to focus on today. Be specific, direct, and actionable. Reference actual company names from their data when possible.

Return ONLY a JSON array of 4 strings, no markdown:
["bullet 1","bullet 2","bullet 3","bullet 4"]`;

    const text = await callClaude(prompt);
    const start = text.indexOf('[');
    const end = text.lastIndexOf(']');
    if (start === -1 || end === -1) return res.json({ bullets: ['Check your follow-ups and prioritize High priority contacts today.', 'Log all calls and visits in RepRoute to keep your pipeline current.', 'Use AI Lead Finder to add new prospects to your territory.', 'Review your pipeline and move deals forward.'] });
    const bullets = JSON.parse(text.substring(start, end + 1));
    res.json({ bullets });
  } catch(e) {
    res.json({ bullets: ['Start your day by reviewing follow-ups due today.', 'Log all calls and visits to keep your pipeline accurate.', 'Focus on your High priority contacts first.', 'Use AI Lead Finder to keep your pipeline full.'] });
  }
});

// Weekly Route Planner
router.post('/route-planner', async (req, res) => {
  const uid = req.session.user.id;
  const user = req.session.user;
  const { extra_leads, crm_only } = req.body;

  let crmContacts = [];
  if (!crm_only) {
    const prospects = await pool.query(
      "SELECT company, category, city, state, phone, priority FROM prospects WHERE user_id=$1 AND city IS NOT NULL AND city != '' ORDER BY CASE priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END",
      [uid]
    );
    crmContacts = prospects.rows;
  }

  const rawLeads = extra_leads && extra_leads.length > 0
    ? extra_leads.slice(0, 50)
    : [...crmContacts, ...(extra_leads || [])].slice(0, 50);

  const allLeads = rawLeads.map(l => ({
    company: l.company || 'Unknown',
    category: l.category || 'Contractor',
    city: l.city || '',
    state: l.state || '',
    phone: l.phone || null,
    priority: l.priority || 'Medium'
  }));

  if (allLeads.length === 0) return res.json({ error: 'No contacts found. Add prospects to your CRM or search for leads first.' });
  console.log('Building route for', allLeads.length, 'leads');

  const sorted = [...allLeads].sort((a, b) => (a.city || '').localeCompare(b.city || ''));
  const dayNames = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'];
  const chunks = dayNames.map((day, i) => ({
    day, leads: sorted.slice(i * 10, (i + 1) * 10)
  })).filter(c => c.leads.length > 0);

  function makeDayPrompt(dayName, leads) {
    const list = leads.map((l, i) =>
      (i+1) + '. ' + l.company + ' — ' + l.category + ' — ' + l.city + ', ' + l.state + ' — ' + l.priority + ' priority — ' + (l.phone || 'no phone')
    ).join('\n');
    return 'You are a route planning expert for a manufacturer rep. Order these ' + leads.length + ' stops for ' + dayName + ' to minimize drive time, grouping nearby cities together:\n' + list + '\n\nReturn ONLY valid JSON starting with { and ending with }:\n{"day":"' + dayName + '","focus_area":"main area","estimated_drive":"est time","stops":[{"order":1,"company":"name","category":"type","city":"city","state":"ST","phone":"number or null","priority":"High/Medium/Low","goal":"visit goal"}]}';
  }

  try {
    const dayResults = await Promise.all(
      chunks.map(async ({ day, leads }) => {
        try {
          const text = await callClaude(makeDayPrompt(day, leads));
          let clean = text.replace(/```json[\n]?/g, '').replace(/```/g, '').trim();
          const si = clean.indexOf('{');
          const ei = clean.lastIndexOf('}');
          if (si === -1 || ei === -1) throw new Error('No JSON');
          return JSON.parse(clean.substring(si, ei + 1));
        } catch(e) {
          console.error('Day error:', day, e.message);
          return {
            day, focus_area: leads[0]?.city || 'Territory', estimated_drive: 'varies',
            stops: leads.map((l, i) => ({ order: i+1, ...l, goal: 'Introduce products and build relationship' }))
          };
        }
      })
    );

    res.json({
      route: {
        week_summary: 'Optimized ' + allLeads.length + '-stop weekly route for ' + (user.territory || 'your territory') + ' — 10 stops per day',
        total_stops: allLeads.length,
        days: dayResults
      }
    });
  } catch(e) {
    console.error('Route error:', e.message);
    res.json({ error: 'Could not build route: ' + e.message });
  }
});

module.exports = router;
