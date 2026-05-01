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

// Weekly Route Planner
router.post('/route-planner', async (req, res) => {
  const uid = req.session.user.id;
  const user = req.session.user;
  const { extra_leads, crm_only } = req.body;

  let crmContacts = [];
  if (!crm_only) {
    const prospects = await pool.query(
      "SELECT company, category, city, state, phone, priority, pipeline_stage FROM prospects WHERE user_id=$1 AND city IS NOT NULL AND city != '' ORDER BY CASE priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END",
      [uid]
    );
    crmContacts = prospects.rows;
  }

  // If leads passed directly use those, otherwise combine CRM + extras
  const allLeads = extra_leads && extra_leads.length > 0
    ? extra_leads.slice(0, 50)
    : [...crmContacts, ...(extra_leads || [])];

  if (allLeads.length === 0) return res.json({ error: 'No contacts found. Add prospects to your CRM or search for leads first.' });

  const contactList = allLeads.map((p, i) =>
    `${i+1}. ${p.company} — ${p.category || 'Contractor'} — ${p.city}, ${p.state || 'GA'} — Priority: ${p.priority || 'Medium'} — Phone: ${p.phone || 'unknown'}`
  ).join('\n');

  const prompt = `You are a route planning expert for a manufacturer's rep in the Southeast US.

Rep: ${user.name}
Territory: ${user.territory || 'Southeast'}

Here are all the prospects and contacts to visit this week:
${contactList}

Build an optimized Mon-Fri drive route that:
1. Groups nearby cities/areas together on the same day
2. Prioritizes High priority contacts
3. Minimizes total drive time by clustering geographically
4. Puts 8-10 stops per day minimum
5. Considers that the rep starts and ends each day at home base in the territory

Return ONLY valid JSON, no markdown, no backticks:
{
  "week_summary": "Brief description of the routing strategy",
  "total_stops": 0,
  "days": [
    {
      "day": "Monday",
      "focus_area": "City/area focus for the day",
      "estimated_drive": "e.g. ~45 min total",
      "stops": [
        {
          "order": 1,
          "company": "Company name",
          "category": "Type",
          "city": "City",
          "state": "ST",
          "phone": "number or null",
          "priority": "High/Medium/Low",
          "goal": "What to accomplish at this stop"
        }
      ]
    }
  ]
}`;

  try {
    const text = await callClaude(prompt);
    let clean = text.replace(/\`\`\`json\n?/g, '').replace(/\`\`\`/g, '').trim();
    const startIdx = clean.indexOf('{');
    const endIdx = clean.lastIndexOf('}');
    if (startIdx === -1 || endIdx === -1) return res.json({ error: 'Could not build route. Try again.' });
    const route = JSON.parse(clean.substring(startIdx, endIdx + 1));
    res.json({ route });
  } catch(e) {
    res.json({ error: 'Could not build route: ' + e.message });
  }
});

module.exports = router;
