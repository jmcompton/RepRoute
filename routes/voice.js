const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
// Card reading: same Sonnet model the rest of RepRoute already runs on
// (was pinned to an older Opus model, and errors were hidden from the rep).
const CARD_MODEL = 'claude-sonnet-4-6';

// ── Levenshtein + fuzzy matching ─────────────────────────────────
// Shared implementation lives in lib/fuzzy.js (also used by the commission
// import account matcher). Behavior is identical to the prior inline copy.
const { levenshtein, normalize, similarity } = require('../lib/fuzzy');
const { accountKey } = require('../lib/account-key');

// ── Voice account scoring ────────────────────────────────────────
// Speech-to-text mangles company names in predictable ways: drops or adds
// "The", says "and" for "&", merges or splits words ("ABC Supply" ↔
// "ABCsupply"), leaves off the city or "Supply" at the end, or gets one word
// slightly wrong. Plain whole-string Levenshtein misses most of these, so the
// rep's existing account wasn't found and a duplicate got created.
function voiceKey(s) {
  return accountKey(String(s || '').replace(/['’`]/g, '')).replace(/^the\s+/, '');
}
function tokenSim(a, b) {
  if (a === b) return 1;
  const L = Math.max(a.length, b.length);
  return L ? (L - levenshtein(a, b)) / L : 0;
}
function voiceScore(spoken, company) {
  const ka = voiceKey(spoken), kb = voiceKey(company);
  if (!ka || !kb) return { sim: 0, exact: false };
  if (ka === kb) return { sim: 1, exact: true };

  let sim = similarity(spoken, company);       // original whole-string score
  sim = Math.max(sim, tokenSim(ka, kb));        // same, on cleaned keys
  const ca = ka.replace(/ /g, ''), cb = kb.replace(/ /g, '');
  if (ca === cb) return { sim: 0.98, exact: false }; // only spacing differs
  sim = Math.max(sim, tokenSim(ca, cb));

  // Word-by-word: every spoken word found (allowing small misspellings) in
  // the account name — e.g. "Beacon Roofing" vs "Beacon Roofing Supply - Duluth".
  const ta = ka.split(' ').filter(w => w !== 'and');
  const tb = kb.split(' ').filter(w => w !== 'and');
  if (ta.length && tb.length) {
    let hit = 0;
    ta.forEach(w => {
      const best = Math.max(...tb.map(v => tokenSim(w, v)));
      if (best >= (w.length <= 3 ? 1 : 0.75)) hit++;
    });
    const covA = hit / ta.length, covB = hit / tb.length;
    if (covA === 1) sim = Math.max(sim, 0.72 + 0.26 * covB);
    else if (covA >= 0.5) sim = Math.max(sim, 0.45 + 0.3 * covA * covB + 0.15 * covA);
  }

  // One name contains the other ("ABC" ↔ "ABC Roofing")
  if (ca.length >= 3 && (cb.includes(ca) || ca.includes(cb))) {
    const ratio = Math.min(ca.length, cb.length) / Math.max(ca.length, cb.length);
    sim = Math.max(sim, 0.65 + ratio * 0.3);
  }
  return { sim: Math.min(sim, 0.99), exact: false };
}

// ── POST /api/voice/process ──────────────────────────────────────
// Turn the spoken note into fields (see lib/voice-extract.js for what it
// guards against). too_short:true means the app must not save it as a visit.
const llm = require('../lib/llm');
const vx = require('../lib/voice-extract');
const { reverseGeocode, milesBetween } = require('../lib/geocode');

router.post('/process', async (req, res) => {
  const transcript = String((req.body || {}).transcript || '').trim();
  if (!transcript) return res.status(400).json({ error: 'Transcript required' });
  const today = vx.todayYMD();
  if (vx.wordCount(transcript) < vx.MIN_WORDS) return res.json(vx.finalize({}, transcript, today));
  try {
    const text = await llm.complete({ prompt: vx.buildPrompt(transcript, today), maxTokens: 1200, timeoutMs: 30000 });
    return res.json(vx.finalize(llm.parseJson(text), transcript, today));
  } catch (e) {
    console.error('[voice/process] AI error:', e.message);
    const out = vx.finalize({}, transcript, today);
    out._ai_error = 'AI unavailable';
    return res.json(out);
  }
});

// ── GET /api/voice/where?lat=&lng= ───────────────────────────────
// City and state for where the rep is standing, to fill new accounts.
router.get('/where', async (req, res) => {
  const g = await reverseGeocode(req.query.lat, req.query.lng);
  res.json(g || {});
});

// ── GET /api/voice/match-account?name=&city=&lat=&lng= ───────────
// Matches the spoken name against EVERY active rep's accounts, not just the
// caller's: a rep logging a visit at a teammate's account used to create a
// second copy of it. Location decides between branches with the same name
// (a Beacon in Gainesville is not the Beacon in Toccoa).
router.get('/match-account', async (req, res) => {
  const uid = req.session.user.id;
  const name = String(req.query.name || '').trim();
  if (!name) return res.json({ matches: [], best_match: null });
  const city = String(req.query.city || '').trim().toLowerCase();
  const lat = parseFloat(req.query.lat), lng = parseFloat(req.query.lng);
  const here = Number.isFinite(lat) && Number.isFinite(lng) ? { lat, lng } : null;
  try {
    const result = await pool.query(
      `SELECT p.id, p.user_id, p.company, p.city, p.state, p.category, p.company_type, p.lat, p.lng,
              COALESCE(NULLIF(TRIM(u.name),''), u.email) AS owner_name
         FROM prospects p JOIN users u ON u.id = p.user_id
        WHERE u.disabled_at IS NULL AND p.company IS NOT NULL AND TRIM(p.company) <> ''`);

    // One entry per account (name + city); prefer the caller's own row.
    const byKey = new Map();
    for (const a of result.rows) {
      const { sim, exact } = voiceScore(name, a.company);
      if (sim < 0.60) continue;
      const aCity = String(a.city || '').trim().toLowerCase();
      let score = sim, dist = null;
      if (here && a.lat != null && a.lng != null) {
        dist = milesBetween(here, { lat: Number(a.lat), lng: Number(a.lng) });
        if (dist <= 15) score += 0.06; else if (dist > 60) score -= 0.15;
      }
      if (city && aCity) score += aCity === city ? 0.05 : -0.08;
      if (a.user_id === uid) score += 0.02;
      // Same name but clearly another branch: not an exact hit.
      const farAway = (dist != null && dist > 25) || (dist == null && city && aCity && aCity !== city);
      const k = voiceKey(a.company) + '|' + aCity;
      const prev = byKey.get(k);
      const cand = {
        id: a.id, company: a.company, city: a.city || '', state: a.state || '', category: a.category || '',
        owner_name: a.owner_name, is_mine: a.user_id === uid,
        distance_mi: dist == null ? null : Math.round(dist * 10) / 10,
        sim: Math.max(0, Math.min(score, 0.99)), exact: exact && !farAway
      };
      if (!prev || cand.sim > prev.sim || (cand.sim === prev.sim && cand.is_mine && !prev.is_mine)) byKey.set(k, cand);
    }
    const matches = [...byKey.values()].sort((a, b) => b.sim - a.sim).slice(0, 5);
    const exactHits = matches.filter(m => m.exact);
    // Two exact hits in different places means branches: let the rep pick.
    const exactOne = exactHits.length === 1 ? exactHits[0] : null;
    const best_match = matches.length && matches[0].sim >= 0.70 ? matches[0] : null;
    res.json({ matches, best_match, exact: exactOne });
  } catch (e) {
    console.error('[voice/match-account] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/voice/account-names ─────────────────────────────────
// Distinct account names for the rep, for autocomplete on the confirm screen.
router.get('/account-names', async (req, res) => {
  const uid = req.session.user.id;
  try {
    const r = await pool.query(
      `SELECT DISTINCT ON (LOWER(TRIM(p.company))) TRIM(p.company) AS company
         FROM prospects p JOIN users u ON u.id = p.user_id
        WHERE u.disabled_at IS NULL AND p.company IS NOT NULL AND TRIM(p.company) <> ''
        ORDER BY LOWER(TRIM(p.company))
        LIMIT 8000`,
      []
    );
    res.json(r.rows.map(x => x.company));
  } catch (e) {
    console.error('[voice/account-names] error:', e.message);
    res.json([]);
  }
});

// ── GET /api/voice/account-contacts?company= ────────────────────
// Get all contacts (prospects) under a given company name
router.get('/account-contacts', async (req, res) => {
  const uid = req.session.user.id;
  const { company } = req.query;
  if (!company || !company.trim()) return res.json([]);

  try {
    const result = await pool.query(
      `SELECT id, contact, phone, email, city, state, category
       FROM prospects
       WHERE user_id = $1 AND LOWER(TRIM(company)) = LOWER(TRIM($2))
       ORDER BY id ASC`,
      [uid, company.trim()]
    );
    res.json(result.rows);
  } catch (e) {
    console.error('[voice/account-contacts] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/voice/scan-card ────────────────────────────────────
// Send a base64 business card image to Claude Vision and extract fields
router.post('/scan-card', async (req, res) => {
  const { image, mimeType } = req.body;
  if (!image || !image.trim()) {
    return res.status(400).json({ error: 'Image data required' });
  }

  // Accept a full data URL too, and only send types the vision API takes.
  let data = String(image).trim();
  let mediaType = mimeType || 'image/jpeg';
  const m = data.match(/^data:([^;]+);base64,(.*)$/s);
  if (m) { mediaType = m[1]; data = m[2]; }
  if (!['image/jpeg', 'image/png', 'image/gif', 'image/webp'].includes(mediaType)) {
    return res.json({ error: 'Unsupported image type (' + mediaType + '). Take the photo with the camera or save it as JPG/PNG.', extracted: {} });
  }

  const prompt = `You are a business card reader. Extract all information from this business card image and return ONLY a JSON object with these fields (use null for any field not found):
{
  "contact_name": "full name on card",
  "first_name": "first name only",
  "last_name": "last name only",
  "company_name": "company or organization name",
  "title": "job title or position",
  "phone": "main phone number",
  "mobile": "mobile/cell number if different from phone",
  "email": "email address",
  "address": "street address",
  "city": "city",
  "state": "state abbreviation",
  "zip": "zip code",
  "website": "website URL"
}
Return only the JSON object, no other text, no markdown, no backticks.`;

  try {
    const response = await fetch(CLAUDE_API, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: CARD_MODEL,
        max_tokens: 1024,
        messages: [{
          role: 'user',
          content: [
            {
              type: 'image',
              source: {
                type: 'base64',
                media_type: mediaType,
                data: data
              }
            },
            {
              type: 'text',
              text: prompt
            }
          ]
        }]
      })
    });

    const ai = await response.json();
    if (ai.error) {
      console.error('[voice/scan-card] API error:', response.status, JSON.stringify(ai.error));
      return res.json({ error: 'Card reader error: ' + (ai.error.message || response.status), extracted: {} });
    }

    const text = (ai.content || []).filter(b => b.type === 'text').map(b => b.text).join('');
    const start = text.indexOf('{');
    const end = text.lastIndexOf('}');

    if (start === -1 || end === -1) {
      console.error('[voice/scan-card] no JSON in reply:', text.slice(0, 200));
      return res.json({ error: 'Could not read card', extracted: {} });
    }

    let extracted;
    try {
      extracted = JSON.parse(text.substring(start, end + 1));
    } catch (parseErr) {
      console.error('[voice/scan-card] bad JSON:', text.slice(0, 200));
      return res.json({ error: 'Could not read card', extracted: {} });
    }
    // Drop empty fields; if nothing was read, say so instead of "success"
    Object.keys(extracted).forEach(k => {
      const v = extracted[k];
      if (v == null || String(v).trim() === '' || String(v).toLowerCase() === 'null') delete extracted[k];
    });
    if (Object.keys(extracted).length === 0) {
      return res.json({ error: 'No text found on the card. Hold it closer, fill the frame, and try again.', extracted: {} });
    }
    // Return extracted fields + the image data so frontend can store it
    return res.json({ extracted, image_data: data });
  } catch (e) {
    console.error('[voice/scan-card] error:', e.message);
    return res.json({ error: 'Could not read card', extracted: {} });
  }
});

module.exports = router;
