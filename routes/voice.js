const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-haiku-4-5-20251001';

// ── Levenshtein + fuzzy matching ─────────────────────────────────
function levenshtein(a, b) {
  const m = a.length, n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const dp = [];
  for (let i = 0; i <= m; i++) { dp[i] = [i]; }
  for (let j = 0; j <= n; j++) { dp[0][j] = j; }
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = a[i-1] === b[j-1]
        ? dp[i-1][j-1]
        : 1 + Math.min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]);
    }
  }
  return dp[m][n];
}

function normalize(s) {
  return (s || '').toLowerCase()
    .replace(/[-_&,\.]/g, ' ')
    .replace(/\bllc\b|\binc\b|\bcorp\b|\bco\b|\bltd\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function similarity(a, b) {
  const na = normalize(a), nb = normalize(b);
  if (!na || !nb) return 0;
  const longer = Math.max(na.length, nb.length);
  if (longer === 0) return 1;
  return (longer - levenshtein(na, nb)) / longer;
}

// ── POST /api/voice/process ──────────────────────────────────────
// Extract structured data from a transcript using Anthropic
router.post('/process', async (req, res) => {
  const { transcript } = req.body;
  if (!transcript || !transcript.trim()) {
    return res.status(400).json({ error: 'Transcript required' });
  }

  const prompt = `You are a CRM data extraction assistant for a building products manufacturer's rep. Extract key information from this sales call transcript.

TRANSCRIPT:
"${transcript.trim().substring(0, 4000)}"

Extract and return ONLY a JSON object with exactly these fields:
- account_name: The company/business name mentioned (null if not mentioned). Extract verbatim as spoken.
- contact_name: First name only of the person spoken with at the account (null if not mentioned). First name only.
- summary: Clean 2-3 sentence summary of what was discussed, outcomes, and any products mentioned.
- outcome: Must be exactly one of: "Left Voicemail" | "Dropped In - Interested" | "Dropped In - Not Interested" | "Left Sample" | "Ready to Order" | "Follow-up Needed" | "Not Available" | "Phone - Interested" | "Phone - Not Interested" | "Order Placed" | "Demo Scheduled" | "Other"
- next_steps: Specific next action to take, verbatim if mentioned (null if none mentioned)

Choose the outcome that best matches what was described. Default to "Other" if unclear.

Return ONLY valid JSON, no markdown, no backticks, no explanation:
{"account_name":null,"contact_name":null,"summary":"...","outcome":"Other","next_steps":null}`;

  try {
    const response = await fetch(CLAUDE_API, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: 800,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const data = await response.json();
    if (data.error) {
      console.error('[voice/process] API error:', data.error.message);
      // Graceful fallback
      return res.json({
        account_name: null, contact_name: null,
        summary: transcript.trim().substring(0, 400),
        outcome: 'Other', next_steps: null
      });
    }

    const text = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('');
    const start = text.indexOf('{');
    const end = text.lastIndexOf('}');

    if (start === -1 || end === -1) {
      return res.json({
        account_name: null, contact_name: null,
        summary: transcript.trim().substring(0, 400),
        outcome: 'Other', next_steps: null
      });
    }

    try {
      const extracted = JSON.parse(text.substring(start, end + 1));
      res.json(extracted);
    } catch (parseErr) {
      res.json({
        account_name: null, contact_name: null,
        summary: transcript.trim().substring(0, 400),
        outcome: 'Other', next_steps: null
      });
    }
  } catch (e) {
    console.error('[voice/process] error:', e.message);
    // Never fail — return transcript as summary
    res.json({
      account_name: null, contact_name: null,
      summary: transcript.trim().substring(0, 400),
      outcome: 'Other', next_steps: null
    });
  }
});

// ── GET /api/voice/match-account?name= ──────────────────────────
// Fuzzy match a spoken account name against the user's prospects
router.get('/match-account', async (req, res) => {
  const uid = req.session.user.id;
  const { name } = req.query;
  if (!name || !name.trim()) return res.json({ matches: [], best_match: null });

  try {
    // Get all distinct company names for this user
    const result = await pool.query(
      `SELECT DISTINCT ON (LOWER(TRIM(company)))
              id, company, city, state, category, company_type
       FROM prospects
       WHERE user_id = $1
       ORDER BY LOWER(TRIM(company)), id ASC`,
      [uid]
    );

    const accounts = result.rows;
    const inputNorm = normalize(name);

    // Score each account
    let scored = accounts.map(a => ({
      id: a.id,
      company: a.company,
      city: a.city || '',
      state: a.state || '',
      category: a.category || '',
      sim: similarity(name, a.company)
    }));

    // Also check substring containment for short or partial names
    scored = scored.map(a => {
      const compNorm = normalize(a.company);
      let sim = a.sim;
      // Boost if one contains the other (e.g. "ABC" matches "ABC Roofing")
      if (inputNorm.length >= 3 && (compNorm.includes(inputNorm) || inputNorm.includes(compNorm))) {
        const ratio = Math.min(inputNorm.length, compNorm.length) / Math.max(inputNorm.length, compNorm.length);
        sim = Math.max(sim, 0.65 + ratio * 0.3);
      }
      return { ...a, sim };
    });

    const matches = scored
      .filter(a => a.sim >= 0.60)
      .sort((a, b) => b.sim - a.sim)
      .slice(0, 3);

    const best_match = matches.length > 0 && matches[0].sim >= 0.70 ? matches[0] : null;

    res.json({ matches, best_match });
  } catch (e) {
    console.error('[voice/match-account] error:', e.message);
    res.status(500).json({ error: e.message });
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

module.exports = router;
