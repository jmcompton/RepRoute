const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

// ─── Anthropic client ─────────────────────────────────────────────────────────
// Reuse the SAME client config and active model as routes/ai.js. The shared
// callClaude() helper there takes no system prompt, so we replicate the minimal
// client here with a `system` field. Keep MODEL in sync with routes/ai.js.
const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-6';

// System prompt — intent is verbatim from the spec. Drives a recap that does
// NOT read as AI-written.
const RECAP_SYSTEM_PROMPT = `You are the sales rep writing a short daily recap email to your team about the calls and visits you made today. Output ONLY the email body as plain text, ready to copy and paste. Write the way a sharp, busy salesperson writes: plain, direct, professional.
Hard rules:
- Never use em dashes or en dashes. Use periods, commas, or parentheses.
- No markdown at all: no asterisks, bold, headers, or bullet characters.
- No emoji.
- Do not use AI-tell words or phrases (delve, leverage, robust, furthermore, moreover, additionally, streamline, foster, navigate, underscore, testament, 'in today's landscape', 'it's worth noting', 'I hope this email finds you well').
- No throat-clearing intro and no rule-of-three cadence.
Structure: one short opening line, then a concise rundown by account of who you spoke with, what happened, and the next step, then a brief sign off with the rep's name. Use only the real names and details from the notes. If a note is thin, keep that account to one line. Never invent details that aren't in the notes.`;

async function callClaudeWithSystem(system, userMessage) {
  const res = await fetch(CLAUDE_API, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: 1500,
      system,
      messages: [{ role: 'user', content: userMessage }]
    })
  });
  const data = await res.json();
  if (data.error) throw new Error(data.error.message || 'AI error');
  return (data.content || []).filter(b => b.type === 'text').map(b => b.text || '').join('').trim();
}

// Belt-and-suspenders cleanup of the model output: enforce the no-dash / no-markdown
// rules server-side even if the model slips. Conservative — only touches dashes,
// markdown emphasis/headers, and leading bullet markers.
function sanitizeEmail(text) {
  return (text || '')
    .replace(/\s*[—–]\s*/g, ', ')               // em/en dashes → comma
    .replace(/\*\*(.+?)\*\*/g, '$1')                        // **bold**
    .replace(/\*(.+?)\*/g, '$1')                            // *italic*
    .replace(/^[ \t]*#{1,6}[ \t]+/gm, '')                   // # headers
    .replace(/^[ \t]*[\*•‣◦⁃+\-][ \t]+/gm, '')  // leading bullets
    .replace(/`+/g, '')                                     // stray backticks
    .trim();
}

// Map the logged-in user to a Fortress promo rep token, mirroring the frontend
// fortressDefaultRep() exactly: the fortress_promo_stops.rep column holds first
// names ('Kody' / 'Jack'). Managers / unknown users have no single-rep promo
// activity, so they map to 'all' and we skip the Fortress source for the recap.
function fortressRepFromUser(user) {
  const name = (user.name || '').toLowerCase();
  if (user.role === 'manager') return 'all';
  if (name.indexOf('kody') !== -1) return 'Kody';
  if (name.indexOf('jack') !== -1) return 'Jack';
  return 'all';
}

// ─── Gather today's activity for a rep ─────────────────────────────────────────
// Merges two sources. Each item: { source, name, contact, time, outcome, note }.
async function gatherTodayActivity(user) {
  const uid = user.id;
  const items = [];

  // (a) Today's call-log notes. Same date basis as the "calls logged today"
  //     counter the reps already see: calls.call_date = CURRENT_DATE.
  const calls = await pool.query(
    `SELECT p.company AS name,
            p.contact AS contact,
            c.outcome AS outcome,
            c.notes   AS note,
            to_char(c.created_at, 'HH12:MI AM') AS time
       FROM calls c
       JOIN prospects p ON c.prospect_id = p.id
      WHERE c.user_id = $1 AND c.call_date = CURRENT_DATE
      ORDER BY c.created_at ASC`,
    [uid]
  );
  for (const r of calls.rows) {
    items.push({
      source: 'call',
      name: r.name || 'Unknown account',
      contact: r.contact || '',
      time: r.time || '',
      outcome: r.outcome || '',
      note: r.note || ''
    });
  }

  // (b) Today's Fortress promo visits from fortress_promo_stops. Match the rep
  //     the same way the Fortress tab does (rep token Kody/Jack). 'all' (manager
  //     or unknown) has no personal promo activity → skip. The to_regclass guard
  //     keeps this safe on a fresh DB where the table hasn't been seeded yet.
  const repToken = fortressRepFromUser(user);
  if (repToken !== 'all') {
    try {
      const exists = await pool.query("SELECT to_regclass('public.fortress_promo_stops') AS t");
      if (exists.rows[0] && exists.rows[0].t) {
        const fr = await pool.query(
          `SELECT company AS name,
                  outcome AS outcome,
                  notes   AS note,
                  to_char(visited_at, 'HH12:MI AM') AS time
             FROM fortress_promo_stops
            WHERE rep = $1 AND visited_at::date = CURRENT_DATE
            ORDER BY visited_at ASC`,
          [repToken]
        );
        for (const r of fr.rows) {
          items.push({
            source: 'fortress',
            name: r.name || 'Unknown dealer',
            contact: '',
            time: r.time || '',
            outcome: r.outcome || '',
            note: r.note || ''
          });
        }
      }
    } catch (e) {
      // Table missing or shape differs — skip this source, never break the recap.
      console.error('[recap] fortress source skipped:', e.message);
    }
  }

  return items;
}

// Group activity items by account/dealer name, preserving first-seen order.
function groupByName(items) {
  const map = new Map();
  for (const it of items) {
    const key = it.name || 'Unknown';
    if (!map.has(key)) map.set(key, { name: key, items: [] });
    map.get(key).items.push(it);
  }
  return Array.from(map.values());
}

// ─── GET /api/recap/today ──────────────────────────────────────────────────────
router.get('/today', async (req, res) => {
  try {
    const items = await gatherTodayActivity(req.session.user);
    const dateRow = await pool.query("SELECT to_char(CURRENT_DATE, 'FMDay, FMMonth FMDD, YYYY') AS d");
    res.json({
      rep: req.session.user.name || 'Rep',
      date: dateRow.rows[0] ? dateRow.rows[0].d : '',
      call_count: items.filter(i => i.source === 'call').length,
      visit_count: items.filter(i => i.source === 'fortress').length,
      groups: groupByName(items)
    });
  } catch (e) {
    console.error('[recap] today error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── POST /api/recap/generate-email ─────────────────────────────────────────────
// Recompute the same day's activity server-side, send to Anthropic, return plain
// text. Persists nothing.
router.post('/generate-email', async (req, res) => {
  try {
    const user = req.session.user;
    const items = await gatherTodayActivity(user);
    if (!items.length) {
      return res.json({ email: '' });
    }

    // Build the structured list the model writes from.
    const groups = groupByName(items);
    const lines = [];
    for (const g of groups) {
      lines.push(`Account: ${g.name}`);
      for (const it of g.items) {
        const meta = [];
        if (it.contact) meta.push(`spoke with ${it.contact}`);
        if (it.outcome) meta.push(`outcome: ${it.outcome}`);
        if (it.time) meta.push(`time: ${it.time}`);
        const kind = it.source === 'fortress' ? 'Fortress visit' : 'Call';
        const metaStr = meta.length ? ` (${meta.join(', ')})` : '';
        lines.push(`  ${kind}${metaStr}: ${it.note && it.note.trim() ? it.note.trim() : '(no note recorded)'}`);
      }
    }

    const repName = user.name || 'Rep';
    const userMessage =
      `Rep name: ${repName}\n` +
      `This is the activity to recap (today). Group the email by account.\n\n` +
      lines.join('\n');

    const raw = await callClaudeWithSystem(RECAP_SYSTEM_PROMPT, userMessage);
    res.json({ email: sanitizeEmail(raw) });
  } catch (e) {
    console.error('[recap] generate-email error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
