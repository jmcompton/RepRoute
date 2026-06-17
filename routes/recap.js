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
Structure:
- The first line is the date on its own clean line, written exactly as "Recap for <DATE>." using the date provided in the data. Put nothing before it.
- Then a concise rundown, one account at a time. Lead each entry with the company/account name as a natural lead-in, not a label. For example write "Smith Lumber Supply. Spoke with Mike, he wants ShurTape samples, sending them Thursday." Do not write "Company:" or "Account:" or any templated label before the name. After the name, say who you spoke with, what happened, and the next step, using only the notes.
- If an entry has no company/account name, lead with the contact's name instead. Never print an empty name, a placeholder, "Unknown", or a dangling label.
- End with a brief sign off using the rep's name.
Use only the real names and details from the data. If a note is thin, keep that account to one line. Never invent details that aren't in the notes.`;

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
    const company = (r.name || '').trim();
    const contact = (r.contact || '').trim();
    items.push({
      source: 'call',
      company: company,                                  // raw account name (may be blank)
      name: company || contact || 'Unknown account',     // display / group key (contact fallback)
      contact: contact,
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
          const company = (r.name || '').trim();
          items.push({
            source: 'fortress',
            company: company,
            name: company || 'Unknown dealer',
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

// Today's date, formatted like "Wednesday, June 17, 2026", from the DB so it
// uses the same CURRENT_DATE basis as the rest of the recap (and the calls
// counter the reps already see).
async function todayFormatted() {
  const r = await pool.query("SELECT to_char(CURRENT_DATE, 'FMDay, FMMonth FMDD, YYYY') AS d");
  return r.rows[0] ? r.rows[0].d : '';
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
    const date = await todayFormatted();
    res.json({
      rep: req.session.user.name || 'Rep',
      date: date,
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

    const date = await todayFormatted();
    const repName = user.name || 'Rep';

    // Build the structured list the model writes from. Each entry carries the
    // company/account name explicitly (the customer called on, NOT the product
    // brands) so the model can lead the line with it. We still group by account
    // so repeated calls on the same account consolidate. The account name is a
    // natural lead-in for the email; the system prompt forbids a templated label.
    const groups = groupByName(items);
    const blocks = [];
    for (const g of groups) {
      for (const it of g.items) {
        // Lead-in = company/account name; fall back to the contact when there is
        // no company. Empty only if truly unknown (prompt tells the model to skip
        // a placeholder rather than print one). Uses the RAW company, never the
        // display placeholder, so "Unknown account" can't leak into the email.
        const company = (it.company || '').trim();
        const lead = company || (it.contact ? it.contact.trim() : '');
        const fields = [];
        fields.push(`Company/account (lead the line with this name): ${lead || '(none on record, do not invent a name)'}`);
        if (it.contact) fields.push(`Spoke with: ${it.contact}`);
        if (it.outcome) fields.push(`Outcome: ${it.outcome}`);
        fields.push(`Type: ${it.source === 'fortress' ? 'Fortress promo visit' : 'Sales call'}`);
        fields.push(`What happened (from the note): ${it.note && it.note.trim() ? it.note.trim() : '(no note recorded)'}`);
        blocks.push(fields.join('\n'));
      }
    }

    const userMessage =
      `Date: ${date}\n` +
      `Rep name: ${repName}\n\n` +
      `Write the recap email. Open with "Recap for ${date}." on its own first line, ` +
      `then cover each entry below, leading every entry with its company/account name.\n\n` +
      `Entries to recap (today):\n\n` +
      blocks.join('\n\n');

    const raw = await callClaudeWithSystem(RECAP_SYSTEM_PROMPT, userMessage);
    res.json({ email: sanitizeEmail(raw) });
  } catch (e) {
    console.error('[recap] generate-email error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
