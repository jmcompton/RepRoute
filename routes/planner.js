const express = require('express');
const fetch = require('node-fetch');
const router = express.Router();
const { pool } = require('../db');

// ── Anthropic helper — reuses the SAME client/model/key pattern as
//    routes/ai.js & routes/weekly_report.js (no new AI dependency) ──
const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';
async function callClaude(prompt, maxTokens) {
  const res = await fetch(CLAUDE_API, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({ model: MODEL, max_tokens: maxTokens || 2000, messages: [{ role: 'user', content: prompt }] })
  });
  const data = await res.json();
  if (!data.content) return '';
  return data.content.filter(b => b.type === 'text').map(b => b.text || '').join('');
}

// ── helpers ──────────────────────────────────────────────────────
function isManager(u) { return !!u && (u.role === 'manager' || u.role === 'admin'); }

// Resolve which rep's plan we're operating on. Reps may only touch their own;
// managers may pass rep_id to view/operate on any rep. Mirrors the Weekly
// Report rep-scoping pattern.
function resolveRepId(req) {
  const me = req.session.user;
  const q = req.query.rep_id || req.body.rep_id;
  if (q && parseInt(q) !== me.id) {
    if (!isManager(me)) return { error: 'Forbidden' };
    return { repId: parseInt(q) };
  }
  return { repId: me.id };
}

// Normalize any date-ish value to a YYYY-MM-DD string (local, no TZ shift).
function ymd(d) {
  if (!d) return null;
  if (typeof d === 'string') return d.slice(0, 10);
  const x = new Date(d);
  return x.toISOString().slice(0, 10);
}

// Given a date string, return that week's Monday (YYYY-MM-DD).
function mondayOf(dateStr) {
  const d = new Date((dateStr || new Date().toISOString().slice(0, 10)) + 'T12:00:00');
  const dow = d.getDay();                 // 0=Sun..6=Sat
  const diff = (dow === 0 ? -6 : 1 - dow); // back to Monday
  d.setDate(d.getDate() + diff);
  return d.toISOString().slice(0, 10);
}
function addDays(dateStr, n) {
  const d = new Date(dateStr + 'T12:00:00');
  d.setDate(d.getDate() + n);
  return d.toISOString().slice(0, 10);
}

// ── GET /api/planner?rep_id=&week_start= ─────────────────────────
// Returns the week's items. Each STOP is annotated with live `visited` +
// latest call outcome (computed from the calls table, never stored).
router.get('/', async (req, res) => {
  try {
    const r = resolveRepId(req);
    if (r.error) return res.status(403).json({ error: r.error });
    const repId = r.repId;
    const weekStart = mondayOf(req.query.week_start);
    const friday = addDays(weekStart, 4);

    const items = await pool.query(
      `SELECT pi.*, p.company AS account_company, p.city AS account_city,
              p.phone AS account_phone, p.category AS account_category
         FROM planner_items pi
         LEFT JOIN prospects p ON p.id = pi.account_id
        WHERE pi.rep_id = $1 AND pi.planned_date BETWEEN $2 AND $3
        ORDER BY pi.planned_date ASC, pi.sort_order ASC, pi.id ASC`,
      [repId, weekStart, friday]
    );

    // Live "visited": a stop is visited if a call exists for that account by
    // this rep during the plan week (Mon–Fri). Annotate with latest outcome.
    const acctIds = items.rows.filter(i => i.account_id).map(i => i.account_id);
    let visitMap = {};
    if (acctIds.length) {
      const calls = await pool.query(
        `SELECT DISTINCT ON (prospect_id) prospect_id, outcome, call_date
           FROM calls
          WHERE user_id = $1 AND prospect_id = ANY($2::int[])
            AND call_date BETWEEN $3 AND $4
          ORDER BY prospect_id, call_date DESC, id DESC`,
        [repId, acctIds, weekStart, friday]
      );
      for (const c of calls.rows) {
        visitMap[c.prospect_id] = { outcome: c.outcome, call_date: ymd(c.call_date) };
      }
    }

    const out = items.rows.map(i => {
      const v = i.account_id ? visitMap[i.account_id] : null;
      return {
        ...i,
        planned_date: ymd(i.planned_date),
        visited: i.item_type === 'stop' && !!v,
        visit_outcome: v ? v.outcome : null,
        visit_date: v ? v.call_date : null
      };
    });

    res.json({ week_start: weekStart, week_end: friday, rep_id: repId, items: out });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/planner/panel?rep_id= ───────────────────────────────
// Smart-panel default sources: follow-ups due, not-contacted-recently,
// active pipeline. (Live account search uses /api/prospects/contacts/search.)
router.get('/panel', async (req, res) => {
  try {
    const r = resolveRepId(req);
    if (r.error) return res.status(403).json({ error: r.error });
    const repId = r.repId;

    const followUps = await pool.query(
      `SELECT DISTINCT ON (p.id) p.id, p.company, p.city, p.phone, p.category,
              c.next_step, c.next_step_date
         FROM calls c JOIN prospects p ON p.id = c.prospect_id
        WHERE c.user_id = $1 AND c.next_step_date IS NOT NULL
          AND c.next_step_date <= CURRENT_DATE + INTERVAL '7 days'
        ORDER BY p.id, c.next_step_date ASC
        LIMIT 25`,
      [repId]
    );

    const notContacted = await pool.query(
      `SELECT p.id, p.company, p.city, p.phone, p.category, p.priority
         FROM prospects p
        WHERE p.user_id = $1
          AND p.id NOT IN (SELECT DISTINCT prospect_id FROM calls WHERE user_id = $1 AND prospect_id IS NOT NULL)
        ORDER BY CASE p.priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END, p.created_at ASC
        LIMIT 25`,
      [repId]
    );

    const pipeline = await pool.query(
      `SELECT p.id, p.company, p.city, p.phone, p.category, p.pipeline_stage
         FROM prospects p
        WHERE p.user_id = $1
          AND p.pipeline_stage IS NOT NULL
          AND p.pipeline_stage NOT IN ('New Lead', 'Closed Won', 'Closed Lost')
        ORDER BY p.last_activity_at DESC NULLS LAST
        LIMIT 25`,
      [repId]
    );

    res.json({
      followUps: followUps.rows,
      notContacted: notContacted.rows,
      pipeline: pipeline.rows
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/planner/items ──────────────────────────────────────
router.post('/items', async (req, res) => {
  try {
    const r = resolveRepId(req);
    if (r.error) return res.status(403).json({ error: r.error });
    const repId = r.repId;
    const { planned_date, item_type, account_id, title, appt_time, note, source, ai_reason, ai_prep } = req.body;
    if (!planned_date) return res.status(400).json({ error: 'planned_date required' });
    const type = item_type === 'appointment' ? 'appointment' : 'stop';
    const src = source === 'ai' ? 'ai' : 'manual';

    // place new item at end of that day
    const ord = await pool.query(
      'SELECT COALESCE(MAX(sort_order), -1) + 1 AS next FROM planner_items WHERE rep_id=$1 AND planned_date=$2',
      [repId, ymd(planned_date)]
    );

    const ins = await pool.query(
      `INSERT INTO planner_items (rep_id, planned_date, item_type, account_id, title, appt_time, note, sort_order, source, ai_reason, ai_prep)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [repId, ymd(planned_date), type, account_id || null, title || null,
       appt_time || null, note || null, ord.rows[0].next,
       src, src === 'ai' ? (ai_reason || null) : null, src === 'ai' ? (ai_prep || null) : null]
    );
    res.json(ins.rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── PUT /api/planner/items/:id ───────────────────────────────────
// Edit content, move to another day, or reorder.
router.put('/items/:id', async (req, res) => {
  try {
    const me = req.session.user;
    const id = parseInt(req.params.id);
    const own = await pool.query('SELECT rep_id FROM planner_items WHERE id=$1', [id]);
    if (!own.rows.length) return res.status(404).json({ error: 'Not found' });
    if (own.rows[0].rep_id !== me.id && !isManager(me)) return res.status(403).json({ error: 'Forbidden' });

    const fields = [];
    const params = [];
    const set = (col, val) => { params.push(val); fields.push(`${col}=$${params.length}`); };
    const b = req.body;
    if (b.planned_date !== undefined) set('planned_date', ymd(b.planned_date));
    if (b.item_type !== undefined)    set('item_type', b.item_type === 'appointment' ? 'appointment' : 'stop');
    if (b.account_id !== undefined)   set('account_id', b.account_id || null);
    if (b.title !== undefined)        set('title', b.title || null);
    if (b.appt_time !== undefined)    set('appt_time', b.appt_time || null);
    if (b.note !== undefined)         set('note', b.note || null);
    if (b.sort_order !== undefined)   set('sort_order', parseInt(b.sort_order) || 0);
    if (!fields.length) return res.json(own.rows[0]);

    params.push(id);
    const upd = await pool.query(
      `UPDATE planner_items SET ${fields.join(', ')} WHERE id=$${params.length} RETURNING *`,
      params
    );
    res.json(upd.rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── DELETE /api/planner/items/:id ────────────────────────────────
router.delete('/items/:id', async (req, res) => {
  try {
    const me = req.session.user;
    const id = parseInt(req.params.id);
    const own = await pool.query('SELECT rep_id FROM planner_items WHERE id=$1', [id]);
    if (!own.rows.length) return res.status(404).json({ error: 'Not found' });
    if (own.rows[0].rep_id !== me.id && !isManager(me)) return res.status(403).json({ error: 'Forbidden' });
    await pool.query('DELETE FROM planner_items WHERE id=$1', [id]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ════════════════════════════════════════════════════════════════
//  AI ASSIST — "AI proposes, rep disposes"
//  Button-triggered only. One Claude call per request. Nothing is
//  committed to planner_items here; these endpoints only RETURN a
//  reviewable proposal. The rep commits via the normal POST /items
//  (with source='ai') after reviewing in the overlay.
// ════════════════════════════════════════════════════════════════

// Coarse "area" from an account's city/address — used to cluster stops by
// region (v1 = area grouping only, NOT turn-by-turn routing).
function areaOf(p) {
  if (p.city && String(p.city).trim()) return String(p.city).trim();
  if (p.address) {
    // last comma-separated chunk before any state/zip tends to be the city
    const parts = String(p.address).split(',').map(s => s.trim()).filter(Boolean);
    if (parts.length >= 2) return parts[parts.length - 2];
  }
  return 'Unknown area';
}

// Gather candidate accounts for a rep, each annotated with the signals the AI
// needs to reason about (reason category, area, last-call info, days-since-contact,
// deal status). Returns at most `limit` candidates, excluding already-planned ones.
async function gatherCandidates(repId, excludeAccountIds, limit) {
  const exclude = (excludeAccountIds && excludeAccountIds.length) ? excludeAccountIds : [-1];
  const rows = (await pool.query(
    `SELECT p.id, p.company, p.city, p.address, p.phone, p.category, p.priority,
            p.pipeline_stage,
            lc.outcome      AS last_outcome,
            lc.notes        AS last_notes,
            lc.next_step    AS next_step,
            lc.next_step_date AS next_step_date,
            lc.call_date    AS last_call_date
       FROM prospects p
       LEFT JOIN LATERAL (
         SELECT outcome, notes, next_step, next_step_date, call_date
           FROM calls c
          WHERE c.prospect_id = p.id AND c.user_id = $1
          ORDER BY c.call_date DESC, c.id DESC
          LIMIT 1
       ) lc ON TRUE
      WHERE p.user_id = $1
        AND p.id <> ALL($2::int[])
        AND COALESCE(p.pipeline_stage,'') NOT IN ('Closed Won','Closed Lost')`,
    [repId, exclude]
  )).rows;

  const today = new Date(); today.setHours(0, 0, 0, 0);
  const out = [];
  for (const p of rows) {
    const lastCall = p.last_call_date ? new Date(p.last_call_date) : null;
    const daysSince = lastCall ? Math.round((today - lastCall) / 86400000) : null;
    const nsDate = p.next_step_date ? ymd(p.next_step_date) : null;
    const activePipeline = p.pipeline_stage && !['New Lead', 'Closed Won', 'Closed Lost'].includes(p.pipeline_stage);

    // Classify the strongest real reason this account is worth a visit.
    let reason_hint = null, rank = 99;
    if (nsDate && nsDate <= ymd(today)) { reason_hint = 'Overdue follow-up (promised ' + nsDate + ')'; rank = 1; }
    else if (nsDate) { reason_hint = 'Follow-up due ' + nsDate; rank = 2; }
    else if (p.next_step) { reason_hint = 'Promised next step: ' + p.next_step; rank = 3; }
    else if (activePipeline) { reason_hint = 'Open deal — ' + p.pipeline_stage; rank = 4; }
    else if (daysSince === null) { reason_hint = 'Never contacted'; rank = 6; }
    else if (daysSince >= 30) { reason_hint = 'Cold — ' + daysSince + ' days since last contact'; rank = 5; }

    if (!reason_hint) continue; // no real reason → not a candidate
    out.push({
      account_id: p.id,
      name: p.company,
      area: areaOf(p),
      deal_status: p.pipeline_stage || 'New Lead',
      days_since_contact: daysSince,
      last_outcome: p.last_outcome || null,
      last_notes: p.last_notes ? String(p.last_notes).slice(0, 240) : null,
      next_step: p.next_step || null,
      next_step_date: nsDate,
      reason_hint,
      _rank: rank
    });
  }
  out.sort((a, b) => a._rank - b._rank);
  return out.slice(0, limit || 60).map(c => { const { _rank, ...rest } = c; return rest; });
}

// Safely extract the first JSON object from a Claude text response.
function parseJsonObject(text) {
  if (!text) return null;
  const m = text.match(/\{[\s\S]*\}/);
  if (!m) return null;
  try { return JSON.parse(m[0]); } catch (_) { return null; }
}

// Run the shared engine: build a prompt over candidates + day list + goal,
// call Claude once, then VALIDATE every returned stop against the candidate
// set + the allowed day list. Invalid/duplicate/invented entries are dropped.
async function aiPropose({ dayList, goal, candidates, mode }) {
  if (!candidates.length) return { stops: [] };
  const byId = {};
  candidates.forEach(c => { byId[c.account_id] = c; });
  const daySet = new Set(dayList);

  const intro = mode === 'fill-day'
    ? `Top up a single sales day toward the rep's daily call goal of ${goal} stops. Only the day ${dayList[0]} is in scope.`
    : `Build a Monday–Friday field-sales week for a rep. Aim each day toward the daily call goal of ${goal} stops.`;

  const prompt = `You are a field-sales route planner. ${intro}

RULES (follow exactly):
- ONLY choose accounts from the CANDIDATES list below, by their numeric account_id. NEVER invent accounts or IDs.
- Prioritize REAL reasons: overdue follow-ups, promised callbacks/next-steps, stalled/open deals, then long-cold or never-contacted accounts.
- CLUSTER stops by geographic AREA so each day stays in ONE region (area grouping only — do not attempt turn-by-turn routing).
- Aim toward the goal of ${goal} stops per day, but DO NOT pad with junk. Fewer high-quality stops beats filler.
- Do not place the same account on more than one day.
- For each stop, write a SHORT reason (why it's worth visiting) and a SHORT prep line drawn from the account's last call (outcome / notes / next step). Keep both under ~120 chars.

ALLOWED DAYS (use only these date strings): ${JSON.stringify(dayList)}

CANDIDATES (JSON): ${JSON.stringify(candidates)}

Return STRICT JSON ONLY, no markdown, in exactly this shape:
{"days":[{"date":"YYYY-MM-DD","stops":[{"account_id":123,"reason":"...","prep":"..."}]}]}`;

  const raw = await callClaude(prompt, 3000);
  const parsed = parseJsonObject(raw);
  const seen = new Set();
  const stops = [];
  if (parsed && Array.isArray(parsed.days)) {
    for (const day of parsed.days) {
      const date = day && day.date ? String(day.date).slice(0, 10) : null;
      if (!date || !daySet.has(date)) continue;                 // validate day
      if (!Array.isArray(day.stops)) continue;
      for (const s of day.stops) {
        const aid = parseInt(s && s.account_id);
        if (!aid || !byId[aid]) continue;                       // must be a real candidate
        if (seen.has(aid)) continue;                            // no dupes across the plan
        seen.add(aid);
        const c = byId[aid];
        stops.push({
          account_id: aid,
          day: date,
          name: c.name,
          area: c.area,
          reason: (s.reason && String(s.reason).trim()) || c.reason_hint,
          prep: (s.prep && String(s.prep).trim()) || null
        });
      }
    }
  }
  return { stops };
}

// ── POST /api/planner/build-week ─────────────────────────────────
// Returns a proposed Mon–Fri plan (NOT committed). Rep reviews + applies.
router.post('/build-week', async (req, res) => {
  try {
    const r = resolveRepId(req);
    if (r.error) return res.status(403).json({ error: r.error });
    const repId = r.repId;
    const weekStart = mondayOf(req.body.week_start);
    const friday = addDays(weekStart, 4);
    const dayList = [0, 1, 2, 3, 4].map(n => addDays(weekStart, n));
    const goal = parseInt(req.body.goal) || 10;

    // Accounts already planned this week → excluded so we never propose dupes.
    const planned = (await pool.query(
      `SELECT DISTINCT account_id FROM planner_items
        WHERE rep_id=$1 AND planned_date BETWEEN $2 AND $3 AND account_id IS NOT NULL`,
      [repId, weekStart, friday]
    )).rows.map(x => x.account_id);

    const candidates = await gatherCandidates(repId, planned, 60);
    if (!candidates.length) {
      return res.json({ rep_id: repId, week_start: weekStart, week_end: friday, suggestions: [], message: 'No candidate accounts found to plan.' });
    }
    const { stops } = await aiPropose({ dayList, goal, candidates, mode: 'build-week' });
    res.json({ rep_id: repId, week_start: weekStart, week_end: friday, goal, suggestions: stops });
  } catch (e) {
    console.error('[planner/build-week]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/planner/fill-day ───────────────────────────────────
// Gap-fill one day toward goal with same-area, not-recently-contacted accounts.
router.post('/fill-day', async (req, res) => {
  try {
    const r = resolveRepId(req);
    if (r.error) return res.status(403).json({ error: r.error });
    const repId = r.repId;
    const date = ymd(req.body.date);
    if (!date) return res.status(400).json({ error: 'date required' });
    const goal = parseInt(req.body.goal) || 10;

    // That day's existing stops: exclude their accounts; bias toward their areas.
    const dayItems = (await pool.query(
      `SELECT pi.account_id, p.city, p.address
         FROM planner_items pi LEFT JOIN prospects p ON p.id = pi.account_id
        WHERE pi.rep_id=$1 AND pi.planned_date=$2 AND pi.item_type='stop'`,
      [repId, date]
    )).rows;
    const planned = dayItems.filter(d => d.account_id).map(d => d.account_id);
    const focusAreas = new Set(dayItems.map(d => areaOf(d)).filter(a => a && a !== 'Unknown area'));
    const remaining = Math.max(1, goal - dayItems.length);

    let candidates = await gatherCandidates(repId, planned, 60);
    // If the day already has stops, prefer candidates in the same area(s).
    if (focusAreas.size) {
      const inArea = candidates.filter(c => focusAreas.has(c.area));
      if (inArea.length) candidates = inArea;
    }
    if (!candidates.length) {
      return res.json({ rep_id: repId, date, suggestions: [], message: 'No candidate accounts to fill this day.' });
    }
    const { stops } = await aiPropose({ dayList: [date], goal: remaining, candidates, mode: 'fill-day' });
    res.json({ rep_id: repId, date, goal: remaining, suggestions: stops });
  } catch (e) {
    console.error('[planner/fill-day]', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
