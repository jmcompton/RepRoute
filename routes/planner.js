const express = require('express');
const fetch = require('node-fetch');
const router = express.Router();
const { pool } = require('../db');
const { buildCooccurrence, recommendForAccount } = require('../lib/crosssell');
const { getReconnect } = require('../lib/reconnect-store');

// ── Anthropic helper — reuses the SAME client/model/key pattern as
//    routes/ai.js & routes/weekly_report.js (no new AI dependency) ──
const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-6';
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

// ── Reason classifier (single source of truth) ───────────────────
// Classifies the strongest real reason an account is worth a visit from its
// call/pipeline signals. Reused by gatherCandidates (AI assist) and the Today
// endpoint so both label stops identically. Returns { reason, kind } where
// kind ∈ followup_overdue | followup_due | next_step | open_deal | cold | new
// (or { reason:null, kind:null } when no real reason exists).
function classifyReason({ next_step, next_step_date, pipeline_stage, last_call_date }) {
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const todayStr = ymd(today);
  const nsDate = next_step_date ? ymd(next_step_date) : null;
  const lastCall = last_call_date ? new Date(last_call_date) : null;
  const daysSince = lastCall ? Math.round((today - lastCall) / 86400000) : null;
  const activePipeline = pipeline_stage && !['New Lead', 'Closed Won', 'Closed Lost'].includes(pipeline_stage);

  if (nsDate && nsDate <= todayStr) return { reason: 'Overdue follow-up (promised ' + nsDate + ')', kind: 'followup_overdue' };
  if (nsDate) return { reason: 'Follow-up due ' + nsDate, kind: 'followup_due' };
  if (next_step) return { reason: 'Promised next step: ' + next_step, kind: 'next_step' };
  if (activePipeline) return { reason: 'Open deal — ' + pipeline_stage, kind: 'open_deal' };
  if (daysSince === null) return { reason: 'Never contacted', kind: 'new' };
  if (daysSince >= 30) return { reason: 'Cold — ' + daysSince + ' days since last contact', kind: 'cold' };
  return { reason: null, kind: null };
}
// Visit-priority rank per kind — lower wins (matches the original inline ordering).
const REASON_RANK = { followup_overdue: 1, followup_due: 2, next_step: 3, open_deal: 4, cold: 5, new: 6 };

// ── Revenue-aware tuning (tunable constants) ─────────────────────
// A commission-earning account counts as "neglected" once it's gone this many
// days without a logged visit (or has never been logged).
const COLD_DAYS = 21;
// Revenue-aware visit-priority order — lower wins. Folds the two new revenue
// tiers (top_account, crosssell) in above the existing non-revenue reasons.
const REV_RANK = {
  followup_overdue: 1,   // broken promise — always top, unchanged
  top_account:      2,   // commission account gone cold / never logged
  crosssell:        3,   // a strong cross-sell rec exists
  followup_due:     4,
  next_step:        4,
  open_deal:        5,
  cold:             6,
  new:              7
};
function roundDollars(n) { return Math.round(Number(n) || 0); }
function fmtDollars(n) { return roundDollars(n).toLocaleString('en-US'); }

// ── Revenue model (loaded ONCE per request, not per-account) ─────
// Combines firm-wide cross-sell co-occurrence (patterns are stronger agency-wide)
// with a per-account commission rollup. Read-only over account_lines/lines.
async function loadRevenueModel() {
  const rows = (await pool.query(
    `SELECT al.account_id, al.line_id, l.name, al.total_sales
       FROM account_lines al JOIN lines l ON l.id = al.line_id`
  )).rows;
  const coocc = buildCooccurrence(rows);

  const commRows = (await pool.query(
    `SELECT account_id, SUM(total_commission) AS commission, SUM(total_sales) AS sales
       FROM account_lines GROUP BY account_id`
  )).rows;
  const commissionByAccount = {};
  for (const r of commRows) {
    commissionByAccount[r.account_id] = { commission: Number(r.commission) || 0, sales: Number(r.sales) || 0 };
  }
  return { coocc, commissionByAccount };
}

// Top cross-sell rec for one account using the already-built co-occurrence
// (no extra query). Returns { line_name, est_sales, confidence, reason } | null.
function crosssellFor(accountId, model) {
  const ownedSet = model && model.coocc && model.coocc.linesByAccount[accountId];
  if (!ownedSet || !ownedSet.size) return null;
  const owned = Array.from(ownedSet).map(line_id => ({ line_id }));
  const recs = recommendForAccount(accountId, model.coocc, owned);
  return recs.length ? recs[0] : null;
}

// Trim a rec to the lightweight shape carried on candidates/stops.
function trimRec(rec) {
  return rec ? { line_name: rec.line_name, est_sales: roundDollars(rec.est_sales), reason: rec.reason } : null;
}

// ── Revenue-aware classifier ─────────────────────────────────────
// Wraps the base classifyReason and overlays the two revenue tiers. `crosssell`
// is ALWAYS passed through so a card can show the pitch even when the PRIMARY
// reason is something else. Returns { reason, kind, rank, crosssell }.
function classifyReasonRevenue(base) {
  const crosssell = base.crosssell || null;
  const commission = roundDollars(base.commission);
  const daysSince = (base.days_since_contact == null) ? null : base.days_since_contact;
  const core = classifyReason(base);

  // rank 1 — broken promise always tops (unchanged).
  if (core.kind === 'followup_overdue') {
    return { reason: core.reason, kind: 'followup_overdue', rank: REV_RANK.followup_overdue, crosssell };
  }
  // rank 2 — top-paying account that's gone cold or never been logged.
  if (commission > 0 && (daysSince === null || daysSince >= COLD_DAYS)) {
    const tail = daysSince === null ? 'never logged' : (daysSince + ' days no visit');
    return {
      reason: 'Top account ($' + fmtDollars(commission) + ') · ' + tail,
      kind: 'top_account', rank: REV_RANK.top_account, crosssell
    };
  }
  // rank 3 — a strong cross-sell rec.
  if (crosssell) {
    return {
      reason: 'Cross-sell: ' + crosssell.line_name + ' (~$' + fmtDollars(crosssell.est_sales) + ')',
      kind: 'crosssell', rank: REV_RANK.crosssell, crosssell
    };
  }
  // rank 4–7 — existing non-revenue reasons, carried through with the overlay.
  if (core.kind) {
    return { reason: core.reason, kind: core.kind, rank: REV_RANK[core.kind] || 99, crosssell };
  }
  return { reason: null, kind: null, rank: 99, crosssell };
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

    // Per-day anchors: MANUAL (stored) wins; otherwise AUTO = the earliest stop's
    // city on that day. Returned as { date: { city, manual } } so the grid can
    // render an editable chip per column.
    const manualMap = await loadManualAnchors(repId, weekStart, friday);
    const autoMap = {};
    for (const i of out) {
      if (i.item_type !== 'stop' || !i.account_city) continue;
      const d = i.planned_date;
      if (!autoMap[d]) autoMap[d] = i.account_city;   // items already ordered date,sort,id
    }
    const anchors = {};
    for (let n = 0; n < 5; n++) {
      const d = addDays(weekStart, n);
      if (manualMap[d]) anchors[d] = { city: manualMap[d], manual: true };
      else if (autoMap[d]) anchors[d] = { city: autoMap[d], manual: false };
      else anchors[d] = { city: null, manual: false };
    }

    res.json({ week_start: weekStart, week_end: friday, rep_id: repId, items: out, anchors });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/planner/anchors?rep_id=&week_start= ─────────────────
// Manual anchor cities for the week, as a { date: city } map.
router.get('/anchors', async (req, res) => {
  try {
    const r = resolveRepId(req);
    if (r.error) return res.status(403).json({ error: r.error });
    const weekStart = mondayOf(req.query.week_start);
    const map = await loadManualAnchors(r.repId, weekStart, addDays(weekStart, 4));
    res.json({ rep_id: r.repId, week_start: weekStart, anchors: map });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── PUT /api/planner/anchors ─────────────────────────────────────
// Set or clear a MANUAL anchor for one day. Body: { date, city }. A blank/empty
// city CLEARS the manual anchor (reverts to auto behavior). Managers may pass
// rep_id; reps are pinned to their own plan.
router.put('/anchors', async (req, res) => {
  try {
    const r = resolveRepId(req);
    if (r.error) return res.status(403).json({ error: r.error });
    const repId = r.repId;
    const date = ymd(req.body.date);
    if (!date) return res.status(400).json({ error: 'date required' });
    const city = (req.body.city == null ? '' : String(req.body.city)).trim();
    if (!city) {
      await pool.query('DELETE FROM planner_anchors WHERE rep_id=$1 AND anchor_date=$2', [repId, date]);
      return res.json({ ok: true, date, city: null, manual: false });
    }
    await pool.query(
      `INSERT INTO planner_anchors (rep_id, anchor_date, city, updated_at)
       VALUES ($1,$2,$3,NOW())
       ON CONFLICT (rep_id, anchor_date) DO UPDATE SET city=EXCLUDED.city, updated_at=NOW()`,
      [repId, date, city]
    );
    res.json({ ok: true, date, city, manual: true });
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

    // Revenue source: the rep's commission accounts that are cold/never-logged
    // (highest $ first) plus any with a cross-sell rec. Revenue-aware drag-in.
    const model = await loadRevenueModel();
    const revRows = (await pool.query(
      `SELECT p.id, p.company, p.city, p.phone, p.category,
              COALESCE(SUM(al.total_commission), 0) AS commission,
              lc.call_date AS last_call_date
         FROM prospects p
         JOIN account_lines al ON al.account_id = p.id
         LEFT JOIN LATERAL (
           SELECT call_date FROM calls c
            WHERE c.prospect_id = p.id AND c.user_id = $1
            ORDER BY c.call_date DESC, c.id DESC LIMIT 1
         ) lc ON TRUE
        WHERE p.user_id = $1
        GROUP BY p.id, p.company, p.city, p.phone, p.category, lc.call_date`,
      [repId]
    )).rows;

    const todayMid = new Date(); todayMid.setHours(0, 0, 0, 0);
    const revenue = [];
    for (const p of revRows) {
      const commission = roundDollars(p.commission);
      const lastCall = p.last_call_date ? new Date(p.last_call_date) : null;
      const daysSince = lastCall ? Math.round((todayMid - lastCall) / 86400000) : null;
      const xs = crosssellFor(p.id, model);
      const coldTop = commission > 0 && (daysSince === null || daysSince >= COLD_DAYS);
      if (!coldTop && !xs) continue;
      revenue.push({
        id: p.id, company: p.company, city: p.city, phone: p.phone, category: p.category,
        commission,
        crosssell: trimRec(xs),
        reason: coldTop
          ? ('Top account ($' + fmtDollars(commission) + ')' + (daysSince === null ? ' · never logged' : ' · ' + daysSince + ' days'))
          : ('Cross-sell: ' + xs.line_name)
      });
    }
    revenue.sort((a, b) => (b.commission || 0) - (a.commission || 0));

    res.json({
      followUps: followUps.rows,
      notContacted: notContacted.rows,
      pipeline: pipeline.rows,
      revenue: revenue.slice(0, 10)
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

    // NEVER DUPLICATE (rule 3d): if this account already has a stop ANYWHERE in
    // the same Mon–Fri week, adding again is a no-op — return the existing item
    // flagged as a duplicate (with its day) so the UI can toast "Already planned
    // Thursday". Guards drag re-adds and repeated "Apply" of AI suggestions.
    if (type === 'stop' && account_id) {
      const wkStart = mondayOf(ymd(planned_date));
      const wkEnd = addDays(wkStart, 4);
      const dup = await pool.query(
        `SELECT * FROM planner_items
          WHERE rep_id=$1 AND item_type='stop' AND account_id=$2
            AND planned_date BETWEEN $3 AND $4
          ORDER BY planned_date ASC, id ASC LIMIT 1`,
        [repId, account_id, wkStart, wkEnd]
      );
      if (dup.rows.length) {
        return res.json(Object.assign({}, dup.rows[0], {
          duplicate: true,
          planned_date: ymd(dup.rows[0].planned_date)
        }));
      }
    }

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

// ── Geography: per-day anchor + radius fill ──────────────────────
// prospects carry a city but NO stored lat/lng, so "within N miles" is computed
// against geocoded CITY centroids (cached). When no Places key is available (or
// a city won't geocode) we fall back to a strict SAME-CITY match so the radius
// rule still holds deterministically and a day never mixes far-apart cities.
const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;
const DEFAULT_RADIUS_MI = 25;
const _geoCache = {};   // normalized city string → {lat,lng} | null

function distanceMiles(lat1, lng1, lat2, lng2) {
  const R = 3958.8;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function normCity(c) { return c ? String(c).trim().toLowerCase() : ''; }

// Geocode a city centroid (cached). Returns {lat,lng} or null. Never throws.
async function geocodeCity(city) {
  const key = normCity(city);
  if (!key || key === 'unknown area') return null;
  if (Object.prototype.hasOwnProperty.call(_geoCache, key)) return _geoCache[key];
  let coords = null;
  if (PLACES_KEY) {
    try {
      const r = await fetch(
        `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(city)}&key=${PLACES_KEY}`
      );
      const d = await r.json();
      if (d.results && d.results[0]) {
        const loc = d.results[0].geometry.location;
        coords = { lat: loc.lat, lng: loc.lng };
      }
    } catch (e) { console.error('[planner geocodeCity]', e.message); }
  }
  _geoCache[key] = coords;
  return coords;
}

// Is `cand` (with .area = its city) within `radiusMi` of an anchor?
// Strict same-city fallback when either side has no geocode.
function withinRadius(anchor, cand, radiusMi) {
  const sameCity = anchor.cityKey && anchor.cityKey === normCity(cand.area);
  if (anchor.coords && cand.coords) {
    return distanceMiles(anchor.coords.lat, anchor.coords.lng, cand.coords.lat, cand.coords.lng) <= radiusMi;
  }
  return sameCity;   // no coords on one side → only same-city counts
}

// ── Manual anchors (per rep + day) ───────────────────────────────
// Load the rep's MANUAL anchor cities for a Mon–Fri week as a {date: city} map.
async function loadManualAnchors(repId, weekStart, weekEnd) {
  const rows = (await pool.query(
    `SELECT anchor_date, city FROM planner_anchors
      WHERE rep_id=$1 AND anchor_date BETWEEN $2 AND $3`,
    [repId, weekStart, weekEnd]
  )).rows;
  const map = {};
  for (const r of rows) map[ymd(r.anchor_date)] = r.city;
  return map;
}

// Accounts the rep has VISITED (logged a call) during this Mon–Fri week — they
// must not be re-proposed (NEVER-DUPLICATE rule 3a "or was visited this week").
async function visitedThisWeek(repId, weekStart, weekEnd) {
  const rows = (await pool.query(
    `SELECT DISTINCT prospect_id FROM calls
      WHERE user_id=$1 AND prospect_id IS NOT NULL AND call_date BETWEEN $2 AND $3`,
    [repId, weekStart, weekEnd]
  )).rows;
  return rows.map(r => r.prospect_id);
}

// ── Ranked candidate pool for radius fill ────────────────────────
// Priority is explicit (task rule 2c):
//   tier 0 — Reconnect accounts (real $ going quiet), highest trailing commission first
//   tier 1 — other commission customers, by annualized run_rate
//   tier 2 — AI leads / unvetted prospects last
// Each candidate carries .area (its city) for radius placement; coords are
// attached later by the caller. Excludes anything in excludeAccountIds.
async function gatherRankedCandidates(repId, excludeAccountIds) {
  const exclude = new Set((excludeAccountIds || []).map(Number));
  const seen = new Set();
  const out = [];

  // Tier 0 — Reconnect (commission customers gone quiet past their cadence).
  let reconnect = { accounts: [] };
  try {
    reconnect = await getReconnect(pool, { uid: repId, scope: 'rep', filter: 'customers' });
  } catch (e) { console.error('[planner reconnect]', e.message); }
  for (const a of reconnect.accounts) {
    if (exclude.has(a.id) || seen.has(a.id)) continue;
    seen.add(a.id);
    out.push({
      account_id: a.id, name: a.company, area: (a.city && String(a.city).trim()) || null,
      tier: 0, sortKey: Number(a.trailing_commission) || 0,
      reason_hint: 'Reconnect — $' + fmtDollars(a.trailing_commission) + ' going quiet · ' + a.days_quiet + 'd',
      prep: a.lines ? ('Lines: ' + a.lines) : null
    });
  }

  // Tier 1 (commission, by run_rate) + Tier 2 (leads). One pass over the rep's
  // open prospects with a commission rollup; reconnect ids already captured above.
  const rows = (await pool.query(
    `SELECT p.id, p.company, p.city, p.address,
            COALESCE(SUM(al.total_commission), 0) AS commission,
            COUNT(DISTINCT cl.period_start)        AS months_loaded
       FROM prospects p
       LEFT JOIN account_lines  al ON al.account_id = p.id
       LEFT JOIN commission_lines cl ON cl.account_id = p.id
      WHERE p.user_id = $1
        AND COALESCE(p.pipeline_stage,'') NOT IN ('Closed Won','Closed Lost')
      GROUP BY p.id, p.company, p.city, p.address`,
    [repId]
  )).rows;
  for (const p of rows) {
    if (exclude.has(p.id) || seen.has(p.id)) continue;
    seen.add(p.id);
    const commission = Number(p.commission) || 0;
    const months = Number(p.months_loaded) || 0;
    const area = (p.city && String(p.city).trim()) ? String(p.city).trim()
      : (areaOf(p) !== 'Unknown area' ? areaOf(p) : null);
    if (commission > 0) {
      const runRate = months > 0 ? (commission / months) * 12 : commission;
      out.push({
        account_id: p.id, name: p.company, area, tier: 1, sortKey: runRate,
        reason_hint: 'Customer · $' + fmtDollars(commission) + ' trailing', prep: null
      });
    } else {
      out.push({
        account_id: p.id, name: p.company, area, tier: 2, sortKey: 0,
        reason_hint: 'Prospect — new opportunity', prep: null
      });
    }
  }

  out.sort((a, b) => (a.tier - b.tier) || (b.sortKey - a.sortKey));
  return out;
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

  // Load the revenue model ONCE for the whole candidate set (not per-account).
  const model = await loadRevenueModel();

  const today = new Date(); today.setHours(0, 0, 0, 0);
  const out = [];
  for (const p of rows) {
    const lastCall = p.last_call_date ? new Date(p.last_call_date) : null;
    const daysSince = lastCall ? Math.round((today - lastCall) / 86400000) : null;
    const nsDate = p.next_step_date ? ymd(p.next_step_date) : null;
    const commission = (model.commissionByAccount[p.id] || {}).commission || 0;
    const crosssell = crosssellFor(p.id, model);

    // Revenue-aware classification (wraps classifyReason, adds top_account/crosssell).
    const cls = classifyReasonRevenue({
      next_step: p.next_step,
      next_step_date: p.next_step_date,
      pipeline_stage: p.pipeline_stage,
      last_call_date: p.last_call_date,
      days_since_contact: daysSince,
      commission,
      crosssell
    });
    const reason_hint = cls.reason;

    // Include if there's ANY existing reason OR a top-account/cross-sell signal.
    if (!reason_hint && !cls.crosssell) continue;
    out.push({
      account_id: p.id,
      name: p.company,
      area: areaOf(p),
      deal_status: p.pipeline_stage || 'New Lead',
      days_since_contact: daysSince,
      commission: roundDollars(commission),
      crosssell: trimRec(cls.crosssell),
      last_outcome: p.last_outcome || null,
      last_notes: p.last_notes ? String(p.last_notes).slice(0, 240) : null,
      next_step: p.next_step || null,
      next_step_date: nsDate,
      reason_hint: reason_hint || (cls.crosssell ? 'Cross-sell: ' + cls.crosssell.line_name : null),
      _rank: cls.rank,
      _commission: roundDollars(commission)
    });
  }
  // Primary by tier; within a tier (esp. top_account) biggest dollars float up.
  out.sort((a, b) => (a._rank - b._rank) || (b._commission - a._commission));
  return out.slice(0, limit || 60).map(c => { const { _rank, _commission, ...rest } = c; return rest; });
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
- Prioritize protecting top-paying accounts (see each candidate's "commission") that have gone cold, and propose cross-sell visits where the data supports them; when a candidate has a "crosssell" rec, put the pitch (its "reason") in the prep line.
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
          prep: (s.prep && String(s.prep).trim()) || (c.crosssell ? c.crosssell.reason : null)
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
    const radius = Math.max(1, parseFloat(req.body.radius) || DEFAULT_RADIUS_MI);

    // ── Existing stops this week, ordered so the EARLIEST stop on each day
    //    becomes that day's ANCHOR (its city/coords define the day's region).
    const existing = (await pool.query(
      `SELECT pi.planned_date, pi.account_id, pi.id, p.city, p.address
         FROM planner_items pi
         LEFT JOIN prospects p ON p.id = pi.account_id
        WHERE pi.rep_id=$1 AND pi.planned_date BETWEEN $2 AND $3 AND pi.item_type='stop'
        ORDER BY pi.planned_date, pi.id`,
      [repId, weekStart, friday]
    )).rows;

    // Auto anchor = the EARLIEST existing stop's city on a day. MANUAL anchor
    // (set by the rep) wins and is never overwritten by auto behavior.
    const autoCityByDay = {};     // date → auto (first-stop) city string
    const dayCounts = {};         // date → existing stop count
    for (const row of existing) {
      const d = ymd(row.planned_date);
      dayCounts[d] = (dayCounts[d] || 0) + 1;
      if (!autoCityByDay[d] && row.account_id) autoCityByDay[d] = areaOf(row);
    }
    const manualAnchors = await loadManualAnchors(repId, weekStart, friday);

    // ── DEDUP: exclude accounts already planned THIS week or ANY FUTURE week
    //    (planned_date >= weekStart) OR visited this week. Reconnect's own cadence
    //    already keeps recently-touched accounts out of the tier-0 pool.
    const planned = (await pool.query(
      `SELECT DISTINCT account_id FROM planner_items
        WHERE rep_id=$1 AND planned_date >= $2 AND account_id IS NOT NULL`,
      [repId, weekStart]
    )).rows.map(x => x.account_id);
    const visited = await visitedThisWeek(repId, weekStart, friday);
    const exclude = Array.from(new Set(planned.concat(visited)));

    // Territory fallback for days with no anchor.
    const ures = (await pool.query(
      `SELECT territory, home_base_lat, home_base_lng FROM users WHERE id=$1`, [repId]
    )).rows[0] || {};
    const territoryCity = ures.territory && String(ures.territory).trim() ? String(ures.territory).trim() : null;

    // Ranked candidate pool: reconnect $ first, then commission run_rate, then leads.
    const candidates = await gatherRankedCandidates(repId, exclude);
    if (!candidates.length) {
      return res.json({ rep_id: repId, week_start: weekStart, week_end: friday, suggestions: [], couldnt_place: [], messages: [], message: 'No candidate accounts found to plan.' });
    }

    // Geocode candidate cities once (cached); attach coords for radius math.
    // Accounts with NO city are un-placeable by region — never geocode/guess them.
    for (const c of candidates) c.coords = c.area ? await geocodeCity(c.area) : null;

    // Resolve each day's anchor (city + coords): MANUAL → first existing stop →
    // rep territory city → home base coords.
    const used = new Set();
    const suggestions = [];
    const messages = [];
    for (const day of dayList) {
      const remaining = goal - (dayCounts[day] || 0);
      if (remaining <= 0) continue;

      const city = manualAnchors[day] || autoCityByDay[day] || territoryCity;
      let coords = city ? await geocodeCity(city) : null;
      if (!coords && ures.home_base_lat && ures.home_base_lng) {
        coords = { lat: parseFloat(ures.home_base_lat), lng: parseFloat(ures.home_base_lng) };
      }
      const anchor = { cityKey: normCity(city), coords };

      // No anchor city at all → cannot enforce a region; skip (rep seeds the day).
      if (!anchor.cityKey && !anchor.coords) continue;

      let picked = 0;
      for (const c of candidates) {
        if (picked >= remaining) break;
        if (used.has(c.account_id)) continue;
        if (!c.area) continue;                            // no location → couldn't place
        if (!withinRadius(anchor, c, radius)) continue;   // GEOGRAPHIC SANITY
        used.add(c.account_id);
        picked++;
        suggestions.push({
          account_id: c.account_id, day, name: c.name, area: c.area,
          reason: c.reason_hint, prep: c.prep || null
        });
      }
      // Geographic honesty: if we couldn't reach the goal in radius, say so.
      if (picked < remaining && city) {
        messages.push('Only ' + picked + ' good ' + (picked === 1 ? 'stop' : 'stops') +
          ' within ' + radius + 'mi of ' + city + ' on ' + day + '.');
      }
    }

    // Candidates with no usable location — surfaced so the rep can drag manually.
    const couldnt_place = candidates
      .filter(c => !c.area && !used.has(c.account_id))
      .slice(0, 25)
      .map(c => ({ account_id: c.account_id, name: c.name, reason: c.reason_hint }));

    res.json({ rep_id: repId, week_start: weekStart, week_end: friday, goal, radius, suggestions, couldnt_place, messages });
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
    const radius = Math.max(1, parseFloat(req.body.radius) || DEFAULT_RADIUS_MI);
    const weekStart = mondayOf(date);

    // That day's existing stops: earliest is the day's ANCHOR city.
    const dayItems = (await pool.query(
      `SELECT pi.account_id, pi.id, p.city, p.address
         FROM planner_items pi LEFT JOIN prospects p ON p.id = pi.account_id
        WHERE pi.rep_id=$1 AND pi.planned_date=$2 AND pi.item_type='stop'
        ORDER BY pi.id`,
      [repId, date]
    )).rows;
    const anchorRow = dayItems.find(d => d.account_id);
    const friday = addDays(weekStart, 4);
    const remaining = Math.max(1, goal - dayItems.length);

    // DEDUP across this week + future weeks (same rule as build-week) + visited.
    const planned = (await pool.query(
      `SELECT DISTINCT account_id FROM planner_items
        WHERE rep_id=$1 AND planned_date >= $2 AND account_id IS NOT NULL`,
      [repId, weekStart]
    )).rows.map(x => x.account_id);
    const visited = await visitedThisWeek(repId, weekStart, friday);
    const exclude = Array.from(new Set(planned.concat(visited)));

    const ures = (await pool.query(
      `SELECT territory, home_base_lat, home_base_lng FROM users WHERE id=$1`, [repId]
    )).rows[0] || {};
    // Anchor: MANUAL → day's first existing stop → rep territory city.
    const manualAnchors = await loadManualAnchors(repId, date, date);
    const city = manualAnchors[date] || (anchorRow ? areaOf(anchorRow)
      : (ures.territory && String(ures.territory).trim() ? String(ures.territory).trim() : null));
    let coords = city ? await geocodeCity(city) : null;
    if (!coords && ures.home_base_lat && ures.home_base_lng) {
      coords = { lat: parseFloat(ures.home_base_lat), lng: parseFloat(ures.home_base_lng) };
    }
    const anchor = { cityKey: normCity(city), coords };
    if (!anchor.cityKey && !anchor.coords) {
      return res.json({ rep_id: repId, date, suggestions: [], couldnt_place: [], messages: [], message: 'No anchor for this day — drop an account or set a territory first.' });
    }

    const candidates = await gatherRankedCandidates(repId, exclude);
    for (const c of candidates) c.coords = c.area ? await geocodeCity(c.area) : null;

    const used = new Set();
    const suggestions = [];
    for (const c of candidates) {
      if (suggestions.length >= remaining) break;
      if (!c.area) continue;
      if (!withinRadius(anchor, c, radius)) continue;
      used.add(c.account_id);
      suggestions.push({
        account_id: c.account_id, day: date, name: c.name, area: c.area,
        reason: c.reason_hint, prep: c.prep || null
      });
    }
    const messages = [];
    if (suggestions.length < remaining && city) {
      messages.push('Only ' + suggestions.length + ' good ' + (suggestions.length === 1 ? 'stop' : 'stops') +
        ' within ' + radius + 'mi of ' + city + '.');
    }
    const couldnt_place = candidates
      .filter(c => !c.area && !used.has(c.account_id))
      .slice(0, 25)
      .map(c => ({ account_id: c.account_id, name: c.name, reason: c.reason_hint }));
    res.json({ rep_id: repId, date, goal: remaining, radius, suggestions, couldnt_place, messages });
  } catch (e) {
    console.error('[planner/fill-day]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ════════════════════════════════════════════════════════════════
//  TODAY VIEW — the rep's execution surface for the current day
// ════════════════════════════════════════════════════════════════

// Build a Today-view "stop" object from a planner_items row joined with its
// account + latest call signals. reason/reason_kind come from ai_reason or the
// shared classifier; prep falls back ai_prep → note → last-call-notes snippet.
function shapeStop(row, model) {
  let cls = { reason: null, kind: null, crosssell: null };
  if (row.account_id) {
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const lastCall = row.last_call_date ? new Date(row.last_call_date) : null;
    const daysSince = lastCall ? Math.round((today - lastCall) / 86400000) : null;
    const commission = model && model.commissionByAccount[row.account_id]
      ? model.commissionByAccount[row.account_id].commission : 0;
    const crosssell = model ? crosssellFor(row.account_id, model) : null;
    cls = classifyReasonRevenue({
      next_step: row.next_step,
      next_step_date: row.next_step_date,
      pipeline_stage: row.pipeline_stage,
      last_call_date: row.last_call_date,
      days_since_contact: daysSince,
      commission,
      crosssell
    });
  }
  const xs = trimRec(cls.crosssell);
  const reason = row.ai_reason || cls.reason || null;
  const reason_kind = row.account_id ? cls.kind : null;
  const noteSnippet = row.last_notes ? String(row.last_notes).slice(0, 120) : null;
  const prep = row.ai_prep || row.note || noteSnippet || (xs ? xs.reason : null) || null;
  return {
    id: row.id,
    item_type: row.item_type,
    account_id: row.account_id,
    title: row.title,
    appt_time: row.appt_time,
    company: row.company,
    city: row.city,
    phone: row.phone,
    address: row.address,
    google_place_id: row.google_place_id,
    reason,
    reason_kind,
    prep,
    crosssell: xs,
    done: row.completed_at != null
  };
}

// ── GET /api/planner/today?rep_id= ───────────────────────────────
router.get('/today', async (req, res) => {
  try {
    const r = resolveRepId(req);
    if (r.error) return res.status(403).json({ error: r.error });
    const repId = r.repId;
    const today = ymd(new Date());
    const monday = mondayOf(today);

    const goalRow = await pool.query('SELECT daily_call_goal FROM users WHERE id=$1', [repId]);
    const goal = (goalRow.rows[0] && goalRow.rows[0].daily_call_goal) || 10;

    // Revenue model loaded once → stops show live money reasons + cross-sell.
    const model = await loadRevenueModel();

    // Shared SELECT shape for stops: item + account + latest-call signals.
    const stopSelect = `
      SELECT pi.*, p.company, p.city, p.phone, p.address, p.google_place_id, p.pipeline_stage,
             lc.next_step, lc.next_step_date, lc.call_date AS last_call_date, lc.notes AS last_notes
        FROM planner_items pi
        LEFT JOIN prospects p ON p.id = pi.account_id
        LEFT JOIN LATERAL (
          SELECT next_step, next_step_date, call_date, notes
            FROM calls c
           WHERE c.prospect_id = pi.account_id AND c.user_id = $1
           ORDER BY c.call_date DESC, c.id DESC
           LIMIT 1
        ) lc ON TRUE`;

    const todayRows = (await pool.query(
      `${stopSelect}
        WHERE pi.rep_id = $1 AND pi.planned_date = $2
        ORDER BY pi.sort_order ASC, pi.id ASC`,
      [repId, today]
    )).rows;
    const stops = todayRows.map(row => shapeStop(row, model));

    const done = stops.filter(s => s.done).length;
    const progress = { done, total: stops.length };

    // Carried over: incomplete stops earlier this week (>= Monday, < today).
    const carriedRows = (await pool.query(
      `${stopSelect}
        WHERE pi.rep_id = $1 AND pi.completed_at IS NULL
          AND pi.planned_date >= $2 AND pi.planned_date < $3
        ORDER BY pi.planned_date ASC, pi.sort_order ASC, pi.id ASC`,
      [repId, monday, today]
    )).rows;
    const carried_over = carriedRows.map(row => {
      const s = shapeStop(row, model);
      s.planned_date = ymd(row.planned_date);
      return s;
    });

    // Nearby: ≤3 never-contacted accounts in today's stop cities (Lead Finder tie-in).
    const todayAcctIds = stops.filter(s => s.account_id).map(s => s.account_id);
    const cities = Array.from(new Set(stops.filter(s => s.account_id && s.city).map(s => s.city)));
    let nearby = [];
    if (cities.length) {
      const exclude = todayAcctIds.length ? todayAcctIds : [-1];
      nearby = (await pool.query(
        `SELECT p.id, p.company, p.city, p.phone, p.category
           FROM prospects p
          WHERE p.user_id = $1
            AND p.city = ANY($2::text[])
            AND p.id <> ALL($3::int[])
            AND p.id NOT IN (SELECT DISTINCT prospect_id FROM calls WHERE user_id = $1 AND prospect_id IS NOT NULL)
          ORDER BY CASE p.priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END, p.created_at ASC
          LIMIT 3`,
        [repId, cities, exclude]
      )).rows;
    }

    // Today's working city = MANUAL anchor for today if set, else the day's first
    // stop city (cockpit + Lead Finder default). Frontend falls back to territory.
    const manualToday = await loadManualAnchors(repId, today, today);
    const firstStopCity = (stops.find(s => s.city) || {}).city || null;
    const anchor_city = manualToday[today] || firstStopCity || null;

    res.json({ date: today, rep_id: repId, goal, progress, stops, carried_over, nearby, anchor_city });
  } catch (e) {
    console.error('[planner/today]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/planner/items/:id/done ─────────────────────────────
// Mark a stop done. If account-linked and not already logged today, write a
// lightweight 'Visit' call + mirror the calls.js prospect side-effects.
router.post('/items/:id/done', async (req, res) => {
  try {
    const me = req.session.user;
    const id = parseInt(req.params.id);
    const own = await pool.query('SELECT rep_id, account_id FROM planner_items WHERE id=$1', [id]);
    if (!own.rows.length) return res.status(404).json({ error: 'Not found' });
    if (own.rows[0].rep_id !== me.id && !isManager(me)) return res.status(403).json({ error: 'Forbidden' });

    const repId = own.rows[0].rep_id;
    const acctId = own.rows[0].account_id;
    const upd = await pool.query('UPDATE planner_items SET completed_at = NOW() WHERE id=$1 RETURNING *', [id]);

    if (acctId) {
      // Stamp call_date with the DB's CURRENT_DATE (the same basis the
      // "calls logged today" counter filters on), so a stop marked done always
      // counts for today. The same-day dedup also keys off CURRENT_DATE, so a
      // stop done twice today still yields exactly one Visit call (no double-count).
      const existing = await pool.query(
        'SELECT 1 FROM calls WHERE user_id=$1 AND prospect_id=$2 AND call_date=CURRENT_DATE LIMIT 1',
        [repId, acctId]
      );
      if (!existing.rows.length) {
        await pool.query(
          `INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome)
           VALUES ($1,$2,CURRENT_DATE,'Visit','Visited')`,
          [repId, acctId]
        );
        await pool.query(
          `UPDATE prospects
              SET data_status = CASE WHEN data_status = 'Unvetted' THEN 'Contacted' ELSE data_status END,
                  last_activity_at = NOW()
            WHERE id=$1 AND user_id=$2`,
          [acctId, repId]
        );
      }
    }
    res.json(upd.rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/planner/items/:id/undone ───────────────────────────
// Rep mis-tapped — clear the done flag. Any logged visit call is left alone.
router.post('/items/:id/undone', async (req, res) => {
  try {
    const me = req.session.user;
    const id = parseInt(req.params.id);
    const own = await pool.query('SELECT rep_id FROM planner_items WHERE id=$1', [id]);
    if (!own.rows.length) return res.status(404).json({ error: 'Not found' });
    if (own.rows[0].rep_id !== me.id && !isManager(me)) return res.status(403).json({ error: 'Forbidden' });
    const upd = await pool.query('UPDATE planner_items SET completed_at = NULL WHERE id=$1 RETURNING *', [id]);
    res.json(upd.rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
