const express = require('express');
const router = express.Router();
const { pool } = require('../db');

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
    const { planned_date, item_type, account_id, title, appt_time, note } = req.body;
    if (!planned_date) return res.status(400).json({ error: 'planned_date required' });
    const type = item_type === 'appointment' ? 'appointment' : 'stop';

    // place new item at end of that day
    const ord = await pool.query(
      'SELECT COALESCE(MAX(sort_order), -1) + 1 AS next FROM planner_items WHERE rep_id=$1 AND planned_date=$2',
      [repId, ymd(planned_date)]
    );

    const ins = await pool.query(
      `INSERT INTO planner_items (rep_id, planned_date, item_type, account_id, title, appt_time, note, sort_order)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [repId, ymd(planned_date), type, account_id || null, title || null,
       appt_time || null, note || null, ord.rows[0].next]
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

module.exports = router;
