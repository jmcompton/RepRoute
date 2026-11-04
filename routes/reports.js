// Reports — one endpoint that answers the questions reps and managers actually
// ask ("how many visits did I make", "who has open quotes", "which samples are
// overdue"), filtered by date range, rep and product line, plus CSV export of
// the rows behind every number.
//
// GET /api/reports/summary?from=YYYY-MM-DD&to=YYYY-MM-DD&rep=me|all|<id>&line=
// GET /api/reports/export?type=visits|quotes|samples|followups|accounts&(same filters)
const express = require('express');
const { pool } = require('../db');
const router = express.Router();

const TZ = 'America/New_York';
const isYmd = s => /^\d{4}-\d{2}-\d{2}$/.test(s || '');

// Normalize filters. Default range = this month in Eastern time.
async function filters(req) {
  const me = req.session.user;
  const d = (await pool.query(
    `SELECT date_trunc('month', NOW() AT TIME ZONE '${TZ}')::date::text AS f,
            (NOW() AT TIME ZONE '${TZ}')::date::text AS t`)).rows[0];
  let from = isYmd(req.query.from) ? req.query.from : d.f;
  let to = isYmd(req.query.to) ? req.query.to : d.t;
  if (from > to) [from, to] = [to, from];
  let rep = String(req.query.rep || 'me');
  let repId = null;
  if (rep === 'me') repId = me.id;
  else if (rep !== 'all') { repId = parseInt(rep); if (!Number.isFinite(repId)) repId = me.id; }
  // Reps only ever see their own numbers; the team view is for managers.
  if (me.role !== 'manager') repId = me.id;
  const line = String(req.query.line || '').trim().slice(0, 60);
  return { from, to, repId, line, today: d.t };
}

// Build the WHERE fragments shared by every query.
function callWhere(f, alias = 'c') {
  // Notes typed on an account aren't calls or visits; keep them out of the counts.
  const p = [f.from, f.to]; const w = [`${alias}.call_date >= $1`, `${alias}.call_date <= $2`, `COALESCE(${alias}.call_type,'') <> 'Note'`];
  if (f.repId) { p.push(f.repId); w.push(`${alias}.user_id = $${p.length}`); }
  if (f.line) { p.push('%' + f.line.toLowerCase() + '%'); w.push(`LOWER(COALESCE(${alias}.products_discussed,'')) LIKE $${p.length}`); }
  return { w: w.join(' AND '), p };
}
function quoteWhere(f) {
  const p = [f.from, f.to]; const w = [`q.quote_date >= $1`, `q.quote_date <= $2`];
  if (f.repId) { p.push(f.repId); w.push(`(q.user_id = $${p.length} OR q.rep_id = $${p.length})`); }
  if (f.line) { p.push('%' + f.line.toLowerCase() + '%'); w.push(`LOWER(COALESCE(q.products,'')) LIKE $${p.length}`); }
  return { w: w.join(' AND '), p };
}
function sampleWhere(f) {
  const p = [f.from, f.to]; const w = [`s.sent_date >= $1`, `s.sent_date <= $2`];
  if (f.repId) { p.push(f.repId); w.push(`s.user_id = $${p.length}`); }
  if (f.line) { p.push('%' + f.line.toLowerCase() + '%'); w.push(`LOWER(COALESCE(s.product_line,'')) LIKE $${p.length}`); }
  return { w: w.join(' AND '), p };
}

const REP_NAME = `COALESCE(NULLIF(TRIM(u.name),''), u.email)`;

router.get('/summary', async (req, res) => {
  try {
    const f = await filters(req);
    const cw = callWhere(f), qw = quoteWhere(f), sw = sampleWhere(f);
    const [kc, weeks, outcomes, byRep, qStatus, qByRep, sKpi, sByRep, newAccts, overdue, lineRows, topAccts] = await Promise.all([
      pool.query(`SELECT COUNT(*)::int AS visits, COUNT(DISTINCT c.prospect_id)::int AS accounts
                    FROM calls c WHERE ${cw.w}`, cw.p),
      pool.query(`SELECT date_trunc('week', c.call_date)::date::text AS week, COUNT(*)::int AS n
                    FROM calls c WHERE ${cw.w} GROUP BY 1 ORDER BY 1`, cw.p),
      pool.query(`SELECT COALESCE(NULLIF(TRIM(c.outcome),''),'Not set') AS outcome, COUNT(*)::int AS n
                    FROM calls c WHERE ${cw.w} GROUP BY 1 ORDER BY 2 DESC`, cw.p),
      pool.query(`SELECT c.user_id AS rep_id, ${REP_NAME} AS rep, COUNT(*)::int AS visits,
                         COUNT(DISTINCT c.prospect_id)::int AS accounts
                    FROM calls c LEFT JOIN users u ON u.id = c.user_id
                   WHERE ${cw.w} GROUP BY 1,2 ORDER BY 3 DESC`, cw.p),
      pool.query(`SELECT COALESCE(q.status,'Draft') AS status, COUNT(*)::int AS n, COALESCE(SUM(q.amount),0)::float AS value
                    FROM quotes q WHERE ${qw.w} GROUP BY 1`, qw.p),
      pool.query(`SELECT COALESCE(NULLIF(TRIM(ru.name),''), q.rep_name, ${REP_NAME}) AS rep, COUNT(*)::int AS quotes,
                         COALESCE(SUM(q.amount),0)::float AS value,
                         COALESCE(SUM(q.amount) FILTER (WHERE q.status='Won'),0)::float AS won
                    FROM quotes q LEFT JOIN users u ON u.id = q.user_id LEFT JOIN users ru ON ru.id = q.rep_id
                   WHERE ${qw.w} GROUP BY 1`, qw.p),
      pool.query(`SELECT COUNT(*)::int AS sent FROM samples s WHERE ${sw.w}`, sw.p),
      pool.query(`SELECT ${REP_NAME} AS rep, COUNT(*)::int AS samples
                    FROM samples s LEFT JOIN users u ON u.id = s.user_id WHERE ${sw.w} GROUP BY 1`, sw.p),
      pool.query(`SELECT COUNT(*)::int AS n FROM prospects p
                   WHERE (p.created_at AT TIME ZONE '${TZ}')::date BETWEEN $1 AND $2
                     ${f.repId ? 'AND p.user_id = $3' : ''}`,
                 f.repId ? [f.from, f.to, f.repId] : [f.from, f.to]),
      // Overdue = the LATEST visit on an account (by anyone) set a next step
      // that has passed. A later visit with no next step closes it.
      pool.query(`SELECT COUNT(*)::int AS n FROM (
                    SELECT DISTINCT ON (c.prospect_id) c.next_step_date, c.user_id
                      FROM calls c WHERE c.prospect_id IS NOT NULL
                     ORDER BY c.prospect_id, c.call_date DESC, c.id DESC) x
                   WHERE x.next_step_date IS NOT NULL AND x.next_step_date < $1
                     ${f.repId ? 'AND x.user_id = $2' : ''}`, f.repId ? [f.today, f.repId] : [f.today]),
      pool.query(`SELECT c.products_discussed, c.prospect_id FROM calls c WHERE ${cw.w}`, cw.p),
      pool.query(`SELECT p.id, p.company, p.city, COUNT(*)::int AS visits, MAX(c.call_date)::text AS last_visit
                    FROM calls c JOIN prospects p ON p.id = c.prospect_id
                   WHERE ${cw.w} GROUP BY p.id, p.company, p.city ORDER BY 4 DESC, 5 DESC LIMIT 10`, cw.p)
    ]);

    // Lines: products_discussed is free text like "Soudal, ShurTape".
    const lineMap = {};
    lineRows.rows.forEach(r => {
      String(r.products_discussed || '').split(/[,;/]+/).map(x => x.trim()).filter(Boolean).forEach(name => {
        const k = name.toLowerCase();
        if (!lineMap[k]) lineMap[k] = { line: name, visits: 0, accounts: new Set() };
        lineMap[k].visits++; if (r.prospect_id) lineMap[k].accounts.add(r.prospect_id);
      });
    });
    const by_line = Object.values(lineMap).map(x => ({ line: x.line, visits: x.visits, accounts: x.accounts.size }))
      .sort((a, b) => b.visits - a.visits).slice(0, 12);

    // Merge per-rep rows from visits, quotes and samples.
    const reps = {};
    const rep = name => reps[name] || (reps[name] = { rep: name, visits: 0, accounts: 0, quotes: 0, quote_value: 0, won_value: 0, samples: 0 });
    byRep.rows.forEach(r => Object.assign(rep(r.rep || 'Unknown'), { visits: r.visits, accounts: r.accounts }));
    qByRep.rows.forEach(r => Object.assign(rep(r.rep || 'Unknown'), { quotes: r.quotes, quote_value: r.value, won_value: r.won }));
    sByRep.rows.forEach(r => { rep(r.rep || 'Unknown').samples = r.samples; });

    const qs = {}; qStatus.rows.forEach(r => { qs[r.status] = r; });
    const sum = k => qStatus.rows.reduce((t, r) => t + (r[k] || 0), 0);
    res.json({
      filters: { from: f.from, to: f.to, rep: f.repId || 'all', line: f.line },
      kpis: {
        visits: kc.rows[0].visits,
        accounts_visited: kc.rows[0].accounts,
        new_accounts: newAccts.rows[0].n,
        quotes: sum('n'),
        quote_value: sum('value'),
        won_value: (qs.Won && qs.Won.value) || 0,
        samples_sent: sKpi.rows[0].sent,
        followups_overdue: overdue.rows[0].n
      },
      by_week: weeks.rows,
      by_outcome: outcomes.rows,
      by_rep: Object.values(reps).sort((a, b) => b.visits - a.visits),
      quotes_by_status: qStatus.rows,
      by_line,
      top_accounts: topAccts.rows
    });
  } catch (e) {
    console.error('[reports/summary]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── CSV export ───────────────────────────────────────────────────
function csv(rows, cols) {
  const esc = v => {
    if (v == null) return '';
    const s = v instanceof Date ? v.toISOString().slice(0, 10) : String(v);
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  return [cols.map(c => c[1]).join(',')]
    .concat(rows.map(r => cols.map(c => esc(r[c[0]])).join(','))).join('\r\n');
}

router.get('/export', async (req, res) => {
  try {
    const f = await filters(req);
    const type = String(req.query.type || 'visits');
    let rows, cols;
    if (type === 'visits') {
      const cw = callWhere(f);
      rows = (await pool.query(
        `SELECT c.call_date::text AS date, ${REP_NAME} AS rep, p.company, p.city, c.call_type, c.outcome,
                c.products_discussed, c.next_step, c.next_step_date::text AS next_step_date, c.notes
           FROM calls c LEFT JOIN prospects p ON p.id = c.prospect_id LEFT JOIN users u ON u.id = c.user_id
          WHERE ${cw.w} ORDER BY c.call_date DESC, c.id DESC`, cw.p)).rows;
      cols = [['date','Date'],['rep','Rep'],['company','Account'],['city','City'],['call_type','Type'],['outcome','Outcome'],
              ['products_discussed','Lines'],['next_step','Next step'],['next_step_date','Next step date'],['notes','Notes']];
    } else if (type === 'quotes') {
      const qw = quoteWhere(f);
      rows = (await pool.query(
        `SELECT q.quote_date::text AS date, q.quote_number, COALESCE(q.rep_name, ${REP_NAME}) AS rep, q.account_name,
                q.contact_name, q.status, q.amount, q.products, q.follow_up_date::text AS follow_up
           FROM quotes q LEFT JOIN users u ON u.id = q.user_id WHERE ${qw.w} ORDER BY q.quote_date DESC`, qw.p)).rows;
      cols = [['date','Date'],['quote_number','Quote #'],['rep','Rep'],['account_name','Account'],['contact_name','Contact'],
              ['status','Status'],['amount','Amount'],['products','Lines'],['follow_up','Follow-up']];
    } else if (type === 'samples') {
      const sw = sampleWhere(f);
      rows = (await pool.query(
        `SELECT s.sent_date::text AS sent, ${REP_NAME} AS rep, p.company, s.product_line, s.quantity, s.status,
                s.follow_up_date::text AS follow_up, s.outcome_notes
           FROM samples s LEFT JOIN prospects p ON p.id = s.prospect_id LEFT JOIN users u ON u.id = s.user_id
          WHERE ${sw.w} ORDER BY s.sent_date DESC`, sw.p)).rows;
      cols = [['sent','Sent'],['rep','Rep'],['company','Account'],['product_line','Line'],['quantity','Qty'],
              ['status','Status'],['follow_up','Follow-up'],['outcome_notes','Outcome']];
    } else if (type === 'followups') {
      const p = [f.today]; let w = '';
      if (f.repId) { p.push(f.repId); w = 'AND x.user_id = $2'; }
      rows = (await pool.query(
        `SELECT x.* FROM (
           SELECT DISTINCT ON (c.prospect_id) c.next_step_date, c.next_step_date::text AS due, c.user_id,
                  ${REP_NAME} AS rep, p.company, p.city, p.phone, c.next_step, c.call_date::text AS last_visit
             FROM calls c JOIN prospects p ON p.id = c.prospect_id LEFT JOIN users u ON u.id = c.user_id
            ORDER BY c.prospect_id, c.call_date DESC, c.id DESC) x
          WHERE x.next_step_date IS NOT NULL AND x.next_step_date <= $1::date + 14 ${w}
          ORDER BY x.next_step_date`, p)).rows;
      cols = [['due','Due'],['rep','Rep'],['company','Account'],['city','City'],['phone','Phone'],['next_step','Next step'],['last_visit','Last visit']];
    } else if (type === 'accounts') {
      const p = []; let w = '';
      if (f.repId) { p.push(f.repId); w = 'WHERE p.user_id = $1'; }
      rows = (await pool.query(
        `SELECT p.company, p.category, p.city, p.state, p.phone, p.status, ${REP_NAME} AS owner,
                (SELECT MAX(call_date)::text FROM calls c WHERE c.prospect_id = p.id) AS last_visit,
                (SELECT COUNT(*) FROM contacts ct WHERE ct.prospect_id = p.id) AS contacts
           FROM prospects p LEFT JOIN users u ON u.id = p.user_id ${w} ORDER BY p.company`, p)).rows;
      cols = [['company','Account'],['category','Category'],['city','City'],['state','State'],['phone','Phone'],
              ['status','Status'],['owner','Owner'],['last_visit','Last visit'],['contacts','Contacts']];
    } else {
      return res.status(400).json({ error: 'Unknown report type' });
    }
    const name = `reproute-${type}-${f.from}-to-${f.to}.csv`;
    res.set('Content-Type', 'text/csv; charset=utf-8');
    res.set('Content-Disposition', `attachment; filename="${name}"`);
    res.send('﻿' + csv(rows, cols));
  } catch (e) {
    console.error('[reports/export]', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
