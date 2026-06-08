// ════════════════════════════════════════════════════════════════
// Weekly Report — per-rep AI activity reports built off logged calls.
//   • Reps see their own report; managers/admins see all reps.
//   • Weekly (Mon–Fri) and monthly periods.
//   • Activity numbers computed directly from data (NOT AI).
//   • Claude produces summary/takeaways, follow-ups, opportunities, risks.
//   • CSV (Excel) export + emailable via the existing Outlook/Graph integration.
//   • Auto-generated Fridays (scheduler lives in server.js, calls generateForRep).
// ════════════════════════════════════════════════════════════════
const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const { getValidToken } = require('./email');
const router = express.Router();

// ── Anthropic helper (self-contained, mirrors routes/ai.js pattern) ──
const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';
async function callClaude(prompt) {
  const res = await fetch(CLAUDE_API, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({ model: MODEL, max_tokens: 1500, messages: [{ role: 'user', content: prompt }] })
  });
  const data = await res.json();
  if (!data.content) return '';
  return data.content.filter(b => b.type === 'text').map(b => b.text || '').join('');
}

// ── Role helper ──────────────────────────────────────────────────
function isManager(u) { return !!u && (u.role === 'manager' || u.role === 'admin'); }

// ── Date helpers (UTC, date-only strings) ────────────────────────
function toDateStr(d) { return d.toISOString().slice(0, 10); }
function parseDate(s) { return new Date(String(s).slice(0, 10) + 'T00:00:00Z'); }

// Normalize a given date into the start of its period.
function currentPeriodStart(period_type, ref) {
  ref = ref || new Date();
  if (period_type === 'month') {
    return toDateStr(new Date(Date.UTC(ref.getUTCFullYear(), ref.getUTCMonth(), 1)));
  }
  // Week: Monday of the current week.
  const day = ref.getUTCDay();           // 0 Sun .. 6 Sat
  const diff = (day === 0 ? -6 : 1 - day); // shift back to Monday
  const mon = new Date(ref);
  mon.setUTCDate(ref.getUTCDate() + diff);
  return toDateStr(new Date(Date.UTC(mon.getUTCFullYear(), mon.getUTCMonth(), mon.getUTCDate())));
}

// Given a normalized period_start, return {start, end}.
// Week = Mon..Fri; Month = first..last calendar day.
function periodBounds(period_type, period_start) {
  const d = parseDate(period_start);
  if (period_type === 'month') {
    const y = d.getUTCFullYear(), m = d.getUTCMonth();
    return { start: toDateStr(new Date(Date.UTC(y, m, 1))), end: toDateStr(new Date(Date.UTC(y, m + 1, 0))) };
  }
  const fri = new Date(d);
  fri.setUTCDate(d.getUTCDate() + 4);
  return { start: toDateStr(d), end: toDateStr(fri) };
}

// Count Mon–Fri working days in an inclusive range (for calls/day average).
function businessDaysBetween(start, end) {
  let n = 0;
  const s = parseDate(start), e = parseDate(end);
  for (let t = new Date(s); t <= e; t.setUTCDate(t.getUTCDate() + 1)) {
    const dow = t.getUTCDay();
    if (dow >= 1 && dow <= 5) n++;
  }
  return n || 1;
}

// Build a previous list of period_start strings (for the "view past periods" toggle).
function recentPeriods(period_type, count) {
  const list = [];
  let ref = new Date();
  for (let i = 0; i < count; i++) {
    const ps = currentPeriodStart(period_type, ref);
    list.push(ps);
    const d = parseDate(ps);
    if (period_type === 'month') d.setUTCMonth(d.getUTCMonth() - 1);
    else d.setUTCDate(d.getUTCDate() - 7);
    ref = d;
  }
  return list;
}

// ── Activity numbers — computed directly from the calls table ────
async function computeStats(repId, start, end) {
  const r = await pool.query(
    `SELECT c.*, p.company FROM calls c
     LEFT JOIN prospects p ON c.prospect_id = p.id
     WHERE c.user_id = $1 AND c.call_date BETWEEN $2 AND $3
     ORDER BY c.call_date ASC`,
    [repId, start, end]
  );
  const calls = r.rows;
  const byDay = {}, byLine = {}, byOutcome = {}, accounts = new Set();
  calls.forEach(c => {
    const d = (c.call_date instanceof Date) ? c.call_date.toISOString().slice(0, 10) : String(c.call_date).slice(0, 10);
    byDay[d] = (byDay[d] || 0) + 1;
    const line = ((c.products_discussed || '').trim()) || 'Unspecified';
    byLine[line] = (byLine[line] || 0) + 1;
    const oc = ((c.outcome || '').trim()) || 'Unspecified';
    byOutcome[oc] = (byOutcome[oc] || 0) + 1;
    if (c.prospect_id) accounts.add(c.prospect_id);
  });
  const workingDays = businessDaysBetween(start, end);
  return {
    rows: calls,
    stats: {
      total_calls: calls.length,
      calls_per_day: { by_date: byDay, avg: +(calls.length / workingDays).toFixed(1), working_days: workingDays },
      calls_by_line: byLine,
      calls_by_outcome: byOutcome,
      accounts_touched: accounts.size
    }
  };
}

// ── AI sections from the period's call notes/outcomes/next-steps ─
async function generateAISections(repName, period_type, start, end, calls) {
  const empty = { summary_takeaways: '', follow_ups: '', opportunities: '', risks: '' };
  if (!calls.length) {
    return Object.assign({}, empty, { summary_takeaways: 'No calls were logged in this ' + period_type + '.' });
  }
  const lines = calls.map(c => {
    const d = (c.call_date instanceof Date) ? c.call_date.toISOString().slice(0, 10) : String(c.call_date).slice(0, 10);
    return '- ' + d + ' | ' + (c.company || 'Unknown account') +
      ' | line: ' + (c.products_discussed || 'n/a') +
      ' | outcome: ' + (c.outcome || 'n/a') +
      ' | next step: ' + (c.next_step || 'n/a') +
      ' | notes: ' + (c.notes || '').replace(/\s+/g, ' ').slice(0, 400);
  }).join('\n');

  const prompt =
    'You are a sales manager reviewing field rep ' + repName + "'s logged sales calls for the " +
    period_type + ' of ' + start + ' through ' + end + '. Here are the calls:\n\n' + lines +
    '\n\nWrite a tight report for a busy field rep. Plain text, punchy, NO markdown, NO bullet symbols, ' +
    'no headers — just sentences separated by line breaks. Respond with ONLY valid JSON in exactly this shape:\n' +
    '{"summary_takeaways":"2-4 sentences summarizing the week and the key takeaways",' +
    '"follow_ups":"the concrete follow-ups to make next week, one per line",' +
    '"opportunities":"opportunities worth chasing, one per line",' +
    '"risks":"risks or stalled deals to watch, one per line"}';

  let text = '';
  try { text = await callClaude(prompt); } catch (e) { text = ''; }
  if (!text) return Object.assign({}, empty, { summary_takeaways: 'AI summary unavailable; ' + calls.length + ' calls logged this ' + period_type + '.' });

  // Strip code fences and isolate the JSON object.
  let raw = text.trim().replace(/^```(?:json)?/i, '').replace(/```$/, '').trim();
  const first = raw.indexOf('{'), last = raw.lastIndexOf('}');
  if (first !== -1 && last !== -1) raw = raw.slice(first, last + 1);
  try {
    const j = JSON.parse(raw);
    return {
      summary_takeaways: String(j.summary_takeaways || ''),
      follow_ups: String(j.follow_ups || ''),
      opportunities: String(j.opportunities || ''),
      risks: String(j.risks || '')
    };
  } catch (e) {
    return Object.assign({}, empty, { summary_takeaways: text.slice(0, 1200) });
  }
}

// ── Generate + persist a report for one rep/period (idempotent) ──
// Exported so the Friday scheduler in server.js can call it.
async function generateForRep(repId, repName, period_type, period_start) {
  period_type = (period_type === 'month') ? 'month' : 'week';
  period_start = period_start || currentPeriodStart(period_type);
  const { start, end } = periodBounds(period_type, period_start);
  if (!repName) {
    const u = await pool.query('SELECT name FROM users WHERE id=$1', [repId]);
    repName = u.rows[0] ? u.rows[0].name : 'Rep';
  }
  const { rows, stats } = await computeStats(repId, start, end);
  const ai = await generateAISections(repName, period_type, start, end, rows);
  const r = await pool.query(
    `INSERT INTO weekly_reports (user_id, period_type, period_start, period_end, generated_at, ai_sections, activity_stats)
     VALUES ($1,$2,$3,$4,NOW(),$5,$6)
     ON CONFLICT (user_id, period_type, period_start)
     DO UPDATE SET period_end = EXCLUDED.period_end, generated_at = NOW(),
                   ai_sections = EXCLUDED.ai_sections, activity_stats = EXCLUDED.activity_stats
     RETURNING *`,
    [repId, period_type, start, end, JSON.stringify(ai), JSON.stringify(stats)]
  );
  return r.rows[0];
}

// ════════════════════════════════════════════════════════════════
// ENDPOINTS  (all mounted under /api/weekly-report behind requireAuth)
// ════════════════════════════════════════════════════════════════

// GET /api/weekly-report/report — one rep's full report (own, or any rep if manager).
// Activity numbers are computed live; AI sections come from the stored row if present.
router.get('/report', async (req, res) => {
  try {
    const me = req.session.user;
    const period_type = req.query.period_type === 'month' ? 'month' : 'week';
    const period_start = currentPeriodStart(period_type, req.query.period_start ? parseDate(req.query.period_start) : null);
    let repId = me.id;
    if (req.query.rep_id && parseInt(req.query.rep_id) !== me.id) {
      if (!isManager(me)) return res.status(403).json({ error: 'Forbidden' });
      repId = parseInt(req.query.rep_id);
    }
    const { start, end } = periodBounds(period_type, period_start);
    const { stats } = await computeStats(repId, start, end);
    const stored = await pool.query(
      'SELECT ai_sections, generated_at FROM weekly_reports WHERE user_id=$1 AND period_type=$2 AND period_start=$3',
      [repId, period_type, start]
    );
    const repRow = await pool.query('SELECT name FROM users WHERE id=$1', [repId]);
    res.json({
      rep_id: repId,
      rep_name: repRow.rows[0] ? repRow.rows[0].name : 'Rep',
      is_self: repId === me.id,
      can_manage: isManager(me),
      period_type, period_start: start, period_end: end,
      activity_stats: stats,
      ai_sections: stored.rows[0] ? stored.rows[0].ai_sections : null,
      generated_at: stored.rows[0] ? stored.rows[0].generated_at : null
    });
  } catch (e) {
    console.error('[weekly-report/report]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/weekly-report/periods — recent period_starts for the toggle / past-period nav.
router.get('/periods', async (req, res) => {
  try {
    const me = req.session.user;
    const period_type = req.query.period_type === 'month' ? 'month' : 'week';
    const repId = (req.query.rep_id && isManager(me)) ? parseInt(req.query.rep_id) : me.id;
    const list = recentPeriods(period_type, period_type === 'month' ? 6 : 8);
    const stored = await pool.query(
      'SELECT period_start FROM weekly_reports WHERE user_id=$1 AND period_type=$2',
      [repId, period_type]
    );
    const have = new Set(stored.rows.map(r => toDateStr(new Date(r.period_start))));
    res.json({
      period_type,
      periods: list.map(ps => {
        const b = periodBounds(period_type, ps);
        return { period_start: ps, period_end: b.end, has_report: have.has(ps) };
      })
    });
  } catch (e) {
    console.error('[weekly-report/periods]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/weekly-report/roster — manager/admin only: all reps' activity for a period.
router.get('/roster', async (req, res) => {
  try {
    const me = req.session.user;
    if (!isManager(me)) return res.status(403).json({ error: 'Forbidden' });
    const period_type = req.query.period_type === 'month' ? 'month' : 'week';
    const period_start = currentPeriodStart(period_type, req.query.period_start ? parseDate(req.query.period_start) : null);
    const { start, end } = periodBounds(period_type, period_start);
    const users = await pool.query('SELECT id, name, email, role FROM users ORDER BY name ASC');
    const reps = [];
    for (const u of users.rows) {
      const { stats } = await computeStats(u.id, start, end);
      const stored = await pool.query(
        'SELECT 1 FROM weekly_reports WHERE user_id=$1 AND period_type=$2 AND period_start=$3',
        [u.id, period_type, start]
      );
      reps.push({
        rep_id: u.id, rep_name: u.name, role: u.role,
        total_calls: stats.total_calls,
        calls_per_day_avg: stats.calls_per_day.avg,
        calls_by_line: stats.calls_by_line,
        accounts_touched: stats.accounts_touched,
        has_report: stored.rows.length > 0
      });
    }
    res.json({ period_type, period_start: start, period_end: end, reps });
  } catch (e) {
    console.error('[weekly-report/roster]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/weekly-report/generate — generate/regenerate a period.
// body: { period_type, period_start, rep_id }  (rep_id 'all' => every rep, manager only)
router.post('/generate', async (req, res) => {
  try {
    const me = req.session.user;
    const period_type = req.body.period_type === 'month' ? 'month' : 'week';
    const period_start = currentPeriodStart(period_type, req.body.period_start ? parseDate(req.body.period_start) : null);

    if (req.body.rep_id === 'all') {
      if (!isManager(me)) return res.status(403).json({ error: 'Forbidden' });
      const users = await pool.query('SELECT id, name FROM users');
      let n = 0;
      for (const u of users.rows) {
        try { await generateForRep(u.id, u.name, period_type, period_start); n++; } catch (e) { console.error('gen rep', u.id, e.message); }
      }
      return res.json({ ok: true, generated: n });
    }

    let repId = me.id;
    if (req.body.rep_id && parseInt(req.body.rep_id) !== me.id) {
      if (!isManager(me)) return res.status(403).json({ error: 'Forbidden' });
      repId = parseInt(req.body.rep_id);
    }
    const row = await generateForRep(repId, null, period_type, period_start);
    res.json({ ok: true, report: row });
  } catch (e) {
    console.error('[weekly-report/generate]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── CSV builders ─────────────────────────────────────────────────
function csvCell(v) {
  const s = (v == null) ? '' : String(v);
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}
function mapToStr(obj) {
  return Object.keys(obj || {}).map(k => k + ': ' + obj[k]).join('; ');
}

// GET /api/weekly-report/export — CSV (Excel) download, role-scoped.
// query: period_type, period_start, scope ('mine' | 'all')
router.get('/export', async (req, res) => {
  try {
    const me = req.session.user;
    const period_type = req.query.period_type === 'month' ? 'month' : 'week';
    const period_start = currentPeriodStart(period_type, req.query.period_start ? parseDate(req.query.period_start) : null);
    const { start, end } = periodBounds(period_type, period_start);
    const scope = req.query.scope === 'all' ? 'all' : 'mine';

    const header = ['Rep', 'Period Type', 'Period Start', 'Period End', 'Total Calls', 'Calls/Day Avg', 'Accounts Touched', 'Calls By Line', 'Calls By Outcome'];
    const rows = [header];

    let targets = [];
    if (scope === 'all') {
      if (!isManager(me)) return res.status(403).json({ error: 'Forbidden' });
      const users = await pool.query('SELECT id, name FROM users ORDER BY name ASC');
      targets = users.rows;
    } else {
      let repId = me.id, repName = me.name;
      if (req.query.rep_id && parseInt(req.query.rep_id) !== me.id) {
        if (!isManager(me)) return res.status(403).json({ error: 'Forbidden' });
        repId = parseInt(req.query.rep_id);
        const u = await pool.query('SELECT name FROM users WHERE id=$1', [repId]);
        repName = u.rows[0] ? u.rows[0].name : 'Rep';
      }
      targets = [{ id: repId, name: repName }];
    }

    for (const t of targets) {
      const { stats } = await computeStats(t.id, start, end);
      rows.push([
        t.name, period_type, start, end,
        stats.total_calls, stats.calls_per_day.avg, stats.accounts_touched,
        mapToStr(stats.calls_by_line), mapToStr(stats.calls_by_outcome)
      ]);
    }

    const csv = rows.map(r => r.map(csvCell).join(',')).join('\r\n');
    const fname = 'weekly-report-' + scope + '-' + period_type + '-' + start + '.csv';
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="' + fname + '"');
    res.send(csv);
  } catch (e) {
    console.error('[weekly-report/export]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Render a report to HTML for email ────────────────────────────
function renderReportHTML(repName, period_type, start, end, stats, ai) {
  const sec = (title, body) => body && body.trim()
    ? '<h3 style="margin:16px 0 4px;font-size:14px;color:#1e293b">' + title + '</h3>' +
      '<div style="font-size:13px;color:#334155;white-space:pre-line;line-height:1.5">' + body.replace(/</g, '&lt;') + '</div>'
    : '';
  const lineRows = Object.keys(stats.calls_by_line || {}).map(k =>
    '<tr><td style="padding:2px 10px 2px 0">' + k + '</td><td>' + stats.calls_by_line[k] + '</td></tr>').join('');
  return '' +
    '<div style="font-family:Arial,sans-serif;max-width:640px;margin:0 auto">' +
    '<h2 style="color:#0f172a;margin-bottom:2px">' + repName + ' — ' + (period_type === 'month' ? 'Monthly' : 'Weekly') + ' Report</h2>' +
    '<div style="color:#64748b;font-size:12px;margin-bottom:12px">' + start + ' to ' + end + '</div>' +
    '<div style="display:flex;gap:14px;flex-wrap:wrap;margin-bottom:8px">' +
      '<div><strong style="font-size:22px;color:#0f172a">' + stats.total_calls + '</strong><div style="font-size:11px;color:#64748b">Total Calls</div></div>' +
      '<div><strong style="font-size:22px;color:#0f172a">' + stats.calls_per_day.avg + '</strong><div style="font-size:11px;color:#64748b">Calls / Day</div></div>' +
      '<div><strong style="font-size:22px;color:#0f172a">' + stats.accounts_touched + '</strong><div style="font-size:11px;color:#64748b">Accounts Touched</div></div>' +
    '</div>' +
    (lineRows ? '<table style="font-size:12px;color:#334155;margin:8px 0">' + lineRows + '</table>' : '') +
    (ai ? (sec('Summary &amp; Takeaways', ai.summary_takeaways) + sec('Follow-ups', ai.follow_ups) +
           sec('Opportunities', ai.opportunities) + sec('Risks', ai.risks)) : '') +
    '<div style="margin-top:18px;font-size:11px;color:#94a3b8">Sent from RepRoute · Compton Group LLC</div>' +
    '</div>';
}

// POST /api/weekly-report/email — email a rendered report to any recipient (via Outlook/Graph).
// body: { period_type, period_start, rep_id, to }
router.post('/email', async (req, res) => {
  try {
    const me = req.session.user;
    const to = (req.body.to || '').trim();
    if (!to || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(to)) return res.status(400).json({ error: 'Valid recipient email required' });
    const period_type = req.body.period_type === 'month' ? 'month' : 'week';
    const period_start = currentPeriodStart(period_type, req.body.period_start ? parseDate(req.body.period_start) : null);
    let repId = me.id;
    if (req.body.rep_id && parseInt(req.body.rep_id) !== me.id) {
      if (!isManager(me)) return res.status(403).json({ error: 'Forbidden' });
      repId = parseInt(req.body.rep_id);
    }
    const { start, end } = periodBounds(period_type, period_start);
    const { stats } = await computeStats(repId, start, end);
    const stored = await pool.query(
      'SELECT ai_sections FROM weekly_reports WHERE user_id=$1 AND period_type=$2 AND period_start=$3',
      [repId, period_type, start]
    );
    const repRow = await pool.query('SELECT name FROM users WHERE id=$1', [repId]);
    const repName = repRow.rows[0] ? repRow.rows[0].name : 'Rep';
    const ai = stored.rows[0] ? stored.rows[0].ai_sections : null;
    const html = renderReportHTML(repName, period_type, start, end, stats, ai);
    const subject = repName + ' — ' + (period_type === 'month' ? 'Monthly' : 'Weekly') + ' Report (' + start + ')';

    const token = await getValidToken(me.id);
    const sendRes = await fetch('https://graph.microsoft.com/v1.0/me/sendMail', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: { subject, body: { contentType: 'HTML', content: html }, toRecipients: [{ emailAddress: { address: to } }] },
        saveToSentItems: true
      })
    });
    if (!sendRes.ok) {
      const err = await sendRes.json().catch(() => ({}));
      throw new Error((err.error && err.error.message) || 'Send failed');
    }
    res.json({ ok: true, sent_to: to });
  } catch (e) {
    console.error('[weekly-report/email]', e.message);
    if (String(e.message).includes('Not connected')) {
      return res.status(400).json({ error: 'Connect Outlook on the Email tab to send reports.' });
    }
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
module.exports.generateForRep = generateForRep;
