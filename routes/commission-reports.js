// /api/commission-reports: Keith's reports, built from the commission
// statements (the source of truth, not what reps type in).
//   • 80/20: each rep's accounts by commission, the few that make most of it
//   • At risk: accounts that paid commission and nobody has visited in 60 days
//   • By line: commission per manufacturer, month by month
//   • Trend: total commission month by month
// A rep sees their own; a manager sees everyone or picks a rep.
const express = require('express');
const { pool } = require('../db');
const router = express.Router();

const AT_RISK_DAYS = 60;
const TZ = 'America/New_York';

function scope(req) {
  const me = req.session.user;
  if (me.role === 'manager') { const r = parseInt(req.query.rep); return Number.isFinite(r) ? r : null; }
  return me.id;
}

router.get('/', async (req, res) => {
  const rep = scope(req);
  const months = Math.min(Math.max(parseInt(req.query.months) || 12, 1), 36);
  // Only confirmed statements, no chargeback/adjustment rows in the account
  // ranking (they still count in totals).
  const base = `FROM commission_lines cl
                JOIN commission_imports ci ON ci.id = cl.import_id AND ci.status = 'confirmed'
                LEFT JOIN prospects p ON p.id = cl.account_id
               WHERE cl.period_start >= (date_trunc('month', (SELECT MAX(period_start) FROM commission_lines)) - make_interval(months => $1 - 1))::date
                 AND ($2::int IS NULL OR ci.rep_id = $2 OR (ci.rep_id IS NULL AND p.user_id = $2))`;
  const args = [months, rep];

  const [range, byMonth, byLine, accounts] = await Promise.all([
    pool.query(`SELECT MIN(cl.period_start)::text AS first, MAX(COALESCE(cl.period_end, cl.period_start))::text AS last,
                       COALESCE(SUM(cl.commission_amount),0)::float AS commission, COALESCE(SUM(cl.sales_amount),0)::float AS sales,
                       COUNT(DISTINCT cl.account_id)::int AS accounts ${base}`, args),
    pool.query(`SELECT to_char(date_trunc('month', cl.period_start), 'YYYY-MM') AS m,
                       SUM(cl.commission_amount)::float AS commission, SUM(cl.sales_amount)::float AS sales
                  ${base} GROUP BY 1 ORDER BY 1`, args),
    pool.query(`SELECT COALESCE(l.name, cl.manufacturer, 'Unknown') AS line, to_char(date_trunc('month', cl.period_start), 'YYYY-MM') AS m,
                       SUM(cl.commission_amount)::float AS commission, SUM(cl.sales_amount)::float AS sales,
                       COUNT(DISTINCT cl.account_id)::int AS accounts
                  ${base.replace('LEFT JOIN prospects p', 'LEFT JOIN lines l ON l.id = cl.line_id LEFT JOIN prospects p')} GROUP BY 1, 2 ORDER BY 1, 2`, args),
    pool.query(`SELECT cl.account_id AS id, COALESCE(MAX(p.company), MAX(cl.customer_raw)) AS company, MAX(p.city) AS city,
                       MAX(p.user_id) AS owner_id, MAX(COALESCE(NULLIF(TRIM(u.name),''), u.email)) AS owner,
                       SUM(cl.commission_amount)::float AS commission, SUM(cl.sales_amount)::float AS sales,
                       STRING_AGG(DISTINCT COALESCE(l.name, cl.manufacturer), ', ') AS lines,
                       MAX(cl.period_start)::text AS last_paid,
                       (SELECT MAX(call_date) FROM calls c WHERE c.prospect_id = cl.account_id AND COALESCE(c.call_type,'') <> 'Note')::text AS last_visit,
                       ((NOW() AT TIME ZONE '${TZ}')::date - (SELECT MAX(call_date) FROM calls c WHERE c.prospect_id = cl.account_id AND COALESCE(c.call_type,'') <> 'Note')) AS days_since
                  FROM commission_lines cl
                  JOIN commission_imports ci ON ci.id = cl.import_id AND ci.status = 'confirmed'
                  LEFT JOIN prospects p ON p.id = cl.account_id
                  LEFT JOIN users u ON u.id = p.user_id
                  LEFT JOIN lines l ON l.id = cl.line_id
                 WHERE cl.account_id IS NOT NULL AND NOT COALESCE(cl.is_adjustment, FALSE)
                   AND cl.period_start >= (date_trunc('month', (SELECT MAX(period_start) FROM commission_lines)) - make_interval(months => $1 - 1))::date
                   AND ($2::int IS NULL OR ci.rep_id = $2 OR (ci.rep_id IS NULL AND p.user_id = $2))
                 GROUP BY cl.account_id
                HAVING SUM(cl.commission_amount) > 0
                 ORDER BY SUM(cl.commission_amount) DESC`, args)
  ]);

  // 80/20: running share of commission; "A" accounts make the first 80%.
  const total = accounts.rows.reduce((t, a) => t + a.commission, 0);
  let run = 0, aCount = 0;
  const rows = accounts.rows.map((a, i) => {
    const before = run; run += a.commission;
    const isA = total > 0 && before / total < 0.8;
    if (isA) aCount++;
    return Object.assign(a, { rank: i + 1, share: total ? a.commission / total : 0, cum_share: total ? run / total : 0, a_account: isA,
      at_risk: a.days_since == null || a.days_since >= AT_RISK_DAYS });
  });
  const atRisk = rows.filter(a => a.at_risk);

  // Lines: totals plus a month-by-month series.
  const lines = {};
  for (const r of byLine.rows) {
    const l = lines[r.line] || (lines[r.line] = { line: r.line, commission: 0, sales: 0, months: {} });
    l.commission += r.commission; l.sales += r.sales; l.months[r.m] = r.commission;
  }
  res.json({
    rep, months, at_risk_days: AT_RISK_DAYS, range: range.rows[0],
    pareto: { accounts: rows.length, a_accounts: aCount, a_share: total ? rows.filter(r => r.a_account).reduce((t, r) => t + r.commission, 0) / total : 0 },
    accounts: rows.slice(0, 200),
    at_risk: { count: atRisk.length, commission: atRisk.reduce((t, a) => t + a.commission, 0), accounts: atRisk.slice(0, 25) },
    by_month: byMonth.rows,
    by_line: Object.values(lines).sort((a, b) => b.commission - a.commission)
  });
});

// Reps with commission on file (for the manager's picker).
router.get('/reps', async (req, res) => {
  if (req.session.user.role !== 'manager') return res.json({ reps: [] });
  const r = await pool.query(`SELECT u.id, COALESCE(NULLIF(TRIM(u.name),''), u.email) AS name, COALESCE(SUM(cl.commission_amount),0)::float AS commission
                                FROM users u JOIN commission_imports ci ON ci.rep_id = u.id AND ci.status='confirmed'
                                JOIN commission_lines cl ON cl.import_id = ci.id GROUP BY u.id ORDER BY 3 DESC`);
  res.json({ reps: r.rows });
});

module.exports = router;
