// /api/accounts-list: the full Accounts list (Salesforce style). Every active
// rep's accounts, with saved views, filters, sorting and pages.
const express = require('express');
const { pool } = require('../db');
const { COLD_DAYS } = require('../lib/going-cold');
const router = express.Router();

const TZ = 'America/New_York';
const SORTS = {
  name: 'LOWER(p.company)', type: 'LOWER(COALESCE(p.category, \'\'))', city: 'LOWER(COALESCE(p.city, \'\'))',
  owner: 'LOWER(COALESCE(NULLIF(TRIM(u.name), \'\'), u.email))', stage: 'COALESCE(p.pipeline_stage, \'New Lead\')',
  last_visit: 'lv.last_visit', open_quotes: 'COALESCE(oq.amt, 0)'
};

function buildWhere(q, me) {
  const where = ['u.disabled_at IS NULL'], args = [];
  const add = (sql, v) => { args.push(v); where.push(sql.replace('?', '$' + args.length)); };
  const view = q.view || 'all';
  if (view === 'mine' || view === 'cold') add('p.user_id = ?', me.id);
  if (view === 'cold') {
    add(`lv.last_visit <= (NOW() AT TIME ZONE '${TZ}')::date - ?::int`, COLD_DAYS);
    where.push(`COALESCE(p.reconnect_dismissed, FALSE) = FALSE`, `(p.reconnect_snoozed_until IS NULL OR p.reconnect_snoozed_until < NOW())`, `COALESCE(p.status, '') <> 'Cold'`);
  }
  if (view === 'recent') {
    const ids = String(q.ids || '').split(',').map(x => parseInt(x)).filter(Number.isFinite).slice(0, 50);
    add('p.id = ANY(?::int[])', ids.length ? ids : [0]);
  }
  const s = String(q.q || '').trim();
  if (s) {
    const like = '%' + s.replace(/[\\%_]/g, m => '\\' + m) + '%';
    const digits = s.replace(/\D/g, '');
    args.push(like);
    const n = args.length;
    let cond = `(p.company ILIKE $${n} OR p.city ILIKE $${n} OR p.contact ILIKE $${n} OR EXISTS (SELECT 1 FROM contacts c WHERE c.prospect_id = p.id AND c.name ILIKE $${n})`;
    if (digits.length >= 4) { args.push('%' + digits + '%'); cond += ` OR regexp_replace(COALESCE(p.phone, ''), '\\D', '', 'g') LIKE $${args.length}`; }
    where.push(cond + ')');
  }
  if (q.type) add('p.category = ?', String(q.type));
  if (q.city) add('LOWER(TRIM(p.city)) = LOWER(TRIM(?))', String(q.city));
  if (q.owner) add('p.user_id = ?', parseInt(q.owner) || 0);
  if (q.stage) add(`COALESCE(p.pipeline_stage, 'New Lead') = ?`, String(q.stage));
  if (q.line) add('EXISTS (SELECT 1 FROM account_lines al WHERE al.account_id = p.id AND al.line_id = ?)', parseInt(q.line) || 0);
  return { where: where.join(' AND '), args };
}

const FROM = `FROM prospects p JOIN users u ON u.id = p.user_id
  LEFT JOIN LATERAL (SELECT MAX(call_date) AS last_visit FROM calls c WHERE c.prospect_id = p.id) lv ON TRUE
  LEFT JOIN LATERAL (SELECT COUNT(*)::int AS n, SUM(amount)::float AS amt FROM quotes q WHERE q.prospect_id = p.id AND q.status NOT IN ('Won','Lost')) oq ON TRUE`;

router.get('/', async (req, res) => {
  const me = req.session.user;
  const { where, args } = buildWhere(req.query, me);
  const sortKey = SORTS[req.query.sort] ? req.query.sort : 'name';
  const dir = req.query.dir === 'desc' ? 'DESC' : 'ASC';
  const per = Math.min(Math.max(parseInt(req.query.per) || 50, 10), 200);
  const page = Math.max(parseInt(req.query.page) || 1, 1);
  const total = (await pool.query(`SELECT COUNT(*)::int n ${FROM} WHERE ${where}`, args)).rows[0].n;
  const rows = (await pool.query(
    `SELECT p.id, p.company, p.category, p.city, p.state, p.phone, p.user_id,
            COALESCE(NULLIF(TRIM(u.name), ''), u.email) AS owner, (p.user_id = $${args.length + 1}) AS mine,
            COALESCE(p.pipeline_stage, 'New Lead') AS stage, lv.last_visit::text AS last_visit,
            ((NOW() AT TIME ZONE '${TZ}')::date - lv.last_visit) AS days_since,
            COALESCE(oq.n, 0) AS open_quotes, COALESCE(oq.amt, 0) AS open_amount
       ${FROM} WHERE ${where}
      ORDER BY ${SORTS[sortKey]} ${dir} NULLS LAST, LOWER(p.company), p.id
      LIMIT ${per} OFFSET ${(page - 1) * per}`, args.concat([me.id]))).rows;
  res.json({ rows, total, page, per, sort: sortKey, dir: dir.toLowerCase(), cold_days: COLD_DAYS });
});

// Values for the filter menus, and the counts on the saved views.
router.get('/facets', async (req, res) => {
  const me = req.session.user;
  const [types, cities, owners, lines, counts] = await Promise.all([
    pool.query(`SELECT p.category AS v, COUNT(*)::int n FROM prospects p JOIN users u ON u.id=p.user_id AND u.disabled_at IS NULL
                 WHERE COALESCE(TRIM(p.category), '') <> '' GROUP BY 1 ORDER BY 1`),
    pool.query(`SELECT MIN(TRIM(p.city)) AS v, COUNT(*)::int n FROM prospects p JOIN users u ON u.id=p.user_id AND u.disabled_at IS NULL
                 WHERE COALESCE(TRIM(p.city), '') <> '' GROUP BY LOWER(TRIM(p.city)) ORDER BY 1`),
    pool.query(`SELECT u.id, COALESCE(NULLIF(TRIM(u.name), ''), u.email) AS name, COUNT(p.id)::int n FROM users u
                 JOIN prospects p ON p.user_id = u.id WHERE u.disabled_at IS NULL GROUP BY u.id ORDER BY 2`),
    pool.query(`SELECT l.id, l.name FROM lines l WHERE EXISTS (SELECT 1 FROM account_lines al WHERE al.line_id = l.id) ORDER BY l.featured DESC, l.name`),
    pool.query(`SELECT COUNT(*)::int AS all_n, COUNT(*) FILTER (WHERE p.user_id=$1)::int AS mine_n,
                       COUNT(*) FILTER (WHERE p.user_id=$1 AND lv.last_visit <= (NOW() AT TIME ZONE '${TZ}')::date - $2::int
                                        AND COALESCE(p.reconnect_dismissed, FALSE) = FALSE
                                        AND (p.reconnect_snoozed_until IS NULL OR p.reconnect_snoozed_until < NOW())
                                        AND COALESCE(p.status, '') <> 'Cold')::int AS cold_n
                  ${FROM} WHERE u.disabled_at IS NULL`, [me.id, COLD_DAYS])
  ]);
  const leads = (await pool.query(`SELECT COUNT(*)::int n FROM leads WHERE user_id=$1 AND status='new'`, [me.id])).rows[0].n;
  res.json({
    types: types.rows, cities: cities.rows, owners: owners.rows, lines: lines.rows,
    stages: ['New Lead', 'Contacted', 'Sampled', 'Quoted', 'Closed Won', 'Closed Lost'],
    counts: { all: counts.rows[0].all_n, mine: counts.rows[0].mine_n, cold: counts.rows[0].cold_n, leads }
  });
});

// Which layout this person uses (the new one or the classic one).
router.put('/layout', async (req, res) => {
  const v = (req.body || {}).layout === 'new' ? 'new' : 'classic';
  await pool.query('UPDATE users SET ui_layout=$2 WHERE id=$1', [req.session.user.id, v]);
  req.session.user.ui_layout = v;
  res.json({ layout: v });
});

module.exports = router;
