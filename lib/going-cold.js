// ONE definition of "going cold", used by the bell, the morning brief and the
// win-back agent (they used to disagree: 30 days, 60 days, different rules).
//
// An account is going cold when someone on the team has visited or called it
// before, and nobody has in COLD_DAYS or more. Accounts the rep dismissed or
// snoozed on Reconnect are left out. Bigger buyers (commission data) first,
// then the longest gone.
const { pool } = require('../db');

const COLD_DAYS = 45;
const TZ = 'America/New_York';

async function coldAccounts({ userId, team = false, days = COLD_DAYS, limit = 15 }) {
  const r = await pool.query(
    `SELECT p.id, p.company, p.city, p.phone, p.user_id,
            COALESCE(NULLIF(TRIM(u.name),''), u.email) AS owner,
            lc.call_date::text AS last_visit, lc.outcome AS last_outcome, LEFT(lc.notes, 200) AS last_notes,
            lc.products_discussed AS last_lines,
            ((NOW() AT TIME ZONE '${TZ}')::date - lc.call_date) AS days_cold,
            COALESCE(al.sales, 0)::float AS sales, al.lines
       FROM prospects p
       JOIN LATERAL (SELECT call_date, outcome, notes, products_discussed FROM calls c
                      WHERE c.prospect_id = p.id ORDER BY call_date DESC, id DESC LIMIT 1) lc ON TRUE
       LEFT JOIN users u ON u.id = p.user_id
       LEFT JOIN LATERAL (SELECT SUM(x.total_sales) AS sales, STRING_AGG(l.name, ', ' ORDER BY x.total_sales DESC NULLS LAST) AS lines
                            FROM account_lines x JOIN lines l ON l.id = x.line_id WHERE x.account_id = p.id) al ON TRUE
      WHERE lc.call_date <= (NOW() AT TIME ZONE '${TZ}')::date - $1::int
        AND COALESCE(p.reconnect_dismissed, FALSE) = FALSE
        AND (p.reconnect_snoozed_until IS NULL OR p.reconnect_snoozed_until < NOW())
        AND COALESCE(p.status, '') <> 'Cold'
        ${team ? '' : 'AND p.user_id = $3'}
      ORDER BY COALESCE(al.sales, 0) DESC, lc.call_date ASC
      LIMIT $2`, team ? [days, limit] : [days, limit, userId]);
  return r.rows;
}

module.exports = { coldAccounts, COLD_DAYS };
