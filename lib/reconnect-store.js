'use strict';

// ── Reconnect: lapsed-account re-engagement, value-ranked ────────────────────
// Surfaces accounts going quiet, ranked by money at risk. Read-side is fully
// derived from existing tables (no new history table): commission value comes
// from account_lines; "last activity" is the MAX across every real RepRoute
// touch we can attribute to the account — the latest commission period, logged
// calls, completed planner stops/visits, samples sent, name-matched quotes, and
// the generic prospects.last_activity_at marker (which "Log touch" stamps).
//
// SMART CADENCE: each account is tiered by its annualized commission run-rate
// (trailing commission ÷ months loaded × 12) and flagged only once it has been
// quiet past that tier's threshold. All knobs live in RECONNECT_CONFIG so the
// thresholds, run-rate cutoffs, and snooze length are trivial to tune later.

// One config object — easy to tweak.
const RECONNECT_CONFIG = {
  // Tiers are evaluated top-down; the first whose run-rate floor is met wins.
  tiers: [
    { key: 'high', label: 'High value',   minRunRate: 25000, quietDays: 30, color: '#b91c1c' },
    { key: 'mid',  label: 'Mid value',    minRunRate: 5000,  quietDays: 60, color: '#c2410c' },
    { key: 'low',  label: 'Low value',    minRunRate: 0,     quietDays: 90, color: '#a16207' },
  ],
  snoozeDays: 14,        // Snooze hides an account this many days, then it returns.
  staleAfterDays: 90,    // (reference) the longest cadence — used for lead defaults.
};

// Pick the cadence tier for an annualized run-rate.
function tierFor(runRate) {
  const tiers = RECONNECT_CONFIG.tiers;
  for (const t of tiers) { if (runRate >= t.minRunRate) return t; }
  return tiers[tiers.length - 1];
}

// getReconnect(pool, { uid, scope, repId, filter }) → { config, summary, accounts }
//   uid    — caller's user id (a rep is locked to this)
//   scope  — 'manager' sees firm-wide (optional repId filter); anyone else is a rep
//   repId  — manager-only per-rep filter (null/undefined = all reps)
//   filter — 'customers' (has commission history, default) | 'leads' | 'all'
async function getReconnect(pool, { uid, scope, repId, filter } = {}) {
  const isMgr = scope === 'manager';
  const seg = (filter === 'leads' || filter === 'all') ? filter : 'customers';

  // Rep scoping: a rep is always pinned to their own user_id; a manager may pin a
  // chosen rep, else see everyone.
  const params = [];
  let userFilter = '';
  if (!isMgr) {
    params.push(uid); userFilter = `AND p.user_id = $${params.length}`;
  } else if (repId != null && Number.isFinite(parseInt(repId))) {
    params.push(parseInt(repId)); userFilter = `AND p.user_id = $${params.length}`;
  }

  // One row per prospect with its commission rollup + last-activity inputs.
  // last_activity = greatest of all attributable touch dates.
  const sql = `
    WITH al AS (
      SELECT account_id,
             SUM(total_commission)      AS trailing_commission,
             MIN(first_period)          AS first_period,
             MAX(last_period)           AS last_period
        FROM account_lines
       WHERE account_id IS NOT NULL
       GROUP BY account_id
    ),
    al_lines AS (
      SELECT a.account_id, string_agg(l.name, ', ' ORDER BY a.total_commission DESC) AS lines
        FROM account_lines a JOIN lines l ON l.id = a.line_id
       WHERE a.account_id IS NOT NULL
       GROUP BY a.account_id
    ),
    last_call AS (
      SELECT prospect_id, MAX(call_date) AS d FROM calls
       WHERE prospect_id IS NOT NULL GROUP BY prospect_id
    ),
    last_stop AS (
      SELECT account_id, MAX(completed_at) AS d FROM planner_items
       WHERE account_id IS NOT NULL AND completed_at IS NOT NULL GROUP BY account_id
    ),
    last_sample AS (
      SELECT prospect_id, MAX(sent_date) AS d FROM samples
       WHERE prospect_id IS NOT NULL GROUP BY prospect_id
    )
    SELECT
      p.id, p.company, p.category, p.city, p.state, p.phone, p.mobile,
      p.address, p.user_id,
      u.name AS rep_name,
      COALESCE(al.trailing_commission, 0)        AS trailing_commission,
      al.first_period, al.last_period,
      all_l.lines                                AS lines,
      (al.account_id IS NOT NULL)                AS has_commission,
      GREATEST(
        COALESCE(al.last_period,            'epoch'::timestamptz),
        COALESCE(lc.d::timestamptz,         'epoch'::timestamptz),
        COALESCE(ls.d,                      'epoch'::timestamptz),
        COALESCE(lsmp.d::timestamptz,       'epoch'::timestamptz),
        COALESCE(lq.d::timestamptz,         'epoch'::timestamptz),
        COALESCE(p.last_activity_at,        'epoch'::timestamptz),
        COALESCE(p.created_at,              'epoch'::timestamptz)
      ) AS last_activity
      FROM prospects p
      LEFT JOIN users u            ON u.id = p.user_id
      LEFT JOIN al                 ON al.account_id = p.id
      LEFT JOIN al_lines all_l     ON all_l.account_id = p.id
      LEFT JOIN last_call lc       ON lc.prospect_id = p.id
      LEFT JOIN last_stop ls       ON ls.account_id = p.id
      LEFT JOIN last_sample lsmp   ON lsmp.prospect_id = p.id
      LEFT JOIN LATERAL (
        SELECT MAX(COALESCE(q.quote_date, q.created_at::date)) AS d
          FROM quotes q
         WHERE q.user_id = p.user_id
           AND LOWER(q.account_name) = LOWER(p.company)
      ) lq ON TRUE
     WHERE COALESCE(p.reconnect_dismissed, FALSE) = FALSE
       AND (p.reconnect_snoozed_until IS NULL OR p.reconnect_snoozed_until <= NOW())
       ${userFilter}
  `;

  const { rows } = await pool.query(sql, params);
  const now = Date.now();

  const flagged = [];
  for (const r of rows) {
    const hasComm = !!r.has_commission;
    if (seg === 'customers' && !hasComm) continue;
    if (seg === 'leads' && hasComm) continue;

    const trailing = Number(r.trailing_commission) || 0;
    const months = monthsBetween(r.first_period, r.last_period);   // ≥1 when commission exists, else 0
    const runRate = months > 0 ? (trailing / months) * 12 : 0;
    const tier = tierFor(runRate);

    const last = r.last_activity ? new Date(r.last_activity).getTime() : 0;
    const daysQuiet = last > 0 ? Math.floor((now - last) / 86400000) : null;

    // Flag only if quiet past this tier's threshold. Leads (no commission) fall to
    // the lowest tier (longest cadence) by construction.
    if (daysQuiet == null || daysQuiet < tier.quietDays) continue;

    flagged.push({
      id: r.id,
      company: r.company,
      category: r.category,
      city: r.city,
      state: r.state,
      phone: r.phone || r.mobile || null,
      address: r.address || null,
      rep_id: r.user_id,
      rep_name: r.rep_name || null,
      trailing_commission: trailing,
      months_loaded: months,
      run_rate: Math.round(runRate),
      lines: r.lines || null,
      has_commission: hasComm,
      last_activity: r.last_activity,
      days_quiet: daysQuiet,
      tier: tier.key,
      tier_label: tier.label,
      tier_color: tier.color,
      tier_threshold: tier.quietDays,
    });
  }

  // Biggest money first.
  flagged.sort((a, b) => b.trailing_commission - a.trailing_commission ||
                         (b.days_quiet || 0) - (a.days_quiet || 0));

  const totalAtRisk = flagged.reduce((s, a) => s + a.trailing_commission, 0);
  return {
    config: RECONNECT_CONFIG,
    summary: { count: flagged.length, total_at_risk: totalAtRisk, filter: seg },
    accounts: flagged,
  };
}

// Inclusive month span between two period dates (month-end dates from account_lines).
// Returns 0 when no commission window exists.
function monthsBetween(first, last) {
  if (!first || !last) return 0;
  const a = new Date(first), b = new Date(last);
  const m = (b.getFullYear() - a.getFullYear()) * 12 + (b.getMonth() - a.getMonth()) + 1;
  return m > 0 ? m : 1;
}

module.exports = { RECONNECT_CONFIG, tierFor, monthsBetween, getReconnect };
