// The day plan: who a rep should see today, in the order that drives the
// fewest miles. Built overnight for anyone with nothing planned, and on demand
// from Today ("Plan my day" / "Best order").
//
// Who makes the list, highest first:
//   1. Follow-ups that are due (the rep promised a date)
//   2. Quotes due for a follow-up (bigger quotes first)
//   3. Samples due for a check-in
//   4. Accounts due for a touch base, by how far past their rhythm they are:
//      paying customers on the Reconnect cadence (30/60/90 days by commission),
//      accounts in the pipeline every 21 days, everyone else visited every 75
//   5. New leads the rep chose to visit, and never-visited accounts, to fill
// The day is built around one area (the stops worth the most within a short
// drive of each other), then put in driving order: nearest stop next, then
// swaps until no swap makes the loop shorter.
const { pool } = require('../db');
const { milesBetween } = require('./geocode');

const TZ = 'America/New_York';
const ACTIVE_CADENCE = 21, PROSPECT_CADENCE = 75;
const MAX_FROM_START = 60;   // miles
const CUSTOMER_TIERS = [ { min: 25000, days: 30, label: 'top customer' }, { min: 5000, days: 60, label: 'customer' }, { min: 0.01, days: 90, label: 'customer' } ];

function cadenceFor(a) {
  if (a.run_rate > 0) { const t = CUSTOMER_TIERS.find(t => a.run_rate >= t.min); return { days: t.days, label: t.label }; }
  if (['Contacted', 'Sampled', 'Quoted'].includes(a.stage)) return { days: ACTIVE_CADENCE, label: 'in your pipeline' };
  return { days: PROSPECT_CADENCE, label: 'account' };
}
const money = n => '$' + Math.round(n).toLocaleString('en-US');
const located = a => a.lat != null && a.lng != null && Number.isFinite(Number(a.lat)) && Number.isFinite(Number(a.lng));

// Every account a rep owns, with what makes it worth a stop today.
async function candidates(repId, day) {
  const rows = (await pool.query(
    `SELECT p.id, p.company, p.city, p.address, p.phone, p.lat::float AS lat, p.lng::float AS lng,
            COALESCE(p.pipeline_stage, 'New Lead') AS stage, p.source,
            lv.last_visit::text AS last_visit, ($2::date - lv.last_visit) AS days_since,
            lc.id AS call_id, lc.next_step, lc.next_step_date::text AS next_step_date,
            q.id AS quote_id, q.amount::float AS quote_amount, q.follow_up_date::text AS quote_due, q.products AS quote_products,
            s.id AS sample_id, s.product_line AS sample_line, s.follow_up_date::text AS sample_due,
            COALESCE(cm.comm, 0)::float AS commission, COALESCE(cm.months, 0)::int AS months
       FROM prospects p
       LEFT JOIN LATERAL (SELECT MAX(call_date) AS last_visit FROM calls WHERE prospect_id = p.id AND COALESCE(call_type,'') <> 'Note') lv ON TRUE
       LEFT JOIN LATERAL (SELECT id, next_step, next_step_date FROM calls WHERE prospect_id = p.id AND user_id = $1
                           ORDER BY call_date DESC, id DESC LIMIT 1) lc ON TRUE
       LEFT JOIN LATERAL (SELECT id, amount, follow_up_date, products FROM quotes
                           WHERE prospect_id = p.id AND status NOT IN ('Won','Lost') AND follow_up_date IS NOT NULL AND follow_up_date <= $2::date
                           ORDER BY amount DESC NULLS LAST LIMIT 1) q ON TRUE
       LEFT JOIN LATERAL (SELECT id, product_line, follow_up_date FROM samples
                           WHERE prospect_id = p.id AND closed_at IS NULL AND follow_up_date <= $2::date
                             AND LOWER(COALESCE(status,'pending')) NOT IN ('converted','no_interest','no interest')
                           ORDER BY follow_up_date LIMIT 1) s ON TRUE
       LEFT JOIN LATERAL (SELECT SUM(al.total_commission) AS comm,
                                 (SELECT COUNT(DISTINCT period_start) FROM commission_lines cl WHERE cl.account_id = p.id) AS months
                            FROM account_lines al WHERE al.account_id = p.id) cm ON TRUE
      WHERE p.user_id = $1
        AND COALESCE(p.pipeline_stage, '') <> 'Closed Lost'
        AND COALESCE(p.reconnect_dismissed, FALSE) = FALSE
        AND COALESCE(p.status, '') <> 'Cold'`, [repId, day])).rows;

  const out = [];
  for (const a of rows) {
    a.run_rate = a.commission > 0 ? (a.months > 0 ? a.commission / a.months * 12 : a.commission) : 0;
    const cad = cadenceFor(a);
    let score = 0, reason = null, kind = null;
    if (a.next_step && a.next_step_date && a.next_step_date <= day) {
      score = 100; kind = 'followup';
      reason = 'Follow up: ' + String(a.next_step).slice(0, 80) + (a.next_step_date < day ? ' (was due ' + a.next_step_date.slice(5).replace('-', '/') + ')' : '');
    } else if (a.quote_id) {
      score = 90 + Math.min(10, (a.quote_amount || 0) / 2000); kind = 'quote';
      reason = 'Quote follow-up' + (a.quote_amount ? ': ' + money(a.quote_amount) : '') + (a.quote_products ? ' ' + String(a.quote_products).slice(0, 40) : '');
    } else if (a.sample_id) {
      score = 80; kind = 'sample';
      reason = 'Check on the ' + (a.sample_line ? a.sample_line + ' ' : '') + 'sample';
    } else if (a.days_since != null && a.days_since >= cad.days) {
      const over = a.days_since / cad.days;
      score = 40 + Math.min(30, over * 15) + Math.min(25, a.run_rate / 4000); kind = 'touch';
      reason = 'Touch base: ' + a.days_since + ' days since a visit' + (a.run_rate > 0 ? ' (' + cad.label + ', ' + money(a.run_rate) + '/yr)' : '');
    } else if (a.days_since == null) {
      score = a.source === 'Lead Hunt' ? 30 : 20; kind = 'new';
      reason = a.run_rate > 0 ? 'Customer (' + money(a.run_rate) + '/yr) with no visit logged yet' : 'Never visited';
      if (a.run_rate > 0) score = 60 + Math.min(25, a.run_rate / 4000);
    }
    if (!kind) continue;
    out.push({ account_id: a.id, company: a.company, city: a.city, address: a.address, lat: a.lat, lng: a.lng,
      score: Math.round(score * 10) / 10, kind, reason, days_since: a.days_since, cadence_days: cad.days,
      run_rate: Math.round(a.run_rate), call_id: a.call_id, quote_id: a.quote_id, quote_amount: a.quote_amount, sample_id: a.sample_id });
  }
  out.sort((x, y) => y.score - x.score);
  return out;
}

// Where the rep starts the day: their phone's location if given, else where
// they work (lib/work-area: their Territory first).
async function startPoint(repId, gps) {
  if (gps && Number.isFinite(gps.lat) && Number.isFinite(gps.lng)) return { lat: gps.lat, lng: gps.lng, label: 'your location' };
  return require('./work-area').workCenter(repId);
}

// Driving order for a loop out of start and back: nearest next, then 2-opt.
function orderStops(start, stops) {
  const pts = stops.filter(located);
  const rest = stops.filter(s => !located(s));
  if (pts.length < 2 || !start) return { order: pts.concat(rest), miles: start ? loopMiles(start, pts) : null };
  const left = pts.slice(), route = [];
  let cur = start;
  while (left.length) {
    let bi = 0, bd = Infinity;
    left.forEach((p, i) => { const d = milesBetween(cur, p); if (d < bd) { bd = d; bi = i; } });
    cur = left.splice(bi, 1)[0]; route.push(cur);
  }
  let improved = true, guard = 0;
  while (improved && guard++ < 50) {
    improved = false;
    for (let i = 0; i < route.length - 1; i++) {
      for (let k = i + 1; k < route.length; k++) {
        const cand = route.slice(0, i).concat(route.slice(i, k + 1).reverse(), route.slice(k + 1));
        if (loopMiles(start, cand) + 1e-9 < loopMiles(start, route)) { route.splice(0, route.length, ...cand); improved = true; }
      }
    }
  }
  return { order: route.concat(rest), miles: Math.round(loopMiles(start, route) * 10) / 10 };
}
function loopMiles(start, route) {
  if (!route.length) return 0;
  let m = milesBetween(start, route[0]);
  for (let i = 1; i < route.length; i++) m += milesBetween(route[i - 1], route[i]);
  return m + milesBetween(route[route.length - 1], start);
}

// Pick the day's area: the seed (one of the top stops) whose neighbors within
// `radius` add up to the most value, then the best stops in that area.
function pickDay(cands, goal, radius) {
  const loc = cands.filter(located);
  if (!loc.length) return [];
  let best = null;
  for (const seed of loc.slice(0, 8)) {
    const near = loc.filter(c => milesBetween(seed, c) <= radius).sort((a, b) => b.score - a.score).slice(0, goal);
    const value = near.reduce((t, c) => t + c.score, 0);
    if (!best || value > best.value) best = { value, near };
  }
  return best.near;
}

async function itemsFor(repId, day) {
  return (await pool.query(
    `SELECT pi.id, pi.account_id, pi.completed_at, pi.sort_order, p.lat::float lat, p.lng::float lng, p.company
       FROM planner_items pi LEFT JOIN prospects p ON p.id = pi.account_id
      WHERE pi.rep_id=$1 AND pi.planned_date=$2 AND pi.item_type='stop' ORDER BY pi.sort_order, pi.id`, [repId, day])).rows;
}

// Build (or top up) a day. Existing stops stay; new ones fill up to `goal`
// near them (or near the best area when the day is empty). Then the whole day
// is put in driving order. → { added, stops, miles, start }
async function planDay(repId, day, opts = {}) {
  const goal = Math.min(Math.max(parseInt(opts.goal) || 7, 1), 15);
  const radius = opts.radius || 20;
  const start = await startPoint(repId, opts.gps);
  const existing = await itemsFor(repId, day);
  const have = new Set(existing.map(i => i.account_id).filter(Boolean));
  // Not if it's on another day this week already.
  const weekPlanned = new Set((await pool.query(
    `SELECT account_id FROM planner_items WHERE rep_id=$1 AND account_id IS NOT NULL AND planned_date <> $2
        AND planned_date BETWEEN $2::date - 3 AND $2::date + 6 AND completed_at IS NULL`, [repId, day])).rows.map(r => r.account_id));
  // Only accounts within a day's drive of where the rep starts, so an account
  // mapped to the wrong place can't pull the day somewhere else.
  const cands = (await candidates(repId, day)).filter(c => !have.has(c.account_id) && !weekPlanned.has(c.account_id)
    && (!start || !located(c) || milesBetween(start, c) <= MAX_FROM_START));
  let picked = [];
  const room = goal - existing.length;
  if (room > 0) {
    const anchors = existing.filter(located);
    if (anchors.length) {
      picked = cands.filter(located).map(c => Object.assign({ near: Math.min.apply(null, anchors.map(a => milesBetween(a, c))) }, c))
        .filter(c => c.near <= radius).sort((a, b) => b.score - a.score).slice(0, room);
    } else {
      picked = pickDay(cands, room, radius);
    }
  }
  let n = existing.length ? Math.max.apply(null, existing.map(e => e.sort_order || 0)) + 1 : 0;
  for (const c of picked) {
    await pool.query(`INSERT INTO planner_items (rep_id, planned_date, item_type, account_id, title, sort_order, source, ai_reason)
                      VALUES ($1,$2,'stop',$3,$4,$5,'ai',$6)`, [repId, day, c.account_id, c.company, n++, c.reason]);
  }
  const order = await optimizeDay(repId, day, start);
  return { added: picked.length, start, miles: order.miles, stops: order.stops };
}

// Put a day's open stops in driving order (done stops stay first, as they were).
async function optimizeDay(repId, day, start) {
  start = start || await startPoint(repId);
  const items = await itemsFor(repId, day);
  const done = items.filter(i => i.completed_at), open = items.filter(i => !i.completed_at);
  const from = done.length && located(done[done.length - 1]) ? done[done.length - 1] : start;
  const { order, miles } = orderStops(from, open);
  const all = done.concat(order);
  for (let i = 0; i < all.length; i++) {
    if (all[i].sort_order !== i) await pool.query('UPDATE planner_items SET sort_order=$2 WHERE id=$1', [all[i].id, i]);
  }
  return { stops: all.map(s => s.id), miles };
}

async function today() {
  return (await pool.query(`SELECT (NOW() AT TIME ZONE '${TZ}')::date::text d`)).rows[0].d;
}

// Overnight: plan today for every active rep who has nothing planned yet.
async function planAll() {
  const day = await today();
  const dow = new Date(day + 'T12:00:00Z').getUTCDay();
  if (dow === 0 || dow === 6) return { skipped: 'weekend' };
  const users = (await pool.query(
    `SELECT u.id FROM users u WHERE u.disabled_at IS NULL AND u.role IN ('rep','manager')
        AND EXISTS (SELECT 1 FROM calls c WHERE c.user_id = u.id AND c.call_date > CURRENT_DATE - 21)
        AND NOT EXISTS (SELECT 1 FROM planner_items pi WHERE pi.rep_id = u.id AND pi.planned_date = $1)
        AND NOT EXISTS (SELECT 1 FROM day_plan_runs r WHERE r.user_id = u.id AND r.run_day = $1)`, [day])).rows;
  let planned = 0;
  for (const u of users) {
    try {
      const r = await planDay(u.id, day);
      await pool.query(`INSERT INTO day_plan_runs (user_id, run_day, added, miles) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, [u.id, day, r.added, r.miles]);
      planned += r.added;
    } catch (e) { console.error('[day-plan]', u.id, e.message); }
  }
  if (users.length) console.log(`[day-plan] ${day}: ${planned} stops for ${users.length} people`);
  return { day, people: users.length, stops: planned };
}

module.exports = { candidates, planDay, optimizeDay, orderStops, pickDay, cadenceFor, startPoint, planAll, loopMiles };
