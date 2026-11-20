// The new Today: who to see (follow-ups, quotes, samples, accounts due for a
// touch base), built around one area and put in driving order.
// Run: node test/e2e-today.test.js
const assert = require('assert');
const { start } = require('./helpers/harness');

(async () => {
  const h = await start();
  try {
    const { pool, ids } = h;
    const kody = await h.login('kody');
    const jack = await h.login('jack');
    const day = (await pool.query(`SELECT (NOW() AT TIME ZONE 'America/New_York')::date::text d`)).rows[0].d;
    await pool.query(`UPDATE users SET home_base_lat=34.0, home_base_lng=-84.0, home_base_city='Duluth' WHERE id=$1`, [ids.kody]);
    const mk = async (uid, name, lat, lng, stage, extra) => (await pool.query(
      `INSERT INTO prospects (user_id, company, category, city, lat, lng, pipeline_stage, reconnect_dismissed) VALUES ($1,$2,'Lumber Dealer','Duluth',$3,$4,$5,$6) RETURNING id`,
      [uid, name, lat, lng, stage || 'New Lead', !!(extra && extra.dismissed)])).rows[0].id;
    const call = (uid, pid, ago, extra = {}) => pool.query(
      `INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, next_step, next_step_date) VALUES ($1,$2,$3::date - $4::int,'In-Person Visit','Visited',$5,$6)`,
      [uid, pid, day, ago, extra.next || null, extra.due || null]);

    const A = await mk(ids.kody, 'A Followup', 34.01, -84.01);
    await call(ids.kody, A, 7, { next: 'Bring Fix All pricing', due: day });
    const B = await mk(ids.kody, 'B Quote', 34.02, -84.00, 'Quoted');
    await call(ids.kody, B, 5);
    await pool.query(`INSERT INTO quotes (user_id, account_name, status, amount, follow_up_date, prospect_id) VALUES ($1,'B Quote','Sent',8000,$2,$3)`, [ids.kody, day, B]);
    const C = await mk(ids.kody, 'C Sample', 34.00, -84.03, 'Sampled');
    await call(ids.kody, C, 8);
    await pool.query(`INSERT INTO samples (user_id, prospect_id, product_line, sent_date, status, follow_up_date) VALUES ($1,$2,'ShurTape',$3::date-8,'pending',$3::date-1)`, [ids.kody, C, day]);
    const D = await mk(ids.kody, 'D Big Customer', 34.03, -84.02);
    await call(ids.kody, D, 40);
    const line = (await pool.query(`INSERT INTO lines (name, normalized_name) VALUES ('Closet Maid','closet maid') RETURNING id`)).rows[0].id;
    await pool.query(`INSERT INTO account_lines (account_id, line_id, total_sales, total_commission) VALUES ($1,$2,60000,3000)`, [D, line]);
    const imp = (await pool.query(`INSERT INTO commission_imports (rep_name, rep_id, status) VALUES ('Kody Slate',$1,'confirmed') RETURNING id`, [ids.kody])).rows[0].id;
    await pool.query(`INSERT INTO commission_lines (import_id, manufacturer, customer_raw, account_id, sales_amount, commission_amount, period_start, period_end)
                      VALUES ($1,'Closet Maid','D Big Customer',$2,60000,3000,'2026-10-01','2026-10-31')`, [imp, D]);
    const E = await mk(ids.kody, 'E Recent', 34.02, -84.02, 'Quoted');
    await call(ids.kody, E, 10);
    const F = await mk(ids.kody, 'F Far Away', 32.84, -83.63);
    await call(ids.kody, F, 9, { next: 'Call back', due: day });
    const G = await mk(ids.kody, 'G Old Prospect', 34.05, -84.05);
    await call(ids.kody, G, 100);
    const H = (await pool.query(`INSERT INTO prospects (user_id, company, category, city) VALUES ($1,'H No Address','Lumber Dealer','Duluth') RETURNING id`, [ids.kody])).rows[0].id;
    await call(ids.kody, H, 3, { next: 'Send spec sheet', due: day });
    const I = await mk(ids.kody, 'I Dismissed', 34.01, -84.0, 'New Lead', { dismissed: true });
    await call(ids.kody, I, 200);

    // ── What's due ──
    const plan = require('../lib/day-plan');
    const c = await plan.candidates(ids.kody, day);
    const by = Object.fromEntries(c.map(x => [x.account_id, x]));
    assert.strictEqual(by[A].kind, 'followup'); assert.ok(/Bring Fix All pricing/.test(by[A].reason));
    assert.strictEqual(by[B].kind, 'quote'); assert.ok(/\$8,000/.test(by[B].reason));
    assert.strictEqual(by[C].kind, 'sample'); assert.ok(/ShurTape sample/.test(by[C].reason));
    assert.strictEqual(by[D].kind, 'touch'); assert.ok(/40 days since a visit \(top customer, \$36,000\/yr\)/.test(by[D].reason), by[D].reason);
    assert.strictEqual(by[D].cadence_days, 30);
    assert.ok(!by[E], 'inside its rhythm');
    assert.strictEqual(by[G].kind, 'touch'); assert.strictEqual(by[G].cadence_days, 75);
    assert.ok(!by[I], 'dismissed accounts left alone');
    assert.ok(by[A].score > by[B].score && by[B].score > by[C].score && by[C].score > by[D].score && by[D].score > by[G].score, 'priority order');

    // ── Driving order ──
    const sq = [{ id: 1, lat: 34.1, lng: -84.0 }, { id: 2, lat: 34.0, lng: -83.9 }, { id: 3, lat: 34.1, lng: -83.9 }, { id: 4, lat: 34.2, lng: -83.95 }];
    const o = plan.orderStops({ lat: 34.0, lng: -84.0 }, sq);
    assert.ok(plan.loopMiles({ lat: 34.0, lng: -84.0 }, o.order) <= plan.loopMiles({ lat: 34.0, lng: -84.0 }, sq) + 1e-9);
    const perms = (a) => a.length < 2 ? [a] : a.flatMap((x, i) => perms(a.slice(0, i).concat(a.slice(i + 1))).map(p => [x].concat(p)));
    const best = Math.min(...perms(sq).map(p => plan.loopMiles({ lat: 34.0, lng: -84.0 }, p)));
    assert.ok(Math.abs(o.miles - Math.round(best * 10) / 10) < 0.11, `optimal loop ${best} vs ${o.miles}`);
    assert.deepStrictEqual(plan.orderStops({ lat: 34, lng: -84 }, [{ id: 9 }]).order.map(x => x.id), [9], 'no location stays');

    // ── Plan my day ──
    let r = await kody('POST', '/api/today/plan', { date: day });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const items = (await pool.query(`SELECT account_id, source, ai_reason, sort_order FROM planner_items WHERE rep_id=$1 AND planned_date=$2 ORDER BY sort_order`, [ids.kody, day])).rows;
    const planned = items.map(i => i.account_id);
    assert.deepStrictEqual([...planned].sort((a, b) => a - b), [A, B, C, D, G].sort((a, b) => a - b), 'the close-together ones, not the far one or the one with no address');
    assert.ok(items.every(i => i.source === 'ai' && i.ai_reason));
    assert.strictEqual(r.body.added, 5);
    // Order is a short loop out of home base
    const coords = await pool.query(`SELECT id, lat::float lat, lng::float lng FROM prospects WHERE id = ANY($1)`, [planned]);
    const P = Object.fromEntries(coords.rows.map(x => [x.id, x]));
    const loop = plan.loopMiles({ lat: 34, lng: -84 }, planned.map(id => P[id]));
    const bestLoop = Math.min(...perms(planned.map(id => P[id])).map(p => plan.loopMiles({ lat: 34, lng: -84 }, p)));
    assert.ok(loop <= bestLoop + 0.05, `loop ${loop} best ${bestLoop}`);
    r = await kody('POST', '/api/today/plan', { date: day });
    assert.strictEqual(r.body.added, 0, 'nothing else close needs a visit');
    r = await kody('GET', `/api/planner/today?date=${day}`);
    assert.strictEqual(r.body.stops.length, 5);
    assert.ok(/^Follow up: Bring Fix All pricing/.test(r.body.stops.find(s => s.account_id === A).reason), 'stop shows why');

    // ── Summary ──
    r = await kody('GET', `/api/today/summary?date=${day}`);
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const w = r.body.waiting;
    assert.ok(w.find(x => x.account_id === F && !x.on_route && x.kind === 'followup' && x.call_id), 'far follow-up still waiting');
    assert.ok(w.find(x => x.account_id === H && !x.on_route), 'no-address follow-up waiting');
    assert.ok(w.find(x => x.account_id === A).on_route);
    assert.ok(w.find(x => x.account_id === B).quote_id);
    assert.ok(!r.body.due.some(x => planned.includes(x.account_id)), 'due list skips today\'s route');
    assert.strictEqual(r.body.overnight.stops, 5);
    assert.strictEqual(r.body.name, 'Kody');
    assert.ok(r.body.week.visits >= 0 && r.body.week.open_quotes === 8000 && r.body.week.samples_out === 1);

    // ── Add one, reorder ──
    r = await kody('POST', '/api/today/add', { account_id: F, date: day });
    assert.strictEqual(r.status, 200);
    assert.ok(r.body.stops.length === 6 && r.body.miles > 100, 'Macon makes it a long day: ' + r.body.miles);
    r = await kody('POST', '/api/today/add', { account_id: F, date: day });
    assert.strictEqual((await pool.query(`SELECT COUNT(*)::int n FROM planner_items WHERE rep_id=$1 AND planned_date=$2`, [ids.kody, day])).rows[0].n, 6, 'no double add');
    r = await kody('POST', '/api/today/add', { account_id: 999999, date: day });
    assert.strictEqual(r.status, 404);
    r = await kody('POST', '/api/today/optimize', { date: day, lat: 32.84, lng: -83.63 });
    assert.strictEqual(r.status, 200);
    const first = (await pool.query(`SELECT account_id FROM planner_items WHERE rep_id=$1 AND planned_date=$2 ORDER BY sort_order LIMIT 1`, [ids.kody, day])).rows[0].account_id;
    assert.strictEqual(first, F, 'starting in Macon, Macon goes first');

    // ── Overnight ──
    const J1 = await mk(ids.jack, 'Jack Followup', 34.5, -83.5);
    await call(ids.jack, J1, 3, { next: 'Drop off samples', due: day });
    await pool.query(`UPDATE users SET home_base_lat=34.5, home_base_lng=-83.5 WHERE id=$1`, [ids.jack]);
    const run = await plan.planAll();
    const dow = new Date(day + 'T12:00:00Z').getUTCDay();
    if (dow === 0 || dow === 6) assert.strictEqual(run.skipped, 'weekend');
    else {
      assert.strictEqual(run.people, 1, 'Jack only (Kody already has a plan): ' + JSON.stringify(run));
      r = await jack('GET', `/api/planner/today?date=${day}`);
      assert.deepStrictEqual(r.body.stops.map(s => s.account_id), [J1]);
      assert.strictEqual((await plan.planAll()).people, 0, 'once a day');
    }
    console.log('e2e-today: all tests passed');
  } catch (e) {
    console.error(e); console.error(h.log().slice(-2000)); process.exitCode = 1;
  } finally { await h.stop(); }
})();
