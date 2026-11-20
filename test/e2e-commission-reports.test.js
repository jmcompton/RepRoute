// Keith's commission reports: 80/20, paying accounts nobody visits, by line, trend.
// Run: node test/e2e-commission-reports.test.js
const assert = require('assert');
const { start } = require('./helpers/harness');

(async () => {
  const h = await start();
  try {
    const { pool, ids } = h;
    const dan = await h.login('dan');
    const kody = await h.login('kody');
    const sean = await h.login('sean');
    const acct = async (uid, name) => (await pool.query(`INSERT INTO prospects (user_id, company, category, city) VALUES ($1,$2,'Lumber Dealer','Birmingham') RETURNING id`, [uid, name])).rows[0].id;
    const line = async n => (await pool.query(`INSERT INTO lines (name, normalized_name) VALUES ($1,$2) RETURNING id`, [n, n.toLowerCase()])).rows[0].id;
    const cm = await line('Closet Maid'), vm = await line('Virginia Mirror');
    const elite = await acct(ids.dan, 'Elite Shower & Closets');
    const haley = await acct(ids.dan, 'Haleys Flooring');
    const mcs = await acct(ids.dan, 'MCS Glass');
    const small = await acct(ids.dan, 'Small Shop');
    const kAcct = await acct(ids.kody, 'Kody Customer');
    const imp = async (rep, status) => (await pool.query(`INSERT INTO commission_imports (rep_name, rep_id, status) VALUES ('x',$1,$2) RETURNING id`, [rep, status])).rows[0].id;
    const dImp = await imp(ids.dan, 'confirmed'), kImp = await imp(ids.kody, 'confirmed'), pending = await imp(ids.dan, 'pending_review');
    const row = (i, mfr, lid, a, sales, comm, month, adj) => pool.query(
      `INSERT INTO commission_lines (import_id, manufacturer, line_id, customer_raw, account_id, sales_amount, commission_amount, period_start, period_end, is_adjustment)
       VALUES ($1,$2,$3,'raw',$4,$5,$6,$7::date,($7::date + interval '1 month - 1 day')::date,$8)`, [i, mfr, lid, a, sales, comm, month, !!adj]);
    // Dan: Elite 6000 + Haley 2500 + MCS 1000 + Small 500 = 10,000 commission
    await row(dImp, 'Closet Maid', cm, elite, 60000, 3000, '2026-08-01');
    await row(dImp, 'Closet Maid', cm, elite, 60000, 3000, '2026-09-01');
    await row(dImp, 'Closet Maid', cm, haley, 50000, 2500, '2026-09-01');
    await row(dImp, 'Virginia Mirror', vm, mcs, 20000, 1000, '2026-10-01');
    await row(dImp, 'Virginia Mirror', vm, small, 10000, 500, '2026-10-01');
    await row(dImp, 'Virginia Mirror', vm, small, -2000, -100, '2026-10-01', true);   // chargeback
    await row(pending, 'Closet Maid', cm, mcs, 999999, 99999, '2026-10-01');          // not confirmed
    await row(dImp, 'Closet Maid', cm, small, 1000, 50, '2024-01-01');                  // outside 12 months
    await row(kImp, 'Closet Maid', cm, kAcct, 30000, 1500, '2026-10-01');
    // Visits: Elite visited 10 days ago, Haley 90 days ago, MCS never, Small 5 days ago
    const visit = (uid, a, ago) => pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome) VALUES ($1,$2,(NOW() AT TIME ZONE 'America/New_York')::date - $3::int,'Visit','Visited')`, [uid, a, ago]);
    await visit(ids.dan, elite, 10); await visit(ids.dan, haley, 90); await visit(ids.dan, small, 5);

    let r = await dan('GET', '/api/commission-reports');
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.strictEqual(r.body.rep, ids.dan, 'a rep sees their own');
    assert.strictEqual(r.body.range.commission, 9900, 'confirmed only, chargeback counted, old months out');
    assert.deepStrictEqual(r.body.accounts.map(a => a.company), ['Elite Shower & Closets', 'Haleys Flooring', 'MCS Glass', 'Small Shop']);
    const [e, hl, m, s] = r.body.accounts;
    assert.strictEqual(e.commission, 6000); assert.strictEqual(s.commission, 500, 'chargeback left out of the ranking');
    assert.ok(e.a_account && hl.a_account && !m.a_account && !s.a_account, '6000 + 2500 makes the first 80% of 10,000');
    assert.strictEqual(r.body.pareto.a_accounts, 2); assert.strictEqual(r.body.pareto.a_share, 0.85);
    assert.strictEqual(e.lines, 'Closet Maid');
    assert.ok(!e.at_risk && hl.at_risk && m.at_risk && !s.at_risk);
    assert.deepStrictEqual(r.body.at_risk.accounts.map(a => a.company), ['Haleys Flooring', 'MCS Glass'], 'biggest first');
    assert.strictEqual(r.body.at_risk.commission, 3500);
    assert.deepStrictEqual(r.body.by_month.map(x => [x.m, x.commission]), [['2026-08', 3000], ['2026-09', 5500], ['2026-10', 1400]]);
    assert.deepStrictEqual(r.body.by_line.map(l => [l.line, l.commission]), [['Closet Maid', 8500], ['Virginia Mirror', 1400]]);
    assert.deepStrictEqual(r.body.by_line[0].months, { '2026-08': 3000, '2026-09': 5500 });
    r = await dan('GET', '/api/commission-reports?months=1');
    assert.strictEqual(r.body.range.commission, 1400, 'last month only');
    r = await dan('GET', `/api/commission-reports?rep=${ids.kody}`);
    assert.strictEqual(r.body.rep, ids.dan, 'a rep cannot look at another rep');

    r = await kody('GET', '/api/commission-reports');
    assert.deepStrictEqual(r.body.accounts.map(a => a.company), ['Kody Customer']);

    r = await sean('GET', '/api/commission-reports');
    assert.strictEqual(r.body.rep, null, 'manager sees the company');
    assert.strictEqual(r.body.range.commission, 11400);
    r = await sean('GET', `/api/commission-reports?rep=${ids.kody}`);
    assert.strictEqual(r.body.range.commission, 1500);
    r = await sean('GET', '/api/commission-reports/reps');
    assert.deepStrictEqual(r.body.reps.map(x => x.name), ['Daniel Compton', 'Kody Slate']);
    r = await dan('GET', '/api/commission-reports/reps');
    assert.deepStrictEqual(r.body.reps, []);

    console.log('e2e-commission-reports: all tests passed');
  } catch (e) {
    console.error(e); console.error(h.log().slice(-2000)); process.exitCode = 1;
  } finally { await h.stop(); }
})();
