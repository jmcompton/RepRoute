// Pipeline moves on its own; quotes link to accounts by id; one-tap Won/Lost.
// Run: node test/e2e-pipeline.test.js
const assert = require('assert');
const { start } = require('./helpers/harness');
const pipeline = require('../lib/pipeline');

(async () => {
  const h = await start();
  try {
    const { pool, ids } = h;
    const kody = await h.login('kody');
    const jack = await h.login('jack');
    const stage = async id => (await pool.query('SELECT pipeline_stage FROM prospects WHERE id=$1', [id])).rows[0].pipeline_stage;

    let r = await kody('POST', '/api/prospects', { company: 'Carter Lumber', category: 'Lumber Dealer', city: 'Winder', state: 'GA' });
    const acct = r.body.id;
    assert.strictEqual(await stage(acct), 'New Lead');

    await kody('POST', '/api/calls', { prospect_id: acct, call_date: '2026-11-20', call_type: 'In-Person Visit', outcome: 'Dropped In - Interested' });
    assert.strictEqual(await stage(acct), 'Contacted', 'visit → Contacted');

    r = await kody('POST', '/api/samples', { prospect_id: acct, product_line: 'ShurTape' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.strictEqual(await stage(acct), 'Sampled', 'sample → Sampled');

    // Another visit must not move it back
    await kody('POST', '/api/calls', { prospect_id: acct, call_date: '2026-11-21', call_type: 'Phone Call', outcome: 'Phone - Interested' });
    assert.strictEqual(await stage(acct), 'Sampled', 'never moves backward');

    // A quote (different spelling) links to the account by id and moves it to Quoted
    r = await kody('POST', '/api/quotes', { account_name: 'Carter Lumber Co.', status: 'Sent', amount: '6400', quote_date: '2026-11-20',
      follow_up_enabled: true, follow_up_date: '2026-11-25' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const qid = r.body.quote.id;
    assert.strictEqual(r.body.quote.prospect_id, acct, 'quote linked by id');
    assert.strictEqual(await stage(acct), 'Quoted');

    // Jack can't close Kody's quote
    r = await jack('POST', `/api/quotes/${qid}/status`, { status: 'Won' });
    assert.strictEqual(r.status, 403);

    // Kody marks it Won in one tap: Closed Won, follow-up cleared
    r = await kody('POST', `/api/quotes/${qid}/status`, { status: 'Won' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.strictEqual(r.body.quote.status, 'Won');
    assert.strictEqual(r.body.quote.follow_up_date, null);
    assert.strictEqual(await stage(acct), 'Closed Won');

    // Bad status is refused
    r = await kody('POST', `/api/quotes/${qid}/status`, { status: 'Maybe' });
    assert.strictEqual(r.status, 400);

    // Catch-up job: history already in the database moves stages forward only
    const old = (await pool.query(`INSERT INTO prospects (user_id, company, category, pipeline_stage) VALUES ($1,'Old Account','Account','New Lead') RETURNING id`, [ids.jack])).rows[0].id;
    const manual = (await pool.query(`INSERT INTO prospects (user_id, company, category, pipeline_stage) VALUES ($1,'Hand Moved','Account','Quoted') RETURNING id`, [ids.jack])).rows[0].id;
    await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome) VALUES ($1,$2,'2026-10-01','Visit','Visited'),($1,$3,'2026-10-01','Visit','Visited')`, [ids.jack, old, manual]);
    const moved = await pipeline.backfill(pool);
    assert.ok(moved.Contacted >= 1);
    assert.strictEqual(await stage(old), 'Contacted');
    assert.strictEqual(await stage(manual), 'Quoted', 'catch-up never moves a card backward');

    // Tap-to-move still works by hand
    r = await kody('PUT', `/api/prospects/${acct}`, { pipeline_stage: 'Contacted' });
    assert.ok(r.status === 200, JSON.stringify(r.body));
    assert.strictEqual(await stage(acct), 'Contacted');

    console.log('e2e-pipeline: all tests passed');
  } catch (e) {
    console.error(e); console.error(h.log().slice(-1500)); process.exitCode = 1;
  } finally { await h.stop(); }
})();
