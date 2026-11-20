// Field notes on accounts and the team-wide visit-notes search.
// Run: node test/e2e-notes.test.js
const assert = require('assert');
const { start } = require('./helpers/harness');

(async () => {
  const h = await start();
  try {
    const { pool, ids } = h;
    const kody = await h.login('kody');
    const carter = (await pool.query(`INSERT INTO prospects (user_id, company, category, city) VALUES ($1,'Carter Lumber','Lumber Dealer','Oakwood') RETURNING id`, [ids.jack])).rows[0].id;
    const avi = (await pool.query(`INSERT INTO prospects (user_id, company, category, city) VALUES ($1,'AVI Windows','Window Dealer','Atlanta') RETURNING id`, [ids.dan])).rows[0].id;
    await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, notes) VALUES
      ($1,$2,'2026-11-10','Visit','Visited','They carry MoistureShield tape, open to ShurTape'),
      ($3,$4,'2026-11-12','Visit','Visited','moistureshield on the shelf next to the door flashing'),
      ($1,$2,'2026-11-01','Visit','Visited','Nothing about tape here')`, [ids.jack, carter, ids.dan, avi]);

    // Kody can read and write field notes on a teammate's account
    let r = await kody('PUT', `/api/prospects/${carter}/field-notes`, { best_time: 'Buyer in Tuesdays at 10', specialty: 'Oakwood does trusses; Winder does decking' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    r = await kody('GET', `/api/prospects/${carter}/field-notes`);
    assert.deepStrictEqual(r.body, { best_time: 'Buyer in Tuesdays at 10', specialty: 'Oakwood does trusses; Winder does decking' });
    r = await kody('PUT', `/api/prospects/999999/field-notes`, { best_time: 'x' });
    assert.strictEqual(r.status, 404);

    // Search finds both accounts, any capitalization, across reps
    r = await kody('GET', '/api/calls/search?q=MoistureShield');
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.results.length, 2);
    assert.strictEqual(r.body.accounts, 2);
    assert.deepStrictEqual(r.body.results.map(x => x.company), ['AVI Windows', 'Carter Lumber'], 'newest first');
    // Wildcards in the search box are literal
    r = await kody('GET', '/api/calls/search?q=%25');
    assert.strictEqual(r.body.results.length, 0);
    r = await kody('GET', '/api/calls/search?q=a');
    assert.strictEqual(r.body.results.length, 0, 'too short');

    console.log('e2e-notes: all tests passed');
  } catch (e) {
    console.error(e); console.error(h.log().slice(-1500)); process.exitCode = 1;
  } finally { await h.stop(); }
})();
