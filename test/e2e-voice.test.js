// Voice logger end to end against a real server and database.
// Run: node test/e2e-voice.test.js   (needs local Postgres, see test/helpers/harness.js)
const assert = require('assert');
const { start } = require('./helpers/harness');

(async () => {
  const h = await start();
  try {
    const { pool, ids } = h;
    // Jack owns Adams Building Supplies in Toccoa (with GPS); Kody owns nothing yet.
    const adams = (await pool.query(
      `INSERT INTO prospects (user_id, company, category, city, state, lat, lng, source) VALUES ($1,'Adams Building Supplies','Lumber Dealer','Toccoa','GA',34.5773,-83.3324,'Manual') RETURNING id`,
      [ids.jack])).rows[0].id;
    // A same-name branch far away (Atlanta) owned by Dan.
    await pool.query(`INSERT INTO prospects (user_id, company, category, city, state, lat, lng) VALUES ($1,'Adams Building Supplies','Lumber Dealer','Atlanta','GA',33.749,-84.388)`, [ids.dan]);
    await pool.query(`INSERT INTO contacts (prospect_id, name, created_by) VALUES ($1,'Mike Adams',$2)`, [adams, ids.jack]);

    const kody = await h.login('kody');

    // 1. Too short: never processed as a visit
    let r = await kody('POST', '/api/voice/process', { transcript: 'Using it' });
    assert.strictEqual(r.body.too_short, true, 'two words must be too_short');

    // 2. Normal note: fields, city split off, follow-up date
    h.stub.reply = JSON.stringify({ insufficient: false, account_name: "Adam's building supply", account_city: 'Toccoa',
      contact_name: 'Mike', summary: 'Mike wants ShurTape pricing on 20 cases.', outcome: 'Dropped In - Interested',
      next_steps: 'Come back early December', next_step_date: null, products: 'ShurTape' });
    r = await kody('POST', '/api/voice/process', { transcript: "Just left Adam's building supply in Toccoa, Mike wants ShurTape pricing, come back early December" });
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.too_short, false);
    assert.strictEqual(r.body.account_name, "Adam's building supply");
    assert.ok(/^\d{4}-12-05$/.test(r.body.next_step_date), 'early December → the 5th, got ' + r.body.next_step_date);

    // 3. Matching finds JACK's account for Kody (team-wide), the Toccoa branch first when Kody is in Toccoa
    r = await kody('GET', "/api/voice/match-account?name=" + encodeURIComponent("Adam's building supply") + '&lat=34.578&lng=-83.33&city=Toccoa');
    assert.ok(r.body.matches.length >= 1, 'should find teammate account');
    assert.strictEqual(r.body.matches[0].id, adams, 'Toccoa branch should rank first');
    assert.strictEqual(r.body.matches[0].is_mine, false);
    assert.strictEqual(r.body.matches[0].owner_name, 'Jack Greene');
    assert.ok(r.body.best_match && r.body.best_match.id === adams);

    // 4. Exact name in Atlanta while standing in Toccoa is not "exact" (another branch)
    r = await kody('GET', '/api/voice/match-account?name=Adams%20Building%20Supplies&lat=34.578&lng=-83.33');
    assert.ok(r.body.exact && r.body.exact.id === adams, 'exact hit should be the nearby branch only');

    // 5. Exact name, no location: two branches → no single exact (rep must pick)
    r = await kody('GET', '/api/voice/match-account?name=Adams%20Building%20Supplies');
    assert.strictEqual(r.body.exact, null, 'two same-name branches without location must not auto-pick');

    // 6. Adding a contact at a teammate's account goes on that account, no new account row
    const before = (await pool.query('SELECT COUNT(*)::int n FROM prospects')).rows[0].n;
    r = await kody('POST', `/api/prospects/${adams}/contacts`, { name: 'Debbie', title: 'buyer' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const after = (await pool.query('SELECT COUNT(*)::int n FROM prospects')).rows[0].n;
    assert.strictEqual(after, before, 'no duplicate account created');

    // 7. Log the visit with a follow-up date; it shows on Kody's calendar and feed
    r = await kody('POST', '/api/calls', { prospect_id: adams, call_date: '2026-11-20', call_type: 'In-Person Visit',
      outcome: 'Dropped In - Interested', next_step: 'Come back early December', next_step_date: '2026-12-05', notes: 'test' });
    assert.strictEqual(r.status, 200);
    r = await kody('GET', '/api/calendar/events');
    const fu = r.body.find(e => e.event_type === 'followup');
    assert.ok(fu && fu.start_time === '2026-12-05' && /Adams Building Supplies/.test(fu.title), 'follow-up on calendar');

    // 8. A new account logged in the field keeps the GPS point
    r = await kody('POST', '/api/prospects', { company: 'Mountain Glass Co', category: 'Unknown', city: 'Clayton', state: 'GA', source: 'Voice Logger', lat: 34.878, lng: -83.401 });
    const g = (await pool.query('SELECT lat, lng FROM prospects WHERE id=$1', [r.body.id])).rows[0];
    assert.ok(Math.abs(g.lat - 34.878) < 1e-6 && Math.abs(g.lng + 83.401) < 1e-6, 'GPS saved');

    console.log('e2e-voice: all tests passed');
  } catch (e) {
    console.error(e); console.error(h.log().slice(-1500)); process.exitCode = 1;
  } finally { await h.stop(); }
})();
