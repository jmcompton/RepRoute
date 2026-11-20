// One going-cold rule across the bell, the brief and the win-back agent.
// Run: node test/e2e-cold.test.js
const assert = require('assert');
const { start } = require('./helpers/harness');

(async () => {
  const h = await start();
  try {
    const { pool, ids } = h;
    const kody = await h.login('kody');
    const mk = async (name) => (await pool.query(`INSERT INTO prospects (user_id, company, category) VALUES ($1,$2,'Account') RETURNING id`, [ids.kody, name])).rows[0].id;
    const cold = await mk('Decks And More');       // visited 50 days ago → cold
    const warm = await mk('Fresh Account');        // visited 10 days ago → not cold
    const never = await mk('Never Visited');       // never visited → not cold (it's a lead)
    await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome) VALUES
      ($1,$2,CURRENT_DATE - 50,'Visit','Visited'), ($1,$3,CURRENT_DATE - 10,'Visit','Visited')`, [ids.jack, cold, warm]);
    // An old monthly notice that used to repeat
    await pool.query(`INSERT INTO notifications (user_id, prospect_id, type, urgency, title, unique_key) VALUES ($1,$2,'long_silent','fyi','old','long_silent_x_2026-10')`, [ids.kody, cold]);

    await kody('POST', '/api/notifications/evaluate');
    await kody('POST', '/api/notifications/evaluate'); // twice: no duplicates
    let r = await kody('GET', '/api/notifications');
    const coldNotes = r.body.notifications.filter(n => n.type === 'going_cold');
    assert.strictEqual(coldNotes.length, 1, 'exactly one going-cold notice: ' + JSON.stringify(r.body.notifications.map(n => n.title)));
    assert.strictEqual(coldNotes[0].prospect_id, cold);
    assert.ok(!r.body.notifications.some(n => n.type === 'long_silent'), 'old repeating notices cleared');
    assert.ok(!r.body.notifications.some(n => n.prospect_id === never || n.prospect_id === warm));

    r = await kody('GET', '/api/agents/winback');
    assert.deepStrictEqual(r.body.accounts.map(a => a.id), [cold], 'win-back uses the same list');
    assert.strictEqual(r.body.days, 45);

    r = await kody('GET', '/api/agents/brief?refresh=1');
    assert.ok((r.body.items || []).some(i => i.kind === 'cold' && i.account_id === cold), 'brief uses the same list: ' + JSON.stringify(r.body));

    // A visit ends it: the notice clears on the next check
    await kody('POST', '/api/calls', { prospect_id: cold, call_date: new Date().toISOString().slice(0, 10), call_type: 'In-Person Visit', outcome: 'Dropped In - Interested' });
    await kody('POST', '/api/notifications/evaluate');
    r = await kody('GET', '/api/notifications');
    assert.ok(!r.body.notifications.some(n => n.type === 'going_cold'), 'cleared after a visit');

    // A closed quote's follow-up notice clears too
    const q = (await pool.query(`INSERT INTO quotes (user_id, rep_id, account_name, status, follow_up_date) VALUES ($1,$1,'Decks And More','Sent',CURRENT_DATE - 1) RETURNING id`, [ids.kody])).rows[0].id;
    await kody('POST', '/api/notifications/evaluate');
    r = await kody('GET', '/api/notifications');
    assert.ok(r.body.notifications.some(n => n.type === 'quote_followup'));
    await kody('POST', `/api/quotes/${q}/status`, { status: 'Lost' });
    await kody('POST', '/api/notifications/evaluate');
    r = await kody('GET', '/api/notifications');
    assert.ok(!r.body.notifications.some(n => n.type === 'quote_followup'), 'closed quote notice cleared');

    console.log('e2e-cold: all tests passed');
  } catch (e) {
    console.error(e); console.error(h.log().slice(-1500)); process.exitCode = 1;
  } finally { await h.stop(); }
})();
