// The new layout's server side: "What you sell", the full Accounts list, and
// the overnight New leads (against a stub Google).
// Run: node test/e2e-new-layout.test.js
const assert = require('assert');
const http = require('http');
const { start } = require('./helpers/harness');

const P = (name, lat, lng, types) => ({ id: 'pid-' + name.replace(/\W/g, ''), displayName: { text: name },
  formattedAddress: '1 Main St, Gainesville, GA 30501', location: { latitude: lat, longitude: lng },
  types: types || ['establishment'], rating: 4.6, userRatingCount: 40, businessStatus: 'OPERATIONAL', websiteUri: 'https://x.test',
  nationalPhoneNumber: '(770) 555-0100' });
const WINDOW = [
  P('Gainesville Window & Door Showroom', 34.29, -83.81, ['store']),
  P('Door4Life', 34.31, -83.83),
  P('Lakeside Millwork', 34.32, -83.84, ['store']),
  P('Carter Lumber', 34.30, -83.82, ['store']),
  P('Knoxville Window Supply', 35.96, -83.92, ['store'])
];
const calls = [];
function stubGoogle(port) {
  return new Promise(resolve => {
    const srv = http.createServer((req, res) => {
      let body = ''; req.on('data', d => body += d);
      req.on('end', () => {
        res.setHeader('Content-Type', 'application/json');
        if (req.url.includes('geocode')) return res.end(JSON.stringify({ status: 'OK', results: [{ geometry: { location: { lat: 34.30, lng: -83.82 } } }] }));
        calls.push(JSON.parse(body || '{}'));
        res.end(JSON.stringify({ places: WINDOW }));
      });
    }).listen(port, () => resolve(srv));
  });
}

(async () => {
  const gport = 4800 + Math.floor(Math.random() * 150);
  const g = await stubGoogle(gport);
  const h = await start({ GOOGLE_PLACES_API_KEY: 'stub', PLACES_BASE_URL: `http://127.0.0.1:${gport}`, GEOCODE_BASE_URL: `http://127.0.0.1:${gport}` });
  try {
    const { pool, ids } = h;
    const kody = await h.login('kody');
    const sean = await h.login('sean');
    const jack = await h.login('jack');

    // ── What you sell ──
    let r = await kody('GET', '/api/sell');
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.deepStrictEqual(r.body.lines.map(l => l.name), ['Soudal', 'ShurTape', 'Alum-A-Pole', 'Fortress', 'BOSS']);
    assert.ok(r.body.lines[1].buyer_segments.includes('Window/Door Installer'));
    assert.ok(r.body.segments.includes('Rebar / Concrete Supply'));
    assert.strictEqual(r.body.can_edit, false);
    const shur = r.body.lines[1].id;
    r = await kody('PUT', `/api/sell/${shur}`, { competitors: 'x' });
    assert.strictEqual(r.status, 403);
    r = await sean('PUT', `/api/sell/${shur}`, { what: 'Flashing tapes', competitors: '3M, Tyvek tape', match_words: 'shurtape, shur tape, flashing tape',
      buyer_segments: ['Window/Door Installer', 'Bogus', 'Window/Door Installer'] });
    assert.strictEqual(r.status, 200);
    r = await sean('GET', '/api/sell');
    const s2 = r.body.lines.find(l => l.id === shur);
    assert.deepStrictEqual(s2.buyer_segments, ['Window/Door Installer'], 'unknown and repeated types dropped');
    assert.strictEqual(s2.competitors, '3M, Tyvek tape');
    assert.strictEqual(r.body.can_edit, true);
    r = await sean('POST', '/api/sell', { name: 'Tremco', what: 'Roof coatings', match_words: 'tremco', buyer_segments: ['Roofing Contractor'] });
    assert.strictEqual(r.status, 200);
    const tremco = r.body.id;
    r = await sean('POST', '/api/sell', { name: ' ' });
    assert.strictEqual(r.status, 400);
    // Agents pick the new line up right away
    const acct0 = (await pool.query(`INSERT INTO prospects (user_id, company, category, city) VALUES ($1,'Hall Roofing','Roofing Contractor','Gainesville') RETURNING id`, [ids.kody])).rows[0].id;
    const v0 = (await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, notes) VALUES ($1,$2,CURRENT_DATE,'In-Person Visit','Left Sample','Left a Tremco sample with the owner') RETURNING id`, [ids.kody, acct0])).rows[0].id;
    r = await kody('POST', '/api/agents/after-visit', { call_id: v0 });
    assert.strictEqual(r.body.steps[0].kind, 'sample');
    assert.strictEqual(r.body.steps[0].line, 'Tremco');
    r = await kody('GET', '/api/documents');
    assert.ok(r.body.lines.includes('Tremco') && r.body.lines.includes('General'));
    r = await sean('DELETE', `/api/sell/${tremco}`);
    r = await kody('GET', '/api/sell');
    assert.ok(!r.body.lines.some(l => l.name === 'Tremco'), 'taken off the page');
    assert.strictEqual((await pool.query('SELECT 1 FROM lines WHERE id=$1', [tremco])).rows.length, 1, 'line itself kept for commission history');

    // ── Accounts list ──
    const mk = async (uid, company, cat, city, extra = {}) => (await pool.query(
      `INSERT INTO prospects (user_id, company, category, city, phone, contact, pipeline_stage, lat, lng) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
      [uid, company, cat, city, extra.phone || null, extra.contact || null, extra.stage || 'New Lead', extra.lat || null, extra.lng || null])).rows[0].id;
    const carter = await mk(ids.kody, 'Carter Lumber', 'Lumber Dealer', 'Gainesville', { stage: 'Quoted', phone: '(770) 555-1234', lat: 34.30, lng: -83.82 });
    const lumberOne = await mk(ids.kody, 'Lumber One', 'Lumber Dealer', 'Commerce', { lat: 34.20, lng: -83.45 });
    const ace = await mk(ids.kody, 'Ace Fence Co', 'Fencing Subcontractor', 'Athens', { lat: 33.95, lng: -83.38 });
    const adams = await mk(ids.jack, 'Adams Building Supply', 'Lumber Dealer', 'Toccoa', { contact: 'Debbie Shaw' });
    const crs = await mk(ids.sean, 'CRS', 'Building Materials Distributor', ' gainesville ');
    await pool.query(`INSERT INTO contacts (prospect_id, name) VALUES ($1,'Ray Cole')`, [carter]);
    await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome) VALUES
      ($1,$2,CURRENT_DATE-3,'Visit','Visited'), ($1,$3,CURRENT_DATE-60,'Visit','Visited'), ($4,$5,CURRENT_DATE-1,'Visit','Visited')`,
      [ids.kody, carter, lumberOne, ids.jack, adams]);
    await pool.query(`INSERT INTO quotes (user_id, account_name, status, amount, prospect_id) VALUES
      ($1,'Carter Lumber','Sent',6400,$2), ($1,'Carter Lumber','Draft',100.5,$2), ($1,'Carter Lumber','Won',999,$2)`, [ids.kody, carter]);
    const gone = (await pool.query(`INSERT INTO users (name,email,password,role,disabled_at) VALUES ('Old Rep','old@test','x','rep',NOW()) RETURNING id`)).rows[0].id;
    await mk(gone, 'Ghost Supply', 'Lumber Dealer', 'Macon');
    const soudal = (await pool.query(`SELECT id FROM lines WHERE name='Soudal'`)).rows[0].id;
    await pool.query(`INSERT INTO account_lines (account_id, line_id, total_sales) VALUES ($1,$2,5000)`, [adams, soudal]);

    r = await kody('GET', '/api/accounts-list');
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const names = r.body.rows.map(x => x.company);
    assert.ok(!names.includes('Ghost Supply'), 'disabled reps left out');
    assert.deepStrictEqual(names, [...names].sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase())), 'A to Z');
    assert.strictEqual(r.body.total, names.length);
    const c = r.body.rows.find(x => x.id === carter);
    assert.strictEqual(c.owner, 'Kody Slate'); assert.strictEqual(c.mine, true); assert.strictEqual(c.stage, 'Quoted');
    assert.strictEqual(c.open_quotes, 2); assert.strictEqual(c.open_amount, 6500.5); assert.strictEqual(c.days_since, 3);
    assert.strictEqual(r.body.rows.find(x => x.id === adams).mine, false);
    r = await kody('GET', '/api/accounts-list?view=mine');
    assert.ok(r.body.rows.every(x => x.mine) && r.body.rows.some(x => x.id === carter));
    r = await kody('GET', '/api/accounts-list?view=cold');
    assert.deepStrictEqual(r.body.rows.map(x => x.company), ['Lumber One']);
    r = await kody('GET', `/api/accounts-list?view=recent&ids=${ace},${adams},abc`);
    assert.deepStrictEqual(r.body.rows.map(x => x.id).sort(), [ace, adams].sort());
    r = await kody('GET', '/api/accounts-list?q=ray%20cole');
    assert.deepStrictEqual(r.body.rows.map(x => x.id), [carter], 'finds by contact');
    r = await kody('GET', '/api/accounts-list?q=debbie');
    assert.deepStrictEqual(r.body.rows.map(x => x.id), [adams], 'finds by the account contact');
    r = await kody('GET', '/api/accounts-list?q=5551234');
    assert.deepStrictEqual(r.body.rows.map(x => x.id), [carter], 'finds by phone digits');
    r = await kody('GET', '/api/accounts-list?q=%25');
    assert.strictEqual(r.body.total, 0, 'wildcards literal');
    r = await kody('GET', '/api/accounts-list?type=Fencing%20Subcontractor');
    assert.deepStrictEqual(r.body.rows.map(x => x.id), [ace]);
    r = await kody('GET', '/api/accounts-list?city=Gainesville');
    assert.deepStrictEqual(r.body.rows.map(x => x.id).sort((a, b) => a - b), [acct0, carter, crs].sort((a, b) => a - b), 'city match ignores case and spaces');
    r = await kody('GET', `/api/accounts-list?owner=${ids.jack}`);
    assert.deepStrictEqual(r.body.rows.map(x => x.id), [adams]);
    r = await kody('GET', '/api/accounts-list?stage=Quoted');
    assert.deepStrictEqual(r.body.rows.map(x => x.id), [carter]);
    r = await kody('GET', `/api/accounts-list?line=${soudal}`);
    assert.deepStrictEqual(r.body.rows.map(x => x.id), [adams], 'buys Soudal per commission data');
    r = await kody('GET', '/api/accounts-list?sort=last_visit&dir=desc');
    assert.deepStrictEqual(r.body.rows.slice(0, 4).map(x => x.id), [acct0, adams, carter, lumberOne], 'newest visit first, never-visited last');
    r = await kody('GET', '/api/accounts-list?sort=open_quotes&dir=desc');
    assert.strictEqual(r.body.rows[0].id, carter);
    r = await kody('GET', '/api/accounts-list?sort=evil;drop&per=10&page=1');
    assert.strictEqual(r.body.sort, 'name'); assert.strictEqual(r.body.per, 10);
    r = await kody('GET', '/api/accounts-list?per=10&page=99');
    assert.strictEqual(r.body.rows.length, 0);
    r = await kody('GET', '/api/accounts-list/facets');
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.counts.cold, 1);
    assert.ok(r.body.counts.mine >= 4 && r.body.counts.all > r.body.counts.mine);
    assert.ok(r.body.types.some(t => t.v === 'Fencing Subcontractor'));
    assert.strictEqual(r.body.cities.filter(t => t.v.toLowerCase() === 'gainesville').length, 1, 'one Gainesville');
    assert.ok(r.body.owners.every(o => o.name !== 'Old Rep'));
    assert.deepStrictEqual(r.body.lines.map(l => l.name), ['Soudal']);

    // ── Layout switch ──
    r = await kody('GET', '/app/data');
    assert.strictEqual(r.body.user.ui_layout, 'new', 'new layout for everyone');
    r = await kody('PUT', '/api/accounts-list/layout', { layout: 'classic' });
    assert.strictEqual(r.body.layout, 'classic');
    r = await kody('GET', '/app/data');
    assert.strictEqual(r.body.user.ui_layout, 'classic');
    r = await jack('GET', '/app/data');
    assert.strictEqual(r.body.user.ui_layout, 'new', 'per person');

    // ── New leads ──
    // Nov 23 call: a few of Kody's accounts are mapped in Tennessee. Leads must
    // still come from his Territory (Atlanta Metro), not the middle of his pins.
    for (const [nm, la, lo] of [['TN One', 35.95, -83.9], ['TN Two', 35.97, -83.93], ['TN Three', 35.96, -83.91], ['TN Four', 35.94, -83.92]])
      await pool.query(`INSERT INTO prospects (user_id, company, category, lat, lng) VALUES ($1,$2,'Contractor',$3,$4)`, [ids.kody, nm, la, lo]);
    r = await kody('GET', '/api/leads');
    assert.deepStrictEqual(r.body.area, { label: 'Atlanta Metro', source: 'territory' }, JSON.stringify(r.body.area));
    r = await kody('GET', '/api/leads/count');
    assert.strictEqual(r.body.count, 0);
    r = await kody('POST', '/api/leads/hunt', { segment: 'Bogus' });
    assert.strictEqual(r.status, 400);
    calls.length = 0;
    r = await kody('POST', '/api/leads/hunt', { segment: 'Window/Door Installer' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.ok(calls.length > 0, 'searched Google');
    assert.ok(calls.every(b => b.locationBias || b.locationRestriction || /near|,/.test(b.textQuery)), 'searched near the rep');
    r = await kody('GET', '/api/leads');
    const ln = r.body.leads.map(l => l.company);
    assert.ok(ln.includes('Gainesville Window & Door Showroom') && ln.includes('Lakeside Millwork'), ln.join(', '));
    assert.ok(!ln.includes('Door4Life'), 'junk filter applied');
    assert.ok(!ln.includes('Carter Lumber'), 'already an account');
    assert.ok(!ln.includes('Knoxville Window Supply'), 'nothing outside the territory');
    const show = r.body.leads.find(l => l.company === 'Gainesville Window & Door Showroom');
    assert.ok(/Window and door dealer or installer\. Under a mile from Carter Lumber\./.test(show.why), show.why);
    assert.strictEqual(show.pitch, 'Soudal, ShurTape');
    assert.strictEqual(show.category, 'Window & Door Installer');
    assert.strictEqual(r.body.last_run.found, ln.length);
    r = await kody('GET', '/api/leads/count');
    assert.strictEqual(r.body.count, ln.length);
    r = await jack('GET', '/api/leads');
    assert.strictEqual(r.body.leads.length, 0, 'each rep has their own');
    r = await kody('GET', '/api/accounts-list/facets');
    assert.strictEqual(r.body.counts.leads, ln.length);

    const lake = (await kody('GET', '/api/leads')).body.leads.find(l => l.company === 'Lakeside Millwork');
    r = await jack('POST', `/api/leads/${show.id}/save`);
    assert.strictEqual(r.status, 404, 'not Jack\'s lead');
    r = await kody('POST', `/api/leads/${show.id}/route`);
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const acct = (await pool.query('SELECT * FROM prospects WHERE id=$1', [r.body.prospect_id])).rows[0];
    assert.strictEqual(acct.user_id, ids.kody); assert.strictEqual(acct.source, 'Lead Hunt');
    assert.strictEqual(acct.category, 'Window & Door Installer'); assert.strictEqual(acct.google_place_id, 'pid-GainesvilleWindowDoorShowroom');
    assert.strictEqual(Number(acct.lat), 34.29);
    const plan = (await pool.query('SELECT planned_date::text d, EXTRACT(DOW FROM planned_date)::int dow FROM planner_items WHERE rep_id=$1 AND account_id=$2', [ids.kody, acct.id])).rows;
    assert.strictEqual(plan.length, 1); assert.ok(plan[0].dow >= 1 && plan[0].dow <= 5, 'a weekday');
    r = await kody('POST', `/api/leads/${show.id}/route`);
    assert.strictEqual(r.body.prospect_id, acct.id, 'no second account');
    assert.strictEqual((await pool.query('SELECT COUNT(*)::int n FROM planner_items WHERE account_id=$1', [acct.id])).rows[0].n, 1, 'no second stop');
    r = await kody('POST', `/api/leads/${lake.id}/skip`);
    assert.strictEqual(r.status, 200);
    r = await kody('GET', '/api/leads');
    assert.strictEqual(r.body.leads.length, 0, 'decided leads leave the list');
    // A second search doesn't bring back skipped or saved ones
    r = await kody('POST', '/api/leads/hunt', { segment: 'Window/Door Installer' });
    assert.strictEqual(r.body.found, 0);
    r = await kody('POST', '/api/leads/hunt', {});
    assert.strictEqual(r.status, 429, 'two searches a day for reps');
    r = await sean('POST', '/api/leads/hunt', {});
    assert.notStrictEqual(r.status, 429, 'managers not limited');
    // Near me right now: searches where the phone is
    r = await sean('POST', '/api/leads/hunt', { segment: 'Window/Door Installer', mode: 'gps' });
    assert.strictEqual(r.status, 400, 'needs a location');
    r = await sean('POST', '/api/leads/hunt', { segment: 'Window/Door Installer', mode: 'gps', lat: 35.96, lng: -83.92 });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    r = await sean('GET', '/api/leads');
    const knox = r.body.leads.find(l => l.company === 'Knoxville Window Supply');
    assert.ok(knox, r.body.leads.map(l => l.company).join(', '));
    assert.ok(/from where you are/.test(knox.why), knox.why);
    assert.ok(!r.body.leads.some(l => l.company === 'Lakeside Millwork' && /where you are/.test(l.why)), 'Gainesville is too far from Knoxville');

    // The nightly run: only people out selling, once a day each
    process.env.GOOGLE_PLACES_API_KEY = 'stub';
    process.env.PLACES_BASE_URL = `http://127.0.0.1:${gport}`;
    process.env.GEOCODE_BASE_URL = `http://127.0.0.1:${gport}`;
    await pool.query(`UPDATE users SET home_base_lat=34.3, home_base_lng=-83.82 WHERE id=$1`, [ids.jack]);
    await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome) VALUES ($1,$2,CURRENT_DATE-5,'Visit','Visited')`, [ids.jack, adams]);
    const hunt = require('../lib/lead-hunt');
    let n = await hunt.huntAll();
    assert.strictEqual(n.people, 1, 'Jack only: Kody and Sean already searched today, Dan has no recent visits: ' + JSON.stringify(n));
    assert.ok(n.found >= 1, JSON.stringify(n));
    r = await jack('GET', '/api/leads');
    assert.ok(r.body.leads.length >= 1);
    assert.ok(!r.body.leads.some(l => l.company === 'Carter Lumber'), 'a teammate\'s account is never a new lead');
    n = await hunt.huntAll();
    assert.strictEqual(n.people, 0, 'once a day');
    process.env.LEAD_HUNT = 'off';
    assert.strictEqual((await hunt.huntAll()).skipped, 'turned off');

    console.log('e2e-new-layout: all tests passed');
  } catch (e) {
    console.error(e); console.error(h.log().slice(-2500)); process.exitCode = 1;
  } finally { await h.stop(); g.close(); }
})();
