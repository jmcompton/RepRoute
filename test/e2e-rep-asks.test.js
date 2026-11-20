// The eight things the reps asked for (Nov 20): recap editing, shelf photos,
// after-visit approvals, pipeline stage actions, the document library, parent
// accounts, quote contact lookup and the new account types.
// Run: node test/e2e-rep-asks.test.js
const assert = require('assert');
const { start } = require('./helpers/harness');

function findItem(groups, callId) {
  let hit = null;
  JSON.stringify(groups, (k, v) => { if (v && typeof v === 'object' && v.call_id === callId) hit = v; return v; });
  return hit;
}

(async () => {
  const h = await start();
  try {
    const { pool, ids, stub } = h;
    const kody = await h.login('kody');
    const jack = await h.login('jack');
    const sean = await h.login('sean');
    const dan = await h.login('dan');
    const today = (await pool.query(`SELECT (NOW() AT TIME ZONE 'America/New_York')::date::text d`)).rows[0].d;
    const ago = n => (new Date(new Date(today + 'T12:00:00Z') - n * 864e5)).toISOString().slice(0, 10);
    const acct = async (uid, company, city, extra = '') =>
      (await pool.query(`INSERT INTO prospects (user_id, company, category, city${extra ? ', source' : ''}) VALUES ($1,$2,'Lumber Dealer',$3${extra ? ',$4' : ''}) RETURNING id`,
        extra ? [uid, company, city, extra] : [uid, company, city])).rows[0].id;

    // ── 1. Edit visits from the Daily Recap (Kody) ──
    const carter = await acct(ids.kody, 'Carter Lumber', 'Oakwood');
    const wrong = await acct(ids.kody, 'Carter Lumbr', 'Oakwood', 'Voice Logger');
    const v1 = (await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, notes)
      VALUES ($1,$2,$3,'In-Person Visit','Visited','Talked Soudal Fix All') RETURNING id`, [ids.kody, wrong, today])).rows[0].id;
    const v2 = (await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, notes)
      VALUES ($1,$2,$3,'In-Person Visit','Visited','Yesterday stop') RETURNING id`, [ids.kody, carter, ago(1)])).rows[0].id;

    let r = await kody('GET', '/api/recap/today');
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.strictEqual(r.body.day, today); assert.strictEqual(r.body.is_today, true);
    const item = findItem(r.body.groups, v1);
    assert.ok(item, 'today\'s visit shows with its call_id');
    assert.strictEqual(item.prospect_id, wrong);
    r = await kody('GET', '/api/recap/today?date=' + ago(1));
    assert.strictEqual(r.body.day, ago(1)); assert.strictEqual(r.body.is_today, false);
    assert.ok(findItem(r.body.groups, v2), 'yesterday is reachable');
    assert.ok(!findItem(r.body.groups, v1));
    r = await kody('GET', '/api/recap/today?date=' + ago(30));
    assert.strictEqual(r.body.day, today, 'more than 14 days back falls back to today');

    // Move the visit to the right account; the empty voice-made copy is archived
    r = await kody('PUT', `/api/calls/${v1}`, { prospect_id: carter, notes: 'Talked Soudal Fix All with the buyer', next_step: 'Bring pricing', next_step_date: ago(-3) });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.strictEqual(r.body.call.prospect_id, carter);
    assert.strictEqual(r.body.archived_account, 'Carter Lumbr');
    assert.strictEqual((await pool.query('SELECT 1 FROM prospects WHERE id=$1', [wrong])).rows.length, 0);
    const arch = (await pool.query(`SELECT data FROM archived_records WHERE table_name='prospects' AND original_id=$1`, [wrong])).rows[0];
    assert.strictEqual(arch.data.company, 'Carter Lumbr', 'archived copy kept');
    assert.strictEqual((await pool.query('SELECT pipeline_stage FROM prospects WHERE id=$1', [carter])).rows[0].pipeline_stage, 'Contacted');
    // Guard rails
    r = await jack('PUT', `/api/calls/${v1}`, { notes: 'hijack' });
    assert.strictEqual(r.status, 403);
    r = await kody('PUT', `/api/calls/${v1}`, { prospect_id: 'abc' });
    assert.strictEqual(r.status, 400);
    r = await kody('PUT', `/api/calls/${v1}`, { prospect_id: 999999 });
    assert.strictEqual(r.status, 400);
    r = await kody('PUT', `/api/calls/${v1}`, { next_step_date: 'soon' });
    assert.strictEqual(r.status, 400);
    r = await kody('PUT', `/api/calls/${v1}`, { next_step_date: '' });
    assert.strictEqual(r.body.call.next_step_date, null, 'clearing the date works');
    // A real account (older or with history) is never archived
    const real = await acct(ids.kody, 'Real Account', 'Athens', 'Voice Logger');
    const v3 = (await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome) VALUES ($1,$2,$3,'Phone Call','Talked') RETURNING id`, [ids.kody, real, today])).rows[0].id;
    await pool.query(`INSERT INTO contacts (prospect_id, name) VALUES ($1,'Someone')`, [real]);
    r = await kody('PUT', `/api/calls/${v3}`, { prospect_id: carter });
    assert.strictEqual(r.body.archived_account, null);
    assert.strictEqual((await pool.query('SELECT 1 FROM prospects WHERE id=$1', [real])).rows.length, 1);

    // ── 2. Shelf photos read by AI (Kody) ──
    const jpg = Buffer.alloc(400, 7).toString('base64');
    stub.calls.length = 0;
    stub.reply = JSON.stringify({ items: [
      { brand: 'Soudal', product: 'Fix All Classic', size: '9.5 oz', price: '$8.97', facings: 3, ours: true },
      { brand: 'Loctite', product: 'PL Premium', size: '10 oz', price: '$7.48', facings: '6' },
      { brand: '', product: '' },
      { brand: 'shurtape', product: 'PF 105 flashing tape', size: '', price: '', facings: 2, ours: false }],
      our_lines_seen: ['Made Up'], empty_spots: 'Two empty hooks under DAP', summary: 'Loctite has twice the facings of Fix All.' });
    r = await kody('POST', `/api/shelf/account/${carter}`, { image: 'data:image/jpeg;base64,' + jpg, note: 'Aisle 7' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const ph = r.body.photo;
    assert.ok(ph.reading, 'read on upload');
    assert.strictEqual(ph.reading.items.length, 3, 'blank rows dropped');
    assert.deepStrictEqual(ph.reading.our_lines_seen, ['Soudal', 'ShurTape'], 'ours decided by our brand list, not the model');
    assert.deepStrictEqual(ph.reading.competitor_brands, ['Loctite']);
    assert.strictEqual(ph.reading.items[1].facings, 6);
    assert.strictEqual(ph.note, 'Aisle 7');
    assert.strictEqual(ph.taken_by, 'Kody Slate');
    const sent = JSON.parse(stub.calls[stub.calls.length - 1]);
    assert.strictEqual(sent.messages[0].content[0].type, 'image');
    assert.strictEqual(sent.messages[0].content[0].source.media_type, 'image/jpeg');
    // The rest of the team sees it on the account; the image is served
    r = await jack('GET', `/api/shelf/account/${carter}`);
    assert.strictEqual(r.body.photos.length, 1);
    assert.ok(!('data' in r.body.photos[0]), 'list does not carry image bytes');
    r = await jack('GET', `/api/shelf/${ph.id}/image`);
    assert.strictEqual(r.status, 200);
    // A failed read keeps the photo and can be retried
    stub.reply = 'not json';
    r = await kody('POST', `/api/shelf/account/${carter}`, { image: jpg, mime: 'image/jpeg' });
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.photo.reading, null);
    assert.ok(/didn't answer/.test(r.body.photo.read_error));
    stub.reply = JSON.stringify({ items: [{ brand: 'DAP', product: 'Alex Plus' }], summary: 'Mostly DAP.' });
    r = await kody('POST', `/api/shelf/${r.body.photo.id}/read`);
    assert.strictEqual(r.body.photo.reading.items[0].brand, 'DAP');
    assert.strictEqual(r.body.photo.read_error, null);
    stub.reply = JSON.stringify({ items: [], summary: '' });
    r = await kody('POST', `/api/shelf/account/${carter}`, { image: jpg });
    assert.ok(/Couldn't read any products/.test(r.body.photo.read_error));
    // Bad input
    r = await kody('POST', `/api/shelf/account/${carter}`, { image: jpg, mime: 'image/heic' });
    assert.strictEqual(r.status, 400);
    r = await kody('POST', `/api/shelf/account/${carter}`, { image: '' });
    assert.strictEqual(r.status, 400);
    r = await kody('POST', `/api/shelf/account/999999`, { image: jpg });
    assert.strictEqual(r.status, 404);
    // Only the rep who took it, or a manager, removes it
    r = await jack('DELETE', `/api/shelf/${ph.id}`);
    assert.strictEqual(r.status, 403);
    r = await sean('DELETE', `/api/shelf/${ph.id}`);
    assert.strictEqual(r.status, 200);
    r = await kody('GET', `/api/shelf/account/${carter}`);
    assert.strictEqual(r.body.photos.length, 2);

    // ── 3. Approval step after each visit (Daniel) ──
    const avi = await acct(ids.dan, 'AVI Windows', 'Atlanta');
    await pool.query(`INSERT INTO contacts (prospect_id, name, email, is_primary) VALUES ($1,'Mike Hall','mike@avi.test',true)`, [avi]);
    const dv = (await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, notes, next_step)
      VALUES ($1,$2,$3,'In-Person Visit','Left Sample','Left him a sample of the flashing tape. Wants pricing on Fix All too.','Check on the tape')
      RETURNING id`, [ids.dan, avi, today])).rows[0].id;
    r = await dan('POST', '/api/agents/after-visit', { call_id: dv });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const kinds = r.body.steps.map(s => s.kind);
    assert.deepStrictEqual(kinds, ['sample', 'quote', 'followup_date', 'email']);
    assert.strictEqual(r.body.steps[0].line, 'ShurTape');
    assert.ok(/Soudal/.test(r.body.steps[1].products) && /ShurTape/.test(r.body.steps[1].products));
    assert.strictEqual(r.body.company, 'AVI Windows');
    // Nothing was created just by asking
    assert.strictEqual((await pool.query('SELECT COUNT(*)::int n FROM samples WHERE prospect_id=$1', [avi])).rows[0].n, 0);
    assert.strictEqual((await pool.query('SELECT COUNT(*)::int n FROM quotes')).rows[0].n, 0);
    // Approve each, the way the app does
    r = await dan('POST', '/api/samples', { prospect_id: avi, product_line: 'ShurTape', quantity: 1, notes: 'From visit notes', sent_date: today });
    assert.strictEqual(r.status, 200);
    r = await dan('POST', '/api/quotes', { account_name: 'AVI Windows', contact_name: 'Mike Hall', status: 'Draft', products: 'Soudal, ShurTape', quote_date: today });
    assert.ok(r.status === 200 || r.status === 201, JSON.stringify(r.body));
    r = await dan('PUT', `/api/calls/${dv}`, { next_step_date: ago(-7) });
    assert.strictEqual(r.status, 200);
    // Asking again doesn't offer what's already done
    r = await dan('POST', '/api/agents/after-visit', { call_id: dv });
    assert.deepStrictEqual(r.body.steps.map(s => s.kind), ['email']);
    assert.strictEqual(r.body.next_step_date, ago(-7));
    assert.strictEqual((await pool.query('SELECT prospect_id FROM quotes WHERE account_name=$1', ['AVI Windows'])).rows[0].prospect_id, avi, 'quote linked to the account');
    r = await kody('POST', '/api/agents/after-visit', { call_id: dv });
    assert.strictEqual(r.status, 403);
    r = await dan('POST', '/api/agents/after-visit', { call_id: 999999 });
    assert.strictEqual(r.status, 404);
    // A plain visit only offers the email
    const plain = (await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, notes) VALUES ($1,$2,$3,'Phone Call','Talked','Quick hello') RETURNING id`, [ids.dan, avi, today])).rows[0].id;
    r = await dan('POST', '/api/agents/after-visit', { call_id: plain });
    assert.deepStrictEqual(r.body.steps.map(s => s.kind), ['email']);

    // ── 4. Pipeline moves kick off the next step (Daniel) ──
    stub.reply = JSON.stringify({ subject: 'Fix All quote', body: 'Hi Mike,\n\nAny questions on the quote?\n\nThanks,\nDaniel' });
    for (const stage of ['Contacted', 'Sampled', 'Quoted', 'Closed Won']) {
      r = await dan('POST', '/api/agents/stage-email', { prospect_id: avi, stage });
      assert.strictEqual(r.status, 200, stage + ' ' + JSON.stringify(r.body));
      assert.ok(r.body.subject && r.body.body, stage);
      assert.strictEqual(r.body.to_email, 'mike@avi.test');
      assert.strictEqual(r.body.stage, stage);
    }
    stub.reply = 'garbage';
    r = await dan('POST', '/api/agents/stage-email', { prospect_id: avi, stage: 'Sampled' });
    assert.strictEqual(r.status, 200);
    assert.ok(/ShurTape sample/.test(r.body.body), 'template names the sample: ' + r.body.body);
    assert.ok(!/—/.test(r.body.body + r.body.subject), 'no em dashes');
    r = await dan('POST', '/api/agents/stage-email', { prospect_id: avi, stage: 'New Lead' });
    assert.strictEqual(r.status, 400);
    r = await kody('POST', '/api/agents/stage-email', { prospect_id: avi, stage: 'Quoted' });
    assert.strictEqual(r.status, 403);
    r = await sean('POST', '/api/agents/stage-email', { prospect_id: avi, stage: 'Quoted' });
    assert.strictEqual(r.status, 200, 'managers can');

    // ── 5. Shared document library (Daniel) ──
    const pdf = Buffer.from('%PDF-1.4 Fix All spec sheet').toString('base64');
    r = await dan('POST', '/api/documents', { name: 'Fix All spec sheet', filename: 'fix-all.pdf', mime: 'application/pdf', data: pdf, line: 'Soudal', kind: 'Spec sheet' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const doc = r.body.document;
    assert.strictEqual(doc.line, 'Soudal'); assert.strictEqual(doc.size, 27);
    r = await dan('POST', '/api/documents', { name: 'Odd', filename: 'Precio año.xlsx', mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', data: pdf, line: 'Nope', kind: 'Nope' });
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.document.line, 'General'); assert.strictEqual(r.body.document.kind, 'Other');
    assert.strictEqual(r.body.document.filename, 'Precio a_o.xlsx', 'header-safe file name');
    const odd = r.body.document.id;
    r = await kody('GET', '/api/documents');
    assert.strictEqual(r.body.documents.length, 2);
    assert.ok(r.body.kinds.includes('Price sheet') && r.body.lines.includes('Fortress'));
    assert.ok(!('data' in r.body.documents[0]));
    r = await kody('GET', `/api/documents/${doc.id}/file`);
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body, '%PDF-1.4 Fix All spec sheet');
    r = await kody('GET', `/api/documents/${odd}/file`);
    assert.strictEqual(r.status, 200, 'non-ASCII name still downloads');
    for (const bad of [
      { name: '', filename: 'a.pdf', mime: 'application/pdf', data: pdf },
      { name: 'x', filename: 'a.exe', mime: 'application/x-msdownload', data: pdf },
      { name: 'x', filename: 'a.pdf', mime: 'application/pdf', data: '' }]) {
      r = await kody('POST', '/api/documents', bad);
      assert.strictEqual(r.status, 400, JSON.stringify(bad));
    }
    // Attach to an account, see it there, take it off
    r = await kody('POST', `/api/documents/account/${carter}`, { document_id: doc.id });
    assert.strictEqual(r.status, 200);
    r = await kody('POST', `/api/documents/account/${carter}`, { document_id: doc.id });
    assert.strictEqual(r.status, 200, 'attaching twice is harmless');
    r = await jack('GET', `/api/documents/account/${carter}`);
    assert.deepStrictEqual(r.body.documents.map(d => d.name), ['Fix All spec sheet']);
    r = await kody('POST', `/api/documents/account/${carter}`, { document_id: 999999 });
    assert.strictEqual(r.status, 404);
    r = await kody('DELETE', `/api/documents/account/${carter}/${doc.id}`);
    r = await kody('GET', `/api/documents/account/${carter}`);
    assert.strictEqual(r.body.documents.length, 0);
    // Removing from the library: uploader or manager only
    r = await kody('DELETE', `/api/documents/${doc.id}`);
    assert.strictEqual(r.status, 403);
    r = await sean('DELETE', `/api/documents/${odd}`);
    assert.strictEqual(r.status, 200);
    r = await dan('DELETE', `/api/documents/${doc.id}`);
    assert.strictEqual(r.status, 200);
    r = await kody('DELETE', `/api/documents/abc`);
    assert.strictEqual(r.status, 404);

    // ── 6. Parent accounts (Sean) ──
    const crs = await acct(ids.sean, 'CRS', 'Atlanta');
    const crsWeb = await acct(ids.jack, 'CRS Internet Sales', 'Atlanta');
    const crsMac = await acct(ids.kody, 'CRS', 'Macon');
    r = await sean('PUT', `/api/prospects/${crsWeb}/parent`, { parent_id: crs });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    r = await kody('PUT', `/api/prospects/${crsMac}/parent`, { parent_id: crs });
    assert.strictEqual(r.status, 200);
    await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome) VALUES ($1,$2,$3,'Phone Call','Talked'),($4,$5,$6,'In-Person Visit','Visited')`,
      [ids.jack, crsWeb, ago(2), ids.kody, crsMac, ago(1)]);
    await pool.query(`INSERT INTO quotes (user_id, account_name, status, amount, prospect_id) VALUES ($1,'CRS Internet Sales','Sent',1200.50,$2),($1,'CRS','Won',500,$3),($1,'CRS','Lost',900,$3)`, [ids.jack, crsWeb, crs]);
    r = await jack('GET', `/api/prospects/${crs}/family`);
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.strictEqual(r.body.is_parent, true);
    assert.strictEqual(r.body.locations.length, 2);
    assert.strictEqual(r.body.totals.visits_90d, 2);
    assert.strictEqual(r.body.totals.last_visit, ago(1));
    assert.strictEqual(r.body.totals.open_quotes, 1);
    assert.strictEqual(r.body.totals.open_amount, 1200.5);
    assert.strictEqual(r.body.totals.won, 1);
    r = await jack('GET', `/api/prospects/${crsMac}/family`);
    assert.strictEqual(r.body.is_parent, false);
    assert.strictEqual(r.body.parent.id, crs);
    assert.strictEqual(r.body.locations.length, 2, 'a location sees its sister locations');
    // Rules
    r = await sean('PUT', `/api/prospects/${crs}/parent`, { parent_id: crs });
    assert.strictEqual(r.status, 400);
    r = await sean('PUT', `/api/prospects/${carter}/parent`, { parent_id: crsMac });
    assert.strictEqual(r.status, 400, 'no grandparents');
    r = await sean('PUT', `/api/prospects/${crs}/parent`, { parent_id: carter });
    assert.strictEqual(r.status, 400, 'a parent with locations stays a parent');
    r = await sean('PUT', `/api/prospects/${carter}/parent`, { parent_id: 999999 });
    assert.strictEqual(r.status, 400);
    r = await kody('PUT', `/api/prospects/${crsMac}/parent`, { parent_id: null });
    assert.strictEqual(r.status, 200);
    r = await jack('GET', `/api/prospects/${crs}/family`);
    assert.strictEqual(r.body.locations.length, 1);
    // Deleting the parent leaves the locations as plain accounts
    await pool.query('DELETE FROM quotes WHERE prospect_id=$1', [crs]);
    await pool.query('DELETE FROM prospects WHERE id=$1', [crs]);
    assert.strictEqual((await pool.query('SELECT parent_id FROM prospects WHERE id=$1', [crsWeb])).rows[0].parent_id, null);

    // ── 7. Contact lookup on quotes (Sean) ──
    await pool.query(`INSERT INTO contacts (prospect_id, name, title, phone, email) VALUES ($1,'Tina Ruiz','Buyer','555-0101','tina@crs.test'),($1,' ','x',null,null)`, [crsWeb]);
    await pool.query(`UPDATE prospects SET contact='Ray Cole', phone='555-0202' WHERE id=$1`, [crsMac]);
    await pool.query(`INSERT INTO prospects (user_id, company, category, contact) VALUES ($1,'Crossroads Supply','Lumber Dealer','Tina Ruiz')`, [ids.dan]);
    r = await sean('GET', '/api/quotes/contact-lookup?q=crs');
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    const names = r.body.contacts.map(c => c.name + '@' + c.company);
    assert.ok(names.includes('Tina Ruiz@CRS Internet Sales') && names.includes('Ray Cole@CRS'), names.join(', '));
    assert.ok(r.body.contacts.every(c => c.name.trim()), 'blank names skipped');
    assert.strictEqual(names[0].split('@')[1].toLowerCase().startsWith('crs'), true, 'companies starting with the text come first');
    r = await sean('GET', '/api/quotes/contact-lookup?q=tina');
    assert.strictEqual(r.body.contacts.length, 2, 'search by person, both companies');
    r = await sean('GET', '/api/quotes/contact-lookup?q=c');
    assert.strictEqual(r.body.contacts.length, 0, 'too short');
    r = await sean('GET', '/api/quotes/contact-lookup?q=%25%25');
    assert.strictEqual(r.body.contacts.length, 0, 'wildcards are literal');

    // ── 8. New account types (Sean) ──
    for (const [cat, type] of [['Roofing Subcontractor', 'Contractor'], ['Fencing Subcontractor', 'Contractor'], ['Precast', 'Contractor'], ['Floor Door', 'Contractor']]) {
      r = await sean('POST', '/api/prospects', { company: cat + ' Co', category: cat, city: 'Athens', state: 'GA' });
      assert.ok(r.status === 200 || r.status === 201, cat + ' ' + JSON.stringify(r.body));
      const row = (await pool.query('SELECT category FROM prospects WHERE company=$1', [cat + ' Co'])).rows[0];
      assert.strictEqual(row.category, cat, cat + ' saved as picked');
    }
    r = await sean('GET', '/api/prospects');
    const list = Array.isArray(r.body) ? r.body : (r.body.prospects || []);
    const pc = list.find(p => p.company === 'Precast Co');
    assert.ok(pc, 'new account listed');
    if ('company_type' in pc) assert.strictEqual(pc.company_type, 'Contractor');

    console.log('e2e-rep-asks: all tests passed');
  } catch (e) {
    console.error(e); console.error(h.log().slice(-2500)); process.exitCode = 1;
  } finally { await h.stop(); }
})();
