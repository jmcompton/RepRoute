// One-time data jobs from Nov 20: junk archived, cities from addresses,
// quotes linked to accounts, pipeline catch-up. Run: node test/migrations.test.js
const assert = require('assert');
const { Client } = require('pg');
const PG = process.env.TEST_PG || 'postgresql://postgres@localhost:5433';
const DB = 'reproute_mig';

(async () => {
  const c = new Client({ connectionString: PG + '/postgres' }); await c.connect();
  await c.query(`DROP DATABASE IF EXISTS ${DB}`); await c.query(`CREATE DATABASE ${DB}`); await c.end();
  process.env.DATABASE_URL = `${PG}/${DB}`;
  const { buildFreshSchema, pool } = require('./helpers/fresh-db');
  const { initDB } = require('../db');
  try {
    await buildFreshSchema();
    const u = (await pool.query(`INSERT INTO users (name,email,password,role) VALUES ('K','k@t','x','rep') RETURNING id`)).rows[0].id;
    const ins = (company, extra = {}) => pool.query(
      `INSERT INTO prospects (user_id, company, category, city, state, address) VALUES ($1,$2,'Account',$3,$4,$5) RETURNING id`,
      [u, company, extra.city || null, extra.state || null, extra.address || null]).then(r => r.rows[0].id);
    const junk1 = await ins('04-05 Demonstration and Strategy for an AI-Powered NIL Agent Platform');
    const junk2 = await ins('ZZ TEST Manufacturer (delete me)');
    const junkWithVisit = await ins('2026-03-30 09:26:05');
    await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, notes) VALUES ($1,$2,'2026-10-01','Visit','real visit')`, [u, junkWithVisit]);
    const home = await ins('Home Glass');
    await pool.query(`INSERT INTO calls (user_id, prospect_id, call_date, call_type, notes) VALUES ($1,$2,'2026-10-02','Outbound','TEST call from Claude, delete')`, [u, home]);
    const a1 = await ins('Adams Building Supplies', { address: '425 Big A Rd, Toccoa, GA 30577' });
    const a2 = await ins('Beacon', { address: '100 Peachtree St NW, Atlanta, GA 30303, USA' });
    const a3 = await ins('Keep City', { city: 'Gainesville', address: '1 Main St, Toccoa, GA 30577' });
    const a4 = await ins('No Pattern', { address: 'Hwy 9 past the Dollar General' });
    const q = (await pool.query(`INSERT INTO quotes (user_id, rep_id, account_name, status) VALUES ($1,$1,'Adams Building Supplies, Inc.','Won') RETURNING id`, [u])).rows[0].id;

    await pool.query(`DELETE FROM app_migrations WHERE name LIKE '2026-11-20%'`);
    await initDB();

    const exists = async id => (await pool.query('SELECT 1 FROM prospects WHERE id=$1', [id])).rows.length === 1;
    assert.strictEqual(await exists(junk1), false, 'meeting-note account archived');
    assert.strictEqual(await exists(junk2), false, 'test account archived');
    assert.strictEqual(await exists(junkWithVisit), true, 'kept because a visit hangs off it');
    assert.strictEqual(await exists(home), true, 'real account kept');
    const arch = await pool.query(`SELECT table_name, data->>'company' AS company, data->>'notes' AS notes FROM archived_records ORDER BY id`);
    assert.strictEqual(arch.rows.length, 3, 'two accounts and one test visit archived: ' + JSON.stringify(arch.rows));
    assert.ok(arch.rows.some(r => r.table_name === 'calls' && r.notes === 'TEST call from Claude, delete'));

    const city = async id => (await pool.query('SELECT city, state FROM prospects WHERE id=$1', [id])).rows[0];
    assert.deepStrictEqual(await city(a1), { city: 'Toccoa', state: 'GA' });
    assert.deepStrictEqual(await city(a2), { city: 'Atlanta', state: 'GA' });
    assert.deepStrictEqual(await city(a3), { city: 'Gainesville', state: null }, 'existing city untouched');
    assert.deepStrictEqual(await city(a4), { city: null, state: null }, 'no guess without a pattern');

    const qp = (await pool.query('SELECT prospect_id FROM quotes WHERE id=$1', [q])).rows[0].prospect_id;
    assert.strictEqual(qp, a1, 'quote linked by account key');
    const st = (await pool.query('SELECT pipeline_stage FROM prospects WHERE id=$1', [a1])).rows[0].pipeline_stage;
    assert.strictEqual(st, 'Closed Won', 'won quote → Closed Won');

    // Running again changes nothing
    await initDB();
    assert.strictEqual((await pool.query('SELECT COUNT(*)::int n FROM archived_records')).rows[0].n, 3);
    console.log('migrations: all tests passed');
  } catch (e) { console.error(e); process.exitCode = 1; }
  finally { await pool.end(); }
})();
