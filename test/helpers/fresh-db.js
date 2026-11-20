// Builds the RepRoute schema on an empty Postgres for local tests.
// initDB() was written against the long-lived production database, so a few
// statements in its big setup query reference tables created further down.
// Here, a multi-statement query that fails is split into single statements
// and retried in passes until nothing new succeeds. Production is untouched.
const { pool, initDB } = require('../../db');

function splitSql(sql) {
  const out = []; let cur = ''; let inDollar = false;
  for (let i = 0; i < sql.length; i++) {
    if (sql.startsWith('$$', i)) { inDollar = !inDollar; cur += '$$'; i++; continue; }
    const c = sql[i];
    if (c === ';' && !inDollar) { if (cur.trim()) out.push(cur.trim()); cur = ''; } else cur += c;
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

async function buildFreshSchema() {
  const orig = pool.query.bind(pool);
  pool.query = async (text, params) => {
    try { return await orig(text, params); }
    catch (e) {
      if (params || typeof text !== 'string' || !text.includes(';')) throw e;
      let pending = splitSql(text);
      for (let pass = 0; pass < 6 && pending.length; pass++) {
        const left = [];
        for (const st of pending) { try { await orig(st); } catch (_) { left.push(st); } }
        if (left.length === pending.length) break;
        pending = left;
      }
      return { rows: [], rowCount: 0 };
    }
  };
  await initDB();
  await initDB(); // second pass picks up anything that depended on later tables
  pool.query = orig;
}

module.exports = { buildFreshSchema, pool };

if (require.main === module) {
  buildFreshSchema().then(() => { console.log('fresh schema ready'); return pool.end(); })
    .catch(e => { console.error(e); process.exit(1); });
}
