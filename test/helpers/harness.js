// End-to-end test harness: a throwaway Postgres database, the real server in a
// child process, a stub AI, and logged-in sessions for a few reps.
// Needs a local Postgres. TEST_PG (default postgresql://postgres@localhost:5433)
// points at the server; a database named reproute_e2e is dropped and rebuilt.
const { spawn } = require('child_process');
const http = require('http');
const path = require('path');
const { Client } = require('pg');
const bcrypt = require('bcryptjs');

const PG = process.env.TEST_PG || 'postgresql://postgres@localhost:5433';
const DB = 'reproute_e2e';
const PORT = 3990 + Math.floor(Math.random() * 8);
const AI_PORT = PORT + 100;

async function resetDb() {
  const c = new Client({ connectionString: PG + '/postgres' });
  await c.connect();
  await c.query(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${DB}' AND pid <> pg_backend_pid()`);
  await c.query(`DROP DATABASE IF EXISTS ${DB}`);
  await c.query(`CREATE DATABASE ${DB}`);
  await c.end();
}

// The stub AI answers like Anthropic. Tests set stub.reply to the JSON text the
// "model" should return for the next call(s).
const stub = { reply: '{}', calls: [] };
function startStubAI() {
  return new Promise(resolve => {
    const srv = http.createServer((req, res) => {
      let body = ''; req.on('data', d => body += d);
      req.on('end', () => {
        stub.calls.push(body);
        const text = typeof stub.reply === 'function' ? stub.reply(body) : stub.reply;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ content: [{ type: 'text', text }] }));
      });
    }).listen(AI_PORT, () => resolve(srv));
  });
}

async function start(extraEnv = {}) {
  await resetDb();
  process.env.DATABASE_URL = `${PG}/${DB}`;
  const { buildFreshSchema, pool } = require('./fresh-db');
  await buildFreshSchema();
  const hash = await bcrypt.hash('pw', 4);
  const users = [
    ['Kody Slate', 'kody@test', 'rep'], ['Jack Greene', 'jack@test', 'rep'],
    ['Sean Conroy', 'sean@test', 'manager'], ['Daniel Compton', 'dan@test', 'rep']
  ];
  const ids = {};
  for (const [name, email, role] of users) {
    const r = await pool.query('INSERT INTO users (name,email,password,role) VALUES ($1,$2,$3,$4) RETURNING id', [name, email, hash, role]);
    ids[email.split('@')[0]] = r.rows[0].id;
  }
  const aiSrv = await startStubAI();
  const child = spawn(process.execPath, ['server.js'], {
    cwd: path.join(__dirname, '..', '..'),
    env: Object.assign({}, process.env, {
      DATABASE_URL: `${PG}/${DB}`, PORT: String(PORT), NODE_ENV: 'test', SESSION_SECRET: 'e2e',
      ANTHROPIC_API_KEY: 'stub', ANTHROPIC_BASE_URL: `http://127.0.0.1:${AI_PORT}`,
      TOGETHER_API_KEY: '', GOOGLE_PLACES_API_KEY: ''
    }, extraEnv),
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let log = '';
  child.stdout.on('data', d => log += d); child.stderr.on('data', d => log += d);
  for (let i = 0; i < 60; i++) {
    await new Promise(r => setTimeout(r, 500));
    if (/running on port/.test(log)) break;
  }
  if (!/running on port/.test(log)) { child.kill(); throw new Error('server did not start:\n' + log.slice(-2000)); }

  const base = `http://127.0.0.1:${PORT}`;
  async function login(who) {
    const r = await fetch(base + '/login', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: who + '@test', password: 'pw' }) });
    const cookie = (r.headers.get('set-cookie') || '').split(';')[0];
    if (!cookie) throw new Error('login failed for ' + who + ': ' + r.status + ' ' + await r.text());
    return async function api(method, url, body) {
      const res = await fetch(base + url, { method, headers: { 'Content-Type': 'application/json', Cookie: cookie },
        body: body === undefined ? undefined : JSON.stringify(body) });
      const text = await res.text();
      let json; try { json = JSON.parse(text); } catch (_) { json = text; }
      return { status: res.status, body: json };
    };
  }
  async function stop() { child.kill(); aiSrv.close(); await pool.end().catch(() => {}); }
  return { pool, ids, login, stop, stub, base, log: () => log };
}

module.exports = { start };
