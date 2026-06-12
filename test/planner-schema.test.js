'use strict';

// ── Planner schema-sanity test ───────────────────────────────────────────────
// Regression guard for the "column does not exist in prod" class of bug. The
// mock-pool harness can't catch schema drift because mocks never validate column
// names. This test instead reads db.js (the SOURCE OF TRUTH for what exists on
// Railway) and asserts every column the build-week / fill-day candidate queries
// depend on is actually declared there. Schema drift fails the build, not prod.
//
//   npm test
//
// HOW IT WORKS: we parse db.js's CREATE TABLE bodies + `ALTER TABLE ... ADD
// COLUMN` statements into a {table: Set(columns)} map, then check REQUIRED below.
// REQUIRED must be kept in sync with the columns referenced in routes/planner.js
// gatherRankedCandidates(), loadManualAnchors(), visitedThisWeek(), and the
// build-week/fill-day inline SQL.

const fs = require('fs');
const path = require('path');

const DB_FILE = path.join(__dirname, '..', 'db.js');

// Columns the NEW geographic build-week / fill-day path selects or filters on.
const REQUIRED = {
  prospects:        ['id', 'company', 'city', 'address', 'user_id', 'pipeline_stage'],
  account_lines:    ['account_id', 'total_commission'],
  commission_lines: ['account_id', 'period_start'],
  planner_items:    ['id', 'rep_id', 'planned_date', 'item_type', 'account_id'],
  planner_anchors:  ['rep_id', 'anchor_date', 'city'],
  calls:            ['user_id', 'prospect_id', 'call_date'],
  users:            ['id', 'territory', 'home_base_lat', 'home_base_lng'],
};

// Reserved table-level constraint keywords that begin a non-column line.
const NON_COLUMN = /^(PRIMARY|FOREIGN|UNIQUE|CHECK|CONSTRAINT|EXCLUDE|LIKE)\b/i;

// Build {table -> Set(columns)} from db.js text.
function parseSchema(src) {
  const schema = {};
  const add = (t, c) => {
    const tbl = t.toLowerCase(), col = c.toLowerCase();
    (schema[tbl] || (schema[tbl] = new Set())).add(col);
  };

  // CREATE TABLE [IF NOT EXISTS] <name> ( <body> );  — match balanced-ish body.
  const createRe = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*\(([\s\S]*?)\n\s*\);/gi;
  let m;
  while ((m = createRe.exec(src))) {
    const table = m[1];
    const body = m[2];
    for (let raw of body.split('\n')) {
      const line = raw.trim().replace(/,$/, '');
      if (!line || line.startsWith('--')) continue;
      if (NON_COLUMN.test(line)) continue;
      const col = (line.match(/^([A-Za-z_][A-Za-z0-9_]*)/) || [])[1];
      if (col) add(table, col);
    }
  }

  // ALTER TABLE <name> ADD COLUMN [IF NOT EXISTS] <col> ...
  const alterRe = /ALTER\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)\s+ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)/gi;
  while ((m = alterRe.exec(src))) add(m[1], m[2]);

  return schema;
}

const failures = [];
function check(label, cond, detail) {
  if (cond) { console.log('  ok  - ' + label); }
  else { failures.push(label + (detail ? ' — ' + detail : '')); console.log('  XX  - ' + label + (detail ? ' — ' + detail : '')); }
}

console.log('Planner schema-sanity test → ' + DB_FILE);
const src = fs.readFileSync(DB_FILE, 'utf8');
const schema = parseSchema(src);

for (const [table, cols] of Object.entries(REQUIRED)) {
  const have = schema[table];
  check('table "' + table + '" is defined in db.js', !!have, 'not found in schema');
  if (!have) continue;
  for (const col of cols) {
    check(table + '.' + col + ' exists', have.has(col.toLowerCase()),
      'planner SQL references it but db.js does not declare it');
  }
}

console.log('');
if (failures.length) {
  console.error('SCHEMA DRIFT (' + failures.length + '):');
  failures.forEach(f => console.error('  - ' + f));
  process.exit(1);
}
console.log('PASS — every planner candidate column exists in db.js schema.');
process.exit(0);
