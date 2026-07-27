'use strict';

// ── Fortress Railing Promo ───────────────────────────────────────────────────
// A self-contained, pre-routed dealer-visit campaign (separate from Accounts).
// Stops are seeded from fortress_promo_routes.csv (see db.js). This route only
// reads stops and records visits — it never re-optimizes or re-orders.

const express = require('express');
const router = express.Router();
const { pool } = require('../db');

// GET /api/fortress/stops[?rep=Kody]
// Returns campaign stops, ordered rep → day → stop_order. Optional rep filter.
router.get('/stops', async (req, res) => {
  try {
    const rep = (req.query.rep || '').trim();
    const params = [];
    let where = '';
    if (rep && rep.toLowerCase() !== 'all') { params.push(rep); where = 'WHERE rep = $1'; }
    const r = await pool.query(
      `SELECT id, rep, day, stop_order, company, address, city, zip, phone, status, source,
              lat, lng, visited_at, outcome, notes
         FROM fortress_promo_stops
         ${where}
        ORDER BY rep ASC, day ASC, stop_order ASC, id ASC`,
      params);
    res.json(r.rows);
  } catch (e) {
    console.error('[fortress/stops]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/fortress/stops/:id/visit  { outcome, notes, visited? }
// Log a visit on a stop. Sets visited_at = NOW() (or clears it when visited:false),
// plus outcome and notes. Returns the updated row.
router.post('/stops/:id/visit', async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!Number.isFinite(id)) return res.status(400).json({ error: 'invalid id' });
    const body = req.body || {};
    const outcome = body.outcome != null ? String(body.outcome).trim() : null;
    const notes = body.notes != null ? String(body.notes).trim() : null;
    const unvisit = body.visited === false; // allow undo
    const r = await pool.query(
      `UPDATE fortress_promo_stops
          SET visited_at = ${unvisit ? 'NULL' : 'NOW()'},
              outcome    = $1,
              notes      = $2
        WHERE id = $3
        RETURNING id, rep, day, stop_order, company, address, city, zip, phone, status, source,
                  lat, lng, visited_at, outcome, notes`,
      [unvisit ? null : (outcome || null), notes || null, id]);
    if (!r.rows.length) return res.status(404).json({ error: 'Stop not found' });
    res.json(r.rows[0]);
  } catch (e) {
    console.error('[fortress/visit]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/fortress/report.xlsx ────────────────────────────────────────────
// Detailed workbook of the promo. One Summary tab with the counts, one tab per
// rep + distributor combination holding the individual stops behind each count,
// and an All Stops tab for filtering. Distributor comes from the seeded `source`
// column; anything unrecognised buckets to "Unspecified" so totals reconcile.
const XLSX = require('xlsx');

// A stop seeded as "Both" carries both lines, so it is reported under Dixie AND
// under Great Southern. Per-line tabs therefore overlap and will not sum to the
// rep's total; the All Calls tab is the unique list.
const DIST_ORDER = [
  { key: 'Dixie', label: 'Dixie' },
  { key: 'GSW',   label: 'Great Southern' },
  { key: 'Other', label: 'Unspecified' }
];

function carries(stop, key){
  const k = distKey(stop.source);
  if (key === 'Dixie') return k === 'Dixie' || k === 'Both';
  if (key === 'GSW')   return k === 'GSW'   || k === 'Both';
  return k === 'Other';
}

function distKey(src){
  const s = String(src == null ? '' : src).trim().toLowerCase();
  if (s === 'dixie') return 'Dixie';
  if (s === 'gsw' || s === 'great southern' || s === 'gs') return 'GSW';
  if (s === 'both') return 'Both';
  return 'Other';
}
function distLabel(key){
  const d = DIST_ORDER.find(x => x.key === key);
  return d ? d.label : 'Unspecified';
}
function fmtDate(v){
  if (!v) return '';
  const d = new Date(v);
  if (isNaN(d.getTime())) return '';
  return d.toISOString().slice(0, 10);
}
// Excel caps sheet names at 31 chars and forbids : \ / ? * [ ]
function safeSheetName(name, used){
  let base = String(name).replace(/[:\\\/\?\*\[\]]/g, '-').slice(0, 31) || 'Sheet';
  let out = base, n = 2;
  while (used.has(out)) { out = base.slice(0, 28) + '~' + n; n++; }
  used.add(out);
  return out;
}

// ── Linking promo stops to Accounts ──────────────────────────────────────────
// The promo table is self-contained, so the rich voice-logged notes live on the
// matching Account (prospects), not on the stop. Company names differ between the
// two ("Circle A Fences, Inc." vs "Circle A Fence"), so phone is the primary key
// and normalised name + city is the fallback. Every row reports whether it matched
// so blank note columns can be told apart from genuine misses.
function normPhone(v){
  const d = String(v == null ? '' : v).replace(/\D/g, '');
  return d.length >= 10 ? d.slice(-10) : '';
}
// Space-insensitive so "Builders FirstSource" and "Builders First Source" agree.
function normName(v){
  return String(v == null ? '' : v)
    .toLowerCase()
    .replace(/[.,'"&\/-]/g, ' ')
    .replace(/\b(inc|llc|l l c|co|company|corp|corporation|the|of|and)\b/g, ' ')
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, '')
    .trim();
}
// A chain stop's branch is often only in its name ("... - Lake Oconee / Greensboro"),
// while the Account carries it as the city. Keep the full lowercased text of both so
// either can be searched.
function normText(v){
  return String(v == null ? '' : v).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}
function normCity(v){ return normText(v); }
function firstName(v){ return normText(v).split(' ')[0] || ''; }

// ── Stop ↔ Account assignment ────────────────────────────────────────────────
// A chain puts many stops behind one Account ("Builders FirstSource" x15 vs a single
// "Builders First Source" record), so this cannot be a per-stop lookup: matching each
// stop independently staples one Account's notes onto every branch. Instead every
// plausible (stop, account) pair is scored on location evidence and the best pairs are
// assigned one-to-one, greedily, highest score first. An Account is consumed once.
// Weak pairs are dropped — a blank note beats another store's note.
const MIN_SCORE = 4;

function nameCandidate(nStop, nAcct){
  if (!nStop || !nAcct) return false;
  if (nStop === nAcct) return true;
  const shorter = nStop.length < nAcct.length ? nStop : nAcct;
  if (shorter.length < 6) return false;           // too generic to trust
  return nStop.startsWith(nAcct) || nAcct.startsWith(nStop);
}

function noteDate(notes){
  const m = String(notes || '').match(/(\d{4}-\d{2}-\d{2})|([A-Z][a-z]{2} \d{1,2}, \d{4})/);
  if (!m) return null;
  const t = Date.parse(m[0]);
  return isNaN(t) ? null : t;
}

function scorePair(stop, a){
  let score = 0;
  const stopCity = normCity(stop.city);
  const stopText = normText(stop.company) + ' ' + stopCity;
  const aCity = normCity(a.city);
  if (aCity) {
    if (aCity === stopCity) score += 4;
    else if (stopText.indexOf(aCity) !== -1) score += 4;  // branch named in the stop
  }
  if (firstName(stop.rep) && firstName(a.rep_name) === firstName(stop.rep)) score += 2;
  const nd = noteDate(a.notes);
  const vis = stop.visited_at ? new Date(stop.visited_at).getTime() : null;
  if (nd && vis && Math.abs(nd - vis) <= 1000 * 60 * 60 * 24 * 14) score += 2;
  return score;
}

function buildMatches(stops, prospects){
  const byPhone = new Map();
  for (const a of prospects) {
    const ph = normPhone(a.phone);
    if (ph && !byPhone.has(ph)) byPhone.set(ph, a);
  }

  const result = new Map();          // stop -> { account, how }
  const usedAccounts = new Set();

  // Phone is decisive and consumes the account immediately.
  for (const st of stops) {
    const ph = normPhone(st.phone);
    if (ph && byPhone.has(ph)) {
      const a = byPhone.get(ph);
      if (!usedAccounts.has(a)) {
        usedAccounts.add(a);
        result.set(st, { account: a, how: 'Phone' });
      }
    }
  }

  // Score every remaining plausible pair, then assign best-first, one-to-one.
  const pairs = [];
  const nAcct = prospects.map(a => normName(a.company));
  for (const st of stops) {
    if (result.has(st)) continue;
    const nS = normName(st.company);
    prospects.forEach((a, i) => {
      if (usedAccounts.has(a)) return;
      if (!nameCandidate(nS, nAcct[i])) return;
      pairs.push({ st, a, score: scorePair(st, a), exact: nS === nAcct[i] });
    });
  }
  pairs.sort((x, y) => y.score - x.score);

  const stopCandidateCount = new Map();
  const acctCandidateCount = new Map();
  for (const pr of pairs) {
    stopCandidateCount.set(pr.st, (stopCandidateCount.get(pr.st) || 0) + 1);
    acctCandidateCount.set(pr.a, (acctCandidateCount.get(pr.a) || 0) + 1);
  }

  for (const pr of pairs) {
    if (result.has(pr.st) || usedAccounts.has(pr.a)) continue;
    // Accept on real location evidence, or when the pairing is unambiguous both ways.
    const unique = stopCandidateCount.get(pr.st) === 1 && acctCandidateCount.get(pr.a) === 1;
    if (pr.score >= MIN_SCORE) {
      usedAccounts.add(pr.a);
      result.set(pr.st, { account: pr.a, how: 'Name + location' });
    } else if (unique && pr.exact) {
      usedAccounts.add(pr.a);
      result.set(pr.st, { account: pr.a, how: 'Name (unique)' });
    }
  }

  // Explain the misses.
  for (const st of stops) {
    if (result.has(st)) continue;
    const n = stopCandidateCount.get(st) || 0;
    result.set(st, { account: null,
      how: n ? 'Too weak to trust — ' + n + ' possible account(s), none clearly this branch'
             : 'No account with a matching name' });
  }
  return result;
}

const DETAIL_HEADER = ['Rep', 'Day', 'Stop #', 'Company', 'Address', 'City', 'ZIP',
  'Phone', 'Distributor', 'Visited', 'Visited Date', 'Outcome', 'Notes'];

// Notes come from the matched Account (where the voice logger writes the detailed
// write-up). If a stop has no matching Account, fall back to whatever the rep typed
// on the stop itself so that text is never silently dropped.
function detailRow(s, matches){
  const a = (matches.get(s) || {}).account;
  const notes = (a && String(a.notes || '').trim()) || s.notes || '';
  return [
    s.rep || '', s.day || '', s.stop_order == null ? '' : s.stop_order,
    s.company || '', s.address || '', s.city || '', s.zip || '', s.phone || '',
    distLabel(distKey(s.source)),
    s.visited_at ? 'Yes' : 'No',
    fmtDate(s.visited_at),
    s.outcome || '', notes
  ];
}

const CALL_HEADER = ['Rep', 'Stop Company', 'Account Name', 'Call Date', 'Call Type',
  'Outcome', 'Products Discussed', 'Next Step', 'Next Step Date', 'Call Notes'];

router.get('/report.xlsx', async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT rep, day, stop_order, company, address, city, zip, phone, status, source,
              visited_at, outcome, notes
         FROM fortress_promo_stops
        ORDER BY rep ASC, day ASC, stop_order ASC, id ASC`);
    const stops = r.rows;

    // Accounts carry the voice-logged notes and contact detail the promo table lacks.
    const pr = await pool.query(
      `SELECT p.id, p.company, p.contact, p.email, p.phone, p.city, p.state, p.status,
              p.priority, p.pipeline_stage, p.source, p.products, p.notes,
              u.name AS rep_name
         FROM prospects p
         LEFT JOIN users u ON u.id = p.user_id`);
    const matches = buildMatches(stops, pr.rows);

    const cl = await pool.query(
      `SELECT c.prospect_id, c.call_date, c.call_type, c.outcome, c.products_discussed,
              c.next_step, c.next_step_date, c.notes, p.company AS account_company
         FROM calls c
         JOIN prospects p ON p.id = c.prospect_id
        ORDER BY c.call_date DESC, c.id DESC`);
    const callsByProspect = new Map();
    for (const c of cl.rows) {
      if (!callsByProspect.has(c.prospect_id)) callsByProspect.set(c.prospect_id, []);
      callsByProspect.get(c.prospect_id).push(c);
    }

    const wb = XLSX.utils.book_new();
    const used = new Set();
    const reps = [...new Set(stops.map(s => s.rep).filter(Boolean))].sort();

    // ── Summary tab ──
    const summary = [['Fortress Railing Promo — Distributor Report'],
      ['Generated', fmtDate(new Date())], []];

    function summaryBlock(title, set){
      summary.push([title]);
      summary.push(['Distributor', 'Stops', 'Visited', 'Remaining', 'Complete %']);
      DIST_ORDER.forEach(d => {
        const g = set.filter(s => carries(s, d.key));
        if (!g.length) return;
        const v = g.filter(s => s.visited_at).length;
        summary.push([d.label, g.length, v, g.length - v, Math.round((v / g.length) * 100) + '%']);
      });
      const v = set.filter(s => s.visited_at).length;
      summary.push(['Total (unique stops)', set.length, v, set.length - v,
        set.length ? Math.round((v / set.length) * 100) + '%' : '']);
      const dual = set.filter(s => distKey(s.source) === 'Both').length;
      if (dual) summary.push([dual + ' of these dealers carry both lines and are counted under ' +
        'Dixie and Great Southern, so the rows above overlap and will not add up to ' + set.length + '.']);
      summary.push([]);
    }

    // Match rate up top: blank note columns should be explainable, not mysterious.
    const mstats = stops.map(st => matches.get(st) || {});
    const matched = mstats.filter(m => m.account).length;
    const withNotes = mstats.filter(m => m.account && String(m.account.notes || '').trim()).length;
    summary.push(['Account matching']);
    summary.push(['Stops linked to an Account', matched + ' of ' + stops.length]);
    summary.push(['Linked accounts carrying notes', withNotes]);
    summary.push(['Matched by phone', mstats.filter(m => m.how === 'Phone').length]);
    summary.push(['Matched by name + location', mstats.filter(m => m.how === 'Name + location').length]);
    summary.push(['Matched by unique name', mstats.filter(m => m.how === 'Name (unique)').length]);
    summary.push(['No match found', mstats.filter(m => !m.account).length]);
    summary.push([]);

    summaryBlock('All reps', stops);
    reps.forEach(rep => summaryBlock(rep, stops.filter(s => s.rep === rep)));

    const wsSum = XLSX.utils.aoa_to_sheet(summary);
    wsSum['!cols'] = [{ wch: 26 }, { wch: 10 }, { wch: 10 }, { wch: 12 }, { wch: 12 }];
    XLSX.utils.book_append_sheet(wb, wsSum, safeSheetName('Summary', used));

    // ── One tab per rep + distributor ──
    const COLS = [{ wch: 8 }, { wch: 6 }, { wch: 7 }, { wch: 34 }, { wch: 30 },
      { wch: 16 }, { wch: 8 }, { wch: 15 }, { wch: 15 }, { wch: 9 },
      { wch: 13 }, { wch: 18 }, { wch: 120 }];

    function addDetailTab(name, set){
      if (!set.length) return;
      const aoa = [DETAIL_HEADER].concat(set.map(st => detailRow(st, matches)));
      const ws = XLSX.utils.aoa_to_sheet(aoa);
      ws['!cols'] = COLS;
      ws['!autofilter'] = { ref: XLSX.utils.encode_range({ s: { r: 0, c: 0 },
        e: { r: aoa.length - 1, c: DETAIL_HEADER.length - 1 } }) };
      XLSX.utils.book_append_sheet(wb, ws, safeSheetName(name, used));
    }

    // Per rep: one tab per line (inclusive of dual-line dealers), then all calls.
    reps.forEach(rep => {
      const mine = stops.filter(s => s.rep === rep);
      DIST_ORDER.forEach(d => addDetailTab(rep + ' - ' + d.label, mine.filter(s => carries(s, d.key))));
      addDetailTab(rep + ' - All Calls', mine);
    });

    // ── All stops tab ──
    addDetailTab('All Stops', stops);

    // ── Unmatched tab: every stop with no Account note, and why ──
    const unmatched = stops.map(st => ({ st, m: matches.get(st) || {} }))
      .filter(x => !x.m.account || !String(x.m.account.notes || '').trim());
    if (unmatched.length) {
      const aoa = [['Rep', 'Day', 'Stop #', 'Company', 'City', 'Phone', 'Reason']]
        .concat(unmatched.map(x => [x.st.rep || '', x.st.day || '',
          x.st.stop_order == null ? '' : x.st.stop_order, x.st.company || '',
          x.st.city || '', x.st.phone || '',
          x.m.account ? 'Account matched but has no notes' : x.m.how]));
      const ws = XLSX.utils.aoa_to_sheet(aoa);
      ws['!cols'] = [{ wch: 8 }, { wch: 6 }, { wch: 7 }, { wch: 40 }, { wch: 18 },
        { wch: 15 }, { wch: 46 }];
      XLSX.utils.book_append_sheet(wb, ws, safeSheetName('Unmatched', used));
    }

    // ── Call Log: one row per logged call on a matched account ──
    const callRows = [];
    for (const st of stops) {
      const m = matches.get(st) || {};
      if (!m.account) continue;
      for (const c of (callsByProspect.get(m.account.id) || [])) {
        callRows.push([st.rep || '', st.company || '', c.account_company || '',
          fmtDate(c.call_date), c.call_type || '', c.outcome || '',
          c.products_discussed || '', c.next_step || '', fmtDate(c.next_step_date),
          c.notes || '']);
      }
    }
    if (callRows.length) {
      const aoa = [CALL_HEADER].concat(callRows);
      const ws = XLSX.utils.aoa_to_sheet(aoa);
      ws['!cols'] = [{ wch: 8 }, { wch: 34 }, { wch: 34 }, { wch: 12 }, { wch: 18 },
        { wch: 16 }, { wch: 26 }, { wch: 26 }, { wch: 14 }, { wch: 90 }];
      ws['!autofilter'] = { ref: XLSX.utils.encode_range({ s: { r: 0, c: 0 },
        e: { r: aoa.length - 1, c: CALL_HEADER.length - 1 } }) };
      XLSX.utils.book_append_sheet(wb, ws, safeSheetName('Call Log', used));
    }

    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    const fname = 'fortress-promo-report-' + fmtDate(new Date()) + '.xlsx';
    res.setHeader('Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="' + fname + '"');
    res.send(buf);
  } catch (e) {
    console.error('[fortress/report.xlsx]', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
