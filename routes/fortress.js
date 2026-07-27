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
function normName(v){
  return String(v == null ? '' : v)
    .toLowerCase()
    .replace(/[.,'"&]/g, ' ')
    .replace(/\b(inc|llc|l l c|co|company|corp|corporation|the|of|and)\b/g, ' ')
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function normCity(v){ return String(v == null ? '' : v).toLowerCase().trim(); }

function buildAccountIndex(prospects){
  const byPhone = new Map();
  const byNameCity = new Map();
  const byName = new Map();
  for (const p of prospects) {
    const ph = normPhone(p.phone);
    if (ph && !byPhone.has(ph)) byPhone.set(ph, p);
    const n = normName(p.company);
    if (!n) continue;
    const nc = n + '|' + normCity(p.city);
    if (!byNameCity.has(nc)) byNameCity.set(nc, p);
    if (!byName.has(n)) byName.set(n, p);
  }
  return { byPhone, byNameCity, byName };
}

// Returns { account, how } — how is why it matched, for auditing bad joins.
function matchAccount(stop, idx){
  const ph = normPhone(stop.phone);
  if (ph && idx.byPhone.has(ph)) return { account: idx.byPhone.get(ph), how: 'Phone' };
  const n = normName(stop.company);
  if (n) {
    const nc = n + '|' + normCity(stop.city);
    if (idx.byNameCity.has(nc)) return { account: idx.byNameCity.get(nc), how: 'Name + city' };
    if (idx.byName.has(n)) return { account: idx.byName.get(n), how: 'Name' };
  }
  return { account: null, how: 'No match' };
}

const DETAIL_HEADER = ['Rep', 'Day', 'Stop #', 'Company', 'Address', 'City', 'ZIP',
  'Phone', 'Distributor', 'Visited', 'Visited Date', 'Outcome', 'Stop Note',
  'Matched', 'Matched On', 'Account Name', 'Contact Name', 'Account Email',
  'Account Phone', 'Account City', 'Account State', 'Account Status',
  'Lead Source', 'Priority', 'Pipeline Stage', 'Products', 'Account Notes'];

function detailRow(s, idx){
  const m = matchAccount(s, idx);
  const a = m.account || {};
  return [
    s.rep || '', s.day || '', s.stop_order == null ? '' : s.stop_order,
    s.company || '', s.address || '', s.city || '', s.zip || '', s.phone || '',
    distLabel(distKey(s.source)),
    s.visited_at ? 'Yes' : 'No',
    fmtDate(s.visited_at),
    s.outcome || '', s.notes || '',
    m.account ? 'Yes' : 'No', m.how,
    a.company || '', a.contact || '', a.email || '', a.phone || '',
    a.city || '', a.state || '', a.status || '', a.source || '',
    a.priority || '', a.pipeline_stage || '', a.products || '', a.notes || ''
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
      `SELECT id, company, contact, email, phone, city, state, status, priority,
              pipeline_stage, source, products, notes
         FROM prospects`);
    const idx = buildAccountIndex(pr.rows);

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
    const matches = stops.map(st => matchAccount(st, idx));
    const matched = matches.filter(m => m.account).length;
    const withNotes = matches.filter(m => m.account && String(m.account.notes || '').trim()).length;
    summary.push(['Account matching']);
    summary.push(['Stops linked to an Account', matched + ' of ' + stops.length]);
    summary.push(['Linked accounts carrying notes', withNotes]);
    summary.push(['Matched by phone', matches.filter(m => m.how === 'Phone').length]);
    summary.push(['Matched by name + city', matches.filter(m => m.how === 'Name + city').length]);
    summary.push(['Matched by name only', matches.filter(m => m.how === 'Name').length]);
    summary.push(['No match found', matches.filter(m => m.how === 'No match').length]);
    summary.push([]);

    summaryBlock('All reps', stops);
    reps.forEach(rep => summaryBlock(rep, stops.filter(s => s.rep === rep)));

    const wsSum = XLSX.utils.aoa_to_sheet(summary);
    wsSum['!cols'] = [{ wch: 26 }, { wch: 10 }, { wch: 10 }, { wch: 12 }, { wch: 12 }];
    XLSX.utils.book_append_sheet(wb, wsSum, safeSheetName('Summary', used));

    // ── One tab per rep + distributor ──
    const COLS = [{ wch: 8 }, { wch: 6 }, { wch: 7 }, { wch: 34 }, { wch: 30 },
      { wch: 16 }, { wch: 8 }, { wch: 15 }, { wch: 15 }, { wch: 9 },
      { wch: 13 }, { wch: 18 }, { wch: 30 }, { wch: 9 }, { wch: 14 },
      { wch: 34 }, { wch: 22 }, { wch: 30 }, { wch: 15 }, { wch: 16 },
      { wch: 8 }, { wch: 14 }, { wch: 16 }, { wch: 10 }, { wch: 16 },
      { wch: 24 }, { wch: 90 }];

    function addDetailTab(name, set){
      if (!set.length) return;
      const aoa = [DETAIL_HEADER].concat(set.map(st => detailRow(st, idx)));
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

    // ── Call Log: one row per logged call on a matched account ──
    const callRows = [];
    for (const st of stops) {
      const m = matchAccount(st, idx);
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
