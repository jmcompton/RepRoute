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

const DIST_ORDER = [
  { key: 'Dixie', label: 'Dixie' },
  { key: 'GSW',   label: 'Great Southern' },
  { key: 'Both',  label: 'Both' },
  { key: 'Other', label: 'Unspecified' }
];

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

const DETAIL_HEADER = ['Rep', 'Day', 'Stop #', 'Company', 'Address', 'City', 'ZIP',
  'Phone', 'Distributor', 'Visited', 'Visited Date', 'Outcome', 'Notes'];

function detailRow(s){
  return [
    s.rep || '', s.day || '', s.stop_order == null ? '' : s.stop_order,
    s.company || '', s.address || '', s.city || '', s.zip || '', s.phone || '',
    distLabel(distKey(s.source)),
    s.visited_at ? 'Yes' : 'No',
    fmtDate(s.visited_at),
    s.outcome || '', s.notes || ''
  ];
}

router.get('/report.xlsx', async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT rep, day, stop_order, company, address, city, zip, phone, status, source,
              visited_at, outcome, notes
         FROM fortress_promo_stops
        ORDER BY rep ASC, day ASC, stop_order ASC, id ASC`);
    const stops = r.rows;

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
        const g = set.filter(s => distKey(s.source) === d.key);
        if (!g.length) return;
        const v = g.filter(s => s.visited_at).length;
        summary.push([d.label, g.length, v, g.length - v, Math.round((v / g.length) * 100) + '%']);
      });
      const v = set.filter(s => s.visited_at).length;
      summary.push(['Total', set.length, v, set.length - v,
        set.length ? Math.round((v / set.length) * 100) + '%' : '']);
      summary.push([]);
    }

    summaryBlock('All reps', stops);
    reps.forEach(rep => summaryBlock(rep, stops.filter(s => s.rep === rep)));

    const wsSum = XLSX.utils.aoa_to_sheet(summary);
    wsSum['!cols'] = [{ wch: 26 }, { wch: 10 }, { wch: 10 }, { wch: 12 }, { wch: 12 }];
    XLSX.utils.book_append_sheet(wb, wsSum, safeSheetName('Summary', used));

    // ── One tab per rep + distributor ──
    reps.forEach(rep => {
      DIST_ORDER.forEach(d => {
        const set = stops.filter(s => s.rep === rep && distKey(s.source) === d.key);
        if (!set.length) return;
        const aoa = [DETAIL_HEADER].concat(set.map(detailRow));
        const ws = XLSX.utils.aoa_to_sheet(aoa);
        ws['!cols'] = [{ wch: 8 }, { wch: 6 }, { wch: 7 }, { wch: 34 }, { wch: 30 },
          { wch: 16 }, { wch: 8 }, { wch: 15 }, { wch: 15 }, { wch: 9 },
          { wch: 13 }, { wch: 18 }, { wch: 50 }];
        ws['!autofilter'] = { ref: XLSX.utils.encode_range({ s: { r: 0, c: 0 },
          e: { r: aoa.length - 1, c: DETAIL_HEADER.length - 1 } }) };
        XLSX.utils.book_append_sheet(wb, ws, safeSheetName(rep + ' - ' + d.label, used));
      });
    });

    // ── All stops tab ──
    const allAoa = [DETAIL_HEADER].concat(stops.map(detailRow));
    const wsAll = XLSX.utils.aoa_to_sheet(allAoa);
    wsAll['!cols'] = [{ wch: 8 }, { wch: 6 }, { wch: 7 }, { wch: 34 }, { wch: 30 },
      { wch: 16 }, { wch: 8 }, { wch: 15 }, { wch: 15 }, { wch: 9 },
      { wch: 13 }, { wch: 18 }, { wch: 50 }];
    wsAll['!autofilter'] = { ref: XLSX.utils.encode_range({ s: { r: 0, c: 0 },
      e: { r: allAoa.length - 1, c: DETAIL_HEADER.length - 1 } }) };
    XLSX.utils.book_append_sheet(wb, wsAll, safeSheetName('All Stops', used));

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
