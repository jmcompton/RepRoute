'use strict';

// ── Fortress Railing Promo ───────────────────────────────────────────────────
// A self-contained, pre-routed dealer-visit campaign (separate from Accounts).
// Stops are seeded from fortress_promo_routes.csv (see db.js). This route only
// reads stops and records visits — it never re-optimizes or re-orders.

const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const XLSX = require('xlsx');

function fdate(d){ if(!d) return ''; var x=new Date(d); if(isNaN(x.getTime())) return ''; return x.getFullYear()+'-'+String(x.getMonth()+1).padStart(2,'0')+'-'+String(x.getDate()).padStart(2,'0'); }

// GET /api/fortress/export[?rep=Kody] — Excel (.xlsx) of the promo stops with
// every rep's outcomes and call notes. Optional rep filter (default: everyone).
router.get('/export', async (req, res) => {
  try {
    const rep = (req.query.rep || '').trim();
    const params = [];
    let where = '';
    if (rep && rep.toLowerCase() !== 'all') { params.push(rep); where = 'WHERE f.rep = $1'; }
    const r = await pool.query(
      `SELECT f.rep, f.day, f.stop_order, f.company, f.address, f.city, f.zip, f.phone, f.status, f.visited_at, f.outcome,
              COALESCE(NULLIF(btrim(f.notes), ''), cn.call_notes, '') AS notes
         FROM fortress_promo_stops f
         LEFT JOIN LATERAL (
           -- Reps write their real notes in the call log (voice or typed), not on
           -- the promo stop. Pull them by matching the dealer name (ignoring a
           -- "- Location" suffix and dash style) and the rep who logged the call.
           SELECT string_agg(to_char(c.call_date, 'YYYY-MM-DD') || ': ' || c.notes, E'\n') AS call_notes
             FROM calls c
             JOIN prospects p ON c.prospect_id = p.id
             LEFT JOIN users u ON c.user_id = u.id
            WHERE c.notes IS NOT NULL AND btrim(c.notes) <> ''
              AND lower(split_part(regexp_replace(p.company, '[–—]', '-', 'g'), ' - ', 1))
                = lower(split_part(regexp_replace(f.company, '[–—]', '-', 'g'), ' - ', 1))
              AND (u.name IS NULL OR lower(u.name) LIKE '%' || lower(f.rep) || '%')
         ) cn ON true
         ${where}
        ORDER BY f.rep ASC, f.day ASC, f.stop_order ASC, f.id ASC`, params);
    const header = ['Rep','Day','Stop #','Company','Address','City','Zip','Phone','Status','Visited','Outcome','Notes'];
    const rows = r.rows.map(function(s){ return [
      s.rep||'', s.day||'', s.stop_order||'', s.company||'', s.address||'', s.city||'',
      s.zip||'', s.phone||'', s.status||'', fdate(s.visited_at), s.outcome||'', s.notes||''
    ]; });
    const visited = r.rows.filter(function(s){ return s.visited_at; }).length;
    const title = 'Fortress Railing Promo' + (rep && rep.toLowerCase() !== 'all' ? (' — ' + rep) : '') + ' — Call Report';
    const meta = 'Generated ' + fdate(new Date()) + '     Stops: ' + rows.length + '     Visited: ' + visited;
    const aoa = [[title],[meta],[],header].concat(rows);
    const ws = XLSX.utils.aoa_to_sheet(aoa);
    ws['!cols'] = [{wch:10},{wch:14},{wch:7},{wch:28},{wch:26},{wch:14},{wch:8},{wch:14},{wch:13},{wch:12},{wch:14},{wch:46}];
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Fortress Promo');
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    const fname = 'fortress_promo' + (rep && rep.toLowerCase() !== 'all' ? ('_' + rep.replace(/[^a-z0-9]+/gi,'_')) : '') + '_report.xlsx';
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="' + fname + '"');
    res.send(buf);
  } catch (e) {
    console.error('[fortress/export]', e.message);
    res.status(500).json({ error: e.message });
  }
});

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

module.exports = router;
