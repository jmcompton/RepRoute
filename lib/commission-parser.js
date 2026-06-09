'use strict';

// ── Commission statement parser ──────────────────────────────────────────────
// Thin dispatcher + per-format workers. Today only the Trilogy "XtraReport"
// Payment Detail Report is supported. Architected so additional formats (or a
// future AI fallback) slot in as new branches — but NONE are built here.

const XLSX = require('xlsx');

// Parse a base64 string / data-URL / Buffer into a workbook (mirrors zoho.js).
function toBuffer(input) {
  if (Buffer.isBuffer(input)) return input;
  const s = String(input || '');
  const raw = s.includes(',') ? s.split(',')[1] : s;
  return Buffer.from(raw, 'base64');
}

// Coerce a cell to a number; blank/garbage → null.
function num(v) {
  if (v === '' || v === null || v === undefined) return null;
  if (typeof v === 'number') return v;
  const n = parseFloat(String(v).replace(/[$,\s]/g, ''));
  return Number.isFinite(n) ? n : null;
}

function cell(row, i) {
  const v = row[i];
  return v === undefined || v === null ? '' : (typeof v === 'string' ? v.trim() : v);
}

// "M/D/YYYY" → "YYYY-MM-DD" (null on failure).
function toISO(mdy) {
  if (!mdy) return null;
  const m = String(mdy).trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!m) return null;
  const [, mo, da, yr] = m;
  return `${yr}-${String(mo).padStart(2, '0')}-${String(da).padStart(2, '0')}`;
}

// Rows we never treat as a manufacturer-group header even though col A is set.
const NON_GROUP_A = new Set(['manufacturer', 'payment detail report', 'salesrep commissions']);

// Flag adjustment / non-account lines (e.g. "Freight").
function isAdjustmentCustomer(name) {
  return /\bfreight\b/i.test(name);
}

// ── Trilogy worker ───────────────────────────────────────────────────────────
function parseTrilogyReport(buffer) {
  const wb = XLSX.read(buffer, { type: 'buffer', cellDates: true, cellText: false });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '', raw: true });

  // Header metadata
  // Row 1 (idx 0): A = "Payment Detail Report", G = "M/D/YYYY to M/D/YYYY"
  // Row 2 (idx 1): B = rep name
  let period_start = null, period_end = null;
  const dateCellRaw = String(cell(rows[0] || [], 6) || '');
  const dm = dateCellRaw.match(/(\d{1,2}\/\d{1,2}\/\d{4})\s+to\s+(\d{1,2}\/\d{1,2}\/\d{4})/i);
  if (dm) { period_start = toISO(dm[1]); period_end = toISO(dm[2]); }
  const rep_name = String(cell(rows[1] || [], 1) || '').trim() || null;

  const lines = [];
  let currentManufacturer = null;

  for (let i = 2; i < rows.length; i++) {
    const r = rows[i] || [];
    const A = cell(r, 0), B = cell(r, 1), C = cell(r, 2);
    const D = num(r[3]), F = num(r[5]);

    // Manufacturer-group header: col A populated, not a known label.
    if (A !== '') {
      const a = String(A).toLowerCase().trim();
      if (NON_GROUP_A.has(a)) continue;          // header / report title rows
      if (a.startsWith('printed on')) continue;  // footer
      currentManufacturer = String(A).trim();    // new group boundary
      continue;
    }

    // Detail row: a manufacturer in col B AND a customer in col C.
    if (B !== '' && C !== '') {
      lines.push({
        manufacturer: currentManufacturer || String(B).trim(),
        customer_raw: String(C).trim(),
        sales: D == null ? 0 : D,
        commission: F == null ? 0 : F,       // col F = Sales Rep Commissions
        is_adjustment: isAdjustmentCustomer(String(C)),
      });
      continue;
    }

    // Anything else (subtotal: only D/F; grand total: only F; blanks) → skip.
  }

  const totals = lines.reduce(
    (acc, l) => {
      acc.sales += l.sales;
      acc.commission += l.commission;
      return acc;
    },
    { sales: 0, commission: 0 }
  );
  // Round to cents to avoid float drift.
  totals.sales = Math.round(totals.sales * 100) / 100;
  totals.commission = Math.round(totals.commission * 100) / 100;

  return { rep_name, period_start, period_end, lines, totals };
}

// ── Dispatcher ───────────────────────────────────────────────────────────────
function parseCommissionReport(input, format = 'trilogy') {
  const buffer = toBuffer(input);
  switch ((format || 'trilogy').toLowerCase()) {
    case 'trilogy':
      return parseTrilogyReport(buffer);
    default:
      throw new Error(`Unsupported commission report format: ${format}`);
  }
}

module.exports = { parseCommissionReport, parseTrilogyReport };
