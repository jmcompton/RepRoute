'use strict';

// ── Trilogy "Payment Detail Report" PDF parser ───────────────────────────────
// Parses the commission PDF (e.g. DCAPRIL2026.pdf) into structured rows using
// pdfjs-dist. Strategy: pull each page's text items WITH positions, group them
// into visual lines by y (small tolerance so split "Totals:" labels re-join
// their amounts), sort within a line by x. Then walk the line stream:
//   • manufacturer/line name  = the non-empty line immediately ABOVE a column
//     header line (detected by forward lookahead).
//   • data row                = a line ending in THREE currency amounts.
//   • wrapped customer city/st = a following no-amount line, appended to the row
//     ONLY when the customer buffer still ends with a comma (this cleanly skips
//     wrapped MANUFACTURER fragments like "Hardware/Zalcow", whose row customer
//     has no trailing comma).
// Reconciliation: per-manufacturer parsed sums are compared to each printed
// "<Mfr> Totals:" line, and the overall sums to "Grand Totals:". Any mismatch
// over $0.01 fails the import.
//
// pdfjs-dist v4 is ESM-only; we load it via dynamic import() from CommonJS.

const Y_TOL = 3;            // px: merge items within this y-distance into one line
const RECON_EPS = 0.01;     // $: reconciliation tolerance

// A currency token: $1,234.56  /  $0.00  /  ($25.25) [parenthesized = negative].
const MONEY_RX = /^\(?\$[\d,]+\.\d{2}\)?$/;

function isMoney(tok) { return MONEY_RX.test(String(tok || '').trim()); }

// "$1,234.56" → 1234.56 ; "($25.25)" → -25.25
function parseMoney(tok) {
  const s = String(tok || '').trim();
  const neg = /^\(/.test(s) || /\)$/.test(s);
  const n = parseFloat(s.replace(/[()$,]/g, ''));
  if (!Number.isFinite(n)) return 0;
  return neg ? -n : n;
}

function round2(n) { return Math.round((Number(n) || 0) * 100) / 100; }
function norm(s) { return String(s || '').toLowerCase().replace(/\s+/g, ' ').trim(); }

// ── Line classification helpers ──────────────────────────────────────────────
const RX_DATE_RANGE = /^\d{1,2}\/\d{1,2}\/\d{4}\s+to\s+\d{1,2}\/\d{1,2}\/\d{4}$/;
const RX_PAGE = /^Page\s+\d+\s+of\s+\d+$/i;

function isColumnHeader(text) {
  const t = norm(text);
  return t.startsWith('manufacturer') && t.includes('invoice number') && t.includes('sales rep commissions');
}
function isSkipLine(text) {
  const t = String(text || '').trim();
  if (!t) return true;
  if (RX_DATE_RANGE.test(t)) return true;
  if (/^Payment Detail Report\b/i.test(t)) return true;
  if (/^Salesrep Commissions$/i.test(t)) return true;
  if (/^Printed on\b/i.test(t)) return true;
  if (RX_PAGE.test(t)) return true;
  return false;
}
function isTotalsLine(text) { return /\bTotals:/i.test(String(text || '')); }

// ── Extract visual lines from the PDF buffer ─────────────────────────────────
// Returns a flat, page-ordered array of { tokens:[str], text } lines.
async function extractLines(buffer) {
  const { getDocument } = await import('pdfjs-dist/legacy/build/pdf.mjs');
  const data = new Uint8Array(buffer);
  const doc = await getDocument({ data, useSystemFonts: true, isEvalSupported: false }).promise;

  const allLines = [];
  for (let p = 1; p <= doc.numPages; p++) {
    const page = await doc.getPage(p);
    const tc = await page.getTextContent();

    // Bucket items into visual lines by y with a small tolerance.
    const buckets = []; // { y, items:[{x,str}] }
    for (const it of tc.items) {
      if (!it.str || !String(it.str).trim()) continue;
      const x = it.transform[4], y = it.transform[5];
      let b = null;
      for (const cand of buckets) { if (Math.abs(cand.y - y) <= Y_TOL) { b = cand; break; } }
      if (!b) { b = { y, items: [] }; buckets.push(b); }
      b.items.push({ x, str: it.str });
    }
    buckets.sort((a, b) => b.y - a.y); // top → bottom
    for (const b of buckets) {
      const tokens = b.items.sort((a, c) => a.x - c.x)
        .map(o => String(o.str).trim()).filter(Boolean);
      if (!tokens.length) continue;
      allLines.push({ tokens, text: tokens.join(' ') });
    }
  }
  await doc.destroy();
  return allLines;
}

// ── Main parse ───────────────────────────────────────────────────────────────
async function parsePdfCommissionReport(buffer) {
  const lines = await extractLines(buffer);

  // (1) Date range / period.
  let dateRange = null;
  for (const l of lines) { if (RX_DATE_RANGE.test(l.text)) { dateRange = l.text; break; } }
  if (!dateRange) throw new Error('Could not find the "M/D/YYYY to M/D/YYYY" date range — is this a Trilogy Payment Detail Report?');
  const m = dateRange.match(/to\s+(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  const endMonth = parseInt(m[1]), endYear = parseInt(m[3]);
  const period = `${endYear}-${String(endMonth).padStart(2, '0')}`;          // "2026-04"
  const period_end = `${period}-${String(parseInt(m[2])).padStart(2, '0')}`; // last invoice day
  const period_start = `${period}-01`;

  // (2) Rep name = the line directly above the first "Salesrep Commissions".
  let rep_name = null;
  for (let i = 0; i < lines.length; i++) {
    if (/^Salesrep Commissions$/i.test(lines[i].text)) { if (i > 0) rep_name = lines[i - 1].text.trim(); break; }
  }
  if (!rep_name) throw new Error('Could not detect the sales rep name (no line above "Salesrep Commissions").');

  // (3) Walk the line stream.
  const dataRows = [];          // { manufacturer, customer_raw, invoice, total_sales, total_commission, is_adjustment }
  const printedMfrTotals = [];  // { manufacturer, sales, commission }
  let printedGrand = null;      // { sales, commission }

  let currentMfr = null;
  let lastRow = null;           // for wrapped-customer continuation
  const mfrSums = new Map();    // currentMfr → { sales, commission } (incl. adjustments)

  function addToMfrSum(name, sales, commission) {
    const cur = mfrSums.get(name) || { sales: 0, commission: 0 };
    cur.sales = round2(cur.sales + sales);
    cur.commission = round2(cur.commission + commission);
    mfrSums.set(name, cur);
  }

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const T = line.tokens;
    const text = line.text;

    if (isColumnHeader(text)) { lastRow = null; continue; }
    if (isSkipLine(text)) continue;

    // A line ending in 3 currency tokens is either a data row or a "Totals:" line.
    const endsWith3Money = T.length >= 3 && isMoney(T[T.length - 1]) && isMoney(T[T.length - 2]) && isMoney(T[T.length - 3]);

    if (isTotalsLine(text) && endsWith3Money) {
      const sales = parseMoney(T[T.length - 3]);
      const commission = parseMoney(T[T.length - 1]); // 3rd = Sales Rep Commissions
      if (/^Grand Totals:/i.test(text)) {
        printedGrand = { sales, commission };
      } else if (rep_name && new RegExp('^' + rep_name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s+Totals:', 'i').test(text)) {
        // Rep-section total — captured implicitly by grand; no per-mfr compare.
      } else {
        // "<Manufacturer> Totals:" — the printed subtotal for the current block.
        const label = text.replace(/\s*Totals:.*/i, '').trim();
        printedMfrTotals.push({ manufacturer: currentMfr || label, label, sales, commission });
      }
      lastRow = null;
      continue;
    }

    if (endsWith3Money) {
      // DATA ROW. amounts = last 3; invoice = token before them.
      const total_sales = parseMoney(T[T.length - 3]);
      const total_commission = parseMoney(T[T.length - 1]);
      const invoice = T[T.length - 4] != null ? T[T.length - 4] : null;

      // Everything before the invoice = leading manufacturer tokens + customer.
      const leading = T.slice(0, Math.max(0, T.length - 4));
      // Strip leading tokens while they still form a prefix of the block mfr name.
      const lead = leading.slice();
      const mfrNorm = norm(currentMfr || '');
      let acc = '';
      while (lead.length) {
        const cand = (acc ? acc + ' ' : '') + lead[0];
        if (mfrNorm && mfrNorm.startsWith(norm(cand))) { acc = cand; lead.shift(); } else break;
      }
      const customer_raw = lead.join(' ').replace(/\s+/g, ' ').trim();

      const row = {
        manufacturer: currentMfr || (acc || ''),
        customer_raw,
        invoice,
        total_sales,
        total_commission,
        is_adjustment: /^freight$/i.test(customer_raw),
      };
      dataRows.push(row);
      addToMfrSum(row.manufacturer, total_sales, total_commission);
      lastRow = row;
      continue;
    }

    // NON-AMOUNT line: either a manufacturer TITLE (next non-skip line is the
    // column header) or a wrapped continuation of the previous customer.
    let j = i + 1;
    while (j < lines.length && isSkipLine(lines[j].text)) j++;
    const nextIsHeader = j < lines.length && isColumnHeader(lines[j].text);

    if (nextIsHeader) {
      currentMfr = text.trim();          // start a new manufacturer block
      lastRow = null;
    } else if (lastRow && /,\s*$/.test(lastRow.customer_raw)) {
      // Wrapped customer city/state — append (buffer ended with a comma).
      lastRow.customer_raw = (lastRow.customer_raw + ' ' + text).replace(/\s+/g, ' ').trim();
    }
    // else: stray fragment (e.g. wrapped manufacturer cell) — ignored.
  }

  // (4) Reconciliation.
  const mismatches = [];
  for (const pt of printedMfrTotals) {
    const got = mfrSums.get(pt.manufacturer) || { sales: 0, commission: 0 };
    if (Math.abs(got.sales - pt.sales) > RECON_EPS || Math.abs(got.commission - pt.commission) > RECON_EPS) {
      mismatches.push({
        section: pt.manufacturer || pt.label,
        parsed_sales: round2(got.sales), printed_sales: round2(pt.sales),
        parsed_commission: round2(got.commission), printed_commission: round2(pt.commission),
      });
    }
  }

  const parsedGrandSales = round2(dataRows.reduce((s, r) => s + r.total_sales, 0));
  const parsedGrandCommission = round2(dataRows.reduce((s, r) => s + r.total_commission, 0));
  let grandOk = true;
  if (printedGrand) {
    grandOk = Math.abs(parsedGrandSales - printedGrand.sales) <= RECON_EPS &&
              Math.abs(parsedGrandCommission - printedGrand.commission) <= RECON_EPS;
    if (!grandOk) {
      mismatches.push({
        section: 'Grand Totals',
        parsed_sales: parsedGrandSales, printed_sales: round2(printedGrand.sales),
        parsed_commission: parsedGrandCommission, printed_commission: round2(printedGrand.commission),
      });
    }
  } else {
    mismatches.push({ section: 'Grand Totals', parsed_sales: parsedGrandSales, printed_sales: null,
      parsed_commission: parsedGrandCommission, printed_commission: null });
  }

  const reconciles = mismatches.length === 0;

  // Distinct manufacturers + accountable (non-adjustment) customer count.
  const manufacturers = Array.from(new Set(dataRows.map(r => r.manufacturer).filter(Boolean)));
  const accountRows = dataRows.filter(r => !r.is_adjustment && r.customer_raw);

  return {
    rep_name,
    period,
    period_start,
    period_end,
    date_range: dateRange,
    rows: dataRows,
    manufacturers,
    counts: {
      lines: manufacturers.length,
      accounts: new Set(accountRows.map(r => norm(r.customer_raw))).size,
      rows: dataRows.length,
    },
    grand_totals: {
      parsed_sales: parsedGrandSales,
      parsed_commission: parsedGrandCommission,
      printed_sales: printedGrand ? round2(printedGrand.sales) : null,
      printed_commission: printedGrand ? round2(printedGrand.commission) : null,
    },
    reconciliation: { reconciles, mismatches },
  };
}

module.exports = {
  parsePdfCommissionReport,
  parseMoney,
  isMoney,
  _internal: { extractLines, isColumnHeader, isSkipLine, isTotalsLine },
};
