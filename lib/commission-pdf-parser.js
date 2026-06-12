'use strict';

// ── Trilogy "Payment Detail Report" PDF parser ───────────────────────────────
// Parses the commission PDF (e.g. DCAPRIL2026.pdf) into structured rows.
//
// EXTRACTION: uses `pdf-parse` (v2, PDFParse.getText) which reads this PDF's
// subsetted fonts correctly and yields clean Unicode text. The previous
// pdfjs-dist positional extractor returned encoded glyph bytes (mojibake) and a
// nonsense $30 total for these fonts — a standard extractor reads the same text
// layer perfectly, so the extraction code (not the PDF) was the problem.
//
// TOKEN ORDER (from a clean extractor) for a data row:
//   <Customer, City, ST>  <Manufacturer>  <Invoice>  <Sales> <Comm> <SalesRepComm>
// i.e. the customer comes FIRST, the manufacturer is glued right onto the state
// (often only a tab/space apart, sometimes no space at all), then the invoice,
// then three currency amounts. We anchor on the THREE trailing amounts; the token
// before them is the invoice; the block's manufacturer is known from the block
// title; the clean customer is recovered by stripping the known manufacturer name
// off the END of the customer chunk (tolerating no-space concatenation).
// Subtotal lines come as: <amounts> <Manufacturer> Totals:.
//
// GATE: a parse is only allowed to reconcile when ALL hold — parsed grand total
// matches the printed grand total, >=10 rows, >=5 distinct manufacturers, and
// customer names are >=90% ASCII letters (a mojibake guard). Any failure returns
// reconciles:false with a reason so a broken parse can never reach commit.

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
function tokenize(line) { return String(line || '').split(/\s+/).map(t => t.trim()).filter(Boolean); }

// ── Line classification helpers ──────────────────────────────────────────────
const RX_DATE_RANGE_LINE = /(\d{1,2})\/(\d{1,2})\/(\d{4})\s+to\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/;
const RX_PAGE_SEP = /^--\s*\d+\s*of\s*\d+\s*--$/i;   // pdf-parse page separator

function isColumnHeader(text) {
  const t = norm(text);
  return t.startsWith('manufacturer') && t.includes('invoice number') && t.includes('sales rep commissions');
}
function isSkipLine(text) {
  const t = String(text || '').trim();
  if (!t) return true;
  if (/^Payment Detail Report\b/i.test(t)) return true;
  if (/^Salesrep Commissions$/i.test(t)) return true;
  if (/^Printed on\b/i.test(t)) return true;
  if (RX_PAGE_SEP.test(t)) return true;
  return false;
}
function isTotalsLine(text) { return /\bTotals:/i.test(String(text || '')); }

// Strip a known manufacturer name off the END of a customer chunk, tolerating a
// no-space concatenation (e.g. "...ALBoss" → "...AL"). Returns the cleaned chunk.
function stripTrailingMfr(chunk, mfr) {
  let c = String(chunk || '').replace(/\s+/g, ' ').trim();
  const target = norm(mfr).replace(/\s+/g, '');   // manufacturer, spaces removed
  if (!target) return c;
  // Find the smallest suffix whose space-stripped lowercase equals the mfr.
  for (let k = 1; k <= c.length; k++) {
    const suf = c.slice(c.length - k);
    if (suf.replace(/\s+/g, '').toLowerCase() === target) {
      return c.slice(0, c.length - k).replace(/[\s,]+$/, '').trim();
    }
  }
  return c;   // manufacturer not glued on (already clean)
}

// ── Extraction (pdf-parse v2) ────────────────────────────────────────────────
// Returns { lines:[rawLineStr...] (page-ordered), page1Text }.
async function extractText(buffer) {
  const { PDFParse } = require('pdf-parse');
  const parser = new PDFParse({ data: new Uint8Array(buffer) });
  try {
    const result = await parser.getText();
    const pages = Array.isArray(result.pages) && result.pages.length
      ? result.pages
      : [{ text: result.text || '', num: 1 }];
    const lines = [];
    for (const pg of pages) {
      String(pg.text || '').split('\n').forEach(s => lines.push(s));
    }
    const page1Text = String((pages[0] && pages[0].text) || '');
    return { lines, page1Text };
  } finally {
    try { await parser.destroy(); } catch (_) { /* noop */ }
  }
}

// HARD CHECK: clean text must contain these exact substrings, or we fail loudly
// rather than parse garbage.
const PAGE1_PROOF = ['Service Partners, Huntsville, AL', 'BOSSAPR20', '$10,666.08', 'Boss Totals:'];
function assertCleanExtraction(page1Text) {
  const missing = PAGE1_PROOF.filter(s => !page1Text.includes(s));
  if (missing.length) {
    throw new Error('PDF text extraction looks corrupted (subsetted-font mojibake). ' +
      'Expected substrings missing from page 1: ' + missing.map(s => JSON.stringify(s)).join(', ') +
      '. Refusing to parse garbage.');
  }
}

// ── Main parse ───────────────────────────────────────────────────────────────
async function parsePdfCommissionReport(buffer) {
  const { lines, page1Text } = await extractText(buffer);

  // (0) HARD CHECK before any parsing proceeds.
  assertCleanExtraction(page1Text);

  // (1) Date range / period.
  let dateRange = null, dm = null;
  for (const l of lines) { const m = RX_DATE_RANGE_LINE.exec(l); if (m) { dateRange = m[0]; dm = m; break; } }
  if (!dm) throw new Error('Could not find the "M/D/YYYY to M/D/YYYY" date range — is this a Trilogy Payment Detail Report?');
  const endMonth = parseInt(dm[4]), endDay = parseInt(dm[5]), endYear = parseInt(dm[6]);
  const period = `${endYear}-${String(endMonth).padStart(2, '0')}`;            // "2026-04"
  const period_end = `${period}-${String(endDay).padStart(2, '0')}`;          // last invoice day
  const period_start = `${period}-01`;

  // (2) Rep name = the non-empty line directly above the first "Salesrep Commissions".
  let rep_name = null;
  for (let i = 0; i < lines.length; i++) {
    if (/^Salesrep Commissions$/i.test(String(lines[i]).trim())) {
      for (let k = i - 1; k >= 0; k--) {
        const prev = String(lines[k]).trim();
        if (prev && !isSkipLine(prev) && !RX_DATE_RANGE_LINE.test(prev)) { rep_name = prev; break; }
      }
      break;
    }
  }
  if (!rep_name) throw new Error('Could not detect the sales rep name (no line above "Salesrep Commissions").');
  // Rep-section total can appear as "<amounts> <Rep> Totals: <amount>" — match the
  // rep name + "Totals:" anywhere in the line (not anchored to the start).
  const repRx = new RegExp(rep_name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s+Totals:', 'i');

  // (3) Walk the line stream, accumulating tokens until a data row completes.
  const dataRows = [];          // { manufacturer, customer_raw, invoice, total_sales, total_commission, is_adjustment }
  const printedMfrTotals = [];  // { manufacturer, label, sales, commission }
  let printedGrand = null;      // { sales, commission }

  let currentMfr = null;
  let buf = [];                 // token buffer for the in-progress data row
  const mfrSums = new Map();    // manufacturer → { sales, commission }

  function addToMfrSum(name, sales, commission) {
    const cur = mfrSums.get(name) || { sales: 0, commission: 0 };
    cur.sales = round2(cur.sales + sales);
    cur.commission = round2(cur.commission + commission);
    mfrSums.set(name, cur);
  }

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const text = String(raw).trim();

    if (isSkipLine(text)) continue;                 // (does not disturb an in-progress wrap)
    if (isColumnHeader(text)) { buf = []; continue; }

    // Manufacturer block title: a plain line whose next non-skip line is the header.
    if (!isTotalsLine(text)) {
      let j = i + 1;
      while (j < lines.length && isSkipLine(String(lines[j]).trim())) j++;
      if (j < lines.length && isColumnHeader(String(lines[j]).trim())) {
        currentMfr = text;
        buf = [];
        continue;
      }
    }

    // Totals line: <amounts> <Manufacturer> Totals:  (sales = max money, comm = min).
    if (isTotalsLine(text)) {
      buf = [];
      const monies = tokenize(text).filter(isMoney).map(parseMoney);
      if (monies.length >= 2) {
        const sales = Math.max.apply(null, monies);
        const commission = Math.min.apply(null, monies);
        if (/Grand Totals:/i.test(text)) {
          printedGrand = { sales, commission };
        } else if (repRx.test(text)) {
          // Rep-section total — covered by grand; no per-mfr compare.
        } else {
          const label = text.replace(/\s*Totals:.*/i, '').trim();
          printedMfrTotals.push({ manufacturer: currentMfr || label, label, sales, commission });
        }
      }
      continue;
    }

    if (currentMfr == null) continue;   // before the first block — ignore stray lines

    // Accumulate this line's tokens; a data row completes when the buffer ends in
    // three currency amounts.
    buf.push(...tokenize(raw));
    const n = buf.length;
    if (n >= 4 && isMoney(buf[n - 1]) && isMoney(buf[n - 2]) && isMoney(buf[n - 3])) {
      const total_sales = parseMoney(buf[n - 3]);       // Sales column (first of the three)
      const total_commission = parseMoney(buf[n - 1]);  // Sales Rep Commissions (last)
      const invoice = buf[n - 4];
      const chunk = buf.slice(0, n - 4).join(' ');      // customer + manufacturer
      const customer_raw = stripTrailingMfr(chunk, currentMfr);

      const row = {
        manufacturer: currentMfr,
        customer_raw,
        invoice: invoice || null,
        total_sales,
        total_commission,
        is_adjustment: /^freight$/i.test(customer_raw),
      };
      dataRows.push(row);
      addToMfrSum(currentMfr, total_sales, total_commission);
      buf = [];
    }
  }

  // (4) Reconciliation + hardened sanity gate.
  const mismatches = [];
  const reasons = [];

  for (const pt of printedMfrTotals) {
    const got = mfrSums.get(pt.manufacturer) || { sales: 0, commission: 0 };
    if (Math.abs(got.sales - pt.sales) > RECON_EPS || Math.abs(got.commission - pt.commission) > RECON_EPS) {
      mismatches.push({
        section: pt.manufacturer || pt.label,
        parsed_sales: round2(got.sales), printed_sales: round2(pt.sales),
        parsed_commission: round2(got.commission), printed_commission: round2(pt.commission),
      });
      reasons.push(`${pt.manufacturer || pt.label} subtotal off (sales ${round2(got.sales)} vs ${round2(pt.sales)}, comm ${round2(got.commission)} vs ${round2(pt.commission)})`);
    }
  }

  const parsedGrandSales = round2(dataRows.reduce((s, r) => s + r.total_sales, 0));
  const parsedGrandCommission = round2(dataRows.reduce((s, r) => s + r.total_commission, 0));
  if (!printedGrand) {
    mismatches.push({ section: 'Grand Totals', parsed_sales: parsedGrandSales, printed_sales: null,
      parsed_commission: parsedGrandCommission, printed_commission: null });
    reasons.push('No printed "Grand Totals:" line found.');
  } else if (Math.abs(parsedGrandSales - printedGrand.sales) > RECON_EPS ||
             Math.abs(parsedGrandCommission - printedGrand.commission) > RECON_EPS) {
    mismatches.push({
      section: 'Grand Totals',
      parsed_sales: parsedGrandSales, printed_sales: round2(printedGrand.sales),
      parsed_commission: parsedGrandCommission, printed_commission: round2(printedGrand.commission),
    });
    reasons.push(`Grand total off (sales ${parsedGrandSales} vs ${round2(printedGrand.sales)}, comm ${parsedGrandCommission} vs ${round2(printedGrand.commission)})`);
  }

  const manufacturers = Array.from(new Set(dataRows.map(r => r.manufacturer).filter(Boolean)));
  const accountRows = dataRows.filter(r => !r.is_adjustment && r.customer_raw);

  // Sanity thresholds — independent guards against a structurally broken parse.
  if (dataRows.length < 10) reasons.push(`Too few rows parsed (${dataRows.length} < 10).`);
  if (manufacturers.length < 5) reasons.push(`Too few manufacturers parsed (${manufacturers.length} < 5).`);

  const asciiRatio = customerAsciiRatio(dataRows);
  if (asciiRatio < 0.9) reasons.push(`Customer names are only ${(asciiRatio * 100).toFixed(0)}% ASCII letters (< 90%) — likely mojibake.`);

  const reconciles = reasons.length === 0;

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
    reconciliation: { reconciles, mismatches, reasons },
  };
}

// Fraction of letters in all customer names that are ASCII [A-Za-z]. 1 when there
// are no letters at all. Drops sharply under mojibake (non-ASCII glyph bytes).
function customerAsciiRatio(rows) {
  let letters = 0, ascii = 0;
  for (const r of rows) {
    for (const ch of String(r.customer_raw || '')) {
      if (/\p{L}/u.test(ch)) { letters++; if (/[A-Za-z]/.test(ch)) ascii++; }
    }
  }
  return letters ? ascii / letters : 1;
}

module.exports = {
  parsePdfCommissionReport,
  parseMoney,
  isMoney,
  _internal: { extractText, assertCleanExtraction, isColumnHeader, isSkipLine, isTotalsLine, stripTrailingMfr, customerAsciiRatio },
};
