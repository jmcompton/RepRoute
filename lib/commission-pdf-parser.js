'use strict';

// ── Node-safety polyfill (belt-and-suspenders) ───────────────────────────────
// MUST run BEFORE any PDF library is required. pdf-parse 1.1.1 bundles a pure-JS
// pdfjs-dist 2.x that does NOT need the DOM, so this is just insurance: if the
// resolved tree ever loads a pdfjs build that touches DOMMatrix/Path2D/ImageData,
// these minimal no-op shims keep extraction from crashing in plain Node (Railway)
// the same way it runs locally. We deliberately do NOT pull in the native
// `canvas` package — it fails to build on Railway.
(function ensureDomShims() {
  const g = globalThis;
  if (typeof g.DOMMatrix === 'undefined') {
    g.DOMMatrix = class DOMMatrix {
      constructor() { this.a = 1; this.b = 0; this.c = 0; this.d = 1; this.e = 0; this.f = 0; }
      multiplySelf() { return this; }
      scaleSelf() { return this; }
      translateSelf() { return this; }
    };
  }
  if (typeof g.Path2D === 'undefined') { g.Path2D = class Path2D {}; }
  if (typeof g.ImageData === 'undefined') {
    g.ImageData = class ImageData { constructor(w, h) { this.width = w || 0; this.height = h || 0; this.data = new Uint8ClampedArray(0); } };
  }
})();

// ── Trilogy "Payment Detail Report" PDF parser ───────────────────────────────
// Parses the commission PDF (e.g. DCAPRIL2026.pdf) into structured rows.
//
// EXTRACTION: uses `pdf-parse` PINNED to 1.1.1, which bundles a PURE-JS
// pdfjs-dist 2.x (no DOM globals). Newer pdf-parse/pdfjs (3.x/4.x) require
// browser globals like DOMMatrix and crash in plain Node on Railway
// ("DOMMatrix is not defined"); 1.1.1 + a committed package-lock.json keeps the
// runtime tree identical to what we test locally. pdf-parse 1.1.1 has no per-page
// array, so a `pagerender` callback rebuilds each page's text by Y-baseline and
// `preprocessLines` reshapes that into the spaced, three-amount layout the walk
// below expects.
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

// ── Extraction (pdf-parse 1.1.1 — pure-JS pdfjs-dist 2.x, no DOM) ─────────────
// Returns { lines:[rawLineStr...] (page-ordered), page1Text }.
// pdf-parse 1.1.1 has no `pages` array, so we supply a `pagerender` callback that
// rebuilds each page's text by Y-coordinate (items on the same baseline are
// concatenated; a new baseline starts a new line) and collect the per-page text
// ourselves. This is the same line structure the rest of the parser expects.
async function extractText(buffer) {
  const pdf = require('pdf-parse');
  const pages = [];

  function render_page(pageData) {
    return pageData.getTextContent({ normalizeWhitespace: false, disableCombineTextItems: false })
      .then(function (textContent) {
        let lastY;
        let text = '';
        for (const item of textContent.items) {
          if (lastY === item.transform[5] || lastY === undefined) text += item.str;
          else text += '\n' + item.str;
          lastY = item.transform[5];
        }
        pages.push(text);
        return text;
      });
  }

  const data = await pdf(buffer, { pagerender: render_page });
  const rawLines = [];
  for (const pg of pages) String(pg || '').split('\n').forEach(s => rawLines.push(s));
  if (!rawLines.length) String(data.text || '').split('\n').forEach(s => rawLines.push(s));
  const page1Text = String(pages[0] || data.text || '');
  return { lines: preprocessLines(rawLines), page1Text };
}

// pdf-parse 1.1.1 (pdfjs 2.x) emits each text line by Y-baseline, which for this
// report GLUES the columns with no spaces and SPLITS each subtotal's amounts onto
// the line ABOVE its "<X> Totals:" label. The downstream walk expects the v2
// layout: whitespace-separated tokens that end in three money amounts, and a
// single "<amounts> <X> Totals:" line. This pass rebuilds exactly that:
//   • a data line  "Service Partners, Huntsville, ALBossBOSSAPR20$10,666.08$506.64$506.64"
//       → "Service Partners, Huntsville, ALBoss BOSSAPR20 $10,666.08 $506.64 $506.64"
//   • an amounts-only line immediately followed by a "<X> Totals:" line is merged
//       → "$2,275.26 $47,900.16 $2,275.26 Boss Totals:"
const MONEY_TOKEN_RX = /\(?\$[\d,]+\.\d{2}\)?/;
function isAmountsOnly(s) { return /^(\(?\$[\d,]+\.\d{2}\)?)+$/.test(String(s || '').trim()); }
function spaceMoney(s) {
  return String(s || '').replace(/(\(?\$[\d,]+\.\d{2}\)?)/g, ' $1').replace(/\s+/g, ' ').trim();
}
// Split a glued data line into "<customer+mfr> <invoice> <$ $ $>". Lines without
// money pass through unchanged (e.g. a wrapped customer name).
function normalizeDataLine(raw) {
  const line = String(raw || '');
  const m = line.match(/^(.*?)(\(?\$[\d,]+\.\d{2}\)?.*)$/);
  if (!m) return line.trim();
  const prefix = m[1];
  const moneyPart = spaceMoney(m[2]);
  const inv = prefix.match(/[A-Z0-9]+\d$/);   // invoice = trailing UPPER/digit run
  if (inv) {
    const custMfr = prefix.slice(0, prefix.length - inv[0].length).replace(/\s+$/, '');
    return (custMfr + ' ' + inv[0] + ' ' + moneyPart).replace(/\s+/g, ' ').trim();
  }
  return (prefix + ' ' + moneyPart).replace(/\s+/g, ' ').trim();
}
function preprocessLines(rawLines) {
  const out = [];
  for (let i = 0; i < rawLines.length; i++) {
    const t = String(rawLines[i]).trim();
    if (isAmountsOnly(t)) {
      // Look ahead past skip lines for a "<X> Totals:" label to merge with.
      let j = i + 1;
      while (j < rawLines.length && isSkipLine(String(rawLines[j]).trim())) j++;
      if (j < rawLines.length && isTotalsLine(String(rawLines[j]).trim()) && !isAmountsOnly(String(rawLines[j]).trim())) {
        out.push(spaceMoney(t) + ' ' + String(rawLines[j]).trim());
        i = j;
        continue;
      }
    }
    out.push(normalizeDataLine(rawLines[i]));
  }
  return out;
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
