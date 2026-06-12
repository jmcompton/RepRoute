'use strict';

// ── Commission PDF parser build test ─────────────────────────────────────────
// Parses the real fixture (test/fixtures/DCAPRIL2026.pdf) and asserts the exact
// expected extraction. ANY mismatch > $0.01 (or wrong counts / failed reconcile)
// exits non-zero so the build fails. Run with: npm test
//
// This is the regression guard for the parser+gate fix: the old extractor
// produced mojibake names and a ~$30 grand total; this test pins the correct
// 10-manufacturer / 41-row / $452,686.89 result and the per-manufacturer
// subtotals so a regression can never ship silently.

const fs = require('fs');
const path = require('path');
const { parsePdfCommissionReport } = require('../lib/commission-pdf-parser');

const EPS = 0.01;
const FIXTURE = path.join(__dirname, 'fixtures', 'DCAPRIL2026.pdf');

const EXPECT = {
  rep_name: 'Daniel Compton',
  rows: 41,
  manufacturers: 10,
  grand_sales: 452686.89,
  grand_commission: 22368.39,
  // manufacturer → [sales, commission]
  subtotals: {
    'Boss': [47900.16, 2275.26],
    'Citadel': [28148.00, 2786.65],
    'Closet Maid': [156039.85, 7802.13],
    'Concept Hardware/Zalcow': [2443.00, 244.30],
    'Fortress Fortified': [110290.72, 4411.63],
    'No Burn': [13320.00, 499.50],
    'Quadrant': [15147.00, 479.65],
    'Quality': [32590.73, 1469.99],
    'Safe-T-Nose': [6067.00, 606.70],
    'Virginia Mirror': [40740.43, 1792.58],
  },
};

const failures = [];
function check(label, cond, detail) {
  if (cond) { console.log('  ok  - ' + label); }
  else { failures.push(label + (detail ? ' — ' + detail : '')); console.log('  XX  - ' + label + (detail ? ' — ' + detail : '')); }
}
function near(a, b) { return Math.abs((Number(a) || 0) - (Number(b) || 0)) <= EPS; }

(async () => {
  console.log('Commission PDF parser test → ' + FIXTURE);
  if (!fs.existsSync(FIXTURE)) {
    console.error('FIXTURE MISSING: ' + FIXTURE);
    process.exit(1);
  }

  let parsed;
  try {
    parsed = await parsePdfCommissionReport(fs.readFileSync(FIXTURE));
  } catch (e) {
    console.error('PARSE THREW: ' + e.message);
    process.exit(1);
  }

  // Gate: a correct parse MUST reconcile (this is also the can't-reach-Confirm guard).
  check('reconciles to printed totals', parsed.reconciliation && parsed.reconciliation.reconciles,
    parsed.reconciliation ? JSON.stringify(parsed.reconciliation.reasons) : 'no reconciliation');

  check('rep attributed to ' + EXPECT.rep_name, parsed.rep_name === EXPECT.rep_name, 'got ' + parsed.rep_name);
  check('row count = ' + EXPECT.rows, parsed.rows.length === EXPECT.rows, 'got ' + parsed.rows.length);
  check('manufacturer count = ' + EXPECT.manufacturers, parsed.manufacturers.length === EXPECT.manufacturers, 'got ' + parsed.manufacturers.length);

  check('grand sales = ' + EXPECT.grand_sales, near(parsed.grand_totals.parsed_sales, EXPECT.grand_sales), 'got ' + parsed.grand_totals.parsed_sales);
  check('grand commission = ' + EXPECT.grand_commission, near(parsed.grand_totals.parsed_commission, EXPECT.grand_commission), 'got ' + parsed.grand_totals.parsed_commission);
  // Printed totals must match parsed totals exactly (the reconcile basis).
  check('grand sales matches printed', near(parsed.grand_totals.parsed_sales, parsed.grand_totals.printed_sales), 'printed ' + parsed.grand_totals.printed_sales);
  check('grand commission matches printed', near(parsed.grand_totals.parsed_commission, parsed.grand_totals.printed_commission), 'printed ' + parsed.grand_totals.printed_commission);

  // Per-manufacturer subtotals (summed from the parsed rows).
  const sums = {};
  for (const r of parsed.rows) {
    const m = r.manufacturer;
    if (!sums[m]) sums[m] = { sales: 0, commission: 0 };
    sums[m].sales += r.total_sales;
    sums[m].commission += r.total_commission;
  }
  for (const [mfr, [es, ec]] of Object.entries(EXPECT.subtotals)) {
    const got = sums[mfr] || { sales: 0, commission: 0 };
    check('subtotal ' + mfr, near(got.sales, es) && near(got.commission, ec),
      'sales ' + got.sales.toFixed(2) + ' vs ' + es.toFixed(2) + ', comm ' + got.commission.toFixed(2) + ' vs ' + ec.toFixed(2));
  }

  // No mojibake: customer names should be overwhelmingly ASCII letters.
  check('customer names are clean ASCII', parsed.rows.every(function (r) {
    return !r.customer_raw || /^[\x20-\x7E]*$/.test(r.customer_raw);
  }), 'a customer name contained non-ASCII bytes');

  console.log('');
  if (failures.length) {
    console.error('FAILED (' + failures.length + '):');
    failures.forEach(function (f) { console.error('  - ' + f); });
    process.exit(1);
  }
  console.log('PASS — all commission PDF assertions hold.');
  process.exit(0);
})();
