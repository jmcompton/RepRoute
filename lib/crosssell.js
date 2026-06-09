'use strict';

// ── Cross-sell engine ─────────────────────────────────────────────────────────
// Pure functions over account_lines (the per-account × per-line revenue rollup).
// Computes agency-wide line co-occurrence (market-basket style) and turns it into
// per-account recommendations: "accounts that buy A also buy B — you don't carry
// B here." No hardcoded principals; the signal comes entirely from confirmed
// commission data. Quality improves as more reps' commission data is confirmed.

// ── Tunables (recommendation quality improves with more confirmed data) ───────
const MIN_SUPPORT = 3;   // a candidate line must be bought by >= this many accounts
const MIN_CONF    = 0.5; // keep recommendations with confidence >= this
const MIN_LIFT    = 1.0; // a supporting line must lift the candidate above base rate

function median(nums) {
  const a = (nums || []).map(Number).filter(Number.isFinite).sort((x, y) => x - y);
  if (!a.length) return 0;
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}

// ── buildCooccurrence(rows) ───────────────────────────────────────────────────
// rows: account_lines as [{ account_id, line_id, name, total_sales }].
// Returns: {
//   totalAccounts, lineName:{id->name}, support:{lineId->#distinct accounts},
//   both:{ "a|b" -> #accounts buying both }, confidence(a,b), lift(a,b),
//   accountsByLine:{lineId->Set(accountId)}, salesByLine:{lineId->[sales...]},
//   linesByAccount:{accountId->Set(lineId)} }.
function buildCooccurrence(rows) {
  const lineName = {};
  const accountsByLine = {};   // lineId -> Set(accountId)
  const salesByLine = {};      // lineId -> [sales amounts]
  const salesByLineAcct = {};  // "lineId|accountId" -> sales amount
  const linesByAccount = {};   // accountId -> Set(lineId)
  const accounts = new Set();

  for (const r of (rows || [])) {
    const acct = r.account_id, line = r.line_id;
    if (acct == null || line == null) continue;
    accounts.add(acct);
    if (r.name != null) lineName[line] = r.name;
    (accountsByLine[line] = accountsByLine[line] || new Set()).add(acct);
    (salesByLine[line] = salesByLine[line] || []).push(Number(r.total_sales) || 0);
    salesByLineAcct[line + '|' + acct] = Number(r.total_sales) || 0;
    (linesByAccount[acct] = linesByAccount[acct] || new Set()).add(line);
  }

  const support = {};
  for (const line in accountsByLine) support[line] = accountsByLine[line].size;

  // Pairwise both-counts over ordered pairs (a,b), a != b.
  const both = {};
  const lineIds = Object.keys(accountsByLine).map(Number);
  for (const a of lineIds) {
    for (const b of lineIds) {
      if (a === b) continue;
      let n = 0;
      const setB = accountsByLine[b];
      for (const acct of accountsByLine[a]) if (setB.has(acct)) n++;
      if (n > 0) both[a + '|' + b] = n;
    }
  }

  const totalAccounts = accounts.size;
  const bothCount = (a, b) => both[a + '|' + b] || 0;
  const confidence = (a, b) => (support[a] ? bothCount(a, b) / support[a] : 0);
  const lift = (a, b) => {
    const base = totalAccounts ? (support[b] || 0) / totalAccounts : 0;
    return base ? confidence(a, b) / base : 0;
  };

  return {
    totalAccounts, lineName, support, both, accountsByLine, salesByLine, salesByLineAcct,
    linesByAccount, bothCount, confidence, lift,
  };
}

// ── recommendForAccount(accountId, coocc, accountLines) ───────────────────────
// accountLines: this account's account_lines rows (to know its owned line set S).
// Returns ranked [{ line_id, line_name, est_sales, confidence, reason,
//                    supporting_line, supporting_line_id }].
function recommendForAccount(accountId, coocc, accountLines) {
  const owned = new Set((accountLines || []).map(l => l.line_id).filter(x => x != null));
  if (!owned.size) return [];

  const recs = [];
  const candidateLines = Object.keys(coocc.support).map(Number);

  for (const B of candidateLines) {
    if (owned.has(B)) continue;
    if ((coocc.support[B] || 0) < MIN_SUPPORT) continue;

    // Best supporting owned line a → B with lift above threshold.
    let bestConf = 0, bestA = null;
    for (const a of owned) {
      if (coocc.lift(a, B) <= MIN_LIFT) continue;
      const c = coocc.confidence(a, B);
      if (c > bestConf) { bestConf = c; bestA = a; }
    }
    if (bestA == null || bestConf < MIN_CONF) continue;

    // est_sales: median sales of B among B-buyers that share >=1 line with this
    // account; fallback to median across all B-buyers.
    const buyersB = coocc.accountsByLine[B] || new Set();
    const peerSales = [];
    for (const buyer of buyersB) {
      if (buyer === accountId) continue;
      const buyerLines = coocc.linesByAccount[buyer] || new Set();
      let shares = false;
      for (const l of owned) if (buyerLines.has(l)) { shares = true; break; }
      if (shares) {
        const s = coocc.salesByLineAcct[B + '|' + buyer];
        if (s != null) peerSales.push(s);
      }
    }
    const estSales = peerSales.length
      ? median(peerSales)
      : median(coocc.salesByLine[B] || []);

    const both = coocc.bothCount(bestA, B);
    const aName = coocc.lineName[bestA] || ('line ' + bestA);
    const bName = coocc.lineName[B] || ('line ' + B);
    recs.push({
      line_id: B,
      line_name: bName,
      est_sales: Math.round(estSales * 100) / 100,
      confidence: Math.round(bestConf * 1000) / 1000,
      reason: `${both} of ${coocc.support[bestA]} accounts that buy ${aName} also buy ${bName}`,
      supporting_line: aName,
      supporting_line_id: bestA,
    });
  }

  recs.sort((x, y) => (y.est_sales * y.confidence) - (x.est_sales * x.confidence));
  return recs;
}

module.exports = { buildCooccurrence, recommendForAccount, median, MIN_SUPPORT, MIN_CONF, MIN_LIFT };
