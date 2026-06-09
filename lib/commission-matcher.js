'use strict';

// ── Commission customer → account matching ───────────────────────────────────
// Reuses the shared Levenshtein/fuzzy helpers (lib/fuzzy.js). Pure functions —
// no DB here. The store layer feeds in accounts + the learned customer map.

const { similarity } = require('./fuzzy');

const LEGAL_SUFFIX_RX = /\b(llc|l\.l\.c|inc|incorporated|corp|corporation|co|company|ltd|limited)\b/g;
const STOPWORDS = new Set(['and', 'the', 'of', 'a']);
// Known plural variants to fold together (spec: closet(s), shower(s)).
const PLURALS = { closets: 'closet', showers: 'shower' };
// US state abbreviations — used to strip a trailing ", XX" off the customer.
const STATES = new Set(['al','ga','tn','fl','ms','sc','nc','va','ky','la','tx','ar','mo']);

// Split city/state off a raw customer string WITHOUT destroying the raw.
// Returns { core, city, state }. Handles "Name, City, ST" and "Name, City".
function splitLocation(raw) {
  let s = String(raw || '').trim();
  let city = null, state = null;

  // Trailing state: ", AL" / ", AL." (exactly two letters from the known set).
  const sm = s.match(/,\s*([A-Za-z]{2})\.?\s*$/);
  if (sm && STATES.has(sm[1].toLowerCase())) {
    state = sm[1].toUpperCase();
    s = s.slice(0, sm.index).trim();
  }

  // Trailing ", City" — only when what remains before it still has a name and
  // the segment isn't a legal suffix (so we don't eat ", Inc").
  const cm = s.match(/,\s*([^,]+?)\s*$/);
  if (cm) {
    const seg = cm[1].trim();
    const segCore = seg.toLowerCase().replace(LEGAL_SUFFIX_RX, '').replace(/[^a-z0-9 ]/g, '').trim();
    const before = s.slice(0, cm.index).trim();
    if (before && segCore && !/^(inc|llc|co|corp|ltd|company|incorporated)$/.test(seg.toLowerCase().replace(/[^a-z]/g, ''))) {
      city = seg;
      s = before;
    }
  }

  return { core: s, city, state };
}

// normalizeCustomer(raw): produce a stable match key + parsed city.
function normalizeCustomer(raw) {
  const { core, city, state } = splitLocation(raw);
  let s = ' ' + core.toLowerCase() + ' ';
  s = s.replace(/&/g, ' and ');
  s = s.replace(LEGAL_SUFFIX_RX, ' ');
  s = s.replace(/[^a-z0-9 ]/g, ' ');          // drop punctuation/apostrophes
  let toks = s.split(/\s+/).filter(Boolean)
    .filter(t => !STOPWORDS.has(t))
    .map(t => PLURALS[t] || t);
  const normalized = toks.join(' ').trim();
  return { normalized, city: city || null, state: state || null };
}

// ── Cluster the import's customers ───────────────────────────────────────────
// Single-linkage: a line joins a cluster if it fuzzy-matches ANY member.

function tokensOf(s) { return String(s || '').split(' ').filter(Boolean); }

// Token-aware match: fraction of the SHORTER token set whose tokens each find a
// strong fuzzy partner in the longer set. Catches "orva constructioin" ↔
// "orva construction partners" (typo + extra word) without merging businesses
// that merely share one generic word.
function fuzzyTokenScore(a, b) {
  const ta = tokensOf(a), tb = tokensOf(b);
  if (!ta.length || !tb.length) return 0;
  const [short, long] = ta.length <= tb.length ? [ta, tb] : [tb, ta];
  const used = new Array(long.length).fill(false);
  let matched = 0;
  for (const t of short) {
    let bestI = -1, best = 0;
    for (let i = 0; i < long.length; i++) {
      if (used[i]) continue;
      const s = similarity(t, long[i]);
      if (s > best) { best = s; bestI = i; }
    }
    if (best >= 0.84) { matched++; used[bestI] = true; }
  }
  return matched / short.length;
}

// Combined pairwise score in [0,1].
function pairScore(a, b) {
  if (a === b) return 1;
  const lev = similarity(a, b);                 // whole-string Levenshtein ratio
  const tok = fuzzyTokenScore(a, b);            // token-set fuzzy coverage
  // Containment boost (e.g. "haley flooring" ⊂ "haley flooring interiors").
  let contain = 0;
  if (a.length >= 4 && b.length >= 4 && (a.includes(b) || b.includes(a))) {
    const r = Math.min(a.length, b.length) / Math.max(a.length, b.length);
    contain = 0.7 + r * 0.3;
  }
  return Math.max(lev, tok, contain);
}

const CLUSTER_THRESHOLD = 0.80;

// lines: [{ customer_raw, sales, commission, is_adjustment, ... }]
// Returns clusters of NON-adjustment lines. Adjustment lines (Freight) are
// returned separately — they never get an account.
function clusterCommissionLines(lines) {
  const enriched = lines.map((l, idx) => {
    const nc = normalizeCustomer(l.customer_raw);
    return { idx, line: l, normalized: nc.normalized, city: nc.city, state: nc.state };
  });

  const accountable = enriched.filter(e => !e.line.is_adjustment && e.normalized);
  const adjustments = enriched.filter(e => e.line.is_adjustment || !e.normalized);

  const clusters = []; // { members:[enriched], city:String|null }
  const cityOf = (s) => (s == null ? null : String(s).toLowerCase().trim());
  for (const e of accountable) {
    const eCity = cityOf(e.city);
    let target = null, bestScore = 0;
    for (const c of clusters) {
      if (eCity && c.city && eCity !== c.city) continue; // city guard: don't merge different branches
      let s = 0;
      for (const m of c.members) {
        const ps = pairScore(e.normalized, m.normalized);
        if (ps > s) s = ps;
        if (s === 1) break;
      }
      if (s >= CLUSTER_THRESHOLD && s > bestScore) { bestScore = s; target = c; }
    }
    if (target) {
      target.members.push(e);
      if (!target.city && eCity) target.city = eCity;
    } else {
      clusters.push({ members: [e], city: eCity });
    }
  }

  // Shape each cluster: representative = the longest member name (most complete),
  // member spellings, totals, distinct normalized keys.
  return {
    clusters: clusters.map(c => {
      const rawMembers = c.members.map(m => m.line.customer_raw);
      const normKeys = Array.from(new Set(c.members.map(m => m.normalized)));
      const rep = c.members.slice().sort((a, b) => b.normalized.length - a.normalized.length)[0];
      const total_sales = round2(c.members.reduce((s, m) => s + (m.line.sales || 0), 0));
      const total_commission = round2(c.members.reduce((s, m) => s + (m.line.commission || 0), 0));
      const cities = Array.from(new Set(c.members.map(m => m.city).filter(Boolean)));
      const states = Array.from(new Set(c.members.map(m => m.state).filter(Boolean)));
      return {
        representative: rep.line.customer_raw,
        representative_normalized: rep.normalized,
        normalized_keys: normKeys,
        members: rawMembers,
        member_count: c.members.length,
        line_indexes: c.members.map(m => m.idx),
        total_sales,
        total_commission,
        cities,
        states,
        state: states[0] || null,
      };
    }),
    adjustments: adjustments.map(a => ({
      customer_raw: a.line.customer_raw,
      manufacturer: a.line.manufacturer,
      sales: a.line.sales,
      commission: a.line.commission,
      line_index: a.idx,
    })),
  };
}

function round2(n) { return Math.round(n * 100) / 100; }

// ── Match a cluster to an existing account ───────────────────────────────────
// mapByNorm: Map(customer_normalized -> { account_id, confidence })  (per rep)
// accounts:  [{ id, company, city, state }]  (the rep's prospects)
// Returns { proposed_account|null, suggest_new, confidence, matched_by }.
function matchClusterToAccount(cluster, accounts, mapByNorm) {
  // (a) learned map — any normalized key already linked? auto-match.
  for (const key of cluster.normalized_keys) {
    if (mapByNorm && mapByNorm.has(key)) {
      const m = mapByNorm.get(key);
      const acct = accounts.find(a => a.id === m.account_id);
      return {
        proposed_account: acct ? { id: acct.id, name: acct.company } : { id: m.account_id, name: m.account_name || '(account)' },
        suggest_new: false,
        confidence: m.confidence != null ? Number(m.confidence) : 1,
        matched_by: 'map',
      };
    }
  }

  // (b) fuzzy-match the cluster representative against existing accounts.
  let best = null, bestScore = 0;
  for (const a of accounts) {
    const an = normalizeCustomer(a.company).normalized;
    const s = pairScore(cluster.representative_normalized, an);
    if (s > bestScore) { bestScore = s; best = a; }
  }
  if (best && bestScore >= 0.80) {
    return {
      proposed_account: { id: best.id, name: best.company },
      suggest_new: false,
      confidence: round2(bestScore),
      matched_by: 'fuzzy',
    };
  }

  // (c) nothing solid → suggest creating a new account.
  return {
    proposed_account: best && bestScore >= 0.6 ? { id: best.id, name: best.company } : null,
    suggest_new: true,
    confidence: best ? round2(bestScore) : 0,
    matched_by: 'none',
  };
}

module.exports = {
  normalizeCustomer,
  clusterCommissionLines,
  matchClusterToAccount,
  pairScore,
  CLUSTER_THRESHOLD,
};
