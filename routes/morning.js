const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';

// ═══════════════════════════════════════════════════════════════════════════
// GOLD-STANDARD EXAMPLE ANCHORS — LUMBERYARD / BUILDING-SUPPLY CATEGORY
// ───────────────────────────────────────────────────────────────────────────
// Provided by the principal (Keith). These are PATTERN ANCHORS, not an
// allowlist. The Lead Finder uses them as few-shot guidance to understand what
// a "good" lumber / building-supply lead looks like, then finds businesses of
// the SAME KIND in whatever territory the rep is working — reps work different
// territories, so we must NEVER hardcode or restrict results to these names.
//
// ▸ HOW TO EDIT (Keith / Dan): just add or remove names in the `anchors`
//   arrays below, or tweak a `searchPhrases` line. No code rebuild needed —
//   the search query set is rebuilt from this block on server start.
// ▸ `anchors`      = real example businesses (reference / training signal only).
// ▸ `searchPhrases`= the GENERIC "same-kind" Google queries derived from those
//                    anchors. The rep's city is appended at search time, so
//                    these pull SIMILAR independent dealers in the rep's area.
// ▸ RULES enforced elsewhere in this file:
//     • Big-box retailers (Home Depot, Lowe's, Menards) are ALWAYS excluded.
//     • Hardware / farm-and-home stores that indicate lumber/building materials
//       are INCLUDED; if it's uncertain whether one carries building materials,
//       it is SURFACED (with the normal flags) for the rep to decide — never
//       silently dropped.
//     • The existing verification/quality flags (home-based, closed/non-existent
//       business status) stay intact and apply to these leads too.
// ═══════════════════════════════════════════════════════════════════════════
const LUMBER_BUILDING_SUPPLY_EXAMPLES = {
  // FLAVOR A — dedicated independent lumber yards & pro building-supply dealers
  flavorA: {
    label: 'Independent lumber yards & pro building-supply dealers',
    anchors: [
      'Brand Vaughan', 'Builders FirstSource', 'Lummus Supply', 'Randall Brothers',
      'Metro Building Products', 'North Georgia Building Supply', 'Carolina Lumber',
      'Norcross Supply', 'Stiles Lumber', 'Still Lumber', 'Cofer Brothers',
      'Carl E. Smith', 'Carter Lumber', 'Mac Bee Brothers',
    ],
    // Generic same-kind queries derived from the Flavor-A anchors above:
    searchPhrases: [
      'independent lumber yard',
      'lumber and building supply dealer',
      'pro building supply dealer contractor',
      'building materials dealer',
      'building supply company',
      'wholesale lumber dealer building materials',
      'lumber yard contractor supply',
    ],
    score: 10, // prime target — dedicated lumber/building-supply dealer
  },
  // FLAVOR B — independent hardware / farm-and-home stores that ALSO sell
  // lumber & building materials (e.g. an Ace Hardware that carries lumber).
  flavorB: {
    label: 'Independent hardware / farm-and-home stores that also sell lumber & building materials',
    anchors: [
      'Cornelia Ace Hardware', 'Mooney Hardware', 'Farm & Home',
      'Griffin Lumber', 'Harbin Lumber', 'Patriot Building Supply',
    ],
    // Generic same-kind queries derived from the Flavor-B anchors above:
    searchPhrases: [
      'hardware store that sells lumber',
      'Ace Hardware lumber building materials',
      'farm and home store building materials',
      'independent hardware store building supplies',
      'Do it Best hardware lumber building materials',
      'farm and ranch supply lumber',
    ],
    score: 8, // include — but rep confirms it actually carries building materials
  },
};

// Build the lumber/building-supply segment's query set FROM the anchors config
// above. Each derived phrase becomes a Google Places text query; the rep's city
// is appended at search time so results are local "same-kind" businesses — never
// the literal example names. Editing the config block re-shapes this on restart.
function buildLumberSegmentQueries() {
  const out = [];
  for (const flavor of [LUMBER_BUILDING_SUPPLY_EXAMPLES.flavorA, LUMBER_BUILDING_SUPPLY_EXAMPLES.flavorB]) {
    for (const phrase of flavor.searchPhrases) {
      out.push({ query: phrase, score: flavor.score, category: 'Lumber / Building Supply' });
    }
  }
  return out;
}

// Big-box / national-chain retail — ALWAYS excluded from intern door-knocking
// results. These are not rep-firm prospects (they buy through corporate, not a
// walking rep), so a national chain must never surface regardless of how its
// name/rating scores. Kody's "Garden Center at Tractor Supply" hit is caught
// here ('tractor supply') AND by the garden_center hard type-block below.
// NOTE: intentionally does NOT include "84 Lumber" or "Ace" — Builders
// FirstSource is a Flavor-A anchor and Cornelia Ace is a Flavor-B anchor, so we
// keep independent dealers/distributors and only block true national big-box.
const BIG_BOX_KEYWORDS = [
  'home depot', "lowe's", 'lowes', 'menards',
  'tractor supply', 'tractor supply co',
  'walmart', 'wal-mart', 'wal mart',
  'costco', "sam's club", 'sams club',
  'harbor freight', 'family dollar', 'dollar general', 'dollar tree',
];
function isBigBoxBlocked(name) {
  const lower = (name || '').toLowerCase();
  return BIG_BOX_KEYWORDS.some(kw => lower.includes(kw));
}

// ── HARD TYPE / NAME EXCLUSIONS (all segments) ───────────────────────────────
// Google Places' OWN `types` are used as a HARD filter. A result carrying ANY
// of these types is rejected outright, regardless of name/rating/score — this
// is what makes it *impossible* for a pawn shop, gun range, garden center,
// supermarket, restaurant, gas station, etc. to surface for a building-products
// search. (The live daily-leads pipeline previously had NO type blocklist at
// all — that is exactly how "Trading Place Pawn & Indoor Gun Range" reached an
// intern: its only gates were name keywords + a soft channel filter, and a
// pawn shop trips neither.)
const HARD_EXCLUDE_TYPES = new Set([
  'pawn_shop',
  'gun_store', 'shooting_range', 'firearms_dealer', 'archery_range',
  'garden_center', 'florist', 'plant_nursery',
  'department_store', 'supermarket', 'grocery_or_supermarket', 'grocery_store',
  'convenience_store', 'liquor_store', 'wholesale_grocer', 'tobacco_shop',
  'restaurant', 'food', 'cafe', 'coffee_shop', 'bar', 'meal_takeaway',
  'meal_delivery', 'bakery', 'fast_food_restaurant',
  'gas_station', 'car_dealer', 'car_rental', 'car_wash', 'car_repair',
  'bank', 'atm', 'finance', 'insurance_agency', 'real_estate_agency',
  'lodging', 'hotel', 'motel', 'gym', 'spa', 'beauty_salon', 'hair_care',
  'pharmacy', 'drugstore', 'hospital', 'doctor', 'dentist', 'veterinary_care',
  'school', 'church', 'place_of_worship', 'clothing_store', 'shoe_store',
  'jewelry_store', 'pet_store', 'book_store', 'electronics_store',
  'furniture_store', 'night_club', 'movie_theater', 'casino', 'bowling_alley',
]);
function hardExcludedType(types) {
  if (!Array.isArray(types)) return null;
  for (const t of types) if (HARD_EXCLUDE_TYPES.has(t)) return t;
  return null;
}

// Name-level backstop for the same junk, in case Google mis-types a listing
// (some pawn/gun businesses come back as a generic 'store'/'establishment').
const HARD_EXCLUDE_NAME = [
  'pawn', 'gun range', 'gun shop', 'gun store', 'firearms', 'shooting range',
  'indoor range', 'gun & pawn', 'pawn & gun',
];
function hardExcludedName(name) {
  const lower = (name || '').toLowerCase();
  return HARD_EXCLUDE_NAME.find(kw => lower.includes(kw)) || null;
}

// Few-shot ranking nudge for the lumber/building-supply segment: reward results
// that share the "kind" of Keith's anchors (lumber / building supply / building
// materials / supply co / farm & home / lumber-carrying hardware). Returns a
// small positive boost; does not penalize, so uncertain hardware/farm stores
// still surface for the rep to decide. These signals double as the on-topic
// allowlist for the Lumber segment in SEGMENT_ONTOPIC below.
const LUMBER_KIND_SIGNALS = [
  'lumber', 'building supply', 'building material', 'building product',
  'supply co', 'supply company', 'building center', 'builders supply',
  'farm & home', 'farm and home', 'farm & ranch', 'farm and ranch',
  'do it best', 'true value', 'ace hardware', 'hardware',
];
const LUMBER_KIND_TYPES = new Set(['lumber_yard', 'building_materials_store', 'hardware_store', 'home_improvement_store']);
function lumberKindBoost(name, types) {
  const lower = (name || '').toLowerCase();
  let boost = 0;
  if (LUMBER_KIND_SIGNALS.some(kw => lower.includes(kw))) boost += 2;
  if ((types || []).some(t => LUMBER_KIND_TYPES.has(t))) boost += 1;
  return boost;
}

// ── PER-SEGMENT KEYWORD / TYPE GUARD ─────────────────────────────────────────
// After the hard type/name block, each result must show at least ONE on-topic
// signal for the segment being searched — either an on-topic word in its name
// OR an on-topic Google type. Anything with no on-topic signal is rejected
// (and the reason is logged) so junk that Google's fuzzy text search dragged in
// ("Cherokee Steel Supply" on a lumber search, a garage-door-only shop on a
// window/door search) can't reach an intern. This mirrors the AlumaPole
// category-mapping fix and Keith's pro-lumber vs. independent-hardware rules:
// hardware / farm-and-home stores DO carry an on-topic signal, so they still
// surface for the rep to decide — only truly off-topic results drop out.
const SEGMENT_ONTOPIC = {
  'Lumber / Building Supply': { name: LUMBER_KIND_SIGNALS, types: LUMBER_KIND_TYPES },
  'Window/Door Installer': {
    name: ['window', 'door', 'glass', 'glazing', 'millwork', 'fenestration', 'storefront', 'sash'],
    types: new Set(['window_installation_service', 'door_supplier', 'glazier']),
  },
  'Roofing Contractor': { name: ['roof', 'roofing'], types: new Set(['roofing_contractor']) },
  'Roofing Distributor': {
    name: ['roof', 'roofing', 'building material', 'building product', 'supply', 'wholesale', 'distribut'],
    types: new Set(['roofing_supply_store', 'building_materials_store']),
  },
  'Siding Contractor': {
    name: ['siding', 'exterior', 'fiber cement', 'hardie', 'stucco', 'soffit', 'fascia'],
    types: new Set(['siding_contractor']),
  },
  'Siding Distributor': {
    name: ['siding', 'exterior', 'building material', 'building product', 'supply', 'wholesale', 'distribut'],
    types: new Set(['building_materials_store']),
  },
  'Cornice Contractor': {
    name: ['cornice', 'soffit', 'fascia', 'trim', 'sheet metal', 'gutter', 'exterior'],
    types: new Set([]),
  },
  'Deck Contractor': {
    name: ['deck', 'outdoor living', 'patio', 'composite', 'railing'],
    types: new Set(['deck_builder', 'general_contractor']),
  },
  'Construction Fasteners': {
    name: ['fastener', 'supply', 'hardware', 'tool', 'building material', 'scaffold', 'contractor supply', 'distribut'],
    types: new Set(['hardware_store', 'building_materials_store']),
  },
};
// Window/Door extra rule: a garage-door / overhead-door business with NO
// window/entry-door signal is a garage-door-only company → reject.
function isGarageDoorOnly(name) {
  const lower = (name || '').toLowerCase();
  const isGarage = /garage door|garage doors|overhead door|overhead garage/.test(lower);
  const hasWindowDoorSig = /window|glass|entry door|patio door|exterior door|french door|storm door/.test(lower);
  return isGarage && !hasWindowDoorSig;
}
// Returns a reject reason string if the result is off-topic for the segment,
// or null if it carries an on-topic name/type signal.
function offTopicReason(segment, name, types) {
  const rule = SEGMENT_ONTOPIC[segment];
  if (!rule) return null; // no guard configured for this segment
  const lower = (name || '').toLowerCase();
  const t = types || [];
  if ((segment === 'Window/Door Installer') && isGarageDoorOnly(name)) {
    return 'garage-door-only (no window/entry-door signal)';
  }
  const nameHit = rule.name.some(kw => lower.includes(kw));
  const typeHit = t.some(x => rule.types.has(x));
  if (!nameHit && !typeHit) {
    return `no on-topic signal for "${segment}" in name or Google types [${t.join(', ') || 'none'}]`;
  }
  return null;
}

// Score floor — leads scoring below this are FILTERED OUT (not merely sorted
// low). 1–10 opportunity scale; 4 keeps solid prospects while dropping weak/
// home-based matches that the penalties pushed down. Tunable from intern
// feedback (the per-reject logs below feed this).
const MIN_OPPORTUNITY_SCORE = 4;

// ─── PRODUCT → SEARCH QUERY MAPPING ──────────────────────────────────────────
// Each entry defines what Google Places queries to run and how to score results
// Segment-level search config — each pill maps directly to specific queries
// This ensures "Window/Door" never returns roofing results, etc.
const SEGMENT_SEARCH_CONFIG = {
  // Lumberyard / building-supply category — query set is derived from the
  // editable gold-standard anchors config (LUMBER_BUILDING_SUPPLY_EXAMPLES).
  'Lumber / Building Supply': buildLumberSegmentQueries(),
  'Roofing Contractor': [
    { query: 'commercial roofing contractor', score: 10, category: 'Roofing Contractor' },
    { query: 'commercial roofing company', score: 10, category: 'Roofing Contractor' },
    { query: 'industrial roofing contractor', score: 10, category: 'Roofing Contractor' },
    { query: 'commercial flat roof contractor', score: 9, category: 'Roofing Contractor' },
    { query: 'commercial metal roofing company', score: 9, category: 'Roofing Contractor' },
  ],
  'Roofing Distributor': [
    { query: 'roofing supply distributor', score: 10, category: 'Roofing Distributor' },
    { query: 'roofing wholesale supply', score: 10, category: 'Roofing Distributor' },
    { query: 'ABC Supply roofing materials', score: 9, category: 'Roofing Distributor' },
    { query: 'Beacon Roofing Supply', score: 9, category: 'Roofing Distributor' },
    { query: 'building materials wholesale roofing', score: 8, category: 'Roofing Distributor' },
  ],
  'Window/Door Installer': [
    { query: 'exterior window installation contractor', score: 10, category: 'Window/Door Installer' },
    { query: 'exterior door installation contractor', score: 10, category: 'Window/Door Installer' },
    { query: 'replacement window installer exterior', score: 9, category: 'Window/Door Installer' },
    { query: 'entry door replacement contractor exterior', score: 9, category: 'Window/Door Installer' },
    { query: 'new construction window installer', score: 8, category: 'Window/Door Installer' },
  ],
  'Deck Contractor': [
    { query: 'deck builder contractor', score: 10, category: 'Deck Contractor' },
    { query: 'deck construction company', score: 10, category: 'Deck Contractor' },
    { query: 'composite deck installer', score: 9, category: 'Deck Contractor' },
    { query: 'outdoor deck patio contractor', score: 9, category: 'Deck Contractor' },
    { query: 'wood deck builder residential', score: 8, category: 'Deck Contractor' },
  ],
  'Siding Contractor': [
    { query: 'siding contractor installation', score: 10, category: 'Siding Contractor' },
    { query: 'vinyl siding contractor', score: 10, category: 'Siding Contractor' },
    { query: 'James Hardie siding installer', score: 10, category: 'Siding Contractor' },
    { query: 'fiber cement siding company', score: 9, category: 'Siding Contractor' },
    { query: 'exterior siding replacement company', score: 9, category: 'Siding Contractor' },
  ],
  'Siding Distributor': [
    { query: 'siding supply distributor', score: 10, category: 'Siding Distributor' },
    { query: 'exterior building products distributor siding', score: 9, category: 'Siding Distributor' },
    { query: 'building materials wholesale siding', score: 8, category: 'Siding Distributor' },
    { query: 'LP SmartSide siding distributor', score: 9, category: 'Siding Distributor' },
    { query: 'James Hardie siding supply dealer', score: 9, category: 'Siding Distributor' },
  ],
  'Cornice Contractor': [
    { query: 'cornice contractor soffit fascia', score: 10, category: 'Cornice Contractor' },
    { query: 'soffit fascia installer exterior', score: 10, category: 'Cornice Contractor' },
    { query: 'exterior trim cornice installation', score: 9, category: 'Cornice Contractor' },
    { query: 'fascia board replacement contractor', score: 9, category: 'Cornice Contractor' },
    { query: 'aluminum soffit fascia contractor', score: 8, category: 'Cornice Contractor' },
  ],
  'Construction Fasteners': [
    { query: 'roofing contractor fasteners supplier', score: 10, category: 'Construction Fasteners' },
    { query: 'construction fastener supplier dealer', score: 10, category: 'Construction Fasteners' },
    { query: 'framing contractor supplies hardware store', score: 10, category: 'Construction Fasteners' },
    { query: 'roofing nail supplier contractor supply', score: 9, category: 'Construction Fasteners' },
    { query: 'deck screw supplier contractor hardware', score: 9, category: 'Construction Fasteners' },
  ],
};

// Legacy PRODUCT_SEARCH_CONFIG kept for backwards compat (not used by current UI)
const PRODUCT_SEARCH_CONFIG = {
  'BOSS Products': { Contractor: SEGMENT_SEARCH_CONFIG['Roofing Contractor'], Dealer: SEGMENT_SEARCH_CONFIG['Roofing Distributor'] },
  'ShurTape':      { Contractor: SEGMENT_SEARCH_CONFIG['Roofing Contractor'], Dealer: SEGMENT_SEARCH_CONFIG['Siding Distributor'] },
  'Alum-A-Pole':   { Contractor: SEGMENT_SEARCH_CONFIG['Siding Contractor'],  Dealer: SEGMENT_SEARCH_CONFIG['Construction Fasteners'] },
};

// Google place types that confirm a business is relevant (vs. just having the right name)
const CONTRACTOR_PLACE_TYPES = new Set([
  'roofing_contractor', 'general_contractor',
  'deck_builder', 'siding_contractor', 'window_installation_service',
  'door_supplier', 'masonry_contractor', 'scaffolding_contractor',
  'home_improvement', 'construction_company', 'remodeling_contractor'
]);

const DEALER_PLACE_TYPES = new Set([
  'building_materials_store', 'hardware_store', 'lumber_yard',
  'wholesale_grocer', 'warehouse', 'distribution_center',
  'paint_store', 'home_improvement_store', 'roofing_supply_store'
]);


// Hard block — never return paint-related results for Alum-A-Pole
const PAINT_BLOCKED_KEYWORDS = ['paint', 'painting', 'painter', 'painters'];
function isPaintBlocked(name, brand) {
  if ((brand || '').toLowerCase().includes('alum')) {
    const lower = (name || '').toLowerCase();
    return PAINT_BLOCKED_KEYWORDS.some(kw => lower.includes(kw));
  }
  return false;
}

// Hard block — never return garage/overhead door companies for Window/Door Installer segment
const GARAGE_DOOR_KEYWORDS = [
  // Garage / overhead door types - never relevant to ShurTape/Boss window installs
  'garage door', 'garage doors', 'overhead door', 'overhead garage',
  'garage & door', 'garage and door', 'roll-up door', 'rollup door',
  'rolling door', 'sectional door',
  // Interior door types - not exterior installers
  'shower door', 'shower doors', 'frameless shower', 'glass shower',
  'sliding glass door', 'barn door', 'interior door', 'closet door',
  'pocket door', 'bi-fold door', 'bifold door', 'glass partition',
  // Commercial/industrial (not residential window contractors)
  'commercial door', 'industrial door', 'dock door', 'loading dock',
  'fire door', 'storefront glass', 'curtain wall',
  // Auto / specialty
  'auto glass', 'windshield', 'vehicle glass'
];
function isGarageDoorBlocked(name, segment) {
  if ((segment || '').includes('Window') || (segment || '').includes('Door')) {
    const lower = (name || '').toLowerCase();
    return GARAGE_DOOR_KEYWORDS.some(kw => lower.includes(kw));
  }
  return false;
}

// Hard block — never return residential roofers for Roofing Contractor segment
const RESIDENTIAL_ROOFER_KEYWORDS = [
  'residential roofing', 'home roofing', 'house roofing', 'homeowner roofing',
  'residential roof', 're-roof', 'reroof', 'roof replacement residential',
  'storm damage roofing', 'insurance roofing', 'hail damage roofing',
  'shingle roofing', 'asphalt shingle', 'residential roofer'
];
function isResidentialRooferBlocked(name, types, segment) {
  if (!(segment || '').includes('Roofing Contractor')) return false;
  const lower = (name || '').toLowerCase();
  // Block on name keywords
  if (RESIDENTIAL_ROOFER_KEYWORDS.some(kw => lower.includes(kw))) return true;
  // Block if Google types include only residential
  if (Array.isArray(types) && types.includes('roofing_contractor') &&
      !types.includes('general_contractor') && lower.match(/residential|home|house/)) return true;
  return false;
}

// Hard block — never return heavy equipment / construction machinery companies (all segments)
const HEAVY_EQUIPMENT_KEYWORDS = [
  'excavating contractor', 'excavation contractor', 'heavy equipment', 'heavy machinery',
  'farm equipment', 'agricultural equipment',
  'tractor dealer', 'forklift', 'crane service', 'crane rental',
  'mining equipment', 'bulldozer', 'backhoe', 'earthmoving', 'earthwork contractor',
  'equipment dealer heavy', 'skid steer', 'hydraulic equipment'
];
function isHeavyEquipmentBlocked(name, types) {
  const lower = (name || '').toLowerCase();
  const typesStr = (types || []).join(' ').toLowerCase();
  const combined = lower + ' ' + typesStr;
  return HEAVY_EQUIPMENT_KEYWORDS.some(kw => combined.includes(kw));
}

// ── Home-Based Business Detection ────────────────────────────────────────────
// Primary signal: residential street suffix + no suite/unit + no corporate entity in name
// Secondary signal: no website + no specific Google type (only generic establishment types)
// Returns { primary: bool, secondary: bool }
const HOME_BASED_SUFFIX_RX = /\b(dr|drive|ln|lane|ct|court|way|cir|circle|pl|place|blvd|boulevard|rd|road)\b\.?(?:\s|,|$)/i;
const HOME_BASED_UNIT_RX   = /\b(ste|suite|unit|fl|floor|bldg|building)\b\.?\s*\d*|\s#\s*\d+/i;
const HOME_BASED_CORP_RX   = /\b(llc|inc\.?|corp\.?|co\.|company|group|associates)\b/i;
function isHomeBased(address, company, website, types) {
  const hasResidentialSuffix = HOME_BASED_SUFFIX_RX.test(address || '');
  const hasCommercialUnit    = HOME_BASED_UNIT_RX.test(address || '');
  const hasCorporateName     = HOME_BASED_CORP_RX.test(company || '');
  const primary = hasResidentialSuffix && !hasCommercialUnit && !hasCorporateName;
  const hasWebsite      = !!(website && website.trim());
  const typesArr        = types || [];
  const hasSpecificType = typesArr.some(t =>
    CONTRACTOR_PLACE_TYPES.has(t) || DEALER_PLACE_TYPES.has(t));
  const secondary = !hasWebsite && !hasSpecificType;
  return { primary, secondary };
}

// Haversine distance in miles
function distanceMiles(lat1, lng1, lat2, lng2) {
  const R = 3958.8;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 +
    Math.cos(lat1 * Math.PI/180) * Math.cos(lat2 * Math.PI/180) * Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

// Calculate opportunity score (1-10) based on multiple signals
function calcOpportunityScore(baseScore, distMi, reviewCount, rating, productMatches, channel) {
  let score = baseScore;

  // Distance bonus/penalty
  if (distMi <= 10) score += 1;
  else if (distMi <= 25) score += 0;
  else if (distMi <= 50) score -= 1;
  else score -= 2;

  // Review count signals — more reviews = more established business = better prospect
  if (reviewCount >= 50) score += 1;
  else if (reviewCount >= 20) score += 0.5;
  else if (reviewCount < 5) score -= 0.5;

  // Multi-product bonus
  if (productMatches >= 3) score += 1;
  else if (productMatches >= 2) score += 0.5;

  // Dealer channel tends to be higher value per call
  if (channel === 'Dealer') score += 0.5;

  return Math.min(10, Math.max(1, Math.round(score)));
}

// ─── MORNING BRIEFING ────────────────────────────────────────────────────────
router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const userName = req.session.user.name || 'Rep';
  try {
    const followUps = await pool.query(
      `SELECT p.id, p.company, p.category, p.city, p.address, p.phone, p.priority,
              c.next_step, c.next_step_date, c.outcome as last_outcome,
              'follow_up' as call_reason,
              (CURRENT_DATE - c.next_step_date) as days_overdue
       FROM calls c JOIN prospects p ON c.prospect_id = p.id
       WHERE c.user_id = $1 AND c.next_step_date IS NOT NULL
         AND c.next_step_date <= CURRENT_DATE
         AND c.next_step IS NOT NULL AND c.next_step != ''
       ORDER BY c.next_step_date ASC LIMIT 10`,
      [uid]
    );
    const neverCalled = await pool.query(
      `SELECT p.id, p.company, p.category, p.city, p.address, p.phone, p.priority,
              'new_prospect' as call_reason, p.created_at
       FROM prospects p
       WHERE p.user_id = $1 AND p.priority IN ('High', 'Medium')
         AND p.id NOT IN (SELECT DISTINCT prospect_id FROM calls WHERE user_id = $1)
       ORDER BY CASE p.priority WHEN 'High' THEN 1 ELSE 2 END, p.created_at ASC LIMIT 10`,
      [uid]
    );
    const todayCalls = await pool.query(
      'SELECT COUNT(*) FROM calls WHERE user_id=$1 AND call_date=CURRENT_DATE', [uid]
    );
    const stats = await pool.query(
      `SELECT COUNT(*) as total, COUNT(CASE WHEN priority='High' THEN 1 END) as hot
       FROM prospects WHERE user_id=$1`, [uid]
    );
    const userRow = await pool.query('SELECT daily_call_goal FROM users WHERE id=$1', [uid]);
    const dailyGoal = (userRow.rows[0] && userRow.rows[0].daily_call_goal) || 10;
    const callsMadeToday = parseInt(todayCalls.rows[0].count);

    const followUpList = followUps.rows;
    const followUpIds = new Set(followUpList.map(r => r.id));
    const freshList = neverCalled.rows.filter(r => !followUpIds.has(r.id));
    const combinedList = [...followUpList, ...freshList].slice(0, 15);

    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });
    const dayOfWeek = new Date().toLocaleDateString('en-US', { weekday: 'long' });

    const prompt = `You are a sales coach for Compton Group LLC, a manufacturer's rep selling BOSS Roofing Sealants, ShurTape, and Alum-A-Pole in the Southeast US. Today is ${today}. Write 3-4 direct, punchy sentences for ${userName}: they've made ${callsMadeToday}/${dailyGoal} calls today, have ${followUpList.length} overdue follow-ups, and ${freshList.length} fresh prospects. Be a motivating coach, not a corporate bot. End with ONE specific action they should take right now.`;

    let aiMessage = '';
    try {
      const aiRes = await fetch(CLAUDE_API, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': process.env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify({ model: MODEL, max_tokens: 250, messages: [{ role: 'user', content: prompt }] })
      });
      const aiData = await aiRes.json();
      aiMessage = (aiData.content || []).filter(b => b.type === 'text').map(b => b.text).join('') || '';
    } catch(e) {
      aiMessage = `Good morning ${userName}. You have ${combinedList.length} prospects today. Start with your overdue follow-ups first.`;
    }

    res.json({
      user: { name: userName },
      date: today,
      dayOfWeek,
      aiMessage,
      stats: {
        callsMadeToday,
        dailyGoal,
        overdueFollowUps: followUpList.length,
        freshProspects: freshList.length,
        hotProspects: parseInt(stats.rows[0].hot)
      },
      callList: combinedList
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── DAILY LEAD FINDER (IMPROVED) ────────────────────────────────────────────
router.post('/daily-leads', async (req, res) => {
  const uid = req.session.user.id;
  const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;
  if (!PLACES_KEY) return res.status(500).json({ error: 'Google Places API key not configured' });

  // Get the rep's territory and home base from their user record
  const userRow = await pool.query('SELECT territory, home_base_lat, home_base_lng, home_base_city FROM users WHERE id=$1', [uid]);
  const userInfo = userRow.rows[0] || {};

  const city = (req.body.city || userInfo.home_base_city || '').trim() || 'Atlanta GA';
  const brands = Array.isArray(req.body.brands) && req.body.brands.length
    ? req.body.brands
    : [];
  if (!brands.length) return res.status(400).json({ error: 'Please select at least one brand.' });
  // Translate segment names (sent by UI pills) → Contractor or Dealer
  const SEGMENT_TO_CHANNEL = {
    'Roofing Contractor':    'Contractor',
    'Roofing Distributor':   'Dealer',
    'Siding Contractor':     'Contractor',
    'Siding Distributor':    'Dealer',
    'Cornice Contractor':    'Contractor',
    'Window/Door Installer': 'Contractor',
    'Deck Contractor':       'Contractor',
    'Construction Fasteners': 'Dealer',
    'Lumber / Building Supply': 'Dealer',
    'Contractor':            'Contractor',
    'Dealer':                'Dealer',
  };
  const rawChannel = (req.body.channel || 'Contractor').trim();
  const channel = SEGMENT_TO_CHANNEL[rawChannel] || 'Contractor';
  console.log(`[daily-leads] uid=${req.session.user.id} rawChannel="${rawChannel}" → channel="${channel}" city=${req.body.city}`);
  const radiusMiles = parseInt(req.body.radius_miles) || 50; // Default 50mi vs old 5mi

  try {
    // Get existing prospects to avoid duplicates
    const existing = await pool.query(
      'SELECT LOWER(company) as company, google_place_id FROM prospects WHERE user_id=$1', [uid]
    );
    const existingNames = new Set(existing.rows.map(r => r.company).filter(Boolean));
    const existingPlaceIds = new Set(existing.rows.map(r => r.google_place_id).filter(Boolean));

    // Also exclude leads already shown in this browser session (for refresh)
    const shownPlaceIds = Array.isArray(req.body.shown_place_ids) ? req.body.shown_place_ids : [];
    const shownNames = Array.isArray(req.body.shown_names) ? req.body.shown_names.map(n => n.toLowerCase()) : [];
    for (const id of shownPlaceIds) existingPlaceIds.add(id);
    for (const n of shownNames) existingNames.add(n);

    // Geocode the city — always use the typed city first, fall back to home base
    let centerCoords = null;
    try {
      const geoRes = await fetch(
        `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(city)}&key=${PLACES_KEY}`
      );
      const geoData = await geoRes.json();
      if (geoData.results && geoData.results[0]) {
        const loc = geoData.results[0].geometry.location;
        centerCoords = { lat: loc.lat, lng: loc.lng };
        console.log(`Geocoded "${city}" → ${loc.lat}, ${loc.lng}`);
      }
    } catch(e) { console.error('Geocode failed:', e.message); }
    // Fall back to stored home base if geocode failed
    if (!centerCoords && userInfo.home_base_lat && userInfo.home_base_lng) {
      centerCoords = { lat: parseFloat(userInfo.home_base_lat), lng: parseFloat(userInfo.home_base_lng) };
      console.log('Using home base coords as fallback');
    }

    if (!centerCoords) {
      return res.status(500).json({ error: `Could not locate city: ${city}. Please check the city name.` });
    }

    // Build search queries from segment config directly — segment pill = exact query set
    // rawChannel is the segment name from the UI pill (e.g. 'Roofing Contractor', 'Window/Door Installer')
    const segmentQueries = SEGMENT_SEARCH_CONFIG[rawChannel];
    if (!segmentQueries || !segmentQueries.length) {
      return res.status(400).json({ error: `Unknown segment: "${rawChannel}". Please select a valid segment.` });
    }
    const searchConfigs = segmentQueries.map(sc => ({ ...sc, brand: brands[0] || rawChannel }));
    console.log(`[daily-leads] segment="${rawChannel}" → ${searchConfigs.length} queries for city="${city}"`);

    // Deduplicate search queries (same query from multiple brands)
    const seenQueries = new Set();
    const uniqueConfigs = searchConfigs.filter(sc => {
      if (seenQueries.has(sc.query)) return false;
      seenQueries.add(sc.query);
      return true;
    });

    const allLeads = [];
    const sessionSeen = new Set();
    // Loggable reject tracker — every dropped result records WHY, so we can tune
    // the filters from real intern feedback (e.g. Kody's texts) instead of guessing.
    const rejects = [];
    const rejectLog = (company, reason) => {
      rejects.push({ company, reason });
      console.log(`[daily-leads][reject] "${company}" — ${reason}`);
    };

    for (const config of uniqueConfigs) {
      if (allLeads.length >= 25) break; // Collect 25, return top 10 (more headroom for refresh)

      try {
        // Embed city directly in query — NO locationBias
        // locationBias with radius >50km causes 400 errors from Google Places API
        const searchBody = {
          textQuery: `${config.query} ${city}`,
          maxResultCount: 20
        };

        const placesRes = await fetch('https://places.googleapis.com/v1/places:searchText', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': PLACES_KEY,
            'X-Goog-FieldMask': [
              'places.id',
              'places.displayName',
              'places.formattedAddress',
              'places.nationalPhoneNumber',
              'places.websiteUri',
              'places.rating',
              'places.userRatingCount',
              'places.businessStatus',
              'places.location',
              'places.primaryTypeDisplayName',
              'places.types',
              'places.photos',
              'places.regularOpeningHours'
            ].join(',')
          },
          body: JSON.stringify(searchBody)
        });

        const data = await placesRes.json();
        const places = data.places || [];

        for (const place of places) {
          if (allLeads.length >= 25) break;

          const company = place.displayName?.text || '';
          const placeId = place.id || '';
          const companyLower = company.toLowerCase();

          // Basic validity checks
          if (!company) continue;

          const placeTypesArr = place.types || [];

          // ── Dedup / status gates (silent — not "rejects" worth tuning on) ──
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          if (placeId && existingPlaceIds.has(placeId)) continue;
          if (existingNames.has(companyLower)) continue;
          if (sessionSeen.has(placeId || companyLower)) continue;
          sessionSeen.add(placeId || companyLower);

          // ── HARD FILTER 1: Places' own types ─────────────────────────────
          // A pawn shop / gun range / garden center / supermarket / etc. is
          // impossible to surface, regardless of name or score.
          const badType = hardExcludedType(placeTypesArr);
          if (badType) { rejectLog(company, `hard-excluded Google type: ${badType}`); continue; }
          const badName = hardExcludedName(company);
          if (badName) { rejectLog(company, `hard-excluded name keyword: "${badName}"`); continue; }

          // ── HARD FILTER 2: national big-box / chain retail ───────────────
          if (isBigBoxBlocked(company)) { rejectLog(company, 'national big-box / chain retailer'); continue; }

          // ── HARD FILTER 3: per-segment keyword/type guard ────────────────
          // Must show at least one on-topic signal for the segment searched.
          const offTopic = offTopicReason(rawChannel, company, placeTypesArr);
          if (offTopic) { rejectLog(company, offTopic); continue; }

          // ── Existing brand/segment-specific hard blocks ──────────────────
          if (isPaintBlocked(company, config.brand)) { rejectLog(company, 'paint shop blocked for Alum-A-Pole'); continue; }
          if (isGarageDoorBlocked(company, rawChannel)) { rejectLog(company, 'garage/overhead door blocked for Window/Door'); continue; }
          if (isResidentialRooferBlocked(company, placeTypesArr, rawChannel)) { rejectLog(company, 'residential-only roofer blocked for Roofing Contractor'); continue; }
          if (isHeavyEquipmentBlocked(company, placeTypesArr)) { rejectLog(company, 'heavy equipment / machinery'); continue; }

          // Distance filter — use the rep's actual radius
          let distMi = null;
          if (place.location?.latitude != null) {
            distMi = Math.round(distanceMiles(
              centerCoords.lat, centerCoords.lng,
              place.location.latitude, place.location.longitude
            ) * 10) / 10;
            if (distMi > radiusMiles) continue;
          }

          // Use Google's place types for accurate classification (not just name matching)
          const placeTypes = new Set(place.types || []);
          const primaryType = place.primaryTypeDisplayName?.text || config.category;

          // Soft channel filter — only block clear mismatches, not ambiguous types
          // Many specialty dealers (siding supply, fastener stores) don't have explicit Google types
          const isClassifiedContractor = [...placeTypes].some(t => CONTRACTOR_PLACE_TYPES.has(t));
          const isClassifiedDealer = [...placeTypes].some(t => DEALER_PLACE_TYPES.has(t));

          // Only hard-exclude if Google is very confident it's the WRONG type
          // Allow anything ambiguous (no classification) through — better to include than miss
          if (channel === 'Dealer' && isClassifiedContractor && !isClassifiedDealer && placeTypes.size > 2) continue;
          if (channel === 'Contractor' && isClassifiedDealer && !isClassifiedContractor && placeTypes.size > 2) continue;

          // Count how many product lines this prospect is relevant to
          const matchingBrands = brands.filter(brand => {
            const bc = PRODUCT_SEARCH_CONFIG[brand]?.[channel];
            return bc?.some(sc => sc.category === config.category || sc.query.split(' ')[0] === config.query.split(' ')[0]);
          });

          let opportunityScore = calcOpportunityScore(
            config.score,
            distMi || 25,
            place.userRatingCount || 0,
            place.rating || 0,
            matchingBrands.length,
            channel
          );

          // Lumber / building-supply few-shot ranking nudge: businesses that
          // share the "kind" of Keith's gold-standard anchors rank higher.
          // Positive-only — uncertain hardware/farm stores still surface so the
          // rep can decide (per the category rules).
          if (rawChannel === 'Lumber / Building Supply') {
            opportunityScore = Math.min(10, opportunityScore + lumberKindBoost(company, place.types || []));
          }

          // Home-based business detection — badge + optional score penalty
          const homeBasedResult = isHomeBased(
            place.formattedAddress || '',
            company,
            place.websiteUri || '',
            place.types || []
          );
          let homeBasedBadge = false;
          if (homeBasedResult.primary && homeBasedResult.secondary) {
            // Both signals: badge + strong score penalty (pushes to bottom)
            homeBasedBadge = true;
            opportunityScore = Math.max(1, opportunityScore - 15);
          } else if (homeBasedResult.primary) {
            // Primary only: badge, no score change
            homeBasedBadge = true;
          } else if (homeBasedResult.secondary) {
            // Secondary only: mild score penalty, no badge
            opportunityScore = Math.max(1, opportunityScore - 5);
          }

          // Parse city/state from formatted address
          const addrParts = (place.formattedAddress || '').split(',').map(s => s.trim());
          const cityName = addrParts.length >= 2 ? addrParts[addrParts.length - 3] || addrParts[1] : city;
          const stateZip = addrParts.length >= 2 ? addrParts[addrParts.length - 2] || '' : '';
          const stateName = stateZip.split(' ')[0] || 'GA';

          allLeads.push({
            company,
            category: config.category,
            channel,
            city: cityName,
            state: stateName,
            address: place.formattedAddress || '',
            distance_miles: distMi,
            phone: place.nationalPhoneNumber || '',
            website: place.websiteUri || '',
            products: matchingBrands.join(', ') || brands.join(', '),
            place_id: placeId,
            territory: city,
            opportunity_score: opportunityScore,
            priority: opportunityScore >= 8 ? 'High' : opportunityScore >= 6 ? 'Medium' : 'Low',
            rating: place.rating || null,
            reviews: place.userRatingCount || 0,
            primary_type: primaryType,
            matched_query: config.query,
            homeBased: homeBasedBadge,
            business_status: place.businessStatus || 'OPERATIONAL',
            photo_count: (place.photos || []).length,
            has_hours: !!(place.regularOpeningHours)
          });
        }
      } catch(e) {
        console.error('Places search error for query:', config.query, e.message);
      }
    }

    // ── ENFORCE THE SCORE THRESHOLD ──────────────────────────────────────────
    // Low scorers are FILTERED OUT here, not merely sorted to the bottom. Each
    // drop is logged so the floor can be tuned from intern feedback.
    const scored = allLeads.filter(l => {
      if ((l.opportunity_score || 0) < MIN_OPPORTUNITY_SCORE) {
        rejectLog(l.company, `below score floor (${l.opportunity_score} < ${MIN_OPPORTUNITY_SCORE})`);
        return false;
      }
      return true;
    });

    // Sort: opportunity score DESC, then distance ASC
    scored.sort((a, b) => {
      if (b.opportunity_score !== a.opportunity_score) return b.opportunity_score - a.opportunity_score;
      const da = a.distance_miles ?? 999;
      const db = b.distance_miles ?? 999;
      return da - db;
    });

    const topLeads = scored.slice(0, 10);

    console.log(`[daily-leads] segment="${rawChannel}" kept=${topLeads.length} passed-floor=${scored.length} rejected=${rejects.length}`);

    res.json({
      ok: true,
      leads: topLeads,
      brands_used: brands,
      channel,
      radius_miles: radiusMiles,
      center: { city, coords: centerCoords },
      total_found: scored.length,
      excluded_count: existingNames.size,
      score_floor: MIN_OPPORTUNITY_SCORE,
      rejected_count: rejects.length,
      rejected_sample: rejects.slice(0, 25)
    });

  } catch(e) {
    console.error('daily-leads error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ─── LOG CALL ────────────────────────────────────────────────────────────────
router.post('/log-call', async (req, res) => {
  const uid = req.session.user.id;
  const { prospect_id, outcome, contact_name, next_step, next_step_date, notes } = req.body;
  if (!prospect_id) return res.status(400).json({ error: 'prospect_id required' });
  try {
    const today = new Date().toISOString().split('T')[0];
    await pool.query(
      'INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, next_step, next_step_date, notes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
      [uid, prospect_id, today, 'In-Person Visit', outcome || '', next_step || '', next_step_date || null, notes || null]
    );
    // Sync notes to prospect record if provided
    if (notes && notes.trim()) {
      const todayFmt = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
      const noteEntry = '[' + todayFmt + ' — In-Person Visit' + (outcome ? ' / ' + outcome : '') + ']: ' + notes.trim();
      await pool.query(
        `UPDATE prospects SET notes = CASE WHEN notes IS NULL OR notes = '' THEN $1 ELSE notes || E'\n' || $1 END WHERE id=$2 AND user_id=$3`,
        [noteEntry, prospect_id, uid]
      );
    }
    if (outcome && outcome.includes('Interested') && !outcome.includes('Not')) {
      await pool.query("UPDATE prospects SET status='Warm' WHERE id=$1", [prospect_id]);
    } else if (outcome === 'Ready to Order' || outcome === 'Ready to Buy') {
      await pool.query("UPDATE prospects SET status='Hot' WHERE id=$1", [prospect_id]);
    } else if (outcome && outcome.includes('Not Interested')) {
      await pool.query("UPDATE prospects SET status='Cold' WHERE id=$1", [prospect_id]);
    }
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── VERIFY LEAD ─────────────────────────────────────────────────────────────
// Called before "Add to Route" — returns CRM cross-reference + Street View info
router.post('/verify-lead', async (req, res) => {
  const uid = req.session.user.id;
  const { company, address, phone } = req.body;

  const result = { crm: null, street_view: null };

  // ── Check C: CRM cross-reference ──────────────────────────────────────────
  try {
    // Normalise phone to digits only for matching
    const phoneDigits = (phone || '').replace(/\D/g, '');

    // Match by company name (case-insensitive), phone digits, or address (case-insensitive)
    const crmQuery = `
      SELECT
        p.id,
        p.company,
        p.address,
        p.phone,
        p.notes,
        p.last_activity_at,
        u.name  AS rep_name,
        (SELECT c.call_date FROM calls c WHERE c.prospect_id = p.id ORDER BY c.call_date DESC LIMIT 1) AS last_call_date,
        (SELECT c.notes    FROM calls c WHERE c.prospect_id = p.id ORDER BY c.call_date DESC LIMIT 1) AS last_call_note
      FROM prospects p
      LEFT JOIN users u ON u.id = p.user_id
      WHERE
        ($1::text <> '' AND LOWER(p.company) = LOWER($1))
        OR ($2::text <> '' AND REGEXP_REPLACE(p.phone,'[^0-9]','','g') = $2)
        OR ($3::text <> '' AND LOWER(p.address) = LOWER($3))
      LIMIT 1
    `;
    const crmRes = await pool.query(crmQuery, [
      company || '',
      phoneDigits,
      address || ''
    ]);
    if (crmRes.rows.length > 0) {
      const row = crmRes.rows[0];
      const activityDate = row.last_call_date || row.last_activity_at;
      let daysAgo = null;
      if (activityDate) {
        const diff = Date.now() - new Date(activityDate).getTime();
        daysAgo = Math.round(diff / (1000 * 60 * 60 * 24));
      }
      result.crm = {
        found: true,
        rep_name: row.rep_name || null,
        days_ago: daysAgo,
        last_note: row.last_call_note || row.notes || null
      };
    }
  } catch(e) {
    console.error('[verify-lead] CRM check failed:', e.message);
    // skip silently
  }

  // ── Check D: Street View metadata ────────────────────────────────────────
  try {
    if (address && PLACES_KEY) {
      const svUrl = `https://maps.googleapis.com/maps/api/streetview/metadata?location=${encodeURIComponent(address)}&key=${PLACES_KEY}`;
      const controller = new AbortController();
      const svTimeout = setTimeout(() => controller.abort(), 2500);
      try {
        const svRes = await fetch(svUrl, { signal: controller.signal });
        clearTimeout(svTimeout);
        const svData = await svRes.json();
        result.street_view = {
          status: svData.status || 'UNKNOWN',
          image_url: svData.status === 'OK'
            ? `https://maps.googleapis.com/maps/api/streetview?size=400x300&location=${encodeURIComponent(address)}&key=${PLACES_KEY}`
            : null
        };
      } catch(e2) {
        clearTimeout(svTimeout);
        // timeout or fetch error — skip silently
      }
    }
  } catch(e) {
    console.error('[verify-lead] Street View check failed:', e.message);
    // skip silently
  }

  res.json(result);
});

module.exports = router;

// Test surface — the pure relevance/filter predicates, exposed for unit tests
// and offline verification of the reject path (does not affect routing).
module.exports._filters = {
  hardExcludedType, hardExcludedName, isBigBoxBlocked, offTopicReason,
  isGarageDoorOnly, MIN_OPPORTUNITY_SCORE,
};
