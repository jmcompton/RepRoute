const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const { accountKey } = require('../lib/account-key');
const { geocodeAddress, geocodeLookup } = require('../lib/geocode');
// PLACES_BASE_URL overrides Google's host (used to point tests at a stub).
const PLACES_SEARCH_URL = (process.env.PLACES_BASE_URL || 'https://places.googleapis.com').replace(/\/$/, '') + '/v1/places:searchText';
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-6';

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

// Big-box retail — ALWAYS excluded from the lumber/building-supply category
// (and harmless to block everywhere: these are never walkable rep targets).
// NOTE: intentionally does NOT include "84 Lumber" or "Ace" — Builders
// FirstSource is a Flavor-A anchor and Cornelia Ace is a Flavor-B anchor, so we
// only block true consumer big-box chains here.
const BIG_BOX_KEYWORDS = ['home depot', "lowe's", 'lowes', 'menards'];
function isBigBoxBlocked(name) {
  const lower = (name || '').toLowerCase();
  return BIG_BOX_KEYWORDS.some(kw => lower.includes(kw));
}

// Few-shot ranking nudge for the lumber/building-supply segment: reward results
// that share the "kind" of Keith's anchors (lumber / building supply / building
// materials / supply co / farm & home / lumber-carrying hardware). Returns a
// small positive boost; does not penalize, so uncertain hardware/farm stores
// still surface for the rep to decide.
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
  // Decking / railing supply houses and lumber yards with a decking program.
  // Was missing: the default-checked "Deck Distributors" pill had no query set,
  // so the server rejected it as an unknown segment and it never returned leads.
  'Deck Distributor/Dealer': [
    { query: 'decking supply distributor', score: 10, category: 'Deck Distributor/Dealer' },
    { query: 'composite decking dealer', score: 10, category: 'Deck Distributor/Dealer' },
    { query: 'deck and railing supply', score: 10, category: 'Deck Distributor/Dealer' },
    { query: 'Trex TimberTech decking dealer', score: 9, category: 'Deck Distributor/Dealer' },
    { query: 'lumber yard decking materials', score: 9, category: 'Deck Distributor/Dealer' },
    { query: 'outdoor living building products distributor', score: 8, category: 'Deck Distributor/Dealer' },
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
  // Spray foam insulation / SPF roofing installers. Queries lead with "spray foam"
  // so Google doesn't drift into generic fiberglass/blown-in insulation shops.
  'Spray Foam Contractor': [
    { query: 'spray foam insulation contractor', score: 10, category: 'Spray Foam Contractor' },
    { query: 'spray foam insulation company', score: 10, category: 'Spray Foam Contractor' },
    { query: 'closed cell spray foam insulation installer', score: 9, category: 'Spray Foam Contractor' },
    { query: 'commercial spray foam insulation contractor', score: 9, category: 'Spray Foam Contractor' },
    { query: 'spray foam roofing contractor', score: 8, category: 'Spray Foam Contractor' },
  ],
  // Division 10 specialty building products dealers/installers — toilet partitions,
  // lockers, signage, fire extinguisher cabinets, wall protection, etc. Tuned against
  // live Google results (Atlanta + Charlotte): toilet-partition/accessory terms return
  // the actual Div 10 houses; "lockers" alone returns warehouse racking, and "fire
  // extinguisher cabinets"/"signage" return fire-service companies and sign shops, so
  // those aren't queried directly. "division 10" alone skews to generic GCs → last.
  'Division 10 Dealer': [
    { query: 'toilet partitions and accessories distributor', score: 10, category: 'Division 10 Dealer' },
    { query: 'building specialties toilet partitions lockers', score: 10, category: 'Division 10 Dealer' },
    { query: 'toilet accessories restroom partitions supplier', score: 9, category: 'Division 10 Dealer' },
    { query: 'bathroom partitions commercial', score: 9, category: 'Division 10 Dealer' },
    { query: 'commercial toilet partition installer', score: 8, category: 'Division 10 Dealer' },
    { query: 'division 10 specialties contractor', score: 8, category: 'Division 10 Dealer' },
  ],
  // Commercial + residential glass and glazing companies — storefront, shower doors,
  // mirrors, window glass. Auto glass / windshield shops flood these searches and are
  // filtered in SEGMENT_RELEVANCE below.
  'Glass Shop': [
    { query: 'commercial glass and glazing contractor', score: 10, category: 'Glass Shop' },
    { query: 'storefront glass installation company', score: 10, category: 'Glass Shop' },
    { query: 'glass shop shower doors mirrors', score: 9, category: 'Glass Shop' },
    { query: 'residential glass company windows mirrors', score: 9, category: 'Glass Shop' },
    { query: 'frameless shower door installer', score: 8, category: 'Glass Shop' },
    { query: 'window glass replacement company', score: 8, category: 'Glass Shop' },
  ],
};

// Per-segment relevance rules for segments whose Google results are noisy.
//   block — name patterns that are clearly the wrong business (dropped)
//   boost — name/type signals that confirm the right business (+1 score)
const SEGMENT_RELEVANCE = {
  // Deck supply: drop deck builders/installers (that's the Deck Contractor pill).
  'Deck Distributor/Dealer': {
    block: /\b(decks?|patios?|fences?|outdoor living)\s*(&\s*\w+\s*)?(builders?|contractors?|construction|installers?|installation|pros?|masters?|design(s|ers)?|guys?|company|co\b)|handyman|remodel|(power|pressure) ?wash|staining|\bpainting\b/i,
    boost: /supply|lumber|building (products|materials|center)|decking|rail(ing)?|wholesale|distribut/i,
  },
  'Spray Foam Contractor': {
    block: /pest control|exterminat|termite|foam (fabricat|rubber|packag|cushion)|mattress|upholster|packaging|car wash|\bpool\b/i,
    boost: /spray ?foam|\bspf\b|foam insulation|polyurethane|insulat/i,
  },
  'Glass Shop': {
    block: /auto ?glass|windshield|windscreen|safelite|car glass|vehicle|\bauto\b|automotive|collision|tint|smoke|vape|hookah|head shop|\bpipes?\b|bong|glass ?blow|art glass|stained glass|glass art|studio|eyeglass|optical|eyewear|vision|bottle|recycl/i,
    boost: /\bglass\b|glazing|glazier|mirror|storefront|shower door|window/i,
    blockTypes: /car_repair|car_dealer|car_wash|auto_parts/,   // auto glass shops Google types as car repair
  },
  'Division 10 Dealer': {
    block: /\bfire\b|koorsen|cintas|sprinkler|extinguisher|locksmith|self.?storage|storage units?|mini.?storage|public storage|cubesmart|extra space|vaults?\b|warehouse|material handling|shelving|racking|\bbins?\b|portable toilet|porta.?(john|potty)|restroom (trailer|rental)|plumb|remodel|bath (fitter|center|planet|solutions)|\bbaths\b|home pros|majestic bath|west shore|home outlet|step through|walk.?in|shower|kitchen|glass|glazing|surplus|builders depot|renovate|general (contractors?|construction)|division (?!10\b)\d+|\bgym\b|fitness|fastsigns|sign.?a.?rama|signarama|\bsign (shop|company)\b|banner|vinyl graphics|print/i,
    boost: /specialt|division 10|div\.? ?10|section 10|partition|restroom stall|locker|accessor|washroom|wall protection|architectural products|building products/i,
  },
};

// Segments exempt from the Google-type Contractor/Dealer soft filter. Div 10
// dealers mostly furnish-and-install, so Google often types them general_contractor
// or construction_company — the Dealer filter would wrongly drop real ones.
// Glass shops: Google types many as building_materials_store, which the Contractor filter would drop.
const CHANNEL_FILTER_EXEMPT = new Set(['Division 10 Dealer', 'Glass Shop']);

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
const HOME_BASED_CORP_RX   = /\b(llc|inc|corp|co|company|group|associates|supply|supplies|wholesale|distributors?|lumber|building products)\b/i;
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
              (timezone($$America/New_York$$, NOW())::date - c.next_step_date) as days_overdue
       FROM calls c JOIN prospects p ON c.prospect_id = p.id
       WHERE c.user_id = $1 AND c.next_step_date IS NOT NULL
         AND c.next_step_date <= timezone($$America/New_York$$, NOW())::date
         AND c.next_step IS NOT NULL AND c.next_step != ''
       ORDER BY c.next_step_date ASC LIMIT 10`,
      [uid]
    );
    const neverCalled = await pool.query(
      `SELECT p.id, p.company, p.category, p.city, p.address, p.phone, p.priority,
              'new_prospect' as call_reason, p.created_at
       FROM prospects p
       WHERE p.user_id = $1 AND p.priority IN ('High', 'Medium')
         AND p.id NOT IN (SELECT DISTINCT prospect_id FROM calls WHERE user_id = $1 AND prospect_id IS NOT NULL)
       ORDER BY CASE p.priority WHEN 'High' THEN 1 ELSE 2 END, p.created_at ASC LIMIT 10`,
      [uid]
    );
    const todayCalls = await pool.query(
      'SELECT COUNT(*) FROM calls WHERE user_id=$1 AND call_date=timezone($$America/New_York$$, NOW())::date', [uid]
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
// Territory → coordinates, cached for a day (a territory rarely moves).
const _territoryCache = new Map();
async function territoryCenter(t) {
  const k = String(t || '').trim().toLowerCase();
  if (!k) return null;
  const hit = _territoryCache.get(k);
  if (hit && Date.now() - hit.at < 864e5) return hit.c;
  const c = await geocodeAddress(t);
  _territoryCache.set(k, { c, at: Date.now() });
  return c;
}

// Where a rep works: the middle of their mapped accounts, else their home base,
// else their territory. Used to find unmapped stops by name in the right area.
async function repRegionCenter(uid, userInfo) {
  const r = await pool.query(
    `SELECT AVG(lat)::float AS lat, AVG(lng)::float AS lng, COUNT(*)::int AS n
       FROM prospects WHERE user_id=$1 AND lat IS NOT NULL AND lng IS NOT NULL`, [uid]);
  if (r.rows[0] && r.rows[0].n >= 3) return { lat: r.rows[0].lat, lng: r.rows[0].lng };
  if (userInfo.home_base_lat != null && userInfo.home_base_lng != null) {
    return { lat: parseFloat(userInfo.home_base_lat), lng: parseFloat(userInfo.home_base_lng) };
  }
  return (userInfo.territory && await territoryCenter(userInfo.territory)) || null;
}

// Locate an account that has no usable address (e.g. Zoho imports, which came
// in with no address and a default state of GA): ask Google Places for the
// business by name near where the rep works, and accept it only if the name
// matches. On a match the account's address/city/state get filled in too, so
// it never has to be looked up again.
async function locateAccountByName(acct, region, placesKey) {
  try {
    const body = { textQuery: [acct.company, acct.city].filter(Boolean).join(' '), maxResultCount: 5, regionCode: 'US' };
    if (region) body.locationBias = { circle: { center: { latitude: region.lat, longitude: region.lng }, radius: 50000 } };
    const r = await fetch(PLACES_SEARCH_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Goog-Api-Key': placesKey,
                 'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.location' },
      body: JSON.stringify(body)
    });
    const d = await r.json();
    const want = accountKey(acct.company);
    const hits = (d.places || []).filter(p => {
      const k = accountKey(p.displayName && p.displayName.text);
      return p.location && k && want && (k === want || k.startsWith(want + ' ') || want.startsWith(k + ' '));
    });
    // If the account has a city, only that city's listings count.
    const cityLc = (acct.city || '').trim().toLowerCase();
    const inCity = cityLc ? hits.filter(p => (p.formattedAddress || '').toLowerCase().includes(cityLc)) : hits;
    if (cityLc) hits.splice(0, hits.length, ...inCity);
    if (!hits.length) return null;
    const hit = hits[0];
    // A chain with several branches in the area (ABC Supply, 84 Lumber, Ace):
    // without an address we can't tell which branch is this account, so don't
    // guess. The stop is skipped and the rep sees it needs an address.
    const otherBranch = hits.slice(1).some(p => distanceMiles(hit.location.latitude, hit.location.longitude, p.location.latitude, p.location.longitude) > 3);
    if (otherBranch) { console.log('[daily-leads] several branches match, not guessing:', acct.company); return null; }
    // Too far from where the rep works → probably a different business.
    if (region && distanceMiles(region.lat, region.lng, hit.location.latitude, hit.location.longitude) > 120) return null;
    const parts = (hit.formattedAddress || '').split(',').map(x => x.trim());
    const city = parts.length >= 3 ? parts[parts.length - 3] : null;
    const state = parts.length >= 2 ? (parts[parts.length - 2] || '').split(' ')[0] : null;
    await pool.query(
      `UPDATE prospects SET lat=$1, lng=$2, geocoded_at=NOW(),
              address = COALESCE(NULLIF(TRIM(address),''), $3),
              city = COALESCE(NULLIF(TRIM(city),''), $4),
              state = CASE WHEN NULLIF(TRIM(address),'') IS NULL AND $5::text IS NOT NULL THEN $5 ELSE state END,
              google_place_id = COALESCE(google_place_id, $6)
        WHERE id=$7`,
      [hit.location.latitude, hit.location.longitude, hit.formattedAddress || null, city, state && /^[A-Z]{2}$/.test(state) ? state : null, hit.id || null, acct.pid]);
    return { lat: hit.location.latitude, lng: hit.location.longitude, city, state };
  } catch (e) {
    console.error('[daily-leads] locate by name failed:', acct.company, e.message);
    return null;
  }
}

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
    'Deck Distributor/Dealer': 'Dealer',
    'Construction Fasteners': 'Dealer',
    'Lumber / Building Supply': 'Dealer',
    'Spray Foam Contractor': 'Contractor',
    'Division 10 Dealer':    'Dealer',
    'Glass Shop':            'Contractor',
    'Contractor':            'Contractor',
    'Dealer':                'Dealer',
  };
  const rawChannel = (req.body.channel || 'Contractor').trim();
  const channel = SEGMENT_TO_CHANNEL[rawChannel] || 'Contractor';
  console.log(`[daily-leads] uid=${req.session.user.id} rawChannel="${rawChannel}" → channel="${channel}" city=${req.body.city}`);
  const radiusMiles = parseInt(req.body.radius_miles) || 50; // Default 50mi vs old 5mi
  // Along-today's-route mode: route_date = the phone's local date. Leads are
  // searched around each planned stop and kept only if they're close to the
  // route; route_radius_miles = how far off a stop a lead may be (default 8).
  const routeDate = /^\d{4}-\d{2}-\d{2}$/.test(req.body.route_date || '') ? req.body.route_date : null;
  const routeRadius = Math.min(Math.max(parseFloat(req.body.route_radius_miles) || 8, 2), 25);

  try {
    // Every account on the TEAM (not just this rep's): this rep's own are
    // skipped; a teammate's come back flagged with their name, so reps never
    // cold-call each other's accounts as "new" leads.
    // Every branch is its own customer: a name match only counts when it's the
    // same Google listing, or the same name (accountKey rule) at the same spot
    // (within 1.5 mi; or same city when the account has no coordinates).
    // Chain branches 5 miles apart are different customers. "ABC Supply" in Kennesaw is NOT Sean's
    // "ABC Supply" in Marietta. A same-name account with no location on file
    // (e.g. a Zoho import) can't be ruled in or out, so the lead still shows,
    // with a "maybe already" note.
    const existing = await pool.query(
      `SELECT p.id, p.user_id, p.company, p.google_place_id, p.city, p.lat, p.lng,
              COALESCE(NULLIF(TRIM(u.name),''), u.email) AS owner
         FROM prospects p LEFT JOIN users u ON u.id = p.user_id`);
    const byPlace = new Map(), byKey = new Map();
    for (const r of existing.rows) {
      const rec = { id: r.id, mine: r.user_id === uid, owner: r.owner, owner_id: r.user_id,
                    city: (r.city || '').trim().toLowerCase(),
                    lat: r.lat != null ? parseFloat(r.lat) : null, lng: r.lng != null ? parseFloat(r.lng) : null };
      if (r.google_place_id) { const cur = byPlace.get(r.google_place_id); if (!cur || rec.mine) byPlace.set(r.google_place_id, rec); }
      const k = accountKey(r.company);
      if (k) { if (!byKey.has(k)) byKey.set(k, []); byKey.get(k).push(rec); }
    }
    // Where each rep works, so a teammate's address-less "ABC Supply" only
    // raises "maybe" for leads in that teammate's part of the map (Dan's
    // Birmingham import says nothing about a Kennesaw branch).
    const ownerRegion = new Map();
    {
      const rr = await pool.query(
        `SELECT u.id, u.territory, u.home_base_lat, u.home_base_lng, a.lat, a.lng, COALESCE(a.n, 0) AS n
           FROM users u LEFT JOIN (SELECT user_id, AVG(lat)::float AS lat, AVG(lng)::float AS lng, COUNT(*)::int AS n
                                     FROM prospects WHERE lat IS NOT NULL GROUP BY user_id) a ON a.user_id = u.id`);
      for (const u of rr.rows) {
        let c = null;
        if (u.n >= 3) c = { lat: u.lat, lng: u.lng };
        else if (u.home_base_lat != null && u.home_base_lng != null) c = { lat: parseFloat(u.home_base_lat), lng: parseFloat(u.home_base_lng) };
        else if (u.territory) c = await territoryCenter(u.territory);
        ownerRegion.set(u.id, c);
      }
    }
    // → { known: rec } same place · { maybe: rec } same name, no location · {} new
    function matchExisting(placeId, key, leadCity, lat, lng) {
      if (placeId && byPlace.has(placeId)) return { known: byPlace.get(placeId) };
      const recs = byKey.get(key) || [];
      const city = (leadCity || '').trim().toLowerCase();
      const same = recs.filter(r =>
        (r.lat != null && lat != null && distanceMiles(r.lat, r.lng, lat, lng) <= 1.5) ||
        (r.lat == null && r.city && city && r.city === city));
      if (same.length) return { known: same.find(r => r.mine) || same[0] };
      const unknown = recs.filter(r => {
        if (r.lat != null || r.city) return false;
        const reg = ownerRegion.get(r.owner_id);
        return !reg || lat == null || distanceMiles(reg.lat, reg.lng, lat, lng) <= 75;
      });
      if (unknown.length) return { maybe: unknown.find(r => r.mine) || unknown[0] };
      return {};
    }

    // Also exclude leads already shown on this device (for "10 new ones").
    const shownPlaceIds = new Set(Array.isArray(req.body.shown_place_ids) ? req.body.shown_place_ids : []);
    const shownKeys = new Set((Array.isArray(req.body.shown_names) ? req.body.shown_names : []).map(n => accountKey(n)));

    // Segment pill = exact query set.
    const segmentQueries = SEGMENT_SEARCH_CONFIG[rawChannel];
    if (!segmentQueries || !segmentQueries.length) {
      return res.status(400).json({ error: `Unknown segment: "${rawChannel}". Please select a valid segment.` });
    }

    // ── Where to search ────────────────────────────────────────────
    // City mode: one center (the typed city). Route mode: today's stops in
    // planned order (geocoded if needed), starting from the rep's home base.
    let centerCoords = null;
    let route = null;      // { path:[{lat,lng,label,stop_number}], stops:[...], missing }
    let plans = [];        // [{ textQuery, bias, centerIdx, config }]
    if (routeDate) {
      const st = await pool.query(
        `SELECT pi.id, pi.sort_order, p.id AS pid, p.company, p.city, p.state, p.address, p.lat, p.lng
           FROM planner_items pi JOIN prospects p ON p.id = pi.account_id
          WHERE pi.rep_id = $1 AND pi.planned_date = $2
          ORDER BY pi.sort_order ASC, pi.id ASC`, [uid, routeDate]);
      const stops = [];
      let missing = 0, region;
      for (const [i, r] of st.rows.entries()) {
        let lat = r.lat != null ? parseFloat(r.lat) : null, lng = r.lng != null ? parseFloat(r.lng) : null;
        let sc = r.city, ss = r.state;
        if ((lat == null || lng == null) && r.address && r.address.trim()) {
          // A real street address: geocode it.
          const g = await geocodeAddress([r.address, r.address.includes(',') ? null : r.city, r.address.includes(',') ? null : r.state].filter(Boolean).join(', '));
          if (g) {
            lat = g.lat; lng = g.lng;
            pool.query('UPDATE prospects SET lat=$1, lng=$2, geocoded_at=NOW() WHERE id=$3', [lat, lng, r.pid]).catch(() => {});
          }
        }
        if (lat == null || lng == null) {
          // No address (or it wouldn't map): find the business by name near the rep.
          if (region === undefined) region = await repRegionCenter(uid, userInfo);
          const f = await locateAccountByName(r, region, PLACES_KEY);
          if (f) { lat = f.lat; lng = f.lng; sc = sc || f.city; ss = f.state || ss; }
        }
        if (lat == null || lng == null) { missing++; continue; }
        stops.push({ lat, lng, label: r.company, city: sc, state: ss, stop_number: i + 1 });
      }
      if (!stops.length) {
        return res.status(400).json({
          error: st.rows.length ? "None of today's stops have an address we can map. Add addresses to those accounts or search by city."
                                : "No stops on today's route yet. Add stops, or search by city.",
          code: 'NO_ROUTE' });
      }
      const path = [];
      if (userInfo.home_base_lat != null && userInfo.home_base_lng != null) {
        path.push({ lat: parseFloat(userInfo.home_base_lat), lng: parseFloat(userInfo.home_base_lng), label: 'Home', stop_number: 0 });
      }
      stops.forEach(s => path.push(s));
      route = { path, stops, missing };
      // Search centers: stops, merging ones within 6 miles of the last center,
      // capped at 5 (evenly spread) to keep Google calls in check.
      let centers = [];
      for (const s of stops) {
        const last = centers[centers.length - 1];
        if (!last || distanceMiles(last.lat, last.lng, s.lat, s.lng) > 6) centers.push(s);
      }
      if (centers.length > 5) centers = [0, 1, 2, 3, 4].map(k => centers[Math.round(k * (centers.length - 1) / 4)]);
      route.centers = centers;
      const topQueries = segmentQueries.slice().sort((a, b) => b.score - a.score).slice(0, 3);
      // Interleave centers so every part of the route gets leads, not just the first stop.
      for (const config of topQueries) {
        centers.forEach((c, ci) => plans.push({
          textQuery: c.city ? `${config.query} near ${c.city}${c.state ? ' ' + c.state : ''}` : config.query,
          bias: { circle: { center: { latitude: c.lat, longitude: c.lng }, radius: Math.min(routeRadius * 1609.34 * 1.5, 40000) } },
          centerIdx: ci, config
        }));
      }
      centerCoords = { lat: centers[0].lat, lng: centers[0].lng };
    } else {
      // Geocode the city — always use the typed city first, fall back to home base
      // A typed city that won't geocode is an error, never a silent switch to
      // the rep's home base (they'd get leads for the wrong town).
      let geoErr = null, geoMissing = false;
      if (String(city || '').trim()) {
        const g = await geocodeLookup(city);
        if (g.coords) { centerCoords = g.coords; console.log(`Geocoded "${city}" → ${g.coords.lat}, ${g.coords.lng}`); }
        else if (g.error) geoErr = g.error; else geoMissing = true;
      }
      if (geoErr) {
        console.error('Geocode failed:', geoErr);
        return res.status(502).json({ error: "Google maps isn't responding right now. Try again in a minute.", code: 'SEARCH_FAILED' });
      }
      if (geoMissing) {
        return res.status(400).json({ error: `Couldn't find "${city}". Try the city and state, like Hoover AL.` });
      }
      if (!centerCoords && userInfo.home_base_lat && userInfo.home_base_lng) {
        centerCoords = { lat: parseFloat(userInfo.home_base_lat), lng: parseFloat(userInfo.home_base_lng) };
        console.log('Using home base coords as fallback');
      }
      if (!centerCoords) {
        return res.status(500).json({ error: `Could not locate city: ${city}. Please check the city name.` });
      }
      // City is in the query text; a soft 40 km locationBias (kept under Google's
      // 50 km limit) is added at search time.
      const seenQ = new Set();
      for (const config of segmentQueries) {
        if (seenQ.has(config.query)) continue;
        seenQ.add(config.query);
        plans.push({ textQuery: `${config.query} ${city}`, bias: null, centerIdx: 0, config });
      }
    }
    console.log(`[daily-leads] segment="${rawChannel}" mode=${route ? 'route' : 'city'} → ${plans.length} searches`);

    // Distance from the route: nearest stop, and the extra miles to swing by
    // (best detour between consecutive points on home → stop 1 → stop 2 …).
    function routeFit(lat, lng) {
      let nearest = null, nearestD = Infinity;
      for (const s of route.stops) {
        const d = distanceMiles(s.lat, s.lng, lat, lng);
        if (d < nearestD) { nearestD = d; nearest = s; }
      }
      let detour = 2 * nearestD;
      const pts = route.path;
      for (let i = 0; i + 1 < pts.length; i++) {
        const a = pts[i], b = pts[i + 1];
        const extra = distanceMiles(a.lat, a.lng, lat, lng) + distanceMiles(lat, lng, b.lat, b.lng) - distanceMiles(a.lat, a.lng, b.lat, b.lng);
        if (extra < detour) detour = extra;
      }
      return { nearest, nearestD, detour: Math.max(0, detour) };
    }

    const MAX_LEADS = route ? 40 : 25;
    const perCenterCap = route ? Math.max(6, Math.ceil(MAX_LEADS / route.centers.length)) : Infinity;
    const perCenter = {};
    const allLeads = [];
    const flagged = [];
    const sessionSeen = new Set();

    let searched = 0, searchFails = 0;
    for (const plan of plans) {
      if (allLeads.length >= MAX_LEADS) break;
      if ((perCenter[plan.centerIdx] || 0) >= perCenterCap) continue;
      const config = Object.assign({}, plan.config, { brand: brands[0] || rawChannel });
      searched++;
      try {
        const searchBody = { textQuery: plan.textQuery, maxResultCount: 20, regionCode: 'US' };
        // Route: circle around the stop. City: soft 40 km bias around the city so a
        // name shared by several places ("Birmingham", "Columbus") stays local.
        searchBody.locationBias = plan.bias || { circle: { center: { latitude: centerCoords.lat, longitude: centerCoords.lng }, radius: 40000 } };
        const placesRes = await fetch(PLACES_SEARCH_URL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': PLACES_KEY,
            'X-Goog-FieldMask': [
              'places.id', 'places.displayName', 'places.formattedAddress', 'places.nationalPhoneNumber',
              'places.websiteUri', 'places.rating', 'places.userRatingCount', 'places.businessStatus',
              'places.location', 'places.primaryTypeDisplayName', 'places.types', 'places.photos',
              'places.regularOpeningHours'
            ].join(',')
          },
          body: JSON.stringify(searchBody)
        });
        const data = await placesRes.json();
        if (!placesRes.ok || data.error) throw new Error((data.error && data.error.message) || ('Places ' + placesRes.status));
        if (data.error) console.error('[daily-leads] Places error:', data.error.message || data.error.status);
        const places = data.places || [];

        for (const place of places) {
          if (allLeads.length >= MAX_LEADS) break;
          if ((perCenter[plan.centerIdx] || 0) >= perCenterCap) break;

          const company = place.displayName?.text || '';
          const placeId = place.id || '';
          if (!company) continue;
          const key = accountKey(company);
          if (sessionSeen.has(placeId || key)) continue;

          // Basic validity checks
          if (isResidentialRooferBlocked(company, place.types || [], rawChannel)) continue;
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          if (placeId && shownPlaceIds.has(placeId)) continue;
          if (shownKeys.has(key)) continue;
          // Hard block — big-box retailers (Home Depot, Lowe's, Menards) are
          // ALWAYS excluded; they are never walkable rep targets.
          if (isBigBoxBlocked(company)) continue;
          // Hard block — never serve paint shops/painters as Alum-A-Pole leads
          if (isPaintBlocked(company, config.brand)) continue;
          // Hard block — never serve garage/overhead door companies for Window/Door segment
          if (isGarageDoorBlocked(company, rawChannel)) continue;
          // Hard block — never return heavy equipment / construction machinery companies
          if (isHeavyEquipmentBlocked(company, place.types || [])) continue;
          const relevance = SEGMENT_RELEVANCE[rawChannel];
          if (relevance && relevance.block.test(company)) continue;
          if (relevance && relevance.blockTypes && relevance.blockTypes.test((place.types || []).join(' '))) continue;

          // Distance: from the city center, or how far off today's route.
          let distMi = null, fit = null;
          if (place.location?.latitude != null) {
            if (route) {
              fit = routeFit(place.location.latitude, place.location.longitude);
              if (fit.nearestD > routeRadius && fit.detour > routeRadius * 1.5) continue;
              distMi = Math.round(fit.nearestD * 10) / 10;
            } else {
              distMi = Math.round(distanceMiles(centerCoords.lat, centerCoords.lng, place.location.latitude, place.location.longitude) * 10) / 10;
              if (distMi > radiusMiles) continue;
            }
          } else if (route) {
            continue; // can't place it on the route
          }

          // Use Google's place types for accurate classification (not just name matching)
          const placeTypes = new Set(place.types || []);
          const primaryType = place.primaryTypeDisplayName?.text || config.category;
          // Soft channel filter — only block clear mismatches, not ambiguous types
          const isClassifiedContractor = [...placeTypes].some(t => CONTRACTOR_PLACE_TYPES.has(t));
          const isClassifiedDealer = [...placeTypes].some(t => DEALER_PLACE_TYPES.has(t));
          const channelFilterOn = !CHANNEL_FILTER_EXEMPT.has(rawChannel);
          if (channelFilterOn && channel === 'Dealer' && isClassifiedContractor && !isClassifiedDealer && placeTypes.size > 2) continue;
          if (channelFilterOn && channel === 'Contractor' && isClassifiedDealer && !isClassifiedContractor && placeTypes.size > 2) continue;
          sessionSeen.add(placeId || key);

          // Already in RepRoute? Mine → skip. A teammate's → flag with their name.
          const addrPartsM = (place.formattedAddress || '').split(',').map(x => x.trim());
          const leadCityM = addrPartsM.length >= 3 ? addrPartsM[addrPartsM.length - 3] : '';
          const m = matchExisting(placeId, key, leadCityM, place.location?.latitude ?? null, place.location?.longitude ?? null);
          const known = m.known || null;
          if (known && known.mine) continue;

          // Count how many product lines this prospect is relevant to
          const matchingBrands = brands.filter(brand => {
            const bc = PRODUCT_SEARCH_CONFIG[brand]?.[channel];
            return bc?.some(sc => sc.category === config.category || sc.query.split(' ')[0] === config.query.split(' ')[0]);
          });
          // Route mode scores by detour (miles out of the way), city mode by distance.
          let opportunityScore = calcOpportunityScore(
            config.score,
            route ? fit.detour : (distMi || 25),
            place.userRatingCount || 0,
            place.rating || 0,
            matchingBrands.length,
            channel
          );
          // Lumber / building-supply few-shot ranking nudge.
          if (rawChannel === 'Lumber / Building Supply') {
            opportunityScore = Math.min(10, opportunityScore + lumberKindBoost(company, place.types || []));
          }
          // Specialty segments: +1 when the name/types confirm the right business.
          if (relevance && relevance.boost.test(company + ' ' + (place.types || []).join(' '))) {
            opportunityScore = Math.min(10, opportunityScore + 1);
          }
          // Home-based business detection — badge + optional score penalty
          const homeBasedResult = isHomeBased(place.formattedAddress || '', company, place.websiteUri || '', place.types || []);
          let homeBasedBadge = false;
          // Badge only when both signals agree: a residential-looking street AND no
          // website or store type. "Industrial Blvd" alone is not a house.
          if (homeBasedResult.primary && homeBasedResult.secondary) {
            homeBasedBadge = true;
            opportunityScore = Math.max(1, opportunityScore - 15);
          } else if (homeBasedResult.secondary) {
            opportunityScore = Math.max(1, opportunityScore - 5);
          }

          // Parse city/state from formatted address
          const addrParts = (place.formattedAddress || '').split(',').map(s => s.trim());
          const cityName = addrParts.length >= 2 ? addrParts[addrParts.length - 3] || addrParts[1] : city;
          const stateZip = addrParts.length >= 2 ? addrParts[addrParts.length - 2] || '' : '';
          const stateName = stateZip.split(' ')[0] || 'GA';

          const lead = {
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
            territory: route ? 'Along today\'s route' : city,
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
          };
          if (route) {
            lead.near_stop = fit.nearest.label;
            lead.near_stop_number = fit.nearest.stop_number;
            lead.detour_miles = Math.round(fit.detour * 10) / 10;
            lead.why = `Near stop ${fit.nearest.stop_number} (${fit.nearest.label}), about ${lead.detour_miles} mi out of the way`;
          }
          if (m.maybe) {
            lead.possible_match = { account_id: m.maybe.id, mine: m.maybe.mine, owner: m.maybe.owner };
          }
          if (known) {
            lead.already_contacted = true;
            lead.contacted_by = known.owner;
            lead.account_id = known.id;
            if (flagged.length < 5) flagged.push(lead);
            continue;
          }
          perCenter[plan.centerIdx] = (perCenter[plan.centerIdx] || 0) + 1;
          allLeads.push(lead);
        }
      } catch (e) {
        console.error('Places search error for query:', plan.textQuery, e.message);
        searchFails++;
      }
    }
    // Every Google call failed: say so instead of "no leads in this area".
    if (searchFails && searchFails === searched && !allLeads.length && !flagged.length) {
      return res.status(502).json({ error: "Google search isn't responding right now. Try again in a minute.", code: 'SEARCH_FAILED' });
    }

    // Sort: opportunity score DESC, then (route) least out of the way / (city) closest.
    allLeads.sort((a, b) => {
      if (b.opportunity_score !== a.opportunity_score) return b.opportunity_score - a.opportunity_score;
      const da = route ? (a.detour_miles ?? 999) : (a.distance_miles ?? 999);
      const db = route ? (b.detour_miles ?? 999) : (b.distance_miles ?? 999);
      return da - db;
    });

    const topLeads = allLeads.slice(0, route ? 15 : 10).concat(flagged);

    res.json({
      ok: true,
      leads: require('../lib/defang').defang(topLeads, ''), // Google names are outside text too
      brands_used: brands,
      channel,
      mode: route ? 'route' : 'city',
      radius_miles: route ? routeRadius : radiusMiles,
      center: { city, coords: centerCoords },
      route: route ? {
        stops: route.stops.map(s => ({ stop_number: s.stop_number, company: s.label, city: s.city })),
        searched_near: route.centers.map(c => c.label),
        missing_location: route.missing
      } : null,
      total_found: allLeads.length,
      excluded_count: byKey.size
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
    // Write exactly one calls row (same shape as POST /api/calls) so a Lead Finder
    // log counts toward "calls logged today" and shows in the calls list. Use the
    // DB's CURRENT_DATE for call_date — the SAME basis the today-counter queries
    // (callsMadeToday / /api/calls/today both filter call_date = CURRENT_DATE) — so
    // a call logged now always lands on today regardless of app/DB timezone skew
    // (the previous app-side UTC date could miss the DB's current day).
    await pool.query(
      `INSERT INTO calls (user_id, prospect_id, call_date, call_type, outcome, next_step, next_step_date, notes)
       VALUES ($1,$2,timezone($$America/New_York$$, NOW())::date,$3,$4,$5,$6,$7)`,
      [uid, prospect_id, 'In-Person Visit', outcome || '', next_step || '', next_step_date || null, notes || null]
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
      await pool.query("UPDATE prospects SET status='Warm' WHERE id=$1 AND user_id=$2", [prospect_id, uid]);
    } else if (outcome === 'Ready to Order' || outcome === 'Ready to Buy') {
      await pool.query("UPDATE prospects SET status='Hot' WHERE id=$1 AND user_id=$2", [prospect_id, uid]);
    } else if (outcome && outcome.includes('Not Interested')) {
      await pool.query("UPDATE prospects SET status='Cold' WHERE id=$1 AND user_id=$2", [prospect_id, uid]);
    }
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Street View photo through our server so the Google key stays private.
router.get('/streetview', async (req, res) => {
  const loc = String(req.query.location || '').slice(0, 300);
  const key = process.env.GOOGLE_PLACES_API_KEY;
  if (!loc || !key) return res.status(404).end();
  try {
    const r = await fetch(`https://maps.googleapis.com/maps/api/streetview?size=400x300&location=${encodeURIComponent(loc)}&key=${key}`);
    if (!r.ok) return res.status(502).end();
    res.set('Content-Type', r.headers.get('content-type') || 'image/jpeg');
    res.set('Cache-Control', 'private, max-age=86400');
    r.body.pipe(res);
  } catch (e) { res.status(502).end(); }
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
            ? `/api/morning/streetview?location=${encodeURIComponent(address)}`  // proxied: the Google key never reaches the browser
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
