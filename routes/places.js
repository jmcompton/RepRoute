const express = require('express');
const { pool } = require('../db');
const router = express.Router();
const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;

// ═══════════════════════════════════════════════════════════════
// DISTRIBUTION-FIRST LEAD ENGINE
// This system targets distribution accounts, dealer networks,
// contractor supply chains, and mid-market commercial buyers.
// It does NOT target residential contractors or retail consumers.
// ═══════════════════════════════════════════════════════════════

// Hard block — never return paint shops/painters as Alum-A-Pole leads
const ALUM_PAINT_BLOCK = ['paint', 'painting', 'painter', 'painters'];
function alumPoleBlocked(name) {
  const lower = (name || '').toLowerCase();
  return ALUM_PAINT_BLOCK.some(kw => lower.includes(kw));
}

// Garage door block — applies to all Window/Door segment searches
const GARAGE_DOOR_BLOCK = ['garage door', 'garage doors', 'overhead door', 'overhead garage'];
function garageDoorBlocked(name) {
  const lower = (name || '').toLowerCase();
  return GARAGE_DOOR_BLOCK.some(kw => lower.includes(kw));
}

// ── Global bad-lead blocklist ────────────────────────────────────
const BAD_LEAD_KEYWORDS = [
  'bank','credit union','financial','insurance','mortgage','lending','loan','wealth',
  'investment','securities','advisor','brokerage','finance',
  'hospital','clinic','medical','dental','doctor','physician','healthcare','therapy',
  'urgent care','pharmacy','chiropractor','optom','vision center',
  'restaurant','diner','cafe','coffee','food','grocery','supermarket','walmart',
  'target','home depot','lowes','ace hardware',
  'government','county','city hall','police','fire station','post office','dmv',
  'school district','university','college','church','temple','mosque','synagogue',
  'office park','business center','coworking','regus','wework','executive suites',
  'realty','realtor','real estate','property management','apartment','condo','hoa',
  'auto dealer','car dealer','dealership','used cars','automotive repair',
  'hair salon','nail salon','spa','massage','tattoo','gym','fitness','yoga',
  'daycare','childcare','staffing agency','temp agency',
  // Residential-only signals (excluded — we want commercial/distribution)
  'handyman','honey-do','fix-it','home repair services','odd jobs',
  'house painting','residential painting','interior painting','exterior painting',
  'landscaping','lawn care','lawn service','pest control','pool service',
  'pressure washing','window cleaning','gutter cleaning','house cleaning',
  'carpet cleaning','junk removal','moving company','storage unit',
  // Virtual offices / mail drops — no physical location, no value as lead
  'virtual office','virtual address','mail forwarding','mail drop','mailbox rental',
  'ups store','the ups store','postal annex','pak mail','mailboxes etc',
  'regus','wework','spaces coworking','industrious','coworking space',
  'shared office','executive suite','business address service',
  'registered agent','incorporation service','llc formation'
];

// ── Distribution-positive signals (boost confidence) ──────────
const DISTRIBUTION_SIGNALS = [
  'distributor','distribution','dealer','dealership','dealer network',
  'supply','supplies','supplier','wholesale','wholesaler',
  'building products','building materials','building supply',
  'contractor supply','trade supply','industrial supply',
  'lumber dealer','specialty lumber','hardwood dealer',
  'roofing supply','roofing distributor','roofing materials',
  'siding supply','siding distributor','exterior products',
  'deck supply','decking distributor','deck dealer',
  'fastener supply','tool dealer','equipment dealer',
  'manufacturer rep','authorized dealer','stocking dealer',
  '2-step','two step distributor','regional distributor',
  'commercial roofing','commercial contractor','commercial builder',
  'metal roofing','flat roofing','low slope','tpo','epdm','pvc roofing'
];

// ── Residential-only exclusion (applies to all searches) ────────
// We still include residential contractors if they are explicitly
// a target for a brand — but pure residential-only businesses are out
const RESIDENTIAL_ONLY_SIGNALS = [
  'residential only','homeowner','home owner service','house call',
  'home service','home services','home improvement handyman',
  'home repair handyman','household repair','residential cleaning',
];

// ── Enterprise chains that don't buy from small reps ────────────
const ENTERPRISE_BLOCKLIST = [
  'dr horton','nvr ','toll brothers','kb home','lennar','pulte','centex',
  'beazer','meritage','william lyon','century communities',
  'abc supply','builders firstsource','us lbm','builders general',
  'beacon roofing supply','bradco','gulfeagle','atlas roofing corp',
  'allied building products','northwest building products',
  'servpro','servicemaster','restoration 1','paul davis',
  'home services of america','2-10 home buyers',
];

// ── Home-based / micro signals ───────────────────────────────────
const HOME_BASED_SIGNALS = [
  'home-based','home based','owner operator','owner-operator',
  'sole prop','one man','one-man','1 man','handyman services',
  'my home','from home',
];

// ── Large corporate holding signals ─────────────────────────────
const MEGA_CORP_SIGNALS = [
  'group llc nationwide','holding','holdings','properties llc',
  'development corp','developments llc','realty group','realty corp',
  'global services','international services','worldwide',
];

const REVIEW_MIN_ICP = 5;
const REVIEW_MAX_ICP = 600;

// ── Reachability scorer ──────────────────────────────────────────
function getReachabilityScore(place, name) {
  const lower = (name || '').toLowerCase();
  const reviews = place.userRatingCount || 0;
  const rating = place.rating || 0;
  const hasPhone = !!place.nationalPhoneNumber;
  const hasWebsite = !!place.websiteUri;
  const hasHours = !!(place.regularOpeningHours && place.regularOpeningHours.weekdayDescriptions);

  let score = 0;
  let reasons = [];
  let deductions = [];

  if (hasPhone) { score += 30; reasons.push('direct phone listed'); }
  if (hasWebsite) { score += 10; reasons.push('website present'); }
  if (hasHours) { score += 10; reasons.push('business hours listed'); }

  if (reviews >= REVIEW_MIN_ICP && reviews <= 300) {
    score += 25; reasons.push('review count signals mid-market size');
  } else if (reviews > 300 && reviews <= REVIEW_MAX_ICP) {
    score += 10; reasons.push('established business, may have some gatekeeping');
  }

  if (rating >= 4.0 && reviews >= REVIEW_MIN_ICP) {
    score += 15; reasons.push('highly rated active business');
  }

  // Distribution bonus — these are prime targets
  for (const kw of DISTRIBUTION_SIGNALS) {
    if (lower.includes(kw)) { score += 20; reasons.push('distribution/dealer signal: ' + kw); break; }
  }

  if (reviews < REVIEW_MIN_ICP) {
    score -= 20; deductions.push('very few reviews — likely micro or home-based');
  }
  if (reviews > REVIEW_MAX_ICP) {
    score -= 25; deductions.push('very high review volume — likely large corp');
  }
  for (const kw of ENTERPRISE_BLOCKLIST) {
    if (lower.includes(kw)) { score -= 40; deductions.push('enterprise signal: ' + kw); break; }
  }
  for (const kw of MEGA_CORP_SIGNALS) {
    if (lower.includes(kw)) { score -= 30; deductions.push('corporate holding signal'); break; }
  }
  for (const kw of HOME_BASED_SIGNALS) {
    if (lower.includes(kw)) { score -= 35; deductions.push('home-based or solo operator signal'); break; }
  }

  const finalScore = Math.max(0, Math.min(100, score));
  let tier, label;
  if (finalScore >= 60) { tier = 'HIGH'; label = 'High Reachability'; }
  else if (finalScore >= 35) { tier = 'MEDIUM'; label = 'Medium Reachability'; }
  else { tier = 'LOW'; label = 'Low Reachability'; }

  return { tier, label, score: finalScore, signals: [...reasons, ...deductions.map(d => '(-) ' + d)] };
}

function getSizeTier(place) {
  const reviews = place.userRatingCount || 0;
  if (reviews < REVIEW_MIN_ICP) return 'Micro';
  if (reviews < 30) return 'Small';
  if (reviews < 150) return 'Mid';
  return 'Large';
}

function buildInclusionReason(place, name, category, reachability) {
  const reviews = place.userRatingCount || 0;
  const rating = place.rating || 0;
  const hasPhone = !!place.nationalPhoneNumber;
  const hasWebsite = !!place.websiteUri;
  const lower = (name || '').toLowerCase();

  const parts = [];
  if (hasPhone) parts.push('direct phone available');
  if (rating >= 4.0 && reviews >= 10) parts.push(rating.toFixed(1) + '★ with ' + reviews + ' reviews');
  else if (reviews >= 10) parts.push(reviews + ' reviews');
  if (hasWebsite) parts.push('web presence confirmed');
  // Highlight if it's a distributor or dealer
  for (const kw of ['distributor','dealer','supply','wholesale']) {
    if (lower.includes(kw)) { parts.unshift('distribution/dealer account'); break; }
  }
  return parts.length ? parts.join(', ') + '.' : 'Active ' + category + ' in target territory.';
}

// Google Place type exclusions
const BAD_PLACE_TYPES = new Set([
  'bank','finance','insurance_agency','real_estate_agency','lodging','hotel',
  'motel','campground','rv_park','school','university','hospital','doctor',
  'dentist','pharmacy','veterinary_care','church','mosque','synagogue','hindu_temple',
  'funeral_home','cemetery','police','fire_station','post_office','local_government_office',
  'city_hall','courthouse','embassy','library','museum','movie_theater','night_club',
  'bar','casino','bowling_alley','amusement_park','zoo','park','stadium',
  'food','restaurant','cafe','bakery','meal_delivery','meal_takeaway',
  'grocery_or_supermarket','supermarket','convenience_store','gas_station',
  'car_dealer','car_rental','car_repair','car_wash',
  'beauty_salon','hair_care','spa','gym','fitness_center','laundry',
  'clothing_store','shoe_store','jewelry_store','book_store','pet_store',
  'florist','pharmacy','drugstore','atm','transit_station','parking','bus_station',
  'airport','train_station','subway_station','taxi_stand'
]);

function hasBadPlaceType(types) {
  if (!types || !types.length) return false;
  return types.some(function(t) { return BAD_PLACE_TYPES.has(t); });
}

function isBadLead(name, address, category) {
  if (!name) return true;
  const lower = name.toLowerCase();

  for (const kw of BAD_LEAD_KEYWORDS) {
    if (new RegExp(kw).test(lower)) return true;
  }
  for (const kw of HOME_BASED_SIGNALS) {
    if (lower.includes(kw)) return true;
  }
  for (const kw of MEGA_CORP_SIGNALS) {
    if (lower.includes(kw)) return true;
  }
  for (const kw of ENTERPRISE_BLOCKLIST) {
    if (lower.includes(kw)) return true;
  }
  for (const kw of RESIDENTIAL_ONLY_SIGNALS) {
    if (lower.includes(kw)) return true;
  }

  const nameWords = name.split(' ').filter(w => w.length > 1);
  if (nameWords.length < 2 && !lower.includes('co') && !lower.includes('inc') && !lower.includes('llc')) {
    const hasSignal = DISTRIBUTION_SIGNALS.some(s => lower.includes(s));
    if (!hasSignal) return true;
  }

  if (/po box|p\.o\. box/i.test((address||'').toLowerCase())) return true;
  // Virtual office / mail-drop addresses
  const addrLow = (address||'').toLowerCase();
  if (/suite\s+[a-z]\d+|#[a-z]\d+/i.test(address||'')) {
    // Flag suspiciously small suite numbers common in virtual offices
    // but only if combined with other virtual signals in the name
    const nameHasVirtSig = ['virtual','mail','postal','suite services'].some(s => (name||'').toLowerCase().includes(s));
    if (nameHasVirtSig) return true;
  }

  return false;
}

// ═══════════════════════════════════════════════════════════════
// DISTRIBUTION-FIRST SEARCH QUERY LIBRARY
// Each segment has queries ordered: distributors first, then dealers,
// then commercial contractors. Residential-only queries removed.
// ═══════════════════════════════════════════════════════════════
function getSearchQueries(product, customerType, segment) {
  const p = (product || '').toLowerCase();
  const seg = (segment || customerType || '').toLowerCase();

  // ── Window / Door Installer (exterior only — no garage doors) ──
  if (seg.includes('window') || seg.includes('door')) {
    return [
      'exterior window distributor building products',
      'window and door dealer contractor supply',
      'commercial window installation company',
      'storefront window glazing contractor commercial',
      'exterior door distributor building supply',
      'replacement window dealer contractor',
      'window manufacturer dealer authorized',
      'commercial door installation contractor',
      'building envelope contractor commercial',
      'fenestration dealer building products supply',
      'impact window dealer commercial',
      'window and door supply house wholesale',
    ];
  }

  // ── Deck Distributor / Dealer — Trex, TimberTech, Deckorators ──
  if (seg.includes('deck') && (seg.includes('distrib') || seg.includes('dealer') || seg.includes('supply') || seg.includes('lumber'))) {
    return [
      'Trex composite decking authorized dealer',
      'TimberTech AZEK decking dealer',
      'Deckorators dealer lumber supply',
      'Fiberon composite decking dealer',
      'MoistureShield decking dealer',
      'specialty lumber dealer deck products',
      'decking products distributor wholesale',
      'composite decking supply house',
      'deck materials distributor contractor supply',
      '2-step decking distributor building products',
      'lumber yard deck products contractor',
      'outdoor living products distributor',
    ];
  }

  // ── Deck Contractor (commercial / mid-market) ──
  if (seg.includes('deck') && seg.includes('contractor')) {
    return [
      'commercial deck contractor composite',
      'deck builder composite Trex TimberTech',
      'custom deck construction company commercial',
      'outdoor living contractor deck builder',
      'deck installation contractor commercial',
      'deck contractor steel framing commercial',
      'deck construction company multi-unit',
      'resort hotel deck contractor commercial',
    ];
  }

  // ── Roofing Distributor — Commercial brands only ──
  if (seg.includes('roofing') && (seg.includes('distrib') || seg.includes('supply'))) {
    return [
      'commercial roofing distributor authorized dealer',
      'Carlisle roofing authorized dealer distributor',
      'Johns Manville commercial roofing dealer',
      'CertainTeed commercial roofing distributor',
      'Sika Sarnafil roofing dealer',
      'Tremco roofing products distributor',
      'IKO commercial roofing dealer',
      'Holcim Amrize roofing product distributor',
      'metal roofing distributor dealer supply',
      'TPO EPDM roofing supply house wholesale',
      'low slope roofing materials distributor',
      'commercial roofing supply house contractor',
      'Metal-Era Membranes dealer distributor',
      'Derbigum roofing dealer authorized',
    ];
  }

  // ── Commercial Roofing Contractor ──
  if (seg.includes('roofing') && seg.includes('contractor')) {
    return [
      'commercial roofing contractor TPO EPDM',
      'commercial roofing company flat roof',
      'metal roofing contractor commercial',
      'low slope roofing contractor commercial',
      'industrial roofing contractor',
      'institutional roofing company commercial',
      'Carlisle TPO authorized roofing contractor',
      'Johns Manville roofing contractor certified',
      'CertainTeed commercial roofing applicator',
      'Sika Sarnafil approved contractor',
      'green roof contractor commercial',
      'roofing contractor warehouse industrial',
    ];
  }

  // ── Siding Contractor ──
  if (seg.includes('siding') && seg.includes('contractor')) {
    return [
      'siding contractor James Hardie installer',
      'fiber cement siding contractor commercial',
      'LP SmartSide contractor commercial',
      'vinyl siding contractor commercial',
      'exterior siding company contractor',
      'soffit fascia cornice contractor exterior',
      'commercial siding contractor multi-family',
      'stucco EIFS siding contractor commercial',
    ];
  }

  // ── Siding Distributor ──
  if (seg.includes('siding') && (seg.includes('distrib') || seg.includes('supply') || seg.includes('dealer'))) {
    return [
      'siding distributor building products wholesale',
      'James Hardie authorized distributor dealer',
      'LP SmartSide distributor dealer',
      'exterior building products distributor',
      'siding supply house contractor',
      'vinyl siding distributor wholesale',
      'building products dealer siding soffit',
      '2-step siding distributor regional',
    ];
  }

  // ── Cornice Contractor ──
  if (seg.includes('cornice')) {
    return [
      'cornice contractor exterior trim',
      'soffit fascia installer commercial',
      'exterior trim contractor cornice',
      'aluminum capping contractor exterior',
      'commercial cornice trim contractor',
      'metal trim contractor exterior building',
      'fascia soffit cornice company',
      'exterior sheet metal contractor',
    ];
  }

  // ── Fastener / Tool / Equipment Dealer ──
  if (seg.includes('fastener') || seg.includes('tool') || seg.includes('equipment')) {
    return [
      'fastener supply distributor contractor',
      'construction fastener dealer wholesale',
      'building products tool dealer supply',
      'scaffold equipment dealer rental supply',
      'ladder supply contractor dealer',
      'Alum-A-Pole dealer scaffold supply',
      'pump jack scaffold dealer contractor',
      'roofing tool supply dealer',
      'siding tool fastener supply house',
      'contractor tool equipment dealer',
      'building hardware distributor wholesale',
    ];
  }

  // ── Alum-A-Pole Scaffolding (siding/cornice focus) ──
  if (p.includes('alum') || p.includes('scaffolding')) {
    return [
      'scaffold equipment dealer contractor supply',
      'siding contractor installation commercial',
      'vinyl siding contractor commercial',
      'James Hardie siding installer commercial',
      'fiber cement siding company contractor',
      'LP SmartSide installer commercial',
      'soffit fascia cornice contractor exterior',
      'siding supply distributor building products',
      'fastener supply store construction contractor',
      'scaffolding rental supply contractor',
      'building materials distributor siding exterior',
      'exterior building products distributor',
      'construction equipment dealer scaffolding pump jack',
      'cornice contractor trim exterior',
    ];
  }

  // ── BOSS Sealants — distributor and commercial ──
  if (p.includes('soudal') || p.includes('sealant') || p.includes('adhesive') || p.includes('boss')) {
    return [
      'roofing products distributor authorized',
      'building products distributor sealant adhesive',
      'commercial roofing contractor TPO EPDM',
      'window door distributor building supply',
      'commercial glazing contractor',
      'building envelope distributor contractor supply',
      'waterproofing contractor commercial',
      'insulation contractor commercial',
      'commercial window installer contractor',
      'caulking sealant distributor supply',
      'exterior contractor commercial sealant',
      'roofing supply house distributor',
    ];
  }

  // ── ShurTape Flashing / Deck Tape ──
  if (p.includes('shurtape') || p.includes('flashing') || p.includes('deck tape')) {
    return [
      'roofing distributor authorized dealer commercial',
      'commercial roofing contractor certified',
      'window door dealer contractor supply',
      'deck contractor composite commercial',
      'metal roofing contractor commercial',
      'flat roof contractor commercial TPO',
      'building products distributor flashing tape',
      'exterior contractor commercial flashing',
      'roofing supply house commercial',
      'roofing contractor industrial commercial',
      'deck supply house contractor wholesale',
    ];
  }

  // ── Fortress / Decking (framing and railing) ──
  if (p.includes('framing') || p.includes('fortress')) {
    return [
      'deck products distributor dealer composite',
      'Trex deck authorized dealer',
      'TimberTech AZEK dealer supply',
      'composite deck supply house wholesale',
      'deck contractor commercial composite',
      'outdoor living contractor commercial deck',
      'railing distributor dealer wholesale',
      'aluminum railing dealer contractor supply',
      'cable railing dealer commercial',
      'deck railing distributor supply house',
      'commercial deck contractor multi-family',
    ];
  }

  // Default fallback — distribution focus
  return [
    'building products distributor authorized dealer',
    'contractor supply house wholesale',
    'commercial contractor building products',
    'building materials distributor regional',
    'specialty trade distributor contractor supply',
    'commercial contractor mid-market',
  ];
}

function getProductWhy(product, segment) {
  const p = (product || '').toLowerCase();
  const seg = (segment || '').toLowerCase();

  if (seg.includes('deck') && seg.includes('distrib')) return 'is a dealer or distributor in the decking supply chain and can stock and sell our products to their contractor customer base';
  if (seg.includes('roofing') && seg.includes('distrib')) return 'is a commercial roofing distributor and authorized dealer who can carry and resell our products to roofing contractors';
  if (seg.includes('roofing') && seg.includes('contractor')) return 'is a commercial roofing contractor who applies our products directly on jobs';
  if (p.includes('soudal') || p.includes('sealant') || p.includes('boss')) return 'distributes or uses roofing and siding products and can add Soudal BOSS sealants to their product line';
  if (p.includes('shurtape') || p.includes('flashing')) return 'sells to or works with builders and contractors and needs flashing tape as a stocked or specified SKU';
  if (p.includes('alum') || p.includes('scaffolding')) return 'supplies tools and equipment to contractors and needs Alum-A-Pole scaffolding in inventory';
  if (p.includes('framing')) return 'installs or distributes composite decking and needs rot-proof steel framing as a wood alternative';
  if (p.includes('railing')) return 'installs or distributes railing systems on decks, stairs, and commercial buildings';
  return 'distributes or supplies building products to contractors and builders in the Southeast';
}

function getCities(loc) {
  const t = (loc || '').toLowerCase();
  if (t.includes('atlanta') || t === 'atl')
    return ['Atlanta GA', 'Marietta GA', 'Kennesaw GA', 'Alpharetta GA', 'Roswell GA', 'Smyrna GA', 'Dunwoody GA', 'Decatur GA', 'Norcross GA', 'Duluth GA', 'Lawrenceville GA', 'Buford GA', 'Cumming GA', 'Woodstock GA', 'Acworth GA'];
  if (t.includes('birmingham'))
    return ['Birmingham AL', 'Hoover AL', 'Vestavia Hills AL', 'Homewood AL', 'Bessemer AL', 'Pelham AL', 'Alabaster AL', 'Helena AL', 'Trussville AL', 'Gardendale AL', 'Leeds AL', 'Northport AL', 'Anniston AL', 'Talladega AL', 'Calera AL'];
  if (t.includes('nashville'))
    return ['Nashville TN', 'Brentwood TN', 'Franklin TN', 'Murfreesboro TN', 'Smyrna TN', 'Hendersonville TN', 'Gallatin TN', 'Mount Juliet TN', 'Nolensville TN', 'Spring Hill TN', 'Columbia TN', 'Clarksville TN', 'Lebanon TN', 'Dickson TN', 'Shelbyville TN'];
  if (t.includes('charlotte'))
    return ['Charlotte NC', 'Concord NC', 'Kannapolis NC', 'Gastonia NC', 'Huntersville NC', 'Cornelius NC', 'Mooresville NC', 'Matthews NC', 'Monroe NC', 'Waxhaw NC', 'Rock Hill SC', 'Fort Mill SC', 'Tega Cay SC', 'Indian Trail NC', 'Mint Hill NC'];
  if (t.includes('southeast') || t.includes('south east'))
    return ['Atlanta GA', 'Marietta GA', 'Birmingham AL', 'Hoover AL', 'Nashville TN', 'Franklin TN', 'Charlotte NC', 'Concord NC', 'Columbia SC', 'Greenville SC', 'Chattanooga TN', 'Knoxville TN', 'Memphis TN', 'Savannah GA', 'Augusta GA'];
  return [loc];
}

router.post('/places-leads', async (req, res) => {
  const { category, territory, count, customer_type, segment } = req.body;
  const user = req.session.user;
  const product = category;
  const numLeads = Math.min(parseInt(count) || 20, 50);
  const loc = territory || user.territory || 'Atlanta, GA';

  // Use the new segment-aware query library
  const queries = getSearchQueries(product, customer_type, segment || customer_type);
  const why = getProductWhy(product, segment || customer_type);
  const cities = getCities(loc);
  const isWindowDoor = (segment || customer_type || '').toLowerCase().includes('window') ||
                        (segment || customer_type || '').toLowerCase().includes('door');

  console.log('Distribution search:', product, '|', segment || customer_type, '|', loc, '|', numLeads, 'leads');
  console.log('Queries:', queries.length, '| Cities:', cities.length);

  try {
    const leadsMap = new Map();

    const jobs = queries.map((query, i) => ({
      query: query + ' ' + cities[i % cities.length],
      label: query
    }));

    const BATCH = 5;
    for (let i = 0; i < jobs.length && leadsMap.size < numLeads; i += BATCH) {
      const batch = jobs.slice(i, i + BATCH);
      const results = await Promise.all(batch.map(async ({ query, label }) => {
        try {
          const data = await httpsPost(
            'places.googleapis.com',
            '/v1/places:searchText',
            {
              'Content-Type': 'application/json',
              'X-Goog-Api-Key': PLACES_KEY,
              'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus,places.types,places.regularOpeningHours'
            },
            { textQuery: query, maxResultCount: 20, rankPreference: 'RELEVANCE' }
          );
          return { label, places: data.places || [] };
        } catch(e) {
          console.error('Query error:', label, e.message);
          return { label, places: [] };
        }
      }));

      for (const { label, places } of results) {
        console.log(label, '->', places.length, 'results | total:', leadsMap.size);
        for (const place of places) {
          if (leadsMap.size >= numLeads) break;
          if (leadsMap.has(place.id)) continue;
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          if (place.businessStatus === 'CLOSED_TEMPORARILY') continue;
          if (hasBadPlaceType(place.types || [])) continue;

          const placeName = place.displayName?.text || '';
          const placeAddr = place.formattedAddress || '';

          if (isBadLead(placeName, placeAddr, label)) continue;
          if (!place.nationalPhoneNumber) continue;

          // ── Geo filter: enforce territory state ─────────────────────────
          const addrSegments   = placeAddr.split(',').map(s => s.trim());
          const stateZipToken  = addrSegments[addrSegments.length - 2] || '';
          const placeStateCode = stateZipToken.replace(/\s*\d{5}.*/, '').trim().toUpperCase();
          const territInfo     = resolveTerritory(loc);
          if (territInfo.state && placeStateCode && placeStateCode !== territInfo.state) {
            console.log('[GeoFilter] Rejected:', placeName, '|', placeStateCode, '!==', territInfo.state);
            continue;
          }

          // Alum-A-Pole paint block
          if ((product||'').toLowerCase().includes('alum') && alumPoleBlocked(placeName)) continue;

          // Window/Door segment: block garage door companies
          if (isWindowDoor && garageDoorBlocked(placeName)) continue;

          const reviewCount = place.userRatingCount || 0;
          if (reviewCount < REVIEW_MIN_ICP && !place.websiteUri) continue;

          const reach = getReachabilityScore(place, placeName);
          if (reach.tier === 'LOW') continue;

          const addr = (place.formattedAddress || '').split(',');
          leadsMap.set(place.id, {
            company: placeName || 'Unknown',
            category: label,
            city: addr[1]?.trim() || '',
            state: addr[2]?.trim().split(' ')[0] || '',
            phone: place.nationalPhoneNumber || null,
            email: null,
            website: place.websiteUri || null,
            contact: null,
            products: product,
            why: 'This ' + label + ' ' + why,
            reachability: reach.tier,
            reachabilityLabel: reach.label,
            reachabilityScore: reach.score,
            reachabilitySignals: reach.signals,
            sizeTier: getSizeTier(place),
            inclusionReason: buildInclusionReason(place, placeName, label, reach),
            priority: reach.tier === 'HIGH' ? 'High' : reach.tier === 'MEDIUM' ? 'Medium' : 'Low',
            rating: place.rating || null,
            reviews: place.userRatingCount || 0,
            confidence: reach.score,
            address: place.formattedAddress || null
          });
        }
        if (leadsMap.size >= numLeads) break;
      }
    }

    const leadsArr = Array.from(leadsMap.values()).sort((a, b) => {
      const reachA = a.reachabilityScore || 0;
      const reachB = b.reachabilityScore || 0;
      if (reachB !== reachA) return reachB - reachA;
      const scoreA = (a.rating || 0) * Math.log1p(a.reviews || 0);
      const scoreB = (b.rating || 0) * Math.log1p(b.reviews || 0);
      return scoreB - scoreA;
    });

    console.log('Final distribution-first results:', leadsArr.length, 'leads');
    if (leadsArr.length === 0) return res.json({ error: 'No results found. Try a different territory or segment.' });
    res.json({ leads: leadsArr, source: 'google' });
  } catch(e) {
    console.error('places-leads error:', e.message);
    res.json({ error: 'Search failed: ' + e.message });
  }
});

router.get('/test', async (req, res) => {
  try {
    const data = await httpsPost(
      'places.googleapis.com', '/v1/places:searchText',
      { 'Content-Type': 'application/json', 'X-Goog-Api-Key': PLACES_KEY, 'X-Goog-FieldMask': 'places.id,places.displayName' },
      { textQuery: 'commercial roofing distributor Atlanta GA', maxResultCount: 3 }
    );
    res.json({ ok: true, count: data.places?.length, first: data.places?.[0]?.displayName?.text });
  } catch(e) { res.json({ ok: false, error: e.message }); }
});

router.get('/company-search', async (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q || q.length < 2) return res.json([]);
  if (!PLACES_KEY) return res.json([]);

  try {
    const fetch = require('node-fetch');

    const findUrl = `https://maps.googleapis.com/maps/api/place/findplacefromtext/json` +
      `?input=${encodeURIComponent(q)}` +
      `&inputtype=textquery` +
      `&fields=place_id,name,formatted_address` +
      `&key=${PLACES_KEY}`;

    const findRes = await fetch(findUrl);
    const findData = await findRes.json();
    const candidates = (findData.candidates || []).slice(0, 5);

    const results = await Promise.all(candidates.map(async (c) => {
      try {
        const detailUrl = `https://maps.googleapis.com/maps/api/place/details/json` +
          `?place_id=${c.place_id}` +
          `&fields=name,formatted_phone_number,website,formatted_address,address_components` +
          `&key=${PLACES_KEY}`;
        const detRes = await fetch(detailUrl);
        const detData = await detRes.json();
        const r = detData.result || {};

        let city = '', state = '';
        if (r.address_components) {
          for (const comp of r.address_components) {
            if (comp.types.includes('locality')) city = comp.long_name;
            if (comp.types.includes('administrative_area_level_1')) state = comp.short_name;
          }
        }

        return {
          place_id: c.place_id,
          company: r.name || c.name,
          address: r.formatted_address || c.formatted_address || '',
          phone: r.formatted_phone_number || '',
          website: r.website || '',
          city,
          state,
          source: 'google'
        };
      } catch(e) {
        return {
          place_id: c.place_id,
          company: c.name,
          address: c.formatted_address || '',
          phone: '',
          website: '',
          city: '',
          state: '',
          source: 'google'
        };
      }
    }));

    res.json(results.filter(r => r.company));
  } catch(e) {
    console.error('company-search error:', e.message);
    res.json([]);
  }
});

function httpsPost(hostname, path, headers, body) {
  const https = require('https');
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const opts = { hostname, path, method: 'POST', headers: { ...headers, 'Content-Length': Buffer.byteLength(data) } };
    const req = https.request(opts, (res) => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { reject(new Error('Parse: ' + raw.slice(0,100))); } });
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

function httpsGet(url) {
  const https = require('https');
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { reject(new Error('Parse')); } });
    }).on('error', reject);
  });
}


// ═══════════════════════════════════════════════════════════════════════
// BULK INGESTION MODE
// POST /api/places/bulk-ingest
//
// Single query → full ecosystem expansion → clean → dedup → bulk CRM save
// AI runs ONCE. Data is stored permanently. No re-query needed.
// ═══════════════════════════════════════════════════════════════════════

// Bulk query expansion — turn natural language into ecosystem queries
function expandBulkQuery(rawQuery) {
  const q = (rawQuery || '').toLowerCase();

  // Detect manufacturer / brand
  const brands = {
    trex:       { cat: 'Decking Distributor', mfr: 'Trex' },
    timbertech: { cat: 'Decking Distributor', mfr: 'TimberTech/AZEK' },
    azek:       { cat: 'Decking Distributor', mfr: 'TimberTech/AZEK' },
    fiberon:    { cat: 'Decking Distributor', mfr: 'Fiberon' },
    deckorators:{ cat: 'Decking Distributor', mfr: 'Deckorators' },
    moistureshield:{ cat: 'Decking Distributor', mfr: 'MoistureShield' },
    carlisle:   { cat: 'Roofing Distributor', mfr: 'Carlisle' },
    'johns manville': { cat: 'Roofing Distributor', mfr: 'Johns Manville' },
    sarnafil:   { cat: 'Roofing Distributor', mfr: 'Sika Sarnafil' },
    sika:       { cat: 'Roofing Distributor', mfr: 'Sika' },
    tremco:     { cat: 'Roofing Distributor', mfr: 'Tremco' },
    certainteed:{ cat: 'Roofing Distributor', mfr: 'CertainTeed' },
    iko:        { cat: 'Roofing Distributor', mfr: 'IKO' },
    'james hardie': { cat: 'Siding Contractor', mfr: 'James Hardie' },
    hardie:     { cat: 'Siding Contractor', mfr: 'James Hardie' },
    'lp smartside': { cat: 'Siding Contractor', mfr: 'LP SmartSide' },
    smartside:  { cat: 'Siding Contractor', mfr: 'LP SmartSide' },
    shurtape:   { cat: 'Roofing Distributor', mfr: 'ShurTape' },
    'alum-a-pole': { cat: 'Fastener/Tool Dealer', mfr: 'Alum-A-Pole' },
    'alum a pole': { cat: 'Fastener/Tool Dealer', mfr: 'Alum-A-Pole' },
    'boss':     { cat: 'Roofing Distributor', mfr: 'Boss Products' },
    fortress:   { cat: 'Decking Distributor', mfr: 'Fortress Building Products' },
  };

  // Detect segment from query
  const segmentMap = {
    'decking distributor': 'Decking Distributor',
    'deck distributor':    'Decking Distributor',
    'deck dealer':         'Decking Distributor',
    'decking dealer':      'Decking Distributor',
    'roofing distributor': 'Roofing Distributor',
    'roofing supply':      'Roofing Distributor',
    'siding distributor':  'Siding Distributor',
    'siding supply':       'Siding Distributor',
    'window distributor':  'Window & Door Distributor',
    'door distributor':    'Window & Door Distributor',
    'window dealer':       'Window & Door Distributor',
    'roofing contractor':  'Roofing Contractor',
    'deck contractor':     'Decking Contractor',
    'deck builder':        'Decking Contractor',
    'siding contractor':   'Siding Contractor',
    'window installer':    'Window & Door Installer',
    'door installer':      'Window & Door Installer',
    'cornice':             'Cornice Contractor',
    'fastener':            'Fastener/Tool Dealer',
    'scaffold':            'Fastener/Tool Dealer',
    'tool dealer':         'Fastener/Tool Dealer',
    'lumber dealer':       'Decking Distributor',
    'lumber yard':         'Decking Distributor',
    'building supply':     'Siding Distributor',
    'building products':   'Roofing Distributor',
  };

  // Detect territory / geography
  function extractTerritory(text) {
    const territories = [
      'Atlanta', 'Birmingham', 'Nashville', 'Charlotte', 'Southeast',
      'Georgia', 'Tennessee', 'Alabama', 'North Carolina', 'South Carolina',
      'Florida', 'Virginia', 'Kentucky'
    ];
    for (const t of territories) {
      if (text.toLowerCase().includes(t.toLowerCase())) return t;
    }
    return 'Atlanta';
  }

  // Resolve brand
  let detectedBrand = null;
  for (const [kw, info] of Object.entries(brands)) {
    if (q.includes(kw)) { detectedBrand = { keyword: kw, ...info }; break; }
  }

  // Resolve segment
  let detectedCategory = detectedBrand ? detectedBrand.cat : null;
  if (!detectedCategory) {
    for (const [kw, cat] of Object.entries(segmentMap)) {
      if (q.includes(kw)) { detectedCategory = cat; break; }
    }
  }
  if (!detectedCategory) detectedCategory = 'Roofing Distributor'; // safest fallback

  const territory = extractTerritory(rawQuery);
  const mfrAssoc = detectedBrand ? detectedBrand.mfr : null;

  // Build search query set for the category
  const querySet = getSearchQueriesForCategory(detectedCategory, detectedBrand ? detectedBrand.keyword : null);

  return { category: detectedCategory, territory, mfrAssoc, querySet, rawQuery };
}

function getSearchQueriesForCategory(category, brandHint) {
  const cat = (category || '').toLowerCase();

  if (cat.includes('decking distributor')) {
    const base = [
      'Trex composite decking authorized dealer',
      'TimberTech AZEK decking dealer lumber supply',
      'Fiberon composite decking dealer',
      'Deckorators deck products dealer',
      'MoistureShield decking dealer distributor',
      'composite decking distributor wholesale',
      'specialty lumber dealer deck products',
      'deck materials supply house contractor',
      '2-step decking distributor building products',
      'outdoor living products distributor',
      'lumber yard deck composite contractor',
      'deck products wholesale contractor supply',
    ];
    if (brandHint) base.unshift(`${brandHint} authorized dealer distributor`);
    return base;
  }

  if (cat.includes('decking contractor')) {
    return [
      'composite deck contractor Trex TimberTech commercial',
      'commercial deck builder composite deck',
      'outdoor living contractor deck builder commercial',
      'deck installation contractor commercial multi-family',
      'custom deck construction company commercial',
      'deck contractor steel framing composite',
    ];
  }

  if (cat.includes('roofing distributor')) {
    const base = [
      'commercial roofing distributor authorized dealer',
      'Carlisle roofing authorized dealer distributor',
      'Johns Manville commercial roofing distributor',
      'CertainTeed commercial roofing distributor dealer',
      'Sika Sarnafil roofing dealer distributor',
      'Tremco roofing products distributor',
      'TPO EPDM roofing supply house wholesale',
      'metal roofing distributor dealer commercial',
      'low slope roofing materials distributor',
      'commercial roofing supply house contractor',
      'IKO commercial roofing dealer distributor',
      'roofing products wholesale distributor',
    ];
    if (brandHint) base.unshift(`${brandHint} roofing authorized dealer distributor`);
    return base;
  }

  if (cat.includes('roofing contractor')) {
    return [
      'commercial roofing contractor TPO EPDM flat roof',
      'commercial roofing company low slope',
      'metal roofing contractor commercial industrial',
      'Carlisle TPO certified roofing contractor',
      'Johns Manville roofing contractor certified applicator',
      'CertainTeed commercial roofing applicator',
      'industrial roofing contractor warehouse',
      'institutional roofing contractor commercial',
      'green roof contractor commercial building',
    ];
  }

  if (cat.includes('siding distributor')) {
    const base = [
      'siding products distributor wholesale building',
      'James Hardie authorized distributor dealer',
      'LP SmartSide distributor authorized dealer',
      'exterior building products distributor',
      'siding supply house contractor wholesale',
      'vinyl siding distributor wholesale',
      'building products dealer siding soffit fascia',
      '2-step siding distributor regional',
      'fiber cement siding distributor dealer',
    ];
    if (brandHint) base.unshift(`${brandHint} siding authorized distributor dealer`);
    return base;
  }

  if (cat.includes('siding contractor')) {
    return [
      'James Hardie siding installer contractor commercial',
      'fiber cement siding contractor commercial',
      'LP SmartSide contractor commercial installer',
      'vinyl siding contractor commercial multi-family',
      'exterior siding company commercial contractor',
      'soffit fascia cornice contractor exterior',
      'commercial siding contractor multi-unit',
    ];
  }

  if (cat.includes('window') || cat.includes('door')) {
    return [
      'exterior window distributor building products',
      'window and door dealer contractor supply',
      'commercial window installation contractor',
      'storefront window glazing contractor commercial',
      'exterior door distributor building supply',
      'replacement window dealer contractor authorized',
      'window manufacturer dealer authorized',
      'building envelope contractor commercial',
      'fenestration dealer building products supply',
      'window and door supply house wholesale',
    ];
  }

  if (cat.includes('cornice')) {
    return [
      'cornice contractor exterior trim commercial',
      'soffit fascia installer commercial contractor',
      'aluminum capping contractor exterior building',
      'metal trim contractor exterior commercial',
      'exterior sheet metal contractor commercial',
      'fascia soffit cornice company contractor',
    ];
  }

  if (cat.includes('fastener') || cat.includes('tool')) {
    return [
      'fastener supply distributor contractor wholesale',
      'construction fastener dealer supply house',
      'building products tool dealer supply',
      'scaffold equipment dealer rental supply',
      'ladder supply contractor dealer',
      'Alum-A-Pole dealer scaffold pump jack',
      'roofing tool supply dealer contractor',
      'siding tool fastener supply house',
      'building hardware distributor wholesale',
      'contractor tool equipment dealer supply',
    ];
  }

  // Fallback
  return [
    'building products distributor authorized dealer',
    'contractor supply house wholesale',
    'commercial building products distributor regional',
    'specialty trade distributor contractor supply',
  ];
}

// ── Main bulk ingestion route ────────────────────────────────────
// ─── Per-record smart classification (v3) ───────────────────────────────────
// Uses company name, manufacturer ecosystem signals, and base category context
// to produce accurate per-record category labels. Roofing and Decking ecosystems
// are strictly separated to prevent cross-contamination.

// ── Ecosystem keyword libraries ──────────────────────────────────────────────
// ── Manufacturer/Brand ecosystem signals (definitive) ──────────────────────
const ROOFING_MANUFACTURER_SIGNALS = [
  'carlisle', 'johns manville', 'sarnafil', 'sika', 'tremco', 'certainteed',
  'iko', 'gaf', 'owens corning', 'firestone', 'versico', 'polyglass',
  'henry building', 'soprema', 'mulehide', 'mule-hide', 'atlas roofing',
];
const DECKING_MANUFACTURER_SIGNALS = [
  'trex', 'timbertech', 'azek', 'fiberon', 'deckorators', 'deckorator',
  'moistureshield', 'moisture shield', 'fortress building', 'wolf decking',
];

// ── Known distributor networks (named brands — HIGH confidence) ──────────────
const ROOFING_DISTRIBUTOR_SIGNALS = [
  'abc supply', 'beacon roofing', 'beacon supply', 'gulf eagle', 'gulfeagle',
  'bradco', 'allied building', 'western states roofing', 'famco', 'hepler',
  'srs distribution', 'contractor source', 'roofing supply house',
];
const DECKING_DISTRIBUTOR_SIGNALS = [
  '84 lumber', 'us lbm', 'uslbm', 'bmc stock', 'pro build', 'probuild',
  'builders firstsource', 'universal forest', 'ufp', 'weyerhaeuser',
  'hancock lumber', 'carter lumber', 'mc supply', 'mr. lumber',
  'outdoor living supply', 'deck supply house', 'composite deck supply',
];

// ── HIGH-CONFIDENCE distributor signals (company structure signals) ──────────
// Only words that genuinely indicate a supply/distribution business
const DISTRIBUTOR_STRONG = [
  'supply co', 'supply company', 'supply house', 'supply inc',
  'distribut',   // catches distributor/distribution/distributing
  'wholesale',
  'building supply', 'building products', 'building materials',
  'millwork', 'millworks',
  'lumber yard', 'lumber co ', 'lumber company', 'lumber inc', 'lumber llc',
  'hdw', 'hardware supply',
  'roofing supply', 'siding supply', 'deck supply',
  'material supply', 'materials supply',
];

// ── HIGH-CONFIDENCE contractor signals (company structure signals) ───────────
const CONTRACTOR_STRONG = [
  'contracting', 'contractor', 'construction co', 'construction inc', 'construction llc',
  'construction company', 'construction corp',
  'builders inc', 'builders llc', 'builders co',
  'installation', 'installing', 'installer',
  'renovation', 'renovations', 'remodel', 'remodeling',
  'restoration', 'restorations',
  'roofing co', 'roofing inc', 'roofing llc', 'roofing company', 'roofing corp',
  'deck co', 'deck inc', 'deck llc', 'deck company', 'decking co', 'decking inc',
  'decking llc', 'decking company',
  'siding co', 'siding inc', 'siding company',
  'exterior solutions', 'exteriors inc', 'exteriors llc',
  'home improvement', 'home services',
];

// ── WEAK / AMBIGUOUS signals — only used when combined with ecosystem context ──
// "products", "materials", "services", "company" alone are NOT enough
const DISTRIBUTOR_WEAK  = ['dealer', 'products inc', 'products co', 'products llc', 'products corp'];
const CONTRACTOR_WEAK   = [
  'services', 'company', 'companies', 'builders',  // too generic alone
  'enterprises', 'solutions', 'group',
];

function hasAny(name, signals) {
  return signals.some(sig => name.includes(sig));
}

// ════════════════════════════════════════════════════════════════════════════
//  BULK INGESTION ENGINE v2
//  Tiered, confidence-scored, deduplicated CRM intelligence pipeline
// ════════════════════════════════════════════════════════════════════════════

// ── Tier definitions ────────────────────────────────────────────────────────
// ════════════════════════════════════════════════════════════════════════════
// ENTITY RESOLUTION ENGINE v3  (replaces all previous dedup/classify logic)
// ════════════════════════════════════════════════════════════════════════════

// ── Source tier constants ────────────────────────────────────────────────────

function normName(n) {
  return (n || '')
    .toLowerCase()
    // Remove legal suffixes
    .replace(/\b(llc|inc|corp|co|company|ltd|lp|plc|dba|the)\b\.?/g, '')
    // Remove punctuation and extra spaces
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normPhone(p) { return (p || '').replace(/[^0-9]/g, ''); }

// ── Address normalization (enhanced) ─────────────────────────────────────────
function normAddr(a) {
  return (a || '')
    .toLowerCase()
    // Suite / unit variations → 'suite'
    .replace(/\bste\.?\b/g,    'suite')
    .replace(/\bunit\b/g,      'suite')
    .replace(/\bapt\.?\b/g,    'suite')
    // Street type abbreviations → full words
    .replace(/\bst\.?\b/g,     'street')
    .replace(/\bave\.?\b/g,    'avenue')
    .replace(/\bblvd\.?\b/g,   'boulevard')
    .replace(/\bdr\.?\b/g,     'drive')
    .replace(/\brd\.?\b/g,     'road')
    .replace(/\bln\.?\b/g,     'lane')
    .replace(/\bct\.?\b/g,     'court')
    .replace(/\bpl\.?\b/g,     'place')
    .replace(/\bhwy\.?\b/g,    'highway')
    .replace(/\bpkwy\.?\b/g,   'parkway')
    .replace(/\bfwy\.?\b/g,    'freeway')
    .replace(/\bcir\.?\b/g,    'circle')
    .replace(/\bter\.?\b/g,    'terrace')
    .replace(/\bxing\b/g,      'crossing')
    // Directional abbreviations
    .replace(/\bn\.?\b(?=\s)/g, 'north').replace(/\bs\.?\b(?=\s)/g, 'south')
    .replace(/\be\.?\b(?=\s)/g, 'east').replace(/\bw\.?\b(?=\s)/g,  'west')
    .replace(/\bne\b/g, 'northeast').replace(/\bnw\b/g, 'northwest')
    .replace(/\bse\b/g, 'southeast').replace(/\bsw\b/g, 'southwest')
    // Strip punctuation and collapse spaces
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// ── Entity Resolution Index ───────────────────────────────────────────────────
function buildExistingIndex(rows) {
  const byAddr    = new Map();  // normAddr → array of {nameNorm, placeId, raw}
  const byPlaceId = new Map();  // placeId  → {nameNorm, addrNorm}
  const records   = [];

  for (const r of rows) {
    const addrNorm = normAddr(r.address);
    const nameNorm = normName(r.company);

    if (addrNorm.length > 8) {
      if (!byAddr.has(addrNorm)) byAddr.set(addrNorm, []);
      byAddr.get(addrNorm).push({ nameNorm, placeId: r.google_place_id || '', raw: r });
    }
    if (r.google_place_id) {
      byPlaceId.set(r.google_place_id, { nameNorm, addrNorm });
    }
    records.push({ nameNorm, addrNorm, placeId: r.google_place_id || '' });
  }

  return { byAddr, byPlaceId, records };
}

// ── isDuplicate v5: address-first logic ──────────────────────────────────────
//
//  Priority order:
//    1. Exact normalized address match → duplicate (skip)
//    2. Same company name + same address → duplicate (skip)
//    3. Same company name, different/no address → possible duplicate (flag, import)
//    4. Everything else → import as new
//
//  Place ID, phone, domain = supporting metadata only (never auto-skip)
//
//  Returns:
//    { dup: bool, action: 'skip'|'review'|'import', reason, confidence, address, company }
// ═══════════════════════════════════════════════════════════════════════════════
// BULK INGESTION ENGINE v6 — Geography-First, Dedupe-Last
// Pipeline: Search → Geo-Validate → Classify → Enrich → Dedupe → Insert
// ═══════════════════════════════════════════════════════════════════════════════

// ── Territory registry ────────────────────────────────────────────────────────
const TERRITORY_MAP = {
  // Keys must match what getCities() detects
  'atlanta':      { state: 'GA', cities: ['Atlanta','Marietta','Kennesaw','Alpharetta','Roswell','Smyrna','Dunwoody','Decatur','Norcross','Duluth','Lawrenceville','Buford','Cumming','Woodstock','Acworth'] },
  'birmingham':   { state: 'AL', cities: ['Birmingham','Hoover','Vestavia Hills','Homewood','Bessemer','Pelham','Alabaster','Helena','Trussville','Gardendale','Leeds','Northport','Anniston','Talladega','Calera'] },
  'nashville':    { state: 'TN', cities: ['Nashville','Brentwood','Franklin','Murfreesboro','Smyrna','Hendersonville','Gallatin','Mount Juliet','Nolensville','Spring Hill','Columbia','Clarksville','Lebanon','Dickson','Shelbyville'] },
  'charlotte':    { state: 'NC', cities: ['Charlotte','Concord','Kannapolis','Gastonia','Huntersville','Cornelius','Mooresville','Matthews','Monroe','Waxhaw','Indian Trail','Mint Hill'] },
  'jacksonville': { state: 'FL', cities: ['Jacksonville','Orange Park','Fleming Island','Middleburg','Fernandina Beach','Ponte Vedra','St Augustine','Palatka','Yulee','Callahan','Macclenny','Starke'] },
  'southeast':    { state: null, cities: [] }  // multi-state, no filter
};

// Resolve territory → { state, citySet }
function resolveTerritory(loc) {
  const t = (loc || '').toLowerCase();
  for (const [key, val] of Object.entries(TERRITORY_MAP)) {
    if (t.includes(key)) {
      return { key, state: val.state, citySet: new Set(val.cities.map(c => c.toLowerCase())) };
    }
  }
  // Unknown territory — extract state abbreviation if present (e.g. "Huntsville AL")
  const stateMatch = t.match(/\b([a-z]{2})\s*$/);
  return { key: 'custom', state: stateMatch ? stateMatch[1].toUpperCase() : null, citySet: new Set() };
}

// ── Geo-validation ─────────────────────────────────────────────────────────────
// Returns true if record is within the required territory
function passesGeoFilter(rec, territoryInfo) {
  if (!territoryInfo.state) return true; // no state filter (e.g. Southeast multi-state)

  const addr  = (rec.address || '').toLowerCase();
  const state = (rec.state   || '').trim();

  // Extract state abbreviation from Google's formatted address
  // Google format: "123 Main St, City, ST ZIPCODE, Country"
  // State is in the second-to-last comma segment, before the zip
  const addrParts   = (rec.address || '').split(',').map(s => s.trim());
  const stateZipSeg = addrParts[addrParts.length - 2] || '';          // e.g. "AL 35242"
  const stateAbbr   = stateZipSeg.replace(/\s*\d{5}.*/, '').trim();  // e.g. "AL"

  const recState = stateAbbr || state || '';

  if (!recState) return true; // can't determine — let it through

  const requiredState = territoryInfo.state.toUpperCase();
  const matchesState  = recState.toUpperCase() === requiredState;

  return matchesState;
}

// ── Query expansion (geo-constrained) ────────────────────────────────────────
function expandQuery(rawQuery, territory) {
  const q   = rawQuery.toLowerCase().trim();
  const loc = territory || '';

  // Extract just the city/state portion from territory
  // e.g. "Atlanta Metro, GA" → "Atlanta GA" for queries
  const terr = resolveTerritory(loc);
  const geoSuffix = terr.state ? `${terr.cities ? [...TERRITORY_MAP[terr.key]?.cities || []][0] || loc : loc} ${terr.state}` : loc;
  const primaryLoc = geoSuffix || loc;

  // withLoc: append geo suffix only if not already present
  const withLoc = (base) => {
    const baseLow = base.toLowerCase();
    const stateInQuery = terr.state && baseLow.includes(terr.state.toLowerCase());
    const cityInQuery  = primaryLoc.split(' ')[0] && baseLow.includes(primaryLoc.split(' ')[0].toLowerCase());
    return (stateInQuery || cityInQuery) ? base : `${base} ${primaryLoc}`;
  };

  const expansions = [withLoc(rawQuery)];

  // Brand-specific expansions — geo-locked, no regional fallbacks
  const brandRules = {
    trex:           (l) => [`Trex authorized dealers ${l}`, `Trex decking distributors ${l}`, `composite decking contractors ${l}`],
    fiberon:        (l) => [`Fiberon authorized dealers ${l}`, `Fiberon decking distributors ${l}`, `decking contractors ${l}`],
    azek:           (l) => [`AZEK authorized dealers ${l}`, `AZEK decking distributors ${l}`],
    timbertech:     (l) => [`TimberTech authorized dealers ${l}`, `TimberTech decking distributors ${l}`],
    fortress:       (l) => [`Fortress steel framing dealers ${l}`, `Fortress railing dealers ${l}`],
    shurtape:       (l) => [`ShurTape distributors ${l}`, `construction tape dealers ${l}`],
    'alum-a-pole':  (l) => [`Alum-A-Pole scaffold dealers ${l}`, `pump jack scaffold suppliers ${l}`],
    alumapole:      (l) => [`Alum-A-Pole scaffold dealers ${l}`, `scaffolding equipment dealers ${l}`],
    boss:           (l) => [`BOSS sealant distributors ${l}`, `roofing sealant dealers ${l}`],
    gaf:            (l) => [`GAF roofing distributors ${l}`, `GAF authorized contractors ${l}`],
    certainteed:    (l) => [`CertainTeed roofing distributors ${l}`, `CertainTeed siding distributors ${l}`],
    'james hardie': (l) => [`James Hardie siding distributors ${l}`, `James Hardie contractors ${l}`]
  };

  for (const [brand, fn] of Object.entries(brandRules)) {
    if (q.includes(brand)) {
      fn(primaryLoc).forEach(v => { if (!expansions.includes(v)) expansions.push(v); });
      break;
    }
  }

  // Category expansions — no brand detected
  const hasBrand = Object.keys(brandRules).some(b => q.includes(b));
  if (!hasBrand) {
    const catRules = {
      'roofing contractor': [`commercial roofing contractors ${primaryLoc}`, `roofing companies ${primaryLoc}`],
      'roofing distribut':  [`roofing supply ${primaryLoc}`, `roofing materials ${primaryLoc}`],
      'decking contractor': [`deck builders ${primaryLoc}`, `composite decking contractors ${primaryLoc}`],
      'decking distribut':  [`decking supply ${primaryLoc}`, `lumber composite decking ${primaryLoc}`],
      'siding contractor':  [`siding companies ${primaryLoc}`, `siding installers ${primaryLoc}`],
      'siding distribut':   [`siding supply ${primaryLoc}`, `siding materials ${primaryLoc}`],
      'window':             [`window door dealers ${primaryLoc}`, `window distributors ${primaryLoc}`],
      'scaffold':           [`scaffolding suppliers ${primaryLoc}`, `tool supply dealers ${primaryLoc}`],
      'lumber':             [`lumber yards ${primaryLoc}`, `building supply ${primaryLoc}`]
    };
    for (const [key, variants] of Object.entries(catRules)) {
      if (q.includes(key)) {
        variants.forEach(v => { if (!expansions.includes(v)) expansions.push(v); });
        break;
      }
    }
  }

  // No "Southeast" or neighboring-state fallback ever
  return [...new Set(expansions)].slice(0, 4);
}

// ── Places search ─────────────────────────────────────────────────────────────
async function placesSearch(queryText, locationStr, apiKey, excludedIds) {
  excludedIds = excludedIds || new Set();
  const results = [];
  const seenIds = new Set(excludedIds);
  const terr    = resolveTerritory(locationStr);

  // Use territory cities for query targeting
  const territoryKey = terr.key;
  const cityList     = TERRITORY_MAP[territoryKey]?.cities || [];
  const targetCities = cityList.length > 0
    ? cityList.slice(0, 5).map(c => `${c} ${terr.state || ''}`.trim())
    : [locationStr || queryText];

  for (const city of targetCities) {
    const cityFirstWord = city.split(' ')[0].toLowerCase();
    const alreadyHasCity = queryText.toLowerCase().includes(cityFirstWord);
    const fullQuery = alreadyHasCity ? queryText : `${queryText} ${city}`;

    try {
      const data = await httpsPost(
        'places.googleapis.com',
        '/v1/places:searchText',
        {
          'Content-Type': 'application/json',
          'X-Goog-Api-Key': PLACES_KEY,
          'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus,places.types'
        },
        { textQuery: fullQuery, maxResultCount: 20, rankPreference: 'RELEVANCE' }
      );

      for (const place of (data.places || [])) {
        if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
        if (seenIds.has(place.id)) continue;
        seenIds.add(place.id);

        const addrParts   = (place.formattedAddress || '').split(',').map(s => s.trim());
        const stateZipSeg = addrParts[addrParts.length - 2] || '';
        const stateAbbr   = stateZipSeg.replace(/\s*\d{5}.*/, '').trim();

        const rec = {
          place_id:    place.id,
          company:     (place.displayName || {}).text || '',
          address:     place.formattedAddress || '',
          phone:       place.nationalPhoneNumber || '',
          website:     place.websiteUri || '',
          rating:      place.rating || 0,
          types:       place.types || [],
          category:    '',
          source_tier: SOURCE_TIERS.TIER3,
          city:        addrParts.length >= 3 ? addrParts[addrParts.length - 3].trim() : city.split(' ')[0],
          state:       stateAbbr
        };
        results.push(rec);
      }
    } catch (e) {
      console.error('[placesSearch] error for query:', fullQuery, e.message);
    }
  }
  return results;
}

// ── isDuplicate v6 — exact full-address + name required ──────────────────────
// Pipeline position: LAST STEP before insert
// ONLY skips if: same street + city + state + zip AND same company name (≥75% sim)
function isDuplicate(rec, index) {
  const recAddr  = normAddr(rec.address);
  const recName  = normName(rec.company);
  const recState = (rec.state || '').toUpperCase().trim();

  function nameSim(a, b) {
    if (!a || !b || a.length < 4 || b.length < 4) return 0;
    if (a === b) return 1;
    const tA = new Set(a.split(/\s+/).filter(t => t.length > 2));
    const tB = new Set(b.split(/\s+/).filter(t => t.length > 2));
    if (tA.size === 0 || tB.size === 0) return 0;
    const inter = [...tA].filter(t => tB.has(t)).length;
    const union = new Set([...tA, ...tB]).size;
    return union > 0 ? inter / union : 0;
  }

  // Rule 1: Exact normalized full address + same company name → definite dup
  if (recAddr.length > 8 && index.byAddr.has(recAddr)) {
    const atAddr = index.byAddr.get(recAddr);
    for (const ex of atAddr) {
      // CRITICAL: only match within same state — never cross-state dedup
      const exState = (ex.raw && ex.raw.state ? ex.raw.state : '').toUpperCase().trim();
      if (recState && exState && recState !== exState) continue;

      const sim = nameSim(recName, ex.nameNorm);
      if (sim >= 0.75) {
        return {
          dup:             true,
          action:          'skip',
          reason:          'exact_address_and_name',
          confidence:      100,
          detail:          `Same full address + company name (${Math.round(sim*100)}% match)`,
          matched_company: ex.raw ? (ex.raw.company || '') : ''
        };
      }
      // Same address, different company = new tenant → import
    }
    return { dup: false, action: 'import', reason: '', confidence: 0 };
  }

  // Rule 2: Place ID + name match (same state only)
  if (rec.place_id && index.byPlaceId.has(rec.place_id)) {
    const existing = index.byPlaceId.get(rec.place_id);
    const exState  = (existing.state || '').toUpperCase().trim();
    if (!recState || !exState || recState === exState) {
      const sim = nameSim(recName, existing.nameNorm);
      if (sim >= 0.80) {
        return {
          dup: true, action: 'skip', reason: 'place_id_confirmed', confidence: 100,
          detail: `Place ID + name confirmed (${Math.round(sim*100)}% match)`
        };
      }
    }
    // Place ID match across different states = geo-relocated chain, import it
    return { dup: false, action: 'import', reason: '', confidence: 0 };
  }

  // Rule 3: Very similar name IN SAME STATE only → possible dup, flag but import
  if (recName.length > 5 && recState) {
    for (const ex of index.records) {
      if (ex.nameNorm.length < 5) continue;
      const exState = (ex.state || '').toUpperCase().trim();
      if (exState && recState !== exState) continue; // different state = not a dup
      const sim = nameSim(recName, ex.nameNorm);
      if (sim >= 0.92) {
        return {
          dup: false, action: 'review', reason: 'name_very_similar',
          confidence: Math.round(sim * 65),
          detail: `Very similar name in same state — verify manually`
        };
      }
    }
  }

  return { dup: false, action: 'import', reason: '', confidence: 0 };
}


// ── Exclusion lists ───────────────────────────────────────────────────────────
const RETAIL_EXCLUSIONS = [
  'home depot','lowes','lowe\'s','menards','ace hardware','true value',
  'walmart','costco','amazon','wayfair','big box'
];
const MANUFACTURER_EXCLUSIONS = [
  'manufacturing','manufacturer',' mfg ','fabricat','factory ',
  'raw material','steel mill','chemical plant'
];
const RESIDENTIAL_EXCLUSIONS = [
  'residential only','homeowner','diy','do-it-yourself','consumer grade'
];

const DISTRIBUTOR_SIGNALS = [
  'supply','supplies','distribution','distributing','distributor','distributors',
  'wholesale','wholesaler','building materials','lumber','builders supply',
  'roofing supply','deck supply','siding supply','window supply','door supply',
  'material yard','dealer','dealers','building center','pro dealer',
  '84 lumber','abc supply','beacon roofing','gulfeagle','ply gem',
  'builders firstsource','us lbm','carter lumber','mccoy\'s','sutherland',
  'hd supply','wesco','hajoca','re-michel','famco'
];
const CONTRACTOR_SIGNALS = [
  'roofing contractor','roofing company','roofing co','roofer','roofing services',
  'roofing & sheet','commercial roofing','roof repair',
  'decking contractor','deck builder','custom decks','deck construction',
  'decks and patios','outdoor living',
  'siding contractor','siding company','siding & windows',
  'siding installer','window installer','door installer',
  'window & door','window and door','fenestration',
  'general contractor','construction company','construction co',
  'builder','builders','remodeling','renovation'
];

// Source tier constants
const SOURCE_TIERS = {
  TIER1: 1, TIER2: 2, TIER3: 3, TIER4: 4
};

// ── Category map ──────────────────────────────────────────────────────────────
const CATEGORY_MAP = [
  { slug: 'roofing_contractor',  label: 'Roofing Contractor',        type: 'Contractor',  keys: ['roofing contractor','roofer','roofing company','roofing co','commercial roofing','roof repair','roofing services','roofing & sheet'] },
  { slug: 'roofing_distributor', label: 'Roofing Distributor',       type: 'Distributor', keys: ['roofing supply','roofing distributor','roofing materials','roofing wholesale','roofing dealer'] },
  { slug: 'decking_contractor',  label: 'Decking Contractor',        type: 'Contractor',  keys: ['deck builder','decking contractor','deck construction','custom decks','decks and patios','outdoor living','composite decking contractor'] },
  { slug: 'decking_distributor', label: 'Decking Distributor',       type: 'Distributor', keys: ['deck supply','decking distributor','decking dealer','lumber yard','trex dealer','composite decking dealer','fiberon dealer','decking wholesale'] },
  { slug: 'siding_contractor',   label: 'Siding Contractor',         type: 'Contractor',  keys: ['siding contractor','siding company','siding installer','siding & windows','cornice contractor','siding services'] },
  { slug: 'siding_distributor',  label: 'Siding Distributor',        type: 'Distributor', keys: ['siding supply','siding distributor','siding dealer','siding wholesale','siding materials'] },
  { slug: 'window_contractor',   label: 'Window & Door Installer',   type: 'Contractor',  keys: ['window installer','door installer','window contractor','window & door contractor','fenestration contractor','window replacement'] },
  { slug: 'window_distributor',  label: 'Window & Door Distributor', type: 'Distributor', keys: ['window distributor','window dealer','door dealer','window supply','window wholesale','window & door supply'] },
  { slug: 'tool_dealer',         label: 'Fastener & Tool Dealer',    type: 'Distributor', keys: ['scaffolding dealer','tool supply','fastener dealer','pump jack dealer','equipment supply','tool dealer','scaffold supply'] },
  { slug: 'building_materials',  label: 'Building Materials Dealer', type: 'Distributor', keys: ['building materials','builders supply','building supply','lumber yard','lumber dealer','pro dealer','building center'] },
  { slug: 'general_contractor',  label: 'General Contractor',        type: 'Contractor',  keys: ['general contractor','construction company','construction co','builder','remodeling','renovation'] }
];

// ── classifyRecord ────────────────────────────────────────────────────────────
function classifyRecord(record) {
  const name    = (record.company  || '').toLowerCase();
  const cats    = (record.category || '').toLowerCase();
  const types   = (record.types    || []).map(t => t.toLowerCase()).join(' ');
  const website = (record.website  || '').toLowerCase();
  const allText = `${name} ${cats} ${types} ${website}`;

  // Hard exclusions
  if (RETAIL_EXCLUSIONS.some(ex        => allText.includes(ex))) return null;
  if (MANUFACTURER_EXCLUSIONS.some(ex  => allText.includes(ex))) return null;
  if (RESIDENTIAL_EXCLUSIONS.some(ex   => allText.includes(ex))) return null;
  if (/garage|overhead door/i.test(name))                        return null;
  if (/paint(ing)?\s*(contractor|company|co\b|services)/i.test(name)) return null;

  // Score each category
  let bestScore = 0;
  let bestCat   = null;
  for (const cat of CATEGORY_MAP) {
    const hits = cat.keys.filter(k => allText.includes(k)).length;
    if (hits > bestScore) { bestScore = hits; bestCat = cat; }
  }
  if (bestCat && bestScore > 0) {
    return { company_type: bestCat.type, category_label: bestCat.label, category_slug: bestCat.slug };
  }

  // Fallback broad signals
  const isDist = DISTRIBUTOR_SIGNALS.some(s => allText.includes(s));
  const isCont = CONTRACTOR_SIGNALS.some(s  => allText.includes(s));
  if (isDist && !isCont) return { company_type: 'Distributor', category_label: 'Building Materials Dealer',  category_slug: 'building_materials' };
  if (isCont)            return { company_type: 'Contractor',  category_label: 'General Contractor',         category_slug: 'general_contractor' };
  if (isDist)            return { company_type: 'Distributor', category_label: 'Building Materials Dealer',  category_slug: 'building_materials' };

  return { company_type: 'Unknown', category_label: cats || 'Unknown', category_slug: 'unknown' };
}

// Backwards-compat alias
const classifyAndFilter = classifyRecord;

// ── scoreConfidence ───────────────────────────────────────────────────────────
function scoreConfidence(record, sourceTier) {
  let score = 0;
  const tierBonus = { 1: 35, 2: 28, 3: 20, 4: 8 };
  score += tierBonus[sourceTier] || 15;
  if (record.place_id)                                                   score += 15;
  if (record.phone && record.phone.replace(/\D/g,'').length >= 10)       score += 20;
  if (record.address && record.address.length > 10)                      score += 15;
  if (record.website && record.website.length > 5)                       score += 12;
  if (record.company && record.company.length > 3)                       score += 8;
  return Math.min(score, 100);
}






// ══════════════════════════════════════════════════════════════════════════════
//  MAIN BULK INGEST ROUTE
// ══════════════════════════════════════════════════════════════════════════════
router.post('/bulk-ingest', async (req, res) => {
  const uid = req.session.user.id;
  const { query, territory, max_records, scope } = req.body;
  const dedupScope = scope || 'team';

  if (!query || !query.trim()) {
    return res.status(400).json({ error: 'Query is required' });
  }

  const maxRecs = Math.min(parseInt(max_records) || 40, 80);
  const apiKey  = process.env.GOOGLE_PLACES_API_KEY;

  try {
    console.log(`[BulkIngest v6] Query="${query}" territory="${territory}" max=${maxRecs} scope=${dedupScope}`);

    // Resolve territory for geo filter
    const territoryInfo = resolveTerritory(territory || '');
    console.log(`[BulkIngest v6] Territory: key=${territoryInfo.key} state=${territoryInfo.state}`);

    // ── Phase 1: Expand query (geo-locked) ───────────────────────────────
    const expandedQueries = expandQuery(query, territory);
    console.log('[BulkIngest v6] Expanded to', expandedQueries.length, 'queries:', expandedQueries);

    // ── Phase 2: Fetch from Google Places ────────────────────────────────
    const rawResults   = [];
    const seenPlaceIds = new Set();

    for (const eq of expandedQueries) {
      const batch = await placesSearch(eq, territory, apiKey, seenPlaceIds);
      for (const r of batch) {
        if (!seenPlaceIds.has(r.place_id)) {
          seenPlaceIds.add(r.place_id);
          rawResults.push(r);
        }
      }
      if (rawResults.length >= maxRecs * 4) break;
    }
    console.log(`[BulkIngest v6] Raw: ${rawResults.length}`);

    // ── Phase 3: GEO VALIDATION — hard filter before anything else ───────
    const geoValid    = [];
    let   geoRejected = 0;
    const geoRejectedSample = [];

    for (const rec of rawResults) {
      if (passesGeoFilter(rec, territoryInfo)) {
        geoValid.push(rec);
      } else {
        geoRejected++;
        if (geoRejectedSample.length < 10) {
          geoRejectedSample.push({
            company: rec.company,
            address: rec.address,
            state:   rec.state,
            reason:  `State "${rec.state}" does not match required "${territoryInfo.state}"`
          });
        }
      }
    }
    console.log(`[BulkIngest v6] Geo: ${geoValid.length} valid, ${geoRejected} rejected`);

    // ── Phase 4: Classify ─────────────────────────────────────────────────
    const classified  = [];
    let   filteredCount = 0;

    for (const rec of geoValid) {
      const cls = classifyRecord(rec);
      if (!cls) { filteredCount++; continue; }
      rec.company_type  = cls.company_type;
      rec.category      = cls.category_label;
      rec.category_slug = cls.category_slug;
      classified.push(rec);
    }
    console.log(`[BulkIngest v6] Classified: ${classified.length}, Filtered: ${filteredCount}`);

    // ── Phase 5: Confidence scoring ───────────────────────────────────────
    const scored     = [];
    let   lowConfCount = 0;

    for (const rec of classified) {
      const conf = scoreConfidence(rec, rec.source_tier || SOURCE_TIERS.TIER3);
      if (conf < 35) { lowConfCount++; continue; }
      rec.confidence_score = conf;
      rec.data_status = conf >= 90 ? 'Verified' : conf >= 70 ? 'Likely Valid' : 'Unvetted';
      scored.push(rec);
    }
    scored.sort((a, b) => b.confidence_score - a.confidence_score);
    console.log(`[BulkIngest v6] Scored: ${scored.length}, Low conf: ${lowConfCount}`);

    // ── Phase 6: DEDUPLICATION — final step, exact address+name only ─────
    let existingRows = { rows: [] };
    if (dedupScope !== 'off') {
      const dbQ    = dedupScope === 'mine'
        ? `SELECT google_place_id, company, address, phone, website, category, state FROM prospects WHERE user_id = $1`
        : `SELECT google_place_id, company, address, phone, website, category, state FROM prospects`;
      const dbP    = dedupScope === 'mine' ? [uid] : [];
      existingRows = await pool.query(dbQ, dbP);
      console.log(`[BulkIngest v6] DB index: ${existingRows.rows.length} records (scope=${dedupScope})`);
    }

    const dupIndex      = buildExistingIndex(existingRows.rows);
    const batchAddrs    = new Set();
    const toInsert      = [];
    const possibleDups  = [];
    const skipped       = [];
    const skippedSample = [];

    for (const rec of scored) {
      if (toInsert.length + possibleDups.length >= maxRecs) break;

      // Batch-level: same address twice in this run
      const batchAddrKey = normAddr(rec.address);
      if (batchAddrKey.length > 8 && batchAddrs.has(batchAddrKey)) {
        skipped.push({ reason: 'batch_addr_dup', company: rec.company, address: rec.address });
        continue;
      }

      if (dedupScope !== 'off') {
        const dupCheck = isDuplicate(rec, dupIndex);

        if (dupCheck.action === 'skip') {
          skipped.push({
            reason:          dupCheck.reason,
            company:         rec.company,
            address:         rec.address,
            detail:          dupCheck.detail || '',
            matched_company: dupCheck.matched_company || ''
          });
          if (skippedSample.length < 20) skippedSample.push({
            company:         rec.company,
            address:         rec.address || '(no address)',
            reason:          'Exact address + name match',
            detail:          dupCheck.detail || '',
            matched_company: dupCheck.matched_company || ''
          });
          continue;
        }

        if (dupCheck.action === 'review') {
          rec.data_status    = 'Review';
          rec.dup_reason     = dupCheck.reason;
          rec.dup_detail     = dupCheck.detail || '';
          possibleDups.push(rec);
        }
      }

      if (batchAddrKey.length > 8) batchAddrs.add(batchAddrKey);
      toInsert.push(rec);

      // Extend live index
      const recAddrNorm = normAddr(rec.address);
      if (recAddrNorm.length > 8) {
        if (!dupIndex.byAddr.has(recAddrNorm)) dupIndex.byAddr.set(recAddrNorm, []);
        dupIndex.byAddr.get(recAddrNorm).push({ nameNorm: normName(rec.company), placeId: rec.place_id || '', raw: rec });
      }
      if (rec.place_id) dupIndex.byPlaceId.set(rec.place_id, {
        nameNorm: normName(rec.company), addrNorm: recAddrNorm, state: rec.state || ''
      });
      dupIndex.records.push({ nameNorm: normName(rec.company), addrNorm: recAddrNorm, placeId: rec.place_id || '', state: rec.state || '' });
    }

    // ── Phase 7: Bulk insert ──────────────────────────────────────────────
    let imported = 0;
    const importedRecords = [];

    for (const rec of toInsert) {
      try {
        const result = await pool.query(
          `INSERT INTO prospects
             (user_id, company, category, company_type, city, state, phone, email,
              contact, website, status, priority, source, google_place_id,
              address, data_status, confidence_score, source_tier, last_activity_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NOW())
           ON CONFLICT DO NOTHING
           RETURNING id`,
          [
            uid,
            rec.company,
            rec.category,
            rec.company_type,
            rec.city           || null,
            rec.state          || null,
            rec.phone          || null,
            rec.email          || null,
            null,
            rec.website        || null,
            'New',
            rec.confidence_score >= 80 ? 'High' : rec.confidence_score >= 65 ? 'Medium' : 'Low',
            'Bulk Ingest v6',
            rec.place_id       || null,
            rec.address        || null,
            rec.data_status    || 'Unvetted',
            rec.confidence_score,
            rec.source_tier    || 3,
            null
          ]
        );
        if (result.rows.length > 0) {
          imported++;
          importedRecords.push({
            id:            result.rows[0].id,
            company:       rec.company,
            category:      rec.category,
            category_slug: rec.category_slug,
            company_type:  rec.company_type,
            city:          rec.city,
            state:         rec.state,
            address:       rec.address,
            confidence:    rec.confidence_score,
            status:        rec.data_status,
            flagged:       rec.data_status === 'Review',
            dup_reason:    rec.dup_reason || '',
            dup_detail:    rec.dup_detail || ''
          });
        }
      } catch (insertErr) {
        console.error('[BulkIngest v6] Insert error:', rec.company, insertErr.message);
      }
    }

    // ── Phase 8: Build response ───────────────────────────────────────────
    const exactSkipped    = skipped.filter(s => ['exact_address_and_name','place_id_confirmed'].includes(s.reason)).length;
    const batchSkipped    = skipped.filter(s => s.reason === 'batch_addr_dup').length;
    const reviewCount     = importedRecords.filter(r => r.flagged).length;

    console.log(`[BulkIngest v6] Done — imported:${imported} geo_rejected:${geoRejected} exact_dups:${exactSkipped} possible_dups:${reviewCount}`);

    res.json({
      ok:                  true,
      imported,
      geo_rejected:        geoRejected,
      geo_rejected_sample: geoRejectedSample,
      exact_address_dups:  exactSkipped,
      possible_duplicates: reviewCount,
      excluded_filtered:   filteredCount,
      excluded_low_conf:   lowConfCount,
      skipped_total:       skipped.length,
      queries_run:         expandedQueries.length,
      queries_used:        expandedQueries,
      raw_candidates:      rawResults.length,
      geo_valid:           geoValid.length,
      dedup_scope:         dedupScope,
      db_index_size:       existingRows.rows.length,
      territory_state:     territoryInfo.state,
      skip_breakdown: {
        exact_address: exactSkipped,
        batch_dup:     batchSkipped,
        other:         skipped.length - exactSkipped - batchSkipped
      },
      skipped_sample:  skippedSample,
      records:         importedRecords,
      breakdown: {
        distributors:  importedRecords.filter(r => r.company_type === 'Distributor').length,
        contractors:   importedRecords.filter(r => r.company_type === 'Contractor').length,
        unknown:       importedRecords.filter(r => r.company_type === 'Unknown').length,
        possible_dups: reviewCount,
        verified:      importedRecords.filter(r => r.status === 'Verified').length,
        likely_valid:  importedRecords.filter(r => r.status === 'Likely Valid').length,
        unvetted:      importedRecords.filter(r => r.status === 'Unvetted').length
      },
      message: imported === 0
        ? (geoRejected > 0
            ? `0 imported — ${geoRejected} results rejected (wrong state: results included ${[...new Set(geoRejectedSample.map(r=>r.state))].join(', ')}) · ${exactSkipped} exact address dups`
            : `0 imported — ${exactSkipped} exact address dups · ${filteredCount} filtered · try "No dedup" scope or a different query`)
        : `Imported ${imported} new ${territoryInfo.state || ''} records` +
          (geoRejected > 0    ? ` · ${geoRejected} wrong-state rejected`  : '') +
          (reviewCount > 0    ? ` · ${reviewCount} possible dups flagged`  : '') +
          (exactSkipped > 0   ? ` · ${exactSkipped} exact dups skipped`   : '')
    });

  } catch (e) {
    console.error('[BulkIngest v6] Fatal:', e.message, e.stack);
    res.status(500).json({ error: 'Bulk ingestion failed: ' + e.message });
  }
});



module.exports = router;
