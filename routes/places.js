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

  // (name word count filter removed — too aggressive for Growth Mode)

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

  // Direct state code — return major metros for that state
  const STATE_CITIES = {
    'AL': ['Birmingham AL','Huntsville AL','Mobile AL','Montgomery AL','Tuscaloosa AL','Hoover AL','Decatur AL','Auburn AL','Dothan AL','Gadsden AL','Anniston AL','Alabaster AL','Florence AL','Phenix City AL'],
    'GA': ['Atlanta GA','Marietta GA','Savannah GA','Augusta GA','Columbus GA','Macon GA','Alpharetta GA','Roswell GA','Athens GA','Warner Robins GA','Kennesaw GA','Valdosta GA','Brunswick GA','Rome GA'],
    'TN': ['Nashville TN','Memphis TN','Knoxville TN','Chattanooga TN','Clarksville TN','Murfreesboro TN','Franklin TN','Brentwood TN','Jackson TN','Johnson City TN','Hendersonville TN','Cookeville TN'],
    'FL': ['Jacksonville FL','Tampa FL','Orlando FL','Miami FL','Fort Lauderdale FL','Tallahassee FL','Gainesville FL','Pensacola FL','Naples FL','Sarasota FL','Fort Myers FL','Daytona Beach FL','Ocala FL'],
    'NC': ['Charlotte NC','Raleigh NC','Greensboro NC','Durham NC','Winston-Salem NC','Fayetteville NC','Cary NC','Wilmington NC','High Point NC','Concord NC','Asheville NC','Gastonia NC','Mooresville NC'],
    'SC': ['Columbia SC','Charleston SC','Greenville SC','Spartanburg SC','Rock Hill SC','Florence SC','Myrtle Beach SC','Anderson SC','Aiken SC','Sumter SC','Hilton Head SC'],
    'MS': ['Jackson MS','Gulfport MS','Biloxi MS','Hattiesburg MS','Meridian MS','Tupelo MS','Southaven MS','Olive Branch MS'],
    'AR': ['Little Rock AR','Fort Smith AR','Fayetteville AR','Springdale AR','Jonesboro AR','North Little Rock AR','Conway AR','Rogers AR','Bentonville AR'],
    'LA': ['New Orleans LA','Baton Rouge LA','Shreveport LA','Lafayette LA','Metairie LA','Bossier City LA','Lake Charles LA','Monroe LA','Alexandria LA'],
    'VA': ['Virginia Beach VA','Norfolk VA','Chesapeake VA','Richmond VA','Newport News VA','Alexandria VA','Hampton VA','Roanoke VA','Portsmouth VA','Suffolk VA','Lynchburg VA'],
    'KY': ['Louisville KY','Lexington KY','Bowling Green KY','Owensboro KY','Covington KY','Georgetown KY','Florence KY','Hopkinsville KY','Paducah KY'],
    'WV': ['Charleston WV','Huntington WV','Parkersburg WV','Morgantown WV','Wheeling WV','Martinsburg WV'],
    'TX': ['Houston TX','Dallas TX','San Antonio TX','Austin TX','Fort Worth TX','El Paso TX','Arlington TX','Corpus Christi TX','Plano TX','Lubbock TX','Garland TX','Irving TX','Frisco TX','McKinney TX'],
  };

  // Check if loc is a 2-letter state code
  const trimmed = (loc||'').trim().toUpperCase();
  if (STATE_CITIES[trimmed]) return STATE_CITIES[trimmed].slice(0,8);

  // Check for state name in loc string
  const STATE_NAME_TO_CODE_LC = {
    'alabama':'AL','georgia':'GA','tennessee':'TN','florida':'FL','north carolina':'NC',
    'south carolina':'SC','mississippi':'MS','arkansas':'AR','louisiana':'LA',
    'virginia':'VA','kentucky':'KY','west virginia':'WV','texas':'TX'
  };
  for (const [name, code] of Object.entries(STATE_NAME_TO_CODE_LC)) {
    if (t.includes(name) && STATE_CITIES[code]) return STATE_CITIES[code].slice(0,8);
  }

  // Legacy territory keywords
  if (t.includes('atlanta'))      return STATE_CITIES['GA'].slice(0,8);
  if (t.includes('birmingham'))   return STATE_CITIES['AL'].slice(0,8);
  if (t.includes('nashville'))    return STATE_CITIES['TN'].slice(0,8);
  if (t.includes('charlotte'))    return STATE_CITIES['NC'].slice(0,8);
  if (t.includes('jacksonville')) return STATE_CITIES['FL'].slice(0,8);
  if (t.includes('savannah'))     return ['Savannah GA','Augusta GA','Brunswick GA','Statesboro GA','Valdosta GA'];
  if (t.includes('southeast') || t.includes('south east'))
    return ['Atlanta GA','Birmingham AL','Nashville TN','Charlotte NC','Columbia SC','Savannah GA','Knoxville TN','Chattanooga TN'];

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


// ════════════════════════════════════════════════════════════════════════════
// SUPPLY CHAIN INTELLIGENCE ENGINE v3
// 3-Layer: Manufacturer → Distributor → Contractor
// Territory-locked · Growth Mode default · Exact-address dedup only


// ═══════════════════════════════════════════════════════════════════════════════
// SUPPLY CHAIN EXPANSION ENGINE v4
// Manufacturer → Distributor → Contractor
// Territory-locked · Exact-address dedup only · Growth Mode default
// ═══════════════════════════════════════════════════════════════════════════════

// ── US State Reference ──────────────────────────────────────────────────────
const V4_STATES = {
  'AL':'Alabama','AR':'Arkansas','FL':'Florida','GA':'Georgia','KY':'Kentucky',
  'LA':'Louisiana','MS':'Mississippi','NC':'North Carolina','SC':'South Carolina',
  'TN':'Tennessee','TX':'Texas','VA':'Virginia','WV':'West Virginia',
  'OH':'Ohio','IN':'Indiana','MO':'Missouri','OK':'Oklahoma',
};

const V4_CITIES = {
  'AL':['Birmingham','Huntsville','Mobile','Montgomery','Tuscaloosa','Hoover','Decatur','Auburn','Dothan','Gadsden','Pelham','Alabaster','Northport','Madison'],
  'GA':['Atlanta','Marietta','Savannah','Augusta','Columbus','Macon','Alpharetta','Roswell','Athens','Warner Robins','Kennesaw','Valdosta','Brunswick','Gainesville'],
  'TN':['Nashville','Memphis','Knoxville','Chattanooga','Clarksville','Murfreesboro','Franklin','Brentwood','Jackson','Johnson City','Hendersonville','Smyrna'],
  'FL':['Jacksonville','Tampa','Orlando','Miami','Fort Lauderdale','Tallahassee','Gainesville','Pensacola','Naples','Sarasota','Fort Myers','Daytona Beach','Ocala'],
  'NC':['Charlotte','Raleigh','Greensboro','Durham','Winston-Salem','Fayetteville','Cary','Wilmington','Concord','Asheville','Gastonia','Mooresville','Hickory'],
  'SC':['Columbia','Charleston','Greenville','Spartanburg','Rock Hill','Florence','Myrtle Beach','Anderson','Aiken','Sumter'],
  'MS':['Jackson','Gulfport','Biloxi','Hattiesburg','Meridian','Tupelo','Southaven','Olive Branch','Pascagoula'],
  'AR':['Little Rock','Fort Smith','Fayetteville','Springdale','Jonesboro','North Little Rock','Conway','Rogers','Bentonville'],
  'LA':['New Orleans','Baton Rouge','Shreveport','Lafayette','Metairie','Bossier City','Lake Charles','Monroe','Alexandria'],
  'VA':['Virginia Beach','Norfolk','Chesapeake','Richmond','Newport News','Alexandria','Hampton','Roanoke','Lynchburg'],
  'KY':['Louisville','Lexington','Bowling Green','Owensboro','Covington','Georgetown','Florence','Hopkinsville','Paducah'],
  'WV':['Charleston','Huntington','Parkersburg','Morgantown','Wheeling','Martinsburg','Weirton'],
  'TX':['Houston','Dallas','San Antonio','Austin','Fort Worth','El Paso','Plano','Lubbock','Arlington','Corpus Christi'],
  'OH':['Columbus','Cleveland','Cincinnati','Toledo','Akron','Dayton','Parma','Canton'],
  'IN':['Indianapolis','Fort Wayne','Evansville','South Bend','Carmel','Bloomington','Fishers'],
  'MO':['Kansas City','St. Louis','Springfield','Columbia','Independence','Lee\'s Summit'],
  'OK':['Oklahoma City','Tulsa','Norman','Broken Arrow','Lawton','Edmond'],
};

// ── Manufacturer Seed Definitions ───────────────────────────────────────────
const V4_MANUFACTURERS = {
  decking: [
    { name:'Trex',           terms:['trex dealer','trex authorized contractor','trex pro dealer','trex composite decking dealer'] },
    { name:'TimberTech',     terms:['timbertech dealer','timbertech contractor','azek dealer','azek contractor','timbertech pro'] },
    { name:'Fiberon',        terms:['fiberon dealer','fiberon contractor','fiberon decking dealer'] },
    { name:'Deckorators',    terms:['deckorators dealer','deckorators contractor'] },
    { name:'MoistureShield', terms:['moistureshield dealer','moistureshield contractor'] },
    { name:'Envision',       terms:['envision outdoor living dealer','envision decking contractor'] },
    { name:'NewTechWood',    terms:['newtechwood dealer','newtechwood installer'] },
    { name:'Cali Decking',   terms:['cali decking dealer','cali bamboo dealer'] },
  ],
  roofing: [
    { name:'GAF',            terms:['gaf master elite contractor','gaf certified contractor','gaf authorized dealer','gaf roofing supply dealer'] },
    { name:'Carlisle',       terms:['carlisle roofing dealer','carlisle syntec dealer','carlisle coatings dealer'] },
    { name:'Owens Corning',  terms:['owens corning preferred contractor','owens corning platinum contractor','owens corning roofing dealer'] },
    { name:'CertainTeed',    terms:['certainteed shingle master','certainteed contractor','certainteed select shinglemaster'] },
    { name:'Johns Manville', terms:['johns manville roofing dealer','jm roofing distributor'] },
    { name:'Sika',           terms:['sika roofing dealer','sika authorized contractor'] },
    { name:'Tremco',         terms:['tremco roofing dealer','tremco authorized contractor'] },
    { name:'IKO',            terms:['iko roofing dealer','iko shingles contractor'] },
  ],
};

// ── Hard Exclusion List ──────────────────────────────────────────────────────
const V4_HARD_EXCLUDE = [
  'home depot','lowes',"lowe's",'menards','walmart','costco','amazon','wayfair',
  'ace hardware','true value','big box','84 lumber',
  'bank','credit union','financial','insurance','mortgage','hospital','clinic',
  'medical','dental','restaurant','diner','cafe','food','grocery','pharmacy',
  'government','county','city hall','police','fire station','post office','dmv',
  'school','university','college','church','temple','mosque','synagogue',
  'realty','realtor','real estate','property management','apartment','hoa',
  'auto dealer','car dealer','oil change','auto repair','tire shop',
  'painting contractor','paint company','painters','painting services',
  'garage door','overhead door','garage doors',
  'pest control','lawn care','landscaping','pressure washing','carpet cleaning',
  'plumbing','electrician','hvac','air conditioning','heating',
];

function v4IsExcluded(company, types) {
  const n = (company || '').toLowerCase();
  const t = (types  || []).join(' ').toLowerCase();
  const combined = n + ' ' + t;
  if (V4_HARD_EXCLUDE.some(ex => combined.includes(ex))) return true;
  if (/paint(ing)?\s*(contractor|company|co\b|services|pro\b)/i.test(n)) return true;
  if (/garage|overhead\s*door/i.test(n)) return true;
  return false;
}

// ── Address Normalizer ───────────────────────────────────────────────────────
function v4NormAddr(a) {
  if (!a) return '';
  return (a || '').toLowerCase()
    .replace(/\bstreet\b/g,'st').replace(/\bavenue\b/g,'ave')
    .replace(/\bboulevard\b/g,'blvd').replace(/\bdrive\b/g,'dr')
    .replace(/\broad\b/g,'rd').replace(/\bcourt\b/g,'ct')
    .replace(/\blane\b/g,'ln').replace(/\bplace\b/g,'pl')
    .replace(/\bsuite\b/g,'ste').replace(/\bapartment\b/g,'apt')
    .replace(/[,\.#\-]/g,' ').replace(/\s+/g,' ').trim();
}

function v4NormName(n) {
  return (n || '').toLowerCase()
    .replace(/\b(llc|inc|corp|co\.|company|the|ltd)\b/g,'')
    .replace(/[^a-z0-9]/g,' ').replace(/\s+/g,' ').trim();
}

// ── State parser from Google address ────────────────────────────────────────
function v4ParseState(addr) {
  if (!addr) return null;
  const m = (addr || '').match(/,\s*([A-Z]{2})\s+\d{5}/);
  if (m) return m[1];
  const m2 = (addr || '').match(/\b([A-Z]{2})\b(?=\s+\d{5}|\s*,\s*USA)/);
  return m2 ? m2[1] : null;
}

function v4ParseCity(addr) {
  if (!addr) return '';
  const parts = addr.split(',');
  return parts.length >= 3 ? parts[parts.length - 3].trim() : '';
}

// ── Classify a Google Places result ─────────────────────────────────────────
function v4Classify(rec, mfrName) {
  const n   = (rec.company || '').toLowerCase();
  const t   = (rec.types   || []).join(' ').toLowerCase();
  const w   = (rec.website || '').toLowerCase();
  const combined = `${n} ${t} ${w}`;

  // ── Distributor keywords ──────────────────────────────────────────────────
  const DIST_KW = [
    'roofing supply','roofing materials','roofing wholesale','roofing depot','roof supply',
    'deck supply','decking supply','decking distributor','decking wholesale',
    'building materials','builders supply','building supply','lumber yard','lumber dealer',
    'lumber company','pro dealer','building center','hardware supply','construction supply',
    'building products','material supply','supply house','supply co','supply company',
    'wholesale supply','abc supply','beacon roofing','beacon supply','gulfeagle',
    'srs distribution','bradco','allied building','hepler','famco','srs',
    'window supply','window distributor','door supply','millwork dealer','millwork supply',
    'siding supply','siding distributor','siding materials','siding wholesale',
    'scaffolding supply','tool supply','fastener supply','equipment supply',
    'dealer','distributor',
  ];

  if (DIST_KW.some(k => combined.includes(k))) {
    const sub = combined.includes('roof') ? 'Roofing Distributor'
              : (combined.includes('deck') || combined.includes('trex') || combined.includes('azek') ||
                 combined.includes('timbertech') || combined.includes('fiberon')) ? 'Decking Distributor'
              : combined.includes('siding') ? 'Siding Distributor'
              : (combined.includes('window') || combined.includes('door')) ? 'Window/Door Distributor'
              : combined.includes('scaffold') || combined.includes('fastener') || combined.includes('tool') ? 'Tool/Equipment Dealer'
              : 'Building Materials Distributor';
    return { layer: 2, type: 'Distributor', sub, mfr: mfrName || '' };
  }

  // ── Contractor keywords ───────────────────────────────────────────────────
  const CONT_KW = [
    // Roofing
    'roofing contractor','roofer','roofing company','roofing co','commercial roofing',
    'roof repair','roofing services','roofing systems','roof installation','metal roofing',
    'flat roofing','roofing solutions','roofing group','roofing specialist','roofing',
    // Decking / outdoor
    'deck builder','decking contractor','deck construction','custom decks','decks and patios',
    'outdoor living','composite decking','deck & porch','deck and porch','decks & porch',
    'porch builder','deck & fence','deck & pergola','pergola builder','pergola contractor',
    'outdoor spaces','outdoor structures','patio builder','patio contractor','patio company',
    'screened porch','screen room','sunroom','decking','deck company','deck design',
    'deck restoration','decks & exteriors','porches and decks','custom outdoor',
    'backyard living','outdoor kitchen','deck',
    // Siding
    'siding contractor','siding company','siding installer','siding & windows',
    'cornice contractor','siding services','siding specialist','vinyl siding',
    'fiber cement','hardieplank','james hardie',
    // Windows / Doors
    'window installer','door installer','window contractor','window & door',
    'window replacement','window company','entry door','door replacement',
    'window specialist','fenestration',
    // General exterior
    'general contractor','construction company','general contracting',
    'home builder','remodeling','renovation company','home improvement',
    'construction group','exterior remodeling','home renovation','exterior',
  ];

  if (CONT_KW.some(k => combined.includes(k))) {
    const sub = combined.includes('roof') ? 'Roofing Contractor'
              : (combined.includes('deck') || combined.includes('patio') ||
                 combined.includes('porch') || combined.includes('pergola') ||
                 combined.includes('outdoor') || combined.includes('trex') ||
                 combined.includes('timbertech') || combined.includes('fiberon')) ? 'Decking Contractor'
              : (combined.includes('siding') || combined.includes('hardie') ||
                 combined.includes('cornice')) ? 'Siding Contractor'
              : (combined.includes('window') || combined.includes('door')) ? 'Window/Door Installer'
              : 'Exterior Contractor';
    return { layer: 3, type: 'Contractor', sub, mfr: mfrName || '' };
  }

  // ── Google Places type fallback ───────────────────────────────────────────
  const TYPE_MAP = {
    'roofing_contractor':     { layer: 3, type: 'Contractor',  sub: 'Roofing Contractor' },
    'general_contractor':     { layer: 3, type: 'Contractor',  sub: 'Exterior Contractor' },
    'home_improvement_store': { layer: 2, type: 'Distributor', sub: 'Building Materials Distributor' },
    'hardware_store':         { layer: 2, type: 'Distributor', sub: 'Building Materials Distributor' },
    'lumber_yard':            { layer: 2, type: 'Distributor', sub: 'Building Materials Distributor' },
    'contractor':             { layer: 3, type: 'Contractor',  sub: 'Exterior Contractor' },
    'construction_company':   { layer: 3, type: 'Contractor',  sub: 'Exterior Contractor' },
    'establishment':          { layer: 3, type: 'Contractor',  sub: 'Exterior Contractor' },
    'point_of_interest':      { layer: 3, type: 'Contractor',  sub: 'Exterior Contractor' },
  };
  for (const gType of (rec.types || [])) {
    if (TYPE_MAP[gType]) return { ...TYPE_MAP[gType], mfr: mfrName || '' };
  }

  // ── Growth Mode catch-all — if it passed exclusions, accept it ───────────
  return { layer: 3, type: 'Contractor', sub: 'Exterior Contractor', mfr: mfrName || '' };
}

// ── Google Places Text Search ────────────────────────────────────────────────
async function v4PlacesFetch(query, apiKey) {
  const url  = 'https://places.googleapis.com/v1/places:searchText';
  const body = JSON.stringify({ textQuery: query, maxResultCount: 20, languageCode: 'en' });
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type':     'application/json',
        'X-Goog-Api-Key':   apiKey,
        'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.types,places.location',
      },
      body,
    });
    const data = await resp.json();
    return (data.places || []).map(pl => ({
      place_id: pl.id || null,
      company:  pl.displayName?.text || '',
      address:  pl.formattedAddress   || '',
      phone:    pl.nationalPhoneNumber || '',
      website:  pl.websiteUri          || '',
      types:    pl.types               || [],
      lat:      pl.location?.latitude,
      lng:      pl.location?.longitude,
    }));
  } catch(e) {
    console.error('[v4PlacesFetch]', query, e.message);
    return [];
  }
}

// ── Query Builder ────────────────────────────────────────────────────────────
function v4BuildQueries(category, mfrs, stateCode, customQuery) {
  const cities = (V4_CITIES[stateCode] || []).slice(0, 8);
  const queries = [];

  if (customQuery && customQuery.trim()) {
    // Expand custom query across top cities + state
    queries.push(`${customQuery.trim()} ${stateCode}`);
    cities.slice(0, 4).forEach(c => queries.push(`${customQuery.trim()} ${c} ${stateCode}`));
    return queries.slice(0, 12);
  }

  if (category === 'decking' || category === 'all') {
    // Manufacturer-specific dealer/contractor searches
    mfrs.decking.slice(0, 4).forEach(mfr => {
      cities.slice(0, 3).forEach(city => {
        queries.push(`${mfr.name} dealer ${city} ${stateCode}`);
        queries.push(`${mfr.name} contractor ${city} ${stateCode}`);
      });
      queries.push(`${mfr.name} authorized dealer ${stateCode}`);
    });
    // Generic decking ecosystem searches
    ['deck builder','decking contractor','outdoor living contractor',
     'composite deck installer','patio contractor','porch builder',
     'decking supply','building materials decking',
    ].forEach(kw => {
      cities.slice(0, 3).forEach(city => queries.push(`${kw} ${city} ${stateCode}`));
    });
  }

  if (category === 'roofing' || category === 'all') {
    mfrs.roofing.slice(0, 4).forEach(mfr => {
      cities.slice(0, 3).forEach(city => {
        queries.push(`${mfr.name} roofing contractor ${city} ${stateCode}`);
        queries.push(`${mfr.name} roofing dealer ${city} ${stateCode}`);
      });
      queries.push(`${mfr.name} authorized ${stateCode}`);
    });
    ['commercial roofing contractor','roofing contractor',
     'roofing supply','roofing distributor','flat roofing contractor',
    ].forEach(kw => {
      cities.slice(0, 4).forEach(city => queries.push(`${kw} ${city} ${stateCode}`));
    });
  }

  if (category === 'siding' || category === 'all') {
    ['james hardie installer','LP smartside installer','siding contractor',
     'siding company','vinyl siding installer','siding supply',
    ].forEach(kw => {
      cities.slice(0, 3).forEach(city => queries.push(`${kw} ${city} ${stateCode}`));
    });
  }

  if (category === 'windows' || category === 'all') {
    ['andersen windows dealer','pella windows dealer','window replacement contractor',
     'window door installer','window supply distributor',
    ].forEach(kw => {
      cities.slice(0, 3).forEach(city => queries.push(`${kw} ${city} ${stateCode}`));
    });
  }

  // Always add generic building supply + broad contractor searches
  cities.slice(0, 5).forEach(city => {
    queries.push(`building materials supplier ${city} ${stateCode}`);
    queries.push(`exterior contractor ${city} ${stateCode}`);
  });

  return queries.slice(0, 40); // max 40 queries per run
}

// ── CRM Dedup Index ──────────────────────────────────────────────────────────
function v4BuildDedupIndex(rows) {
  const byAddr = new Set();
  for (const r of rows) {
    const na = v4NormAddr(r.address);
    if (na.length > 10) byAddr.add(na);
  }
  return byAddr;
}

function v4CheckDup(rec, dedupIndex, batchAddrs, scope) {
  if (scope === 'off') return { result: 'import' };

  const na = v4NormAddr(rec.address);
  if (!na || na.length < 10) return { result: 'import' }; // no address = can't dedup, import it

  // Already in this batch
  if (batchAddrs.has(na)) return { result: 'skip', reason: 'batch_duplicate' };

  // Exact address match in CRM
  if (dedupIndex.has(na)) return { result: 'exact_dup', reason: 'exact_address_match' };

  return { result: 'import' };
}

// ── Main Bulk Ingest Route v4 ────────────────────────────────────────────────
router.post('/bulk-ingest', async (req, res) => {
  const uid = req.session?.user?.id;
  if (!uid) return res.status(401).json({ ok: false, error: 'Not authenticated' });

  const {
    query: customQuery,
    search_state,
    category = 'decking',
    max_records = 60,
    scope = 'team',
  } = req.body;

  const stateCode = (search_state || '').trim().toUpperCase();
  if (!stateCode || !V4_STATES[stateCode]) {
    return res.status(400).json({ ok: false, error: 'Valid state required. Select a state from the dropdown.' });
  }

  const maxRecs  = Math.min(parseInt(max_records) || 60, 200);
  const apiKey   = process.env.GOOGLE_PLACES_API_KEY;
  const cat      = (category || 'decking').toLowerCase();

  console.log(`[BulkIngest v4] state=${stateCode} cat=${cat} max=${maxRecs} scope=${scope}`);

  const runLog = {
    state: stateCode,
    category: cat,
    manufacturers_seeded: 0,
    queries_run: 0,
    raw_fetched: 0,
    geo_rejected: 0,
    geo_rejected_sample: [],
    excluded: 0,
    excluded_sample: [],
    exact_dups: 0,
    exact_dup_sample: [],
    possible_dups: 0,
    batch_dups: 0,
    imported: 0,
    import_errors: [],
    breakdown: { distributors: 0, contractors: 0, roofing: 0, decking: 0, siding: 0, windows: 0, general: 0 },
    records: [],
    queries_used: [],
  };

  try {
    // ── Step 1: Build query set ────────────────────────────────────────────
    const queries = v4BuildQueries(cat, V4_MANUFACTURERS, stateCode, customQuery);
    runLog.queries_used  = queries;
    runLog.queries_run   = queries.length;

    // Count manufacturer seeds
    if (!customQuery) {
      runLog.manufacturers_seeded = cat === 'all'
        ? V4_MANUFACTURERS.decking.length + V4_MANUFACTURERS.roofing.length
        : (V4_MANUFACTURERS[cat] || []).length;
    }

    console.log(`[BulkIngest v4] ${queries.length} queries built`);

    // ── Step 2: Fetch from Google Places ─────────────────────────────────
    const seenPlaceIds = new Set();
    const rawResults   = [];

    for (const q of queries) {
      if (rawResults.length >= maxRecs * 4) break; // generous fetch buffer
      const batch = await v4PlacesFetch(q, apiKey);
      for (const r of batch) {
        if (r.place_id && seenPlaceIds.has(r.place_id)) continue;
        if (r.place_id) seenPlaceIds.add(r.place_id);
        rawResults.push(r);
      }
    }
    runLog.raw_fetched = rawResults.length;
    console.log(`[BulkIngest v4] Raw: ${rawResults.length}`);

    // ── Step 3: Hard geo filter — state must match ────────────────────────
    const geoValid = [];
    for (const rec of rawResults) {
      const recState = v4ParseState(rec.address);
      if (!recState || recState !== stateCode) {
        runLog.geo_rejected++;
        if (runLog.geo_rejected_sample.length < 8) {
          runLog.geo_rejected_sample.push({ company: rec.company, address: rec.address, parsed_state: recState });
        }
      } else {
        rec.state = recState;
        rec.city  = v4ParseCity(rec.address);
        geoValid.push(rec);
      }
    }
    console.log(`[BulkIngest v4] Geo valid: ${geoValid.length} / Rejected: ${runLog.geo_rejected}`);

    // ── Step 4: Classify + exclude ────────────────────────────────────────
    const classified = [];
    for (const rec of geoValid) {
      if (v4IsExcluded(rec.company, rec.types)) {
        runLog.excluded++;
        if (runLog.excluded_sample.length < 8) {
          runLog.excluded_sample.push({ company: rec.company, reason: 'exclusion_list' });
        }
        continue;
      }
      // Determine which manufacturer term triggered this result (best guess from query context)
      const cls = v4Classify(rec, '');
      rec.supply_layer  = cls.layer;
      rec.company_type  = cls.type;
      rec.sub_category  = cls.sub;
      rec.mfr_assoc     = cls.mfr;
      classified.push(rec);
    }
    console.log(`[BulkIngest v4] Classified: ${classified.length} / Excluded: ${runLog.excluded}`);

    // ── Step 5: Load CRM dedup index ──────────────────────────────────────
    let existingRows = [];
    if (scope !== 'off') {
      const dbQ = scope === 'mine'
        ? `SELECT address FROM prospects WHERE user_id = $1 AND address IS NOT NULL`
        : `SELECT address FROM prospects WHERE address IS NOT NULL`;
      const dbP = scope === 'mine' ? [uid] : [];
      const result = await pool.query(dbQ, dbP);
      existingRows = result.rows;
    }
    const dedupIndex = v4BuildDedupIndex(existingRows);
    console.log(`[BulkIngest v4] CRM index: ${dedupIndex.size} addresses`);

    // ── Step 6: Apply dedup + collect records ─────────────────────────────
    const batchAddrs = new Set();
    const toInsert   = [];
    const possibles  = [];

    for (const rec of classified) {
      if (toInsert.length >= maxRecs) break;

      const check = v4CheckDup(rec, dedupIndex, batchAddrs, scope);

      if (check.result === 'skip') {
        runLog.batch_dups++;
        continue;
      }
      if (check.result === 'exact_dup') {
        runLog.exact_dups++;
        if (runLog.exact_dup_sample.length < 10) {
          runLog.exact_dup_sample.push({ company: rec.company, address: rec.address, reason: check.reason });
        }
        continue;
      }

      // Mark address as seen in this batch
      const na = v4NormAddr(rec.address);
      if (na.length > 10) batchAddrs.add(na);

      rec.data_status = 'Unvetted';
      toInsert.push(rec);
    }

    console.log(`[BulkIngest v4] To insert: ${toInsert.length} / Exact dups: ${runLog.exact_dups}`);

    // ── Step 7: Insert into CRM ───────────────────────────────────────────
    for (const rec of toInsert) {
      const categoryLabel = rec.sub_category || (rec.company_type === 'Distributor' ? 'Building Materials Distributor' : 'Exterior Contractor');
      const channelLabel  = rec.company_type || 'Contractor';
      const sourceLabel   = `Bulk v4 — ${stateCode} — ${cat}`;
      const notesLabel    = `Supply Chain: ${channelLabel} · Discovered via bulk import`;

      try {
        const result = await pool.query(`
          INSERT INTO prospects
            (user_id, company, category, city, state, phone, website,
             address, google_place_id, source, data_status, company_type,
             manufacturer_assoc, notes, channel, last_activity_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW())
          RETURNING id`,
          [
            uid,
            rec.company,
            categoryLabel,
            rec.city  || '',
            rec.state || stateCode,
            rec.phone || '',
            rec.website || '',
            rec.address || '',
            rec.place_id || null,
            sourceLabel,
            'Unvetted',
            channelLabel,
            rec.mfr_assoc || '',
            notesLabel,
            channelLabel,
          ]
        );

        if (result.rows.length > 0) {
          runLog.imported++;
          // Breakdown
          if (rec.company_type === 'Distributor') runLog.breakdown.distributors++;
          else runLog.breakdown.contractors++;

          const sub = (rec.sub_category || '').toLowerCase();
          if (sub.includes('roof'))    runLog.breakdown.roofing++;
          else if (sub.includes('deck') || sub.includes('outdoor')) runLog.breakdown.decking++;
          else if (sub.includes('siding')) runLog.breakdown.siding++;
          else if (sub.includes('window') || sub.includes('door')) runLog.breakdown.windows++;
          else runLog.breakdown.general++;

          if (runLog.records.length < 30) {
            runLog.records.push({
              company:      rec.company,
              address:      rec.address,
              phone:        rec.phone    || '',
              website:      rec.website  || '',
              category:     categoryLabel,
              type:         channelLabel,
              supply_layer: rec.supply_layer,
              sub_category: rec.sub_category,
            });
          }
        }
      } catch(e) {
        runLog.import_errors.push({ company: rec.company, error: e.message });
        console.error('[BulkIngest v4] Insert error:', rec.company, e.message);
      }
    }

    console.log(`[BulkIngest v4] Imported: ${runLog.imported} / Errors: ${runLog.import_errors.length}`);

    // ── Step 8: Build message ─────────────────────────────────────────────
    let message;
    if (runLog.imported > 0) {
      message = `${runLog.imported} new ${stateCode} records added — ${runLog.breakdown.distributors} distributors, ${runLog.breakdown.contractors} contractors`;
    } else if (runLog.import_errors.length > 0) {
      message = `DB error: ${runLog.import_errors[0]?.error}`;
    } else if (runLog.exact_dups >= toInsert.length && toInsert.length > 0) {
      message = `All ${runLog.exact_dups} records already in CRM (exact address match). Try "No Dedup" scope or different category.`;
    } else if (runLog.geo_rejected > runLog.raw_fetched * 0.8) {
      message = `${runLog.geo_rejected} records rejected for wrong state. Verify state selection.`;
    } else {
      message = `0 imported — ${runLog.excluded} filtered · ${runLog.geo_rejected} wrong state · ${runLog.exact_dups} exact dups · ${runLog.import_errors.length} DB errors`;
    }

    return res.json({
      ok: true,
      ...runLog,
      crm_index_size: dedupIndex.size,
      message,
    });

  } catch(err) {
    console.error('[BulkIngest v4] Fatal:', err.message, err.stack);
    return res.status(500).json({ ok: false, error: err.message, stack: err.stack?.slice(0,300) });
  }
});

module.exports = router;
