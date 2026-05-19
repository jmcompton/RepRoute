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
// ════════════════════════════════════════════════════════════════════════════

// ── State lookup tables ──────────────────────────────────────────────────────
const SC_STATE_NAMES = {
  'alabama':'AL','alaska':'AK','arizona':'AZ','arkansas':'AR','california':'CA',
  'colorado':'CO','connecticut':'CT','delaware':'DE','florida':'FL','georgia':'GA',
  'hawaii':'HI','idaho':'ID','illinois':'IL','indiana':'IN','iowa':'IA','kansas':'KS',
  'kentucky':'KY','louisiana':'LA','maine':'ME','maryland':'MD','massachusetts':'MA',
  'michigan':'MI','minnesota':'MN','mississippi':'MS','missouri':'MO','montana':'MT',
  'nebraska':'NE','nevada':'NV','new hampshire':'NH','new jersey':'NJ',
  'new mexico':'NM','new york':'NY','north carolina':'NC','north dakota':'ND',
  'ohio':'OH','oklahoma':'OK','oregon':'OR','pennsylvania':'PA',
  'rhode island':'RI','south carolina':'SC','south dakota':'SD',
  'tennessee':'TN','texas':'TX','utah':'UT','vermont':'VT','virginia':'VA',
  'washington':'WA','west virginia':'WV','wisconsin':'WI','wyoming':'WY'
};

const SC_STATE_CITIES = {
  'AL':['Birmingham','Huntsville','Mobile','Montgomery','Tuscaloosa','Hoover','Decatur','Auburn','Dothan','Gadsden','Pelham','Alabaster','Northport'],
  'GA':['Atlanta','Marietta','Savannah','Augusta','Columbus','Macon','Alpharetta','Roswell','Athens','Warner Robins','Kennesaw','Valdosta','Brunswick'],
  'TN':['Nashville','Memphis','Knoxville','Chattanooga','Clarksville','Murfreesboro','Franklin','Brentwood','Jackson','Johnson City','Hendersonville'],
  'FL':['Jacksonville','Tampa','Orlando','Miami','Fort Lauderdale','Tallahassee','Gainesville','Pensacola','Naples','Sarasota','Fort Myers','Daytona Beach'],
  'NC':['Charlotte','Raleigh','Greensboro','Durham','Winston-Salem','Fayetteville','Cary','Wilmington','Concord','Asheville','Gastonia','Mooresville'],
  'SC':['Columbia','Charleston','Greenville','Spartanburg','Rock Hill','Florence','Myrtle Beach','Anderson','Aiken'],
  'MS':['Jackson','Gulfport','Biloxi','Hattiesburg','Meridian','Tupelo','Southaven','Olive Branch'],
  'AR':['Little Rock','Fort Smith','Fayetteville','Springdale','Jonesboro','North Little Rock','Conway','Rogers'],
  'LA':['New Orleans','Baton Rouge','Shreveport','Lafayette','Metairie','Bossier City','Lake Charles','Monroe'],
  'VA':['Virginia Beach','Norfolk','Chesapeake','Richmond','Newport News','Alexandria','Hampton','Roanoke'],
  'KY':['Louisville','Lexington','Bowling Green','Owensboro','Covington','Georgetown','Florence','Hopkinsville'],
  'WV':['Charleston','Huntington','Parkersburg','Morgantown','Wheeling','Martinsburg'],
  'TX':['Houston','Dallas','San Antonio','Austin','Fort Worth','El Paso','Plano','Lubbock'],
};

// ── Manufacturer anchor definitions ─────────────────────────────────────────
const SC_MANUFACTURERS = {
  decking: [
    { name:'Trex',       terms:['trex dealer','trex contractor','trex composite decking','trex pro','authorized trex','trex preferred'] },
    { name:'TimberTech', terms:['timbertech dealer','timbertech contractor','timbertech azek','azek dealer','azek contractor','timbertech pro'] },
    { name:'Fiberon',    terms:['fiberon dealer','fiberon contractor','fiberon decking','fiberon pro'] },
    { name:'Wolf',       terms:['wolf decking dealer','wolf pvc decking','wolf home products'] },
  ],
  roofing: [
    { name:'GAF',           terms:['gaf certified contractor','gaf master elite','gaf roofing dealer','gaf shingles dealer','gaf authorized'] },
    { name:'Owens Corning', terms:['owens corning preferred contractor','owens corning dealer','owens corning roofing'] },
    { name:'CertainTeed',   terms:['certainteed contractor','certainteed dealer','certainteed shingles'] },
    { name:'Carlisle',      terms:['carlisle roofing dealer','carlisle syntec','carlisle coatings'] },
  ],
  siding: [
    { name:'James Hardie', terms:['james hardie preferred contractor','hardie preferred','hardi plank installer','hardie pro'] },
    { name:'LP SmartSide', terms:['lp smartside installer','lp building products dealer','louisiana pacific dealer'] },
    { name:'Alside',       terms:['alside siding dealer','alside windows dealer'] },
  ],
  windows: [
    { name:'Andersen',  terms:['andersen certified contractor','andersen authorized dealer','andersen windows dealer'] },
    { name:'Pella',     terms:['pella certified installer','pella dealer','pella authorized'] },
    { name:'Milgard',   terms:['milgard dealer','milgard certified'] },
  ],
};

// ── Field quality score ──────────────────────────────────────────────────────
function scFieldScore(rec) {
  let s = 0;
  if (rec.address && rec.address.length > 10) s += 40;
  if (rec.phone   && rec.phone.replace(/\D/g,'').length >= 10) s += 20;
  if (rec.website && rec.website.length > 5) s += 15;
  if (rec.place_id) s += 15;
  if (rec.company && rec.company.length > 3) s += 10;
  return Math.min(s, 100);
}

// ── Address normalizer ───────────────────────────────────────────────────────
function scNormAddr(a) {
  if (!a) return '';
  return (a || '').toLowerCase()
    .replace(/\bstreet\b/g,'st').replace(/\bavenue\b/g,'ave')
    .replace(/\bboulevard\b/g,'blvd').replace(/\bdrive\b/g,'dr')
    .replace(/\broad\b/g,'rd').replace(/\bcourt\b/g,'ct')
    .replace(/\bsuite\b/g,'ste').replace(/\bapartment\b/g,'apt')
    .replace(/[,\.#]/g,' ').replace(/\s+/g,' ').trim();
}

function scNormPhone(p) { return (p||'').replace(/\D/g,'').slice(0,10); }
function scNormName(n) {
  return (n||'').toLowerCase()
    .replace(/\b(llc|inc|corp|co\.|company|the)\b/g,'')
    .replace(/[^a-z0-9]/g,' ').replace(/\s+/g,' ').trim();
}

// ── Territory resolver ───────────────────────────────────────────────────────
function scResolveState(explicitCode, queryText, profileTerr) {
  // Priority 1: explicit 2-letter dropdown
  if (explicitCode && /^[A-Z]{2}$/.test(explicitCode)) return explicitCode;
  // Priority 2: state name in query
  const ql = (queryText||'').toLowerCase();
  for (const [name, code] of Object.entries(SC_STATE_NAMES)) {
    if (ql.includes(name)) return code;
  }
  // Priority 3: abbrev in query
  const abbrM = (queryText||'').match(/\b([A-Z]{2})\b/);
  if (abbrM && Object.values(SC_STATE_NAMES).includes(abbrM[1])) return abbrM[1];
  // Priority 4: city keyword → state
  const pl = (profileTerr||'').toLowerCase();
  if (pl.includes('atlanta') || pl.includes('savannah') || pl.includes('augusta')) return 'GA';
  if (pl.includes('birmingham') || pl.includes('huntsville') || pl.includes('mobile')) return 'AL';
  if (pl.includes('nashville') || pl.includes('memphis') || pl.includes('knoxville')) return 'TN';
  if (pl.includes('charlotte') || pl.includes('raleigh')) return 'NC';
  if (pl.includes('jacksonville') || pl.includes('tampa') || pl.includes('orlando')) return 'FL';
  if (pl.includes('columbia') || pl.includes('charleston') || pl.includes('greenville')) return 'SC';
  return null;
}

// ── Google Places fetch (low-level) ─────────────────────────────────────────
async function scPlacesFetch(query, apiKey) {
  const url = 'https://places.googleapis.com/v1/places:searchText';
  const body = JSON.stringify({
    textQuery:      query,
    maxResultCount: 20,
    languageCode:   'en',
  });
  const opts = {
    method:  'POST',
    headers: {
      'Content-Type':     'application/json',
      'X-Goog-Api-Key':   apiKey,
      'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.types,places.location',
    },
    body,
  };
  try {
    const resp  = await fetch(url, opts);
    const data  = await resp.json();
    return (data.places || []).map(pl => ({
      place_id: pl.id,
      company:  pl.displayName?.text || '',
      address:  pl.formattedAddress  || '',
      phone:    pl.nationalPhoneNumber || '',
      website:  pl.websiteUri         || '',
      types:    pl.types              || [],
      lat:      pl.location?.latitude,
      lng:      pl.location?.longitude,
    }));
  } catch(e) {
    console.error('[scPlacesFetch] error:', e.message);
    return [];
  }
}

// ── Parse state from a formatted address ────────────────────────────────────
function parseStateFromAddress(addr) {
  if (!addr) return null;
  // "123 Main St, Birmingham, AL 35203, USA" → "AL"
  const m = addr.match(/,\s*([A-Z]{2})\s+\d{5}/);
  if (m) return m[1];
  // Fallback: last 2-letter chunk before ZIP or USA
  const m2 = addr.match(/\b([A-Z]{2})\b(?=\s+\d{5}|\s*,\s*USA)/);
  return m2 ? m2[1] : null;
}

// ── Classification engine ────────────────────────────────────────────────────
function scClassify(rec) {
  const n = (rec.company  || '').toLowerCase();
  const a = (rec.address  || '').toLowerCase();
  const w = (rec.website  || '').toLowerCase();
  const t = (rec.types    || []).map(x => x.toLowerCase()).join(' ');
  const all = `${n} ${t} ${w}`;

  // Hard exclusions
  const HARD_EXCL = ['home depot','lowes',"lowe's",'menards','walmart','costco',
    'amazon','wayfair','ace hardware','true value','big box',
    'plumbing','electrician','hvac','landscaping','lawn care',
    'pest control','pool service','pressure washing','carpet cleaning',
    'junk removal','moving company','car dealership','auto repair',
    'restaurant','food','grocery','pharmacy','hospital','dental',
    'hotel','motel','real estate','insurance','attorney','law firm',
    'painting contractor','paint company','painting company','paint contractor',
    'garage door','overhead door'];
  for (const ex of HARD_EXCL) {
    if (all.includes(ex)) return null;
  }
  if (/paint(ing)?\s*(contractor|company|co\b|services)/i.test(n)) return null;
  if (/garage|overhead door/i.test(n)) return null;

  // ── Layer 2: Distributor / Dealer / Supply ────────────────────────────────
  const DIST_KEYWORDS = [
    // Roofing supply
    'roofing supply','roofing distributor','roofing materials','roofing wholesale','roofing dealer',
    'roofing depot','roof supply',
    // Decking supply
    'deck supply','decking distributor','decking dealer','decking wholesale',
    'timbertech dealer','trex dealer','azek dealer','fiberon dealer',
    'composite decking dealer','composite decking supply',
    // Siding supply
    'siding supply','siding distributor','siding dealer','siding wholesale','siding materials',
    // Window/door supply
    'window distributor','window dealer','door dealer','window supply','window wholesale',
    'window & door supply','millwork dealer','millwork supply',
    // Building materials (general)
    'building materials','builders supply','building supply','lumber yard','lumber dealer',
    'lumber company','pro dealer','building center','hardware supply','construction supply',
    'building products','material supply','supply house','supply co','supply company',
    'wholesale supply','abc supply','beacon roofing','beacon supply','gulfeagle',
    'srs distribution','bradco','allied building','hepler','famco',
    // Tool / fastener / equipment
    'scaffolding dealer','tool supply','fastener dealer','pump jack dealer','equipment supply',
    'tool dealer','scaffold supply','ladder supply','construction equipment','tool rental',
  ];
  if (DIST_KEYWORDS.some(k => all.includes(k))) {
    // Determine subcategory
    const sub = all.includes('roof') ? 'Roofing'
              : all.includes('deck') || all.includes('trex') || all.includes('azek') || all.includes('fiberon') || all.includes('timbertech') ? 'Decking'
              : all.includes('siding') ? 'Siding'
              : all.includes('window') || all.includes('door') ? 'Windows & Doors'
              : all.includes('scaffold') || all.includes('tool') || all.includes('fastener') ? 'Tools & Equipment'
              : 'Building Materials';
    return { layer: 2, type: 'Distributor', sub };
  }

  // ── Layer 3: Contractor ───────────────────────────────────────────────────
  const CONT_KEYWORDS = [
    // Roofing
    'roofing contractor','roofer','roofing company','roofing co','commercial roofing',
    'roof repair','roofing services','roofing systems','roof installation','metal roofing',
    'flat roofing','roofing solutions','roofing group','roofing specialist',
    // Decking / outdoor
    'deck builder','decking contractor','deck construction','custom decks','decks and patios',
    'outdoor living','composite decking contractor','deck & porch','deck and porch',
    'decks & porch','porch builder','deck & fence','decks & fence','deck & pergola',
    'pergola builder','pergola contractor','outdoor spaces','outdoor structures',
    'patio builder','patio contractor','patio company','screened porch','screen room',
    'sunroom builder',' decking','deck company','decks company','deck design',
    'timbertech contractor','trex contractor','fiberon contractor','azek contractor',
    'deck restoration','decks & exteriors','decks & more','porches and decks',
    'custom outdoor','backyard living','exterior contractor','exterior solutions',
    // Siding
    'siding contractor','siding company','siding installer','siding & windows',
    'cornice contractor','siding services','siding specialist','vinyl siding installer',
    'fiber cement installer','hardieplank installer','james hardie installer',
    // Windows / Doors
    'window installer','door installer','window contractor','window & door contractor',
    'window replacement','window company','entry door installer','door replacement',
    'window specialist','fenestration contractor',
    // General (catch-all)
    'general contractor','construction company','general contracting',
    'home builder','remodeling company','renovation company','home improvement company',
    'construction group','exterior remodeling','home renovation',
  ];
  if (CONT_KEYWORDS.some(k => all.includes(k))) {
    const sub = all.includes('roof') ? 'Roofing'
              : (all.includes('deck') || all.includes('patio') || all.includes('porch') || all.includes('pergola') || all.includes('outdoor') || all.includes('trex') || all.includes('timbertech') || all.includes('fiberon')) ? 'Decking & Outdoor'
              : all.includes('siding') || all.includes('hardie') || all.includes('cornice') ? 'Siding'
              : (all.includes('window') || all.includes('door')) ? 'Windows & Doors'
              : 'General';
    return { layer: 3, type: 'Contractor', sub };
  }

  // ── Google Place Types fallback ───────────────────────────────────────────
  const GTYPE_MAP = {
    'roofing_contractor':     { layer: 3, type: 'Contractor',  sub: 'Roofing' },
    'general_contractor':     { layer: 3, type: 'Contractor',  sub: 'General' },
    'home_improvement_store': { layer: 2, type: 'Distributor', sub: 'Building Materials' },
    'hardware_store':         { layer: 2, type: 'Distributor', sub: 'Building Materials' },
    'lumber_yard':            { layer: 2, type: 'Distributor', sub: 'Building Materials' },
    'contractor':             { layer: 3, type: 'Contractor',  sub: 'General' },
    'construction_company':   { layer: 3, type: 'Contractor',  sub: 'General' },
  };
  for (const gType of (rec.types || [])) {
    if (GTYPE_MAP[gType]) return GTYPE_MAP[gType];
  }

  // ── Growth Mode catch-all: accept anything that passed exclusions ──────────
  // A "Deck & Porch" company may have no exact keyword match but IS our target.
  return { layer: 3, type: 'Contractor', sub: 'General' };
}

// ── Deduplication index ──────────────────────────────────────────────────────
function scBuildIndex(rows) {
  const byAddr    = new Map();
  const byPlaceId = new Map();
  for (const r of rows) {
    const na = scNormAddr(r.address);
    const nn = scNormName(r.company);
    if (na.length > 8) byAddr.set(`${na}|${nn}`, r.company);
    if (r.google_place_id) byPlaceId.set(r.google_place_id, r.company);
  }
  return { byAddr, byPlaceId };
}

function scIsDup(rec, idx) {
  const na = scNormAddr(rec.address);
  const nn = scNormName(rec.company);
  // Exact address + exact name = hard duplicate
  const addrKey = `${na}|${nn}`;
  if (na.length > 8 && idx.byAddr.has(addrKey)) {
    return { dup: true, hard: true, reason: 'exact_address_name', matched: idx.byAddr.get(addrKey) };
  }
  // Place ID match + same name = hard duplicate
  if (rec.place_id && idx.byPlaceId.has(rec.place_id)) {
    const existingName = idx.byPlaceId.get(rec.place_id);
    if (scNormName(existingName) === nn) {
      return { dup: true, hard: true, reason: 'place_id_name', matched: existingName };
    }
    // Place ID match but different name = possible duplicate, still import
    return { dup: false, possible: true, reason: 'same_location', matched: existingName };
  }
  return { dup: false };
}

// ── Query expansion for territory ────────────────────────────────────────────
function scBuildQueries(category, subcategory, stateCode) {
  const cities  = (SC_STATE_CITIES[stateCode] || []).slice(0, 6);
  const stateFull = Object.keys(SC_STATE_NAMES).find(k => SC_STATE_NAMES[k] === stateCode) || stateCode;

  let base = [];

  if (category === 'decking') {
    base = [
      `decking contractor ${stateCode}`,
      `deck builder ${stateCode}`,
      `composite deck contractor ${stateCode}`,
      `TimberTech contractor ${stateCode}`,
      `Trex contractor ${stateCode}`,
      `Fiberon contractor ${stateCode}`,
      `outdoor living contractor ${stateCode}`,
      `patio porch contractor ${stateCode}`,
      `decking supply distributor ${stateCode}`,
      `building materials supplier decking ${stateCode}`,
    ];
  } else if (category === 'roofing') {
    base = [
      `roofing contractor ${stateCode}`,
      `commercial roofing contractor ${stateCode}`,
      `GAF certified roofing contractor ${stateCode}`,
      `Owens Corning roofing contractor ${stateCode}`,
      `roofing supply distributor ${stateCode}`,
      `roofing materials supplier ${stateCode}`,
      `roofing wholesale dealer ${stateCode}`,
    ];
  } else if (category === 'siding') {
    base = [
      `siding contractor ${stateCode}`,
      `James Hardie installer ${stateCode}`,
      `vinyl siding contractor ${stateCode}`,
      `siding supply distributor ${stateCode}`,
      `building supply siding ${stateCode}`,
    ];
  } else if (category === 'windows') {
    base = [
      `window door installer ${stateCode}`,
      `window replacement contractor ${stateCode}`,
      `window door distributor ${stateCode}`,
      `millwork supplier ${stateCode}`,
    ];
  } else {
    // Generic: use city names for broader coverage
    base = cities.slice(0, 4).flatMap(city => [
      `building contractor ${city} ${stateCode}`,
      `exterior contractor ${city} ${stateCode}`,
    ]);
  }

  // Add city-specific variants for top 3 metros
  const cityQueries = cities.slice(0, 3).map(city =>
    `${category === 'decking' ? 'deck builder' : category + ' contractor'} ${city} ${stateCode}`
  );

  return [...base, ...cityQueries].slice(0, 12);
}

// ── Main bulk-ingest route (v3) ──────────────────────────────────────────────
router.post('/bulk-ingest', async (req, res) => {
  const uid = req.session.user.id;
  const {
    query, search_state, territory, ingest_mode, max_records, scope,
    category, subcategory
  } = req.body;

  const ingestMode = ingest_mode || 'growth';
  const dedupScope = scope || 'team';
  const maxRecs    = Math.min(parseInt(max_records) || 40, 100);
  const apiKey     = process.env.GOOGLE_PLACES_API_KEY;

  // ── Step 1: Resolve territory state ───────────────────────────────────────
  const explicitState = (search_state || '').trim().toUpperCase();
  const stateCode     = scResolveState(explicitState, query || '', territory || '');

  if (!stateCode) {
    return res.status(400).json({
      error: 'Territory required — please select a state from the dropdown.',
      ok: false
    });
  }

  const cat = (category || '').toLowerCase() || 'general';
  console.log(`[BulkIngest v3] state=${stateCode} cat=${cat} mode=${ingestMode} scope=${dedupScope} max=${maxRecs}`);

  try {
    // ── Step 2: Build search queries ─────────────────────────────────────────
    let queries;
    if (query && query.trim()) {
      // User provided explicit query — expand it with state-city variants
      const cities = (SC_STATE_CITIES[stateCode] || []).slice(0, 4);
      queries = [
        `${query.trim()} ${stateCode}`,
        ...cities.slice(0, 3).map(c => `${query.trim()} ${c} ${stateCode}`)
      ];
    } else {
      queries = scBuildQueries(cat, subcategory || '', stateCode);
    }

    console.log(`[BulkIngest v3] Queries (${queries.length}):`, queries);

    // ── Step 3: Fetch from Google Places ─────────────────────────────────────
    const seenPlaceIds = new Set();
    const rawResults   = [];

    for (const q of queries) {
      if (rawResults.length >= maxRecs * 3) break;
      const batch = await scPlacesFetch(q, apiKey);
      for (const r of batch) {
        if (r.place_id && seenPlaceIds.has(r.place_id)) continue;
        if (r.place_id) seenPlaceIds.add(r.place_id);
        rawResults.push(r);
      }
    }

    console.log(`[BulkIngest v3] Raw fetched: ${rawResults.length}`);

    // ── Step 4: Hard geo filter — state must match ────────────────────────────
    const geoValid      = [];
    const geoRejected   = [];
    for (const rec of rawResults) {
      const recState = parseStateFromAddress(rec.address);
      if (!recState || recState !== stateCode) {
        geoRejected.push({ company: rec.company, address: rec.address, state: recState });
      } else {
        rec.state = recState;
        geoValid.push(rec);
      }
    }
    console.log(`[BulkIngest v3] Geo: ${geoValid.length} valid, ${geoRejected.length} rejected`);

    // ── Step 5: Classify (before dedup) ──────────────────────────────────────
    const classified    = [];
    const filteredOut   = [];
    for (const rec of geoValid) {
      const cls = scClassify(rec);
      if (!cls) {
        filteredOut.push({ company: rec.company, address: rec.address, types: (rec.types||[]).join(',') });
        continue;
      }
      rec.supply_layer     = cls.layer;
      rec.company_type     = cls.type;
      rec.subcategory      = cls.sub;
      rec.territory        = stateCode;
      classified.push(rec);
    }
    console.log(`[BulkIngest v3] Classified: ${classified.length}, Filtered: ${filteredOut.length}`);

    // ── Step 6: Field quality score ───────────────────────────────────────────
    const minScore       = ingestMode === 'growth' ? 40 : 60;
    const scored         = [];
    const lowQuality     = [];
    for (const rec of classified) {
      const score = scFieldScore(rec);
      rec.field_quality_score = score;
      if (score >= minScore) {
        scored.push(rec);
      } else {
        lowQuality.push({ company: rec.company, address: rec.address, score });
      }
    }
    scored.sort((a, b) => b.field_quality_score - a.field_quality_score);
    console.log(`[BulkIngest v3] Scored ≥${minScore}: ${scored.length}, Low quality: ${lowQuality.length}`);

    // ── Step 7: Load dedup index from CRM ────────────────────────────────────
    let existingRows = { rows: [] };
    if (dedupScope !== 'off') {
      const dbQ = dedupScope === 'mine'
        ? `SELECT google_place_id, company, address, phone FROM prospects WHERE user_id = $1`
        : `SELECT google_place_id, company, address, phone FROM prospects`;
      const dbP = dedupScope === 'mine' ? [uid] : [];
      existingRows = await pool.query(dbQ, dbP);
      console.log(`[BulkIngest v3] CRM index: ${existingRows.rows.length} records`);
    }

    const dupIndex      = scBuildIndex(existingRows.rows);
    const batchAddrSet  = new Set();
    const toInsert      = [];
    const possibleDups  = [];
    const hardDups      = [];
    const hardDupSample = [];

    // ── Step 8: Dedup + insert ────────────────────────────────────────────────
    for (const rec of scored) {
      if (toInsert.length + possibleDups.length >= maxRecs) break;

      // Batch-level dedup: same address twice in this run
      const batchKey = scNormAddr(rec.address);
      if (batchKey.length > 8 && batchAddrSet.has(batchKey)) {
        hardDups.push(rec);
        if (hardDupSample.length < 10) hardDupSample.push({ company: rec.company, address: rec.address, reason: 'batch_duplicate' });
        continue;
      }
      if (batchKey.length > 8) batchAddrSet.add(batchKey);

      // CRM dedup check
      if (dedupScope !== 'off') {
        const dupCheck = scIsDup(rec, dupIndex);
        if (dupCheck.dup && dupCheck.hard) {
          hardDups.push(rec);
          if (hardDupSample.length < 10) hardDupSample.push({
            company: rec.company, address: rec.address,
            reason: dupCheck.reason, matched: dupCheck.matched
          });
          // In strict mode: skip. In growth mode: still skip hard dups (true duplicates)
          continue;
        }
        if (dupCheck.possible) {
          rec.data_status = 'Review';
          rec.dup_note    = `Possible dup: same location as "${dupCheck.matched}"`;
          possibleDups.push(rec);
          // Still insert in growth mode
        }
      }

      toInsert.push(rec);
    }

    console.log(`[BulkIngest v3] To insert: ${toInsert.length}, Possible dups: ${possibleDups.length}, Hard dups: ${hardDups.length}`);

    // ── Step 9: Write to CRM (prospects table) ────────────────────────────────
    let imported = 0;
    const insertErrors = [];

    // Combine toInsert + possibleDups (both go in, possibleDups flagged)
    const allToWrite = [...toInsert, ...possibleDups];

    for (const rec of allToWrite) {
      // Map supply chain layer to company_type field
      const companyTypeLabel = rec.supply_layer === 2
        ? `Distributor — ${rec.subcategory}`
        : `Contractor — ${rec.subcategory}`;

      const cityParts = (rec.address || '').split(',');
      const city      = cityParts.length >= 2 ? cityParts[cityParts.length - 3]?.trim() || '' : '';

      const insertSQL = `
        INSERT INTO prospects
          (user_id, company, address, city, state, phone, website,
           google_place_id, category, data_status, source,
           territory, created_at, last_activity_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW(),NOW())
        ON CONFLICT DO NOTHING
        RETURNING id`;

      const vals = [
        uid,
        rec.company,
        rec.address || '',
        city,
        rec.state   || stateCode,
        rec.phone   || '',
        rec.website || '',
        rec.place_id || null,
        companyTypeLabel,
        rec.data_status || 'Unvetted',
        `Bulk Import v3 — ${stateCode}`,
        stateCode,
      ];

      try {
        const result = await pool.query(insertSQL, vals);
        if (result.rows.length > 0) imported++;
      } catch (e) {
        insertErrors.push({ company: rec.company, error: e.message });
        console.error('[BulkIngest v3] Insert error:', e.message);
      }
    }

    console.log(`[BulkIngest v3] Imported: ${imported}`);

    // ── Step 10: Build breakdown stats ────────────────────────────────────────
    const breakdown = {
      distributors:   allToWrite.filter(r => r.supply_layer === 2).length,
      contractors:    allToWrite.filter(r => r.supply_layer === 3).length,
      possible_dups:  possibleDups.length,
      roofing:        allToWrite.filter(r => r.subcategory?.includes('Roof')).length,
      decking:        allToWrite.filter(r => r.subcategory?.includes('Deck') || r.subcategory?.includes('Outdoor')).length,
      siding:         allToWrite.filter(r => r.subcategory?.includes('Siding')).length,
      windows:        allToWrite.filter(r => r.subcategory?.includes('Window')).length,
      general:        allToWrite.filter(r => r.subcategory === 'General').length,
    };

    // ── Step 11: Sample records for UI display ────────────────────────────────
    const records = allToWrite.slice(0, 25).map(rec => ({
      company:             rec.company,
      address:             rec.address,
      phone:               rec.phone   || '',
      website:             rec.website || '',
      category:            rec.company_type === 'Distributor'
                             ? `Distributor — ${rec.subcategory}`
                             : `Contractor — ${rec.subcategory}`,
      supply_layer:        rec.supply_layer,
      field_quality_score: rec.field_quality_score,
      data_status:         rec.data_status || 'Unvetted',
      dup_note:            rec.dup_note || '',
    }));

    return res.json({
      ok:                  true,
      imported,
      state_used:          stateCode,
      ingest_mode:         ingestMode,
      raw_candidates:      rawResults.length,
      geo_valid:           geoValid.length,
      geo_rejected:        geoRejected.length,
      geo_rejected_sample: geoRejected.slice(0, 5),
      classified_count:    classified.length,
      filtered_count:      filteredOut.length,
      filtered_sample:     filteredOut.slice(0, 5),
      low_quality_count:   lowQuality.length,
      low_quality_sample:  lowQuality.slice(0, 5),
      exact_dups:          hardDups.length,
      exact_dup_sample:    hardDupSample,
      possible_dups:       possibleDups.length,
      db_index_size:       existingRows.rows.length,
      queries_used:        queries,
      breakdown,
      records,
      insert_errors:       insertErrors.slice(0, 3),
      message: imported > 0
        ? `${imported} new CRM records added in ${stateCode} — ${breakdown.distributors} distributors, ${breakdown.contractors} contractors`
        : insertErrors.length > 0
          ? `DB error: ${insertErrors[0]?.error} — check server logs`
          : hardDups.length > 0
          ? `${hardDups.length} exact duplicates found at same address — already in CRM. Try "No Dedup" scope to force import.`
          : `0 imported — ${filteredOut.length} filtered · ${lowQuality.length} low quality · ${geoRejected.length} wrong state`,
    });

  } catch (err) {
    console.error('[BulkIngest v3] Fatal:', err.message, err.stack);
    return res.status(500).json({ ok: false, error: err.message });
  }
});


module.exports = router;
