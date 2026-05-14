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
  'carpet cleaning','junk removal','moving company','storage unit'
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
const ROOFING_MANUFACTURER_SIGNALS = [
  'carlisle', 'johns manville', 'sarnafil', 'sika', 'tremco', 'certainteed',
  'iko', 'gaf', 'owens corning', 'firestone', 'versico', 'polyglass',
  'henry', 'soprema', 'mulehide', 'mule-hide', 'atlas roofing',
  'bur', 'tpo', 'epdm', 'mod bit', 'modified bitumen'
];

const DECKING_MANUFACTURER_SIGNALS = [
  'trex', 'timbertech', 'azek', 'fiberon', 'deckorators', 'deckorator',
  'moistureshield', 'moisture shield', 'fortress', 'wolf decking',
  'ipe', 'composite deck', 'composite decking', 'pvc deck', 'pvc decking'
];

const ROOFING_DISTRIBUTOR_SIGNALS = [
  'abc supply', 'beacon', 'gulf eagle', 'bradco', 'allied', 'gulfeagle',
  'beacon roofing', 'western states', 'famco', 'hepler',
  'roofing supply', 'roofing products supply', 'roofing wholesale'
];

const DECKING_DISTRIBUTOR_SIGNALS = [
  '84 lumber', 'us lbm', 'lbm', 'bmc', 'pro build', 'probuild',
  'builders firstsource', 'universal forest', 'ufp', 'weyerhaeuser',
  'hancock lumber', 'carter lumber', 'mc supply', 'mr. lumber',
  'outdoor living supply', 'deck supply', 'decking supply', 'decking wholesale',
  'composite deck supply', 'deck products dealer'
];

const DISTRIBUTOR_GENERIC_SIGNALS = [
  'supply co', 'supply company', 'supply house', 'distribut', 'wholesale',
  'dealer', 'distributor', 'materials inc', 'materials co',
  'building products', 'building supply', 'building materials',
  'millwork', 'millworks', 'lumber yard', 'lumber co', 'lumber company',
  'hdw', 'hardware supply', 'products inc', 'products co', 'products llc'
];

const CONTRACTOR_GENERIC_SIGNALS = [
  'contracting', 'contractor', 'construction', 'builders', 'install',
  'installer', 'renovation', 'restoration', 'remodel', 'remodeling',
  'roofing co', 'roofing inc', 'roofing llc', 'deck co', 'deck inc',
  'deck llc', 'exterior solutions', 'exteriors', 'home improvement',
  'services llc', 'services inc', 'company inc', 'company llc'
];

function hasAny(name, signals) {
  return signals.some(sig => name.includes(sig));
}

function classifyRecord(companyName, baseCategory) {
  const n = (companyName || '').toLowerCase();
  const base = (baseCategory || '').toLowerCase();

  // ── Step 1: Determine base ecosystem from baseCategory ───────────────
  const isRoofingContext = base.includes('roof');
  const isDeckingContext = base.includes('deck');
  const isSidingContext  = base.includes('siding');
  const isWindowContext  = base.includes('window') || base.includes('door');

  // ── Step 2: Strong manufacturer brand signals override everything ─────
  // These are definitive — if the company name contains a known brand, trust it
  if (hasAny(n, ROOFING_MANUFACTURER_SIGNALS)) {
    // Even in a decking query, a company named "Carlisle Supply" is Roofing
    const isDist = hasAny(n, DISTRIBUTOR_GENERIC_SIGNALS) || hasAny(n, ROOFING_DISTRIBUTOR_SIGNALS);
    return isDist ? 'Roofing Distributor' : 'Roofing Contractor';
  }
  if (hasAny(n, DECKING_MANUFACTURER_SIGNALS)) {
    const isDist = hasAny(n, DISTRIBUTOR_GENERIC_SIGNALS) || hasAny(n, DECKING_DISTRIBUTOR_SIGNALS);
    return isDist ? 'Decking Distributor' : 'Decking Contractor';
  }

  // ── Step 3: Distributor network brand signals ─────────────────────────
  if (hasAny(n, ROOFING_DISTRIBUTOR_SIGNALS)) return 'Roofing Distributor';
  if (hasAny(n, DECKING_DISTRIBUTOR_SIGNALS)) return 'Decking Distributor';

  // ── Step 4: Context-specific classification using name signals ─────────
  if (isRoofingContext) {
    if (hasAny(n, DISTRIBUTOR_GENERIC_SIGNALS)) return 'Roofing Distributor';
    if (hasAny(n, CONTRACTOR_GENERIC_SIGNALS))  return 'Roofing Contractor';
    // Roofing-specific name patterns
    if (n.includes('roof')) {
      if (n.includes('supply') || n.includes('product') || n.includes('material') || n.includes('wholesale')) return 'Roofing Distributor';
      return 'Roofing Contractor'; // "ABC Roofing" without supply signals = contractor
    }
    return baseCategory;
  }

  if (isDeckingContext) {
    if (hasAny(n, DISTRIBUTOR_GENERIC_SIGNALS)) return 'Decking Distributor';
    if (hasAny(n, CONTRACTOR_GENERIC_SIGNALS))  return 'Decking Contractor';
    if (n.includes('deck') || n.includes('outdoor living') || n.includes('patio')) {
      if (n.includes('supply') || n.includes('product') || n.includes('material') || n.includes('wholesale')) return 'Decking Distributor';
      if (n.includes('build') || n.includes('install') || n.includes('design')) return 'Decking Contractor';
    }
    return baseCategory;
  }

  if (isSidingContext) {
    if (hasAny(n, DISTRIBUTOR_GENERIC_SIGNALS)) return 'Siding Distributor';
    if (hasAny(n, CONTRACTOR_GENERIC_SIGNALS))  return 'Siding Contractor';
    return baseCategory;
  }

  if (isWindowContext) {
    if (n.includes('supply') || n.includes('distribut') || n.includes('wholesale') ||
        n.includes('products') || n.includes('materials') || n.includes('dealer')) {
      return 'Window & Door Distributor';
    }
    if (n.includes('install') || n.includes('contractor') || n.includes('contracting') ||
        n.includes('services') || n.includes('builders') || n.includes('construction')) {
      return 'Window & Door Installer';
    }
    return baseCategory;
  }

  // ── Step 5: Generic fallback — use base category as-is ───────────────
  return baseCategory;
}

const BULK_DISTRIBUTOR_CATS = new Set([
  'Roofing Distributor', 'Decking Distributor', 'Siding Distributor', 'Window & Door Distributor'
]);
function resolveBulkCompanyType(cat) {
  if (BULK_DISTRIBUTOR_CATS.has(cat)) return 'Distributor';
  const lower = (cat || '').toLowerCase();
  if (lower.includes('distributor') || lower.includes('supply') || lower.includes('dealer') ||
      lower.includes('wholesale') || lower.includes('lumber')) return 'Distributor';
  return 'Contractor';
}

router.post('/bulk-ingest', async (req, res) => {
  const uid = req.session.user.id;
  const { query, territory, max_records } = req.body;

  if (!query || !query.trim()) {
    return res.status(400).json({ error: 'Query is required (e.g. "Trex decking dealers Atlanta")' });
  }

  const maxRecs = Math.min(parseInt(max_records) || 50, 100);
  const { category, territory: detectedTerritory, mfrAssoc, querySet } = expandBulkQuery(query);
  const loc = territory || detectedTerritory || 'Atlanta';
  const cities = getCities(loc);

  const isWindowDoor = category.toLowerCase().includes('window') || category.toLowerCase().includes('door');

  console.log('[BulkIngest] Query:', query, '| Category:', category, '| Territory:', loc, '| Max:', maxRecs);

  try {
    // ── Phase 1: Collect raw results ──────────────────────────────
    const rawMap = new Map(); // place_id → raw place data

    const jobs = querySet.map((q, i) => ({
      query: q + ' ' + cities[i % cities.length],
      label: q
    }));

    const BATCH = 4;
    for (let i = 0; i < jobs.length && rawMap.size < maxRecs * 2; i += BATCH) {
      const batch = jobs.slice(i, i + BATCH);
      const results = await Promise.all(batch.map(async ({ query: bq, label }) => {
        try {
          const data = await httpsPost(
            'places.googleapis.com', '/v1/places:searchText',
            {
              'Content-Type': 'application/json',
              'X-Goog-Api-Key': PLACES_KEY,
              'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus,places.types,places.regularOpeningHours'
            },
            { textQuery: bq, maxResultCount: 20, rankPreference: 'RELEVANCE' }
          );
          return { label, places: data.places || [] };
        } catch(e) {
          console.error('[BulkIngest] Query error:', label, e.message);
          return { label, places: [] };
        }
      }));

      for (const { label, places } of results) {
        for (const place of places) {
          if (rawMap.has(place.id)) continue;
          rawMap.set(place.id, { place, label });
        }
      }
    }

    console.log('[BulkIngest] Raw results collected:', rawMap.size);

    // ── Phase 2: Clean + score ────────────────────────────────────
    const cleaned = [];
    for (const [placeId, { place, label }] of rawMap) {
      if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
      if (place.businessStatus === 'CLOSED_TEMPORARILY') continue;
      if (hasBadPlaceType(place.types || [])) continue;

      const name = place.displayName?.text || '';
      const addr = place.formattedAddress || '';

      if (isBadLead(name, addr, label)) continue;
      if (!place.nationalPhoneNumber && !place.websiteUri) continue; // need at least one contact signal

      if ((category || '').toLowerCase().includes('alum') && alumPoleBlocked(name)) continue;
      if (isWindowDoor && garageDoorBlocked(name)) continue;

      const reach = getReachabilityScore(place, name);
      if (reach.tier === 'LOW') continue; // keep MEDIUM + HIGH only

      const addrParts = addr.split(',');

      const recCategory = classifyRecord(name, category);
      cleaned.push({
        place_id: placeId,
        company: name,
        category: recCategory,
        city: addrParts[1]?.trim() || '',
        state: addrParts[2]?.trim().split(' ')[0] || '',
        phone: place.nationalPhoneNumber || null,
        email: null,
        website: place.websiteUri || null,
        address: addr,
        source: 'Bulk Ingestion',
        data_status: 'Unvetted',
        manufacturer_assoc: mfrAssoc || null,
        reachabilityScore: reach.score,
        reachabilityTier: reach.tier,
      });
    }

    // Sort by reachability score descending, take top maxRecs
    cleaned.sort((a, b) => b.reachabilityScore - a.reachabilityScore);
    const topRecords = cleaned.slice(0, maxRecs);

    console.log('[BulkIngest] Cleaned records:', topRecords.length);

    // ── Phase 3: Confidence-scored deduplication ────────────────────────
    // A record is only skipped if it scores ≥ DUP_THRESHOLD.
    // This prevents false positives from matching only on company name
    // when roofing and decking companies can share generic names.
    const DUP_THRESHOLD = 80; // points needed to flag as duplicate

    function normName(n) { return (n||'').toLowerCase().replace(/[^a-z0-9]/g,''); }
    function normPhone(p) { return (p||'').replace(/[^0-9]/g,''); }
    function normAddr(a)  { return (a||'').toLowerCase().replace(/[^a-z0-9]/g,''); }

    // Pull richer existing data for scoring
    const existingRows = await pool.query(
      `SELECT google_place_id, company, city, phone, address, website, category
       FROM prospects WHERE user_id = $1`,
      [uid]
    );

    // Fast-path Sets for definitive single-field matches
    const existingByPlaceId  = new Set();
    const existingByPhone    = new Set();

    // Lookup structure for scored matching (name+city or address)
    const existingRecords = [];

    for (const r of existingRows.rows) {
      if (r.google_place_id) existingByPlaceId.add(r.google_place_id);
      const np = normPhone(r.phone);
      if (np && np.length >= 10) existingByPhone.add(np);
      existingRecords.push({
        nameNorm:  normName(r.company),
        cityNorm:  (r.city||'').toLowerCase().trim(),
        addrNorm:  normAddr(r.address),
        phoneNorm: np,
        category:  (r.category||'').toLowerCase(),
      });
    }

    // Score a candidate record against all existing records
    function getDupScore(rec) {
      const recName  = normName(rec.company);
      const recCity  = (rec.city||'').toLowerCase().trim();
      const recAddr  = normAddr(rec.address);
      const recPhone = normPhone(rec.phone||'');
      const recCat   = (rec.category||'').toLowerCase();

      let bestScore = 0;
      for (const ex of existingRecords) {
        let score = 0;

        // Name match (up to 40 pts) — partial credit for close-but-not-identical
        if (recName === ex.nameNorm && recName.length > 3) {
          score += 40;
        } else if (recName.length > 5 && ex.nameNorm.length > 5 &&
                   (recName.includes(ex.nameNorm) || ex.nameNorm.includes(recName))) {
          score += 20; // substring match — partial
        }

        // City match (up to 20 pts) — only matters WITH name match
        if (score > 0 && recCity && ex.cityNorm && recCity === ex.cityNorm) {
          score += 20;
        }

        // Address match (up to 30 pts) — strong signal
        if (recAddr.length > 8 && ex.addrNorm.length > 8 && recAddr === ex.addrNorm) {
          score += 30;
        }

        // Phone match (up to 40 pts) — very strong signal
        if (recPhone && recPhone.length >= 10 && ex.phoneNorm === recPhone) {
          score += 40;
        }

        // Cross-category PENALTY — same name in different ecosystem = NOT a dup
        // e.g. "Atlanta Roofing Supply" (Roofing) vs "Atlanta Roofing Supply" (Decking) should NOT merge
        if (score >= 40 && recCat && ex.category) {
          const recIsRoof  = recCat.includes('roof');
          const recIsDeck  = recCat.includes('deck');
          const exIsRoof   = ex.category.includes('roof');
          const exIsDeck   = ex.category.includes('deck');
          const crossEco   = (recIsRoof && exIsDeck) || (recIsDeck && exIsRoof);
          if (crossEco) score = Math.max(0, score - 60); // heavy penalty
        }

        if (score > bestScore) bestScore = score;
        if (bestScore >= DUP_THRESHOLD) break; // early exit once confirmed dup
      }
      return bestScore;
    }

    const toInsert = [];
    const skippedDups = [];
    const sessionSeenByNameCity = new Set();

    for (const rec of topRecords) {
      // ── Definitive single-field matches (always skip) ──────────────
      if (existingByPlaceId.has(rec.place_id)) {
        skippedDups.push({ reason: 'place_id', company: rec.company });
        continue;
      }

      const recPhone = normPhone(rec.phone||'');
      if (recPhone && recPhone.length >= 10 && existingByPhone.has(recPhone)) {
        skippedDups.push({ reason: 'phone', company: rec.company });
        continue;
      }

      // ── Confidence-scored multi-field match ───────────────────────
      const dupScore = getDupScore(rec);
      if (dupScore >= DUP_THRESHOLD) {
        skippedDups.push({ reason: 'scored_dup', score: dupScore, company: rec.company });
        continue;
      }

      // ── Within-batch dedup (prevent same company twice in one run) ──
      const nameCity = normName(rec.company) + '|' + (rec.city||'').toLowerCase();
      if (sessionSeenByNameCity.has(nameCity)) continue;
      sessionSeenByNameCity.add(nameCity);

      toInsert.push(rec);
    }

    console.log('[BulkIngest] After dedup:', toInsert.length, 'new records to insert',
                '| skipped:', skippedDups.length, '(', skippedDups.filter(d=>d.reason==='scored_dup').length, 'scored,',
                skippedDups.filter(d=>d.reason==='place_id').length, 'placeId,',
                skippedDups.filter(d=>d.reason==='phone').length, 'phone )');

    if (toInsert.length === 0) {
      return res.json({
        ok: true,
        imported: 0,
        skipped_duplicates: topRecords.length,
        category,
        territory: loc,
        message: 'All results already exist in your CRM.'
      });
    }

    // ── Phase 4: Bulk insert into CRM ────────────────────────────
    // company_type resolved per-record using classifyRecord output
    let imported = 0;
    for (const rec of toInsert) {
      // Per-record company_type resolution (not shared top-level)
      const rec_company_type = resolveBulkCompanyType(rec.category);
      try {
        await pool.query(
          `INSERT INTO prospects
             (user_id, company, category, company_type, city, state, phone, email,
              contact, website, products, status, priority, source, google_place_id,
              address, data_status, manufacturer_assoc, last_activity_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NOW())
           ON CONFLICT DO NOTHING`,
          [
            uid, rec.company, rec.category, rec_company_type,
            rec.city, rec.state || 'GA',
            rec.phone || null, null,
            null, rec.website || null,
            null, 'New', rec.reachabilityTier === 'HIGH' ? 'High' : 'Medium',
            'Bulk Ingestion', rec.place_id,
            rec.address, 'Unvetted', rec.manufacturer_assoc
          ]
        );
        imported++;
      } catch(e) {
        console.error('[BulkIngest] Insert error:', rec.company, e.message);
      }
    }

    console.log('[BulkIngest] Inserted:', imported, '| Duplicates skipped:', topRecords.length - toInsert.length);

    // Derive display company_type from the category for the response
    const display_company_type = resolveBulkCompanyType(category);
    res.json({
      ok: true,
      imported,
      skipped_duplicates: skippedDups.length,
      total_found: rawMap.size,
      category,
      territory: loc,
      manufacturer: mfrAssoc || null,
      company_type: display_company_type,
      message: `${imported} records imported into your CRM (${display_company_type} / ${category}).`
    });

  } catch(e) {
    console.error('[BulkIngest] Fatal error:', e.message);
    res.status(500).json({ error: 'Bulk ingestion failed: ' + e.message });
  }
});


module.exports = router;
