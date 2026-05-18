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
const SOURCE_TIERS = {
  TIER1: 1, // Manufacturer "Where to Buy" / official dealer locators
  TIER2: 2, // Industry association lists, verified distributor databases
  TIER3: 3, // Google Places (filtered)
  TIER4: 4  // General web / low-confidence fallback
};

// ── Classification signals ─────────────────────────────────────────────────
const CONTRACTOR_SIGNALS = [
  'roofing contractor','roofing company','roofing co','roofer','roofing services',
  'roofing & sheet','commercial roofing','residential roofing','roof repair',
  'decking contractor','deck builder','deck builder','custom decks','deck construction',
  'decks and patios','outdoor living','composite decking',
  'siding contractor','siding company','siding & windows','siding installers',
  'window installer','window company','window & door','door installer',
  'construction company','general contractor','remodeling','home improvement',
  'exteriors','exterior contractor','cladding'
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
const RETAIL_EXCLUSIONS = [
  'home depot','lowe\'s','lowes','menards','ace hardware','true value',
  'walmart','costco','amazon','wayfair','big box'
];
const MANUFACTURER_EXCLUSIONS = [
  'manufacturing','manufacturer','mfg','fabricat','factory','industrial',
  'raw material','steel mill','chemical plant'
];
const RESIDENTIAL_EXCLUSIONS = [
  'residential only','homeowner','diy','do-it-yourself','consumer grade'
];

// ── Query expansion engine ──────────────────────────────────────────────────
function expandQuery(rawQuery, territory) {
  const q = rawQuery.toLowerCase().trim();
  const loc = territory || '';
  const expansions = [];

  // Always include original
  expansions.push(rawQuery);

  // Detect brand intent
  const brands = {
    trex: ['Trex authorized dealers', 'Trex decking suppliers', 'lumber yards Trex', 'decking distributors Trex', 'composite decking dealers'],
    shurtape: ['ShurTape flashing tape dealers', 'ShurTape distributors', 'building tape suppliers', 'construction tape dealers'],
    'alum-a-pole': ['Alum-A-Pole scaffolding dealers', 'pump jack scaffolding suppliers', 'scaffolding equipment dealers', 'tool supply dealers scaffolding'],
    alumapole: ['Alum-A-Pole scaffolding dealers', 'pump jack scaffolding suppliers', 'scaffolding tool supply'],
    boss: ['Soudal BOSS sealant distributors', 'construction sealant dealers', 'roofing sealant distributors'],
    soudal: ['Soudal sealant distributors', 'construction adhesive dealers', 'roofing supply sealants'],
    fortress: ['Fortress steel framing dealers', 'steel railing dealers', 'deck framing suppliers']
  };

  for (const [brand, variants] of Object.entries(brands)) {
    if (q.includes(brand)) {
      variants.forEach(v => expansions.push(loc ? `${v} ${loc}` : v));
      break;
    }
  }

  // Detect category intent and add variations
  const catExpansions = {
    'roof': [
      `roofing distributors ${loc}`, `roofing supply ${loc}`,
      `commercial roofing contractors ${loc}`, `roofing materials dealer ${loc}`
    ],
    'deck': [
      `decking distributors ${loc}`, `deck supply ${loc}`,
      `composite decking dealers ${loc}`, `lumber yard deck supplies ${loc}`
    ],
    'sid': [
      `siding distributors ${loc}`, `siding supply ${loc}`,
      `siding contractors ${loc}`, `building materials siding ${loc}`
    ],
    'window': [
      `window distributors ${loc}`, `window dealers ${loc}`,
      `window and door installers ${loc}`, `door distributors ${loc}`
    ],
    'lumber': [
      `lumber yards ${loc}`, `building supply dealers ${loc}`,
      `pro dealer lumber ${loc}`, `builders supply ${loc}`
    ]
  };

  for (const [key, variants] of Object.entries(catExpansions)) {
    if (q.includes(key)) {
      variants.forEach(v => { if (!expansions.includes(v)) expansions.push(v); });
    }
  }

  // De-dupe and cap at 6 queries
  return [...new Set(expansions)].slice(0, 6);
}

// ── Pre-ingestion classifier ────────────────────────────────────────────────
function classifyAndFilter(record) {
  const name    = (record.company || '').toLowerCase();
  const cats    = (record.category || '').toLowerCase();
  const types   = (record.types || []).map(t => t.toLowerCase());
  const website = (record.website || '').toLowerCase();
  const allText = `${name} ${cats} ${types.join(' ')} ${website}`;

  // Hard exclusions first
  if (RETAIL_EXCLUSIONS.some(ex  => allText.includes(ex)))       return null;
  if (MANUFACTURER_EXCLUSIONS.some(ex => allText.includes(ex)))  return null;
  if (RESIDENTIAL_EXCLUSIONS.some(ex => allText.includes(ex)))   return null;
  // Garage door exclusion (per standing rule)
  if (/garage|overhead door/i.test(name))                        return null;
  // Painting contractor exclusion (Alum-A-Pole rule)
  if (/paint(ing)?\s*(contractor|company|co\b|services)/i.test(name)) return null;

  // Determine company_type and category label
  const isDistributor = DISTRIBUTOR_SIGNALS.some(s => allText.includes(s));
  const isContractor  = CONTRACTOR_SIGNALS.some(s => allText.includes(s));

  let company_type = 'Unknown';
  let category_label = record.category || 'Unknown';

  if (isDistributor && !isContractor) {
    company_type = 'Distributor';
    // Refine distributor category
    if (/roof/i.test(allText))          category_label = 'Roofing Distributor';
    else if (/deck|composite/i.test(allText)) category_label = 'Decking Distributor';
    else if (/sid/i.test(allText))      category_label = 'Siding Distributor';
    else if (/window|door/i.test(allText)) category_label = 'Window & Door Distributor';
    else                                category_label = 'Building Materials Distributor';
  } else if (isContractor) {
    company_type = 'Contractor';
    if (/roof/i.test(allText))          category_label = 'Roofing Contractor';
    else if (/deck/i.test(allText))     category_label = 'Decking Contractor';
    else if (/sid/i.test(allText))      category_label = 'Siding Contractor';
    else if (/window|door/i.test(allText)) category_label = 'Window & Door Installer';
    else                                category_label = 'General Contractor';
  } else if (isDistributor) {
    company_type = 'Distributor';
    category_label = record.category || 'Building Materials Distributor';
  } else {
    // Ambiguous — keep with lower confidence
    company_type = 'Unknown';
  }

  return { company_type, category_label };
}

// ── Confidence scoring engine ───────────────────────────────────────────────
function scoreConfidence(record, sourceTier) {
  let score = 0;

  // Source tier bonus (max 30)
  const tierBonus = { 1: 30, 2: 22, 3: 15, 4: 5 };
  score += tierBonus[sourceTier] || 10;

  // Has phone (20 pts)
  if (record.phone && record.phone.replace(/\D/g,'').length >= 10) score += 20;

  // Has website (15 pts)
  if (record.website && record.website.length > 5) score += 15;

  // Has full address (15 pts)
  if (record.address && record.address.length > 10) score += 15;

  // Has Google Place ID (10 pts — verified Google listing)
  if (record.place_id) score += 10;

  // Has reviews/rating (up to 10 pts)
  if (record.rating >= 4.0) score += 10;
  else if (record.rating >= 3.0) score += 5;

  // Name quality (not too generic, not too short)
  const name = record.company || '';
  if (name.length >= 5 && name.split(' ').length >= 2) score += 5;

  // Penalty: no contact info at all
  if (!record.phone && !record.website && !record.email) score -= 15;

  // Penalty: very generic name
  if (/^(the|a|an)\s/i.test(name) && name.length < 15) score -= 10;

  return Math.max(0, Math.min(100, score));
}

// ── Deduplication (team-wide, multi-signal) ─────────────────────────────────
function normName(n)  { return (n||'').toLowerCase().replace(/[^a-z0-9]/g,''); }
function normPhone(p) { return (p||'').replace(/[^0-9]/g,''); }
function normAddr(a)  { return (a||'').toLowerCase().replace(/[^a-z0-9\s]/g,'').trim(); }
function normDomain(w) {
  return (w||'').toLowerCase()
    .replace(/^https?:\/\//,'').replace(/^www\./,'')
    .split('/')[0].split('?')[0];
}

function buildExistingIndex(rows) {
  const byPlaceId  = new Set();
  const byPhone    = new Set();
  const byDomain   = new Set();
  const records    = [];

  for (const r of rows) {
    if (r.google_place_id) byPlaceId.add(r.google_place_id);
    const np = normPhone(r.phone);
    if (np.length >= 10) byPhone.add(np);
    const nd = normDomain(r.website);
    if (nd.length > 4 && !nd.includes('google')) byDomain.add(nd);
    records.push({
      nameNorm:   normName(r.company),
      addrNorm:   normAddr(r.address),
      phoneNorm:  np,
      domainNorm: nd
    });
  }
  return { byPlaceId, byPhone, byDomain, records };
}

function isDuplicate(rec, index) {
  // Hard matches — definitive
  if (rec.place_id && index.byPlaceId.has(rec.place_id)) return { dup: true, reason: 'place_id' };

  const np = normPhone(rec.phone);
  if (np.length >= 10 && index.byPhone.has(np)) return { dup: true, reason: 'phone' };

  const nd = normDomain(rec.website);
  if (nd.length > 4 && !nd.includes('google') && index.byDomain.has(nd)) return { dup: true, reason: 'domain' };

  // Scored fuzzy match
  const recName = normName(rec.company);
  const recAddr = normAddr(rec.address);

  for (const ex of index.records) {
    let score = 0;
    if (recName === ex.nameNorm && recName.length > 3) score += 50;
    else if (recName.length > 6 && ex.nameNorm.length > 6 &&
             (recName.includes(ex.nameNorm) || ex.nameNorm.includes(recName))) score += 25;

    if (score > 0 && recAddr.length > 8 && ex.addrNorm.length > 8 && recAddr === ex.addrNorm) score += 40;
    if (score > 0 && np.length >= 10 && ex.phoneNorm === np) score += 30;

    if (score >= 70) return { dup: true, reason: 'fuzzy', score };
  }

  return { dup: false };
}

// ── Google Places search with retry ────────────────────────────────────────
async function placesSearch(queryText, locationStr, apiKey, excludedIds = new Set()) {
  const fetch = require('node-fetch');
  const results = [];
  const seenIds = new Set(excludedIds);
  const coordMap = {};

  // Build coordinate bias from territory string
  const geoMap = {
    'atlanta': '33.749,-84.388', 'savannah': '32.0809,-81.0912',
    'brunswick': '31.1499,-81.4915', 'augusta': '33.4735,-82.0105',
    'macon': '32.8407,-83.6324', 'jacksonville': '30.3322,-81.6557',
    'gainesville fl': '29.6516,-82.3248', 'valdosta': '30.8327,-83.2785',
    'tallahassee': '30.4383,-84.2807', 'columbia sc': '34.0007,-81.0348',
    'charleston sc': '32.7765,-79.9311', 'charlotte': '35.2271,-80.8431',
    'southeast': '31.5,-83.0', 'georgia': '32.5,-83.5',
    'north florida': '30.5,-83.5'
  };

  let locationBias = '31.5,-83.0'; // default SE Georgia
  for (const [key, coord] of Object.entries(geoMap)) {
    if ((locationStr||'').toLowerCase().includes(key)) { locationBias = coord; break; }
    if ((queryText||'').toLowerCase().includes(key))   { locationBias = coord; break; }
  }
  const [lat, lng] = locationBias.split(',');

  try {
    const body = {
      textQuery: queryText,
      maxResultCount: 20,
      locationBias: {
        circle: { center: { latitude: parseFloat(lat), longitude: parseFloat(lng) }, radius: 80000 }
      }
    };
    const resp = await fetch(
      'https://places.googleapis.com/v1/places:searchText',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Goog-Api-Key': apiKey,
          'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.types,places.businessStatus'
        },
        body: JSON.stringify(body)
      }
    );
    const data = await resp.json();
    for (const p of (data.places || [])) {
      if (p.businessStatus === 'CLOSED_PERMANENTLY') continue;
      if (seenIds.has(p.id)) continue;
      seenIds.add(p.id);
      const rec = {
        place_id: p.id,
        company:  (p.displayName||{}).text || '',
        address:  p.formattedAddress || '',
        phone:    p.nationalPhoneNumber || '',
        website:  p.websiteUri || '',
        rating:   p.rating || 0,
        types:    p.types || [],
        category: '',
        source_tier: SOURCE_TIERS.TIER3
      };
      // Extract city/state from address
      const addrParts = rec.address.split(',');
      rec.city  = addrParts.length >= 3 ? addrParts[addrParts.length-3].trim() : '';
      rec.state = addrParts.length >= 2 ? addrParts[addrParts.length-2].trim().replace(/\s+\d{5}.*/,'') : '';
      results.push(rec);
    }
  } catch (e) {
    console.error('[placesSearch] error for query:', queryText, e.message);
  }

  return results;
}

// ══════════════════════════════════════════════════════════════════════════════
//  MAIN BULK INGEST ROUTE
// ══════════════════════════════════════════════════════════════════════════════
router.post('/bulk-ingest', async (req, res) => {
  const uid = req.session.user.id;
  const { query, territory, max_records } = req.body;

  if (!query || !query.trim()) {
    return res.status(400).json({ error: 'Query is required (e.g. "roofing distributors Atlanta")' });
  }

  const maxRecs = Math.min(parseInt(max_records) || 40, 80);
  const apiKey  = process.env.GOOGLE_PLACES_API_KEY;

  try {
    console.log(`[BulkIngest v2] Query="${query}" territory="${territory}" max=${maxRecs}`);

    // ── Phase 1: Expand query into multiple search variations ──────────────
    const expandedQueries = expandQuery(query, territory);
    console.log('[BulkIngest v2] Expanded queries:', expandedQueries);

    // ── Phase 2: Fetch from Google Places across all expanded queries ───────
    const rawResults = [];
    const seenPlaceIds = new Set();

    for (const eq of expandedQueries) {
      const batch = await placesSearch(eq, territory, apiKey, seenPlaceIds);
      for (const r of batch) {
        if (!seenPlaceIds.has(r.place_id)) {
          seenPlaceIds.add(r.place_id);
          rawResults.push(r);
        }
      }
      if (rawResults.length >= maxRecs * 3) break; // enough candidates
    }

    console.log(`[BulkIngest v2] Raw results: ${rawResults.length} from ${expandedQueries.length} queries`);

    // ── Phase 3: Pre-ingestion filtering + classification ──────────────────
    const classified = [];
    for (const rec of rawResults) {
      const cls = classifyAndFilter(rec);
      if (!cls) {
        console.log(`[BulkIngest v2] EXCLUDED: ${rec.company} (failed pre-filter)`);
        continue;
      }
      rec.company_type   = cls.company_type;
      rec.category       = cls.category_label;
      classified.push(rec);
    }

    // ── Phase 4: Confidence scoring — discard < 50 ────────────────────────
    const scored = [];
    for (const rec of classified) {
      const conf = scoreConfidence(rec, rec.source_tier || SOURCE_TIERS.TIER3);
      if (conf < 50) {
        console.log(`[BulkIngest v2] DISCARDED (low conf ${conf}): ${rec.company}`);
        continue;
      }
      rec.confidence_score = conf;
      rec.data_status = conf >= 90 ? 'Verified' : conf >= 70 ? 'Likely Valid' : 'Unvetted';
      scored.push(rec);
    }

    // Sort by confidence descending
    scored.sort((a, b) => b.confidence_score - a.confidence_score);

    // ── Phase 5: Team-wide deduplication ──────────────────────────────────
    const existingRows = await pool.query(
      `SELECT google_place_id, company, address, phone, website, category
       FROM prospects`
    );
    const dupIndex = buildExistingIndex(existingRows.rows);

    // Also build session-level dedup
    const sessionSeen = new Set();

    const toInsert   = [];
    const skipped    = [];

    for (const rec of scored) {
      // Session dedup
      const sessionKey = normName(rec.company) + '|' + (rec.city||'').toLowerCase();
      if (sessionSeen.has(sessionKey)) {
        skipped.push({ reason: 'session_dup', company: rec.company });
        continue;
      }
      sessionSeen.add(sessionKey);

      // Team-wide dedup
      const dupCheck = isDuplicate(rec, dupIndex);
      if (dupCheck.dup) {
        skipped.push({ reason: dupCheck.reason, company: rec.company });
        continue;
      }

      // Add to insert queue and update index so later records in this batch won't dup it
      toInsert.push(rec);
      // Update in-memory index to prevent batch-internal dups
      if (rec.place_id) dupIndex.byPlaceId.add(rec.place_id);
      const np = normPhone(rec.phone);
      if (np.length >= 10) dupIndex.byPhone.add(np);
      const nd = normDomain(rec.website);
      if (nd.length > 4) dupIndex.byDomain.add(nd);

      if (toInsert.length >= maxRecs) break;
    }

    console.log(`[BulkIngest v2] Insert queue: ${toInsert.length} | Skipped: ${skipped.length}`);

    // ── Phase 6: Bulk insert into CRM ─────────────────────────────────────
    let imported = 0;
    const importedRecords = [];

    for (const rec of toInsert) {
      try {
        const result = await pool.query(
          `INSERT INTO prospects
             (user_id, company, category, company_type, city, state, phone, email,
              contact, website, status, priority, source, google_place_id,
              address, data_status, manufacturer_assoc, confidence_score,
              source_tier, last_activity_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NOW())
           ON CONFLICT DO NOTHING
           RETURNING id`,
          [
            uid,
            rec.company,
            rec.category,
            rec.company_type,
            rec.city   || null,
            rec.state  || null,
            rec.phone  || null,
            rec.email  || null,
            null,
            rec.website || null,
            'New',
            rec.confidence_score >= 80 ? 'High' : rec.confidence_score >= 65 ? 'Medium' : 'Low',
            'Bulk Ingest v2',
            rec.place_id || null,
            rec.address  || null,
            rec.data_status || 'Unvetted',
            null,
            rec.confidence_score,
            rec.source_tier || 3,
            null
          ]
        );
        if (result.rows.length > 0) {
          imported++;
          importedRecords.push({
            id: result.rows[0].id,
            company: rec.company,
            category: rec.category,
            company_type: rec.company_type,
            city: rec.city,
            confidence: rec.confidence_score,
            status: rec.data_status
          });
        }
      } catch (insertErr) {
        console.error('[BulkIngest v2] Insert error for', rec.company, ':', insertErr.message);
      }
    }

    // ── Phase 7: Return structured summary ─────────────────────────────────
    const summary = {
      ok: true,
      imported,
      skipped_duplicates: skipped.length,
      excluded_filtered: rawResults.length - classified.length,
      excluded_low_confidence: classified.length - scored.length,
      queries_run: expandedQueries.length,
      raw_candidates: rawResults.length,
      records: importedRecords,
      breakdown: {
        distributors: importedRecords.filter(r => r.company_type === 'Distributor').length,
        contractors:  importedRecords.filter(r => r.company_type === 'Contractor').length,
        unknown:      importedRecords.filter(r => r.company_type === 'Unknown').length,
        verified:     importedRecords.filter(r => r.status === 'Verified').length,
        likely_valid: importedRecords.filter(r => r.status === 'Likely Valid').length,
        unvetted:     importedRecords.filter(r => r.status === 'Unvetted').length
      },
      message: imported === 0
        ? `No new records found (${skipped.length} duplicates skipped, ${rawResults.length - classified.length} excluded by filters)`
        : `Imported ${imported} records — ${importedRecords.filter(r=>r.company_type==='Distributor').length} distributors, ${importedRecords.filter(r=>r.company_type==='Contractor').length} contractors`
    };

    console.log('[BulkIngest v2] Complete:', summary.message);
    res.json(summary);

  } catch (e) {
    console.error('[BulkIngest v2] Fatal error:', e.message);
    res.status(500).json({ error: 'Bulk ingestion failed: ' + e.message });
  }
});


module.exports = router;
