const express = require('express');
const { pool } = require('../db');
const router = express.Router();
const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;


// Hard block — never return paint shops/painters as Alum-A-Pole leads
const ALUM_PAINT_BLOCK = ['paint', 'painting', 'painter', 'painters'];
function alumPoleBlocked(name) {
  const lower = (name || '').toLowerCase();
  return ALUM_PAINT_BLOCK.some(kw => lower.includes(kw));
}

// Global bad-lead blocklist — never return these for any contractor search
const BAD_LEAD_KEYWORDS = [
  // Financial
  'bank','credit union','financial','insurance','mortgage','lending','loan','wealth',
  'investment','securities','advisor','brokerage','finance',
  // Medical
  'hospital','clinic','medical','dental','doctor','physician','healthcare','therapy',
  'urgent care','pharmacy','chiropractor','optom','vision center',
  // Retail/unrelated
  'restaurant','diner','cafe','coffee','food','grocery','supermarket','walmart',
  'target','home depot','lowes','ace hardware',
  // Government/institutional
  'government','county','city hall','police','fire station','post office','dmv',
  'school district','university','college','church','temple','mosque','synagogue',
  // Office parks/generic
  'office park','business center','coworking','regus','wework','executive suites',
  // Real estate (not contractors)
  'realty','realtor','real estate','property management','apartment','condo','hoa',
  // Auto
  'auto dealer','car dealer','dealership','used cars','automotive repair',
  // Unrelated services
  'hair salon','nail salon','spa','massage','tattoo','gym','fitness','yoga',
  'daycare','childcare','staffing agency','temp agency',
  // Residential (addresses)
  'unit ','apt ','suite \d','#\d'
];

// Contractor-relevant keywords — a match boosts confidence
const CONTRACTOR_SIGNALS = [
  'contractor','contracting','construction','roofing','siding','deck','window','door',
  'builder','building','remodel','renovati','installation','installer','install',
  'cornice','fascia','soffit','framing','scaffold','supply','distributor','dealer',
  'trade','exterior','interior','structural','commercial','residential','industrial',
  'plumbing','electrical','hvac','flooring','masonry','concrete','waterproof',
  'painting','drywall','insulation','sheet metal','metal',
  'home improvement','handyman','repair','restoration','maintenance services'
];


// ── ICP: Words that signal enterprise/mega-corp gatekeeping ─────
const ENTERPRISE_BLOCKLIST = [
  'inc\. nationwide','national','corp nationwide',
  // Mega-contractors / publicly traded / franchise chains
  'dr horton','nvr ','toll brothers','kb home','lennar','pulte','centex',
  'beazer','meritage','william lyon','century communities',
  'abc supply','builders firstsource','us lbm','builders general',
  'beacon roofing','bradco','gulfeagle','atlas roofing corp',
  'allied building products','northwest building products',
  // Brand-franchise only, not decision-maker accessible
  'servpro','servicemaster','restoration 1','paul davis',
  // National chains with procurement layers
  'home services of america','2-10 home buyers',
];

// ── ICP: Home-based / micro signals ─────────────────────────────
const HOME_BASED_SIGNALS = [
  'home-based','home based','owner operator','owner-operator',
  'sole prop','one man','one-man','1 man','handyman services',
  // Residential address patterns caught at name level
  'my home','from home',
];

// ── ICP: Terms that suggest very large corporate structure ───────
const MEGA_CORP_SIGNALS = [
  'group llc nationwide','holding','holdings','properties llc',
  'development corp','developments llc','realty group','realty corp',
  'global services','international services','worldwide',
];

// ── Review sweet-spot thresholds for mid-market ICP ─────────────
// Too few = micro/home-based. Too many = big corporate.
const REVIEW_MIN_ICP = 5;   // fewer than this = likely micro/home-based
const REVIEW_MAX_ICP = 600; // more than this = likely mega-corp

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

  // ── POSITIVE signals ──
  if (hasPhone) { score += 30; reasons.push('direct phone listed'); }
  if (hasWebsite) { score += 10; reasons.push('website present'); }
  if (hasHours) { score += 10; reasons.push('business hours listed'); }

  // Sweet-spot review count = mid-market (5–300)
  if (reviews >= REVIEW_MIN_ICP && reviews <= 300) {
    score += 25;
    reasons.push('review count signals mid-market size');
  } else if (reviews > 300 && reviews <= REVIEW_MAX_ICP) {
    score += 10;
    reasons.push('established business, may have some gatekeeping');
  }

  // Good rating with reasonable review count = real business, decision-maker accessible
  if (rating >= 4.0 && reviews >= REVIEW_MIN_ICP) {
    score += 15;
    reasons.push('highly rated active business');
  }

  // ── NEGATIVE signals ──
  if (reviews < REVIEW_MIN_ICP) {
    score -= 20;
    deductions.push('very few reviews — likely micro or home-based');
  }
  if (reviews > REVIEW_MAX_ICP) {
    score -= 25;
    deductions.push('very high review volume — likely large corp with procurement layers');
  }

  // Enterprise / mega-corp blockers
  for (const kw of ENTERPRISE_BLOCKLIST) {
    if (lower.includes(kw)) { score -= 40; deductions.push('enterprise signal: ' + kw); break; }
  }
  for (const kw of MEGA_CORP_SIGNALS) {
    if (lower.includes(kw)) { score -= 30; deductions.push('corporate holding signal'); break; }
  }
  for (const kw of HOME_BASED_SIGNALS) {
    if (lower.includes(kw)) { score -= 35; deductions.push('home-based or solo operator signal'); break; }
  }

  // ── Tier assignment ──
  const finalScore = Math.max(0, Math.min(100, score));
  let tier, label;
  if (finalScore >= 60) {
    tier = 'HIGH';
    label = 'High Reachability';
  } else if (finalScore >= 35) {
    tier = 'MEDIUM';
    label = 'Medium Reachability';
  } else {
    tier = 'LOW';
    label = 'Low Reachability';
  }

  const allSignals = [...reasons, ...deductions.map(d => '(-) ' + d)];
  return { tier, label, score: finalScore, signals: allSignals };
}

// ── Estimate company size tier ───────────────────────────────────
function getSizeTier(place) {
  const reviews = place.userRatingCount || 0;
  // Heuristic: reviews strongly correlate with business size/visibility
  if (reviews < REVIEW_MIN_ICP) return 'Micro';
  if (reviews < 30) return 'Small';
  if (reviews < 150) return 'Mid';
  return 'Large';
}

// ── Why this lead? Build a 1-line justification ──────────────────
function buildInclusionReason(place, name, category, reachability) {
  const reviews = place.userRatingCount || 0;
  const rating = place.rating || 0;
  const hasPhone = !!place.nationalPhoneNumber;
  const hasWebsite = !!place.websiteUri;

  const parts = [];
  if (hasPhone) parts.push('direct phone available');
  if (rating >= 4.0 && reviews >= 10) parts.push(rating.toFixed(1) + '★ with ' + reviews + ' reviews');
  else if (reviews >= 10) parts.push(reviews + ' reviews');
  if (hasWebsite) parts.push('web presence confirmed');

  return parts.length ? parts.join(', ') + '.' : 'Active ' + category + ' in target territory.';
}

function isBadLead(name, address, category) {
  if (!name) return true;
  const lower = name.toLowerCase();
  const addrLower = (address||'').toLowerCase();

  // Block bad-lead keywords (financial, medical, retail, etc.)
  for (const kw of BAD_LEAD_KEYWORDS) {
    if (new RegExp(kw).test(lower)) return true;
  }

  // Block home-based / solo operator signals
  for (const kw of HOME_BASED_SIGNALS) {
    if (lower.includes(kw)) return true;
  }

  // Block mega-corp signals
  for (const kw of MEGA_CORP_SIGNALS) {
    if (lower.includes(kw)) return true;
  }

  // Block known enterprise chains
  for (const kw of ENTERPRISE_BLOCKLIST) {
    if (lower.includes(kw)) return true;
  }

  // Block names that are just generic addresses or single words
  const nameWords = name.split(' ').filter(w=>w.length>1);
  if (nameWords.length < 2 && !lower.includes('co') && !lower.includes('inc') && !lower.includes('llc')) {
    const hasSignal = CONTRACTOR_SIGNALS.some(s => lower.includes(s));
    if (!hasSignal) return true;
  }

  // PO Box addresses
  if (/po box|p\.o\. box/i.test(addrLower)) return true;

  return false;
}


// Google Places API type-based exclusion — types that are NEVER valid leads
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


function hasContractorSignal(name, category) {
  const combined = ((name||'') + ' ' + (category||'')).toLowerCase();
  return CONTRACTOR_SIGNALS.some(s => combined.includes(s));
}

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

function getSearchQueries(product, customerType) {
  const p = (product || '').toLowerCase();
  const ct = (customerType || '').toLowerCase();
  if (ct && ct !== 'any building products buyer') return [
    customerType, customerType + ' contractor', customerType + ' company',
    customerType + ' services', customerType + ' specialist', customerType + ' installer',
    customerType + ' builder', customerType + ' pro', customerType + ' repair',
    customerType + ' residential', customerType + ' commercial', customerType + ' local',
    customerType + ' near me', 'best ' + customerType, customerType + ' renovation'
  ];
  if (p.includes('alum') || p.includes('scaffolding')) return [
    'siding contractor installation',
    'vinyl siding contractor',
    'James Hardie siding installer',
    'fiber cement siding company',
    'LP SmartSide installer',
    'soffit fascia cornice contractor',
    'exterior siding company',
    'siding supply distributor',
    'fastener supply store construction',
    'scaffolding rental supply',
    'building materials distributor siding',
    'exterior building products distributor',
    'construction equipment dealer scaffolding',
    'cornice contractor trim',
    'residential siding contractor'
  ];
  if (p.includes('soudal') || p.includes('sealant') || p.includes('adhesive')) return [
    'window installation contractor', 'door installation company', 'glazing contractor',
    'window and door installer', 'commercial glazing company', 'residential window installer',
    'waterproofing contractor', 'insulation contractor', 'weatherproofing company',
    'window replacement company', 'door replacement contractor', 'caulking contractor',
    'commercial window installer', 'building envelope contractor', 'window repair company'
  ];
  if (p.includes('shurtape') || p.includes('flashing')) return [
    'roofing contractor', 'commercial roofing company', 'residential roofing contractor',
    'window installer', 'door installation contractor', 'deck contractor',
    'metal roofing contractor', 'flat roof contractor', 'roofing company',
    'exterior contractor', 'home builder', 'remodeling contractor',
    'general contractor', 'TPO roofing contractor', 'shingle roofing company'
  ];
  if (p.includes('framing') || (p.includes('fortress') && !p.includes('rail'))) return [
    'deck builder', 'deck contractor', 'custom deck builder',
    'composite deck installer', 'Trex deck installer', 'TimberTech installer',
    'deck construction company', 'remodeling contractor', 'outdoor living contractor',
    'porch builder', 'pergola builder', 'deck repair company',
    'residential deck contractor', 'backyard contractor', 'home improvement contractor'
  ];
  if (p.includes('railing') || p.includes('fortress')) return [
    'deck contractor', 'railing installer', 'fence contractor',
    'iron railing company', 'aluminum railing installer', 'cable railing installer',
    'stair railing contractor', 'glass railing installer', 'porch railing company',
    'deck builder', 'balcony railing contractor', 'commercial railing installer',
    'fence and railing company', 'outdoor contractor', 'deck railing company'
  ];
  return [
    'general contractor', 'construction company', 'remodeling contractor',
    'home builder', 'building contractor', 'renovation contractor',
    'residential contractor', 'commercial contractor', 'home improvement contractor',
    'custom home builder', 'design build contractor', 'exterior contractor',
    'specialty contractor', 'construction management company', 'licensed contractor'
  ];
}

function getProductWhy(product) {
  const p = (product || '').toLowerCase();
  if (p.includes('soudal') || p.includes('sealant') || p.includes('boss')) return 'distributes roofing and siding products and can add Soudal BOSS sealants to their product line';
  if (p.includes('shurtape') || p.includes('flashing') || p.includes('deck tape')) return 'sells to builders and contractors and needs flashing tape as a stocked SKU';
  if (p.includes('alum') || p.includes('scaffolding') || p.includes('pump jack')) return 'supplies tools and equipment to contractors and needs Alum-A-Pole scaffolding in inventory';
  if (p.includes('framing')) return 'needs rot-proof steel framing as a wood alternative for decks';
  if (p.includes('railing')) return 'installs railing systems on decks, stairs, and porches';
  return 'distributes building products to contractors and builders in the Southeast';
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
  const { category, territory, count, customer_type } = req.body;
  const user = req.session.user;
  const product = category;
  const numLeads = Math.min(parseInt(count) || 20, 50);
  const loc = territory || user.territory || 'Atlanta, GA';
  const queries = getSearchQueries(product, customer_type);
  const why = getProductWhy(product);
  const cities = getCities(loc);

  console.log('Searching:', product, '|', loc, '|', numLeads, 'leads');
  console.log('Queries:', queries.length, '| Cities:', cities.length);

  try {
    const leadsMap = new Map();

    // Each query gets its own city, rotating through city list
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

          // Type-based exclusion — skip businesses that are never valid leads
          if (hasBadPlaceType(place.types || [])) continue;

          const placeName = place.displayName?.text || '';
          const placeAddr = place.formattedAddress || '';

          // Skip bad leads (banks, offices, unrelated businesses, enterprise, home-based)
          if (isBadLead(placeName, placeAddr, label)) continue;

          // Skip leads with no phone (inactive or uncontactable)
          if (!place.nationalPhoneNumber) continue;

          // Alum-A-Pole paint blocker
          if ((product||'').toLowerCase().includes('alum') && alumPoleBlocked(placeName)) continue;

          // ICP: Skip micro businesses (< REVIEW_MIN_ICP reviews AND no website)
          // A real mid-market contractor will have either a website or at least a few reviews
          const reviewCount = place.userRatingCount || 0;
          if (reviewCount < REVIEW_MIN_ICP && !place.websiteUri) continue;

          // ICP: Compute reachability — skip LOW tier
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
            // Legacy fields kept for UI compatibility
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

    // Sort by ICP quality:
    // Primary: Reachability score (HIGH > MEDIUM — LOW already filtered out)
    // Secondary: rating × log(reviews) within same tier — rewards active established mid-market
    const leadsArr = Array.from(leadsMap.values()).sort((a, b) => {
      const reachA = a.reachabilityScore || 0;
      const reachB = b.reachabilityScore || 0;
      if (reachB !== reachA) return reachB - reachA;
      const scoreA = (a.rating || 0) * Math.log1p(a.reviews || 0);
      const scoreB = (b.rating || 0) * Math.log1p(b.reviews || 0);
      return scoreB - scoreA;
    });
    console.log('Final (sorted by quality):', leadsArr.length, 'leads');
    if (leadsArr.length === 0) return res.json({ error: 'No results found. Try a different territory or product.' });
    res.json({ leads: leadsArr, source: 'google' });
  } catch(e) {
    console.error('Error:', e.message);
    res.json({ error: 'Search failed: ' + e.message });
  }
});

router.get('/test', async (req, res) => {
  try {
    const data = await httpsPost(
      'places.googleapis.com', '/v1/places:searchText',
      { 'Content-Type': 'application/json', 'X-Goog-Api-Key': PLACES_KEY, 'X-Goog-FieldMask': 'places.id,places.displayName' },
      { textQuery: 'siding contractor Atlanta GA', maxResultCount: 3 }
    );
    res.json({ ok: true, count: data.places?.length, first: data.places?.[0]?.displayName?.text });
  } catch(e) { res.json({ ok: false, error: e.message }); }
});


// GET /api/places/company-search?q=searchText
// Typeahead: search existing prospects first (done in frontend),
// this endpoint handles Google Places fallback for companies not in CRM
router.get('/company-search', async (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q || q.length < 2) return res.json([]);
  if (!PLACES_KEY) return res.json([]);

  try {
    const fetch = require('node-fetch');

    // Use findplacefromtext to find candidate companies
    const findUrl = `https://maps.googleapis.com/maps/api/place/findplacefromtext/json` +
      `?input=${encodeURIComponent(q)}` +
      `&inputtype=textquery` +
      `&fields=place_id,name,formatted_address` +
      `&key=${PLACES_KEY}`;

    const findRes = await fetch(findUrl);
    const findData = await findRes.json();
    const candidates = (findData.candidates || []).slice(0, 5);

    // Get details for each candidate
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

module.exports = router;
