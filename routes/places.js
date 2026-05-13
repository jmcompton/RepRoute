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

function isBadLead(name, address, category) {
  if (!name) return true;
  const lower = name.toLowerCase();
  const addrLower = (address||'').toLowerCase();

  // Block bad-lead keywords
  for (const kw of BAD_LEAD_KEYWORDS) {
    if (new RegExp(kw).test(lower)) return true;
  }

  // Block names that are just generic addresses or single words
  const nameWords = name.split(' ').filter(w=>w.length>1);
  if (nameWords.length < 2 && !lower.includes('co') && !lower.includes('inc') && !lower.includes('llc')) {
    // Single-word generic names are suspect unless they have contractor signals
    const hasSignal = CONTRACTOR_SIGNALS.some(s => lower.includes(s));
    if (!hasSignal) return true;
  }

  // PO Box addresses
  if (/po box|p\.o\. box/i.test(addrLower)) return true;

  return false;
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
              'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus'
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

          const placeName = place.displayName?.text || '';
          const placeAddr = place.formattedAddress || '';

          // Skip bad leads (banks, offices, unrelated businesses)
          if (isBadLead(placeName, placeAddr, label)) continue;

          // Skip leads with no phone (likely inactive or spam listings)
          if (!place.nationalPhoneNumber) continue;

          // Alum-A-Pole paint blocker
          if ((product||'').toLowerCase().includes('alum') && alumPoleBlocked(placeName)) continue;
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
            priority: (place.rating >= 4.5 && place.userRatingCount > 50) ? 'High' : (place.rating >= 4.0 && place.userRatingCount > 10) ? 'Medium' : 'Low',
            rating: place.rating || null,
            reviews: place.userRatingCount || 0,
            address: place.formattedAddress || null
          });
        }
        if (leadsMap.size >= numLeads) break;
      }
    }

    // Sort by quality: prioritize high-rating + high review count (biggest, most established businesses)
    const leadsArr = Array.from(leadsMap.values()).sort((a, b) => {
      // Score = rating * log(reviews+1) — rewards both high rating AND volume of reviews
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

module.exports = router;
