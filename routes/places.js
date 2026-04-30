const express = require('express');
const { pool } = require('../db');
const router = express.Router();

const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;

function httpsPost(hostname, path, headers, body) {
  const https = require('https');
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const options = { hostname, path, method: 'POST', headers: { ...headers, 'Content-Length': Buffer.byteLength(data) } };
    const req = https.request(options, (res) => {
      let raw = '';
      res.on('data', chunk => raw += chunk);
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { reject(new Error('Parse error: ' + raw.slice(0,100))); } });
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
      res.on('data', chunk => raw += chunk);
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { reject(new Error('Parse error')); } });
    }).on('error', reject);
  });
}

function getSearchTerms(product, customerType) {
  const p = (product || '').toLowerCase();
  const ct = (customerType || '').toLowerCase();
  if (ct && ct !== 'any building products buyer') return [customerType, customerType + ' company', customerType + ' services'];
  if (p.includes('alum') || p.includes('scaffolding'))
    return ['siding contractor', 'James Hardie installer', 'exterior siding contractor', 'stucco contractor', 'fiber cement siding'];
  if (p.includes('soudal') || p.includes('sealant') || p.includes('adhesive'))
    return ['window installation contractor', 'door installation contractor', 'glazing contractor', 'waterproofing contractor', 'insulation contractor'];
  if (p.includes('shurtape') || p.includes('flashing'))
    return ['roofing contractor', 'window installer', 'deck contractor', 'home builder', 'remodeling contractor'];
  if (p.includes('framing') || (p.includes('fortress') && !p.includes('rail')))
    return ['deck builder', 'deck contractor', 'remodeling contractor', 'general contractor', 'custom home builder'];
  if (p.includes('railing') || p.includes('fortress'))
    return ['deck contractor', 'fence contractor', 'railing contractor', 'general contractor', 'remodeling contractor'];
  return ['general contractor', 'construction company', 'remodeling contractor', 'home builder', 'building contractor'];
}

function getProductWhy(product) {
  const p = (product || '').toLowerCase();
  if (p.includes('alum') || p.includes('scaffolding')) return 'needs OSHA-compliant pump jack scaffolding for exterior work at height';
  if (p.includes('soudal') || p.includes('sealant')) return 'uses high-performance sealants and adhesives on every install';
  if (p.includes('shurtape') || p.includes('flashing')) return 'requires code-compliant flashing tape for moisture barriers';
  if (p.includes('framing')) return 'needs rot-proof steel framing as a wood alternative for decks';
  if (p.includes('railing')) return 'installs railing systems on decks, stairs, and porches';
  return 'purchases building products for construction projects';
}

function getCities(loc) {
  const t = (loc || '').toLowerCase();
  if (t.includes('atlanta metro') || t === 'atlanta' || t === 'atl' || t.includes('atlanta ga') || t.includes('atlanta, ga'))
    return ['Atlanta GA', 'Marietta GA', 'Kennesaw GA', 'Alpharetta GA', 'Roswell GA', 'Smyrna GA', 'Dunwoody GA', 'Decatur GA', 'Norcross GA', 'Duluth GA', 'Lawrenceville GA', 'Buford GA', 'Cumming GA', 'Woodstock GA', 'Acworth GA', 'Canton GA', 'Peachtree City GA', 'Newnan GA', 'Douglasville GA', 'Stockbridge GA', 'McDonough GA', 'Fayetteville GA', 'Cartersville GA', 'Powder Springs GA', 'Sandy Springs GA'];
  if (t.includes('birmingham'))
    return ['Birmingham AL', 'Hoover AL', 'Vestavia Hills AL', 'Homewood AL', 'Bessemer AL', 'Pelham AL', 'Alabaster AL', 'Helena AL', 'Trussville AL', 'Gardendale AL', 'Leeds AL', 'Pell City AL', 'Calera AL', 'Northport AL', 'Anniston AL'];
  if (t.includes('nashville'))
    return ['Nashville TN', 'Brentwood TN', 'Franklin TN', 'Murfreesboro TN', 'Smyrna TN', 'Hendersonville TN', 'Gallatin TN', 'Mount Juliet TN', 'Nolensville TN', 'Spring Hill TN', 'Columbia TN', 'Clarksville TN', 'Lebanon TN'];
  if (t.includes('charlotte'))
    return ['Charlotte NC', 'Concord NC', 'Kannapolis NC', 'Gastonia NC', 'Huntersville NC', 'Cornelius NC', 'Mooresville NC', 'Matthews NC', 'Monroe NC', 'Waxhaw NC', 'Rock Hill SC', 'Fort Mill SC', 'Tega Cay SC'];
  if (t.includes('southeast') || t.includes('south east'))
    return ['Atlanta GA', 'Marietta GA', 'Birmingham AL', 'Hoover AL', 'Nashville TN', 'Franklin TN', 'Charlotte NC', 'Concord NC', 'Columbia SC', 'Greenville SC', 'Chattanooga TN', 'Knoxville TN', 'Memphis TN', 'Savannah GA', 'Augusta GA', 'Huntsville AL', 'Raleigh NC', 'Charleston SC', 'Jackson MS', 'Hattiesburg MS'];
  // Default - use the territory as-is with surrounding area
  return [loc, loc + ' suburbs', loc + ' area'];
}

router.post('/places-leads', async (req, res) => {
  const { category, territory, count, customer_type } = req.body;
  const user = req.session.user;
  const product = category;
  const numLeads = Math.min(parseInt(count) || 20, 50);
  const loc = territory || user.territory || 'Atlanta, GA';
  const searchTerms = getSearchTerms(product, customer_type);
  const why = getProductWhy(product);
  const cityList = getCities(loc);

  console.log('Lead search:', product, loc, numLeads, 'leads requested');
  console.log('Cities:', cityList.slice(0,5).join(', '), '...');
  console.log('Terms:', searchTerms.join(', '));

  try {
    const leadsMap = new Map();

    // Build all job combos: term x city
    const jobs = [];
    for (const term of searchTerms) {
      for (const city of cityList) {
        jobs.push({ term, city });
      }
    }

    // Run in parallel batches of 5 until we have enough leads
    const BATCH = 5;
    for (let i = 0; i < jobs.length && leadsMap.size < numLeads; i += BATCH) {
      const batch = jobs.slice(i, i + BATCH);

      const batchResults = await Promise.all(batch.map(async ({ term, city }) => {
        try {
          const data = await httpsPost(
            'places.googleapis.com',
            '/v1/places:searchText',
            {
              'Content-Type': 'application/json',
              'X-Goog-Api-Key': PLACES_KEY,
              'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus'
            },
            { textQuery: term + ' in ' + city, maxResultCount: 20 }
          );
          return { term, city, places: data.places || [] };
        } catch(e) {
          console.error('Batch error:', term, city, e.message);
          return { term, city, places: [] };
        }
      }));

      for (const { term, city, places } of batchResults) {
        console.log(term, 'in', city, '->', places.length, 'results, total so far:', leadsMap.size);
        for (const place of places) {
          if (leadsMap.size >= numLeads) break;
          if (leadsMap.has(place.id)) continue;
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          const addrParts = (place.formattedAddress || '').split(',');
          leadsMap.set(place.id, {
            company: place.displayName?.text || 'Unknown',
            category: term,
            city: addrParts[1]?.trim() || city,
            state: addrParts[2]?.trim().split(' ')[0] || '',
            phone: place.nationalPhoneNumber || null,
            email: null,
            website: place.websiteUri || null,
            contact: null,
            products: product,
            why: 'This ' + term + ' ' + why,
            priority: (place.rating >= 4.5 && place.userRatingCount > 20) ? 'High' : (place.rating >= 3.5) ? 'Medium' : 'Low',
            rating: place.rating || null,
            reviews: place.userRatingCount || 0,
            address: place.formattedAddress || null
          });
        }
        if (leadsMap.size >= numLeads) break;
      }
    }

    const leads = Array.from(leadsMap.values());
    console.log('Final lead count:', leads.length);
    if (leads.length === 0) return res.json({ error: 'No results found. Try a different territory or product.' });
    res.json({ leads, source: 'google' });
  } catch(e) {
    console.error('Places route error:', e.message);
    res.json({ error: 'Search failed: ' + e.message });
  }
});

router.get('/test', async (req, res) => {
  try {
    const result = await httpsPost(
      'places.googleapis.com',
      '/v1/places:searchText',
      { 'Content-Type': 'application/json', 'X-Goog-Api-Key': PLACES_KEY, 'X-Goog-FieldMask': 'places.id,places.displayName,places.nationalPhoneNumber' },
      { textQuery: 'siding contractor Atlanta GA', maxResultCount: 3 }
    );
    res.json({ ok: true, count: result.places?.length, first: result.places?.[0]?.displayName?.text });
  } catch(e) { res.json({ ok: false, error: e.message }); }
});

module.exports = router;