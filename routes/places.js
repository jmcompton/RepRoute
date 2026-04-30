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
  if (ct && ct !== 'any building products buyer') return [customerType, customerType + ' contractor', customerType + ' company'];
  if (p.includes('alum') || p.includes('scaffolding'))
    return ['siding contractor', 'James Hardie installer', 'exterior siding', 'stucco contractor', 'fiber cement siding contractor'];
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

router.post('/places-leads', async (req, res) => {
  const { category, territory, count, customer_type } = req.body;
  const user = req.session.user;
  const product = category;
  const numLeads = Math.min(parseInt(count) || 20, 50);
  const loc = territory || user.territory || 'Atlanta, GA';
  const searchTerms = getSearchTerms(product, customer_type);
  const why = getProductWhy(product);

  console.log('Lead search:', { product, loc, numLeads, searchTerms });

  try {
    // Geocode
    const geoData = await httpsGet(
      'https://maps.googleapis.com/maps/api/geocode/json?address=' + encodeURIComponent(loc) + '&key=' + PLACES_KEY
    );
    if (geoData.status !== 'OK') return res.json({ error: 'Could not find location: ' + loc });
    const { lat, lng } = geoData.results[0].geometry.location;
    console.log('Geocoded:', lat, lng);

    const leadsMap = new Map();
    const perTerm = Math.max(Math.ceil(numLeads / searchTerms.length), 10);

    for (const term of searchTerms) {
      if (leadsMap.size >= numLeads) break;
      console.log('Searching term:', term, '- current leads:', leadsMap.size);

      const searchData = await httpsPost(
        'places.googleapis.com',
        '/v1/places:searchText',
        {
          'Content-Type': 'application/json',
          'X-Goog-Api-Key': PLACES_KEY,
          'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus'
        },
        {
          textQuery: term + ' in ' + loc,
          maxResultCount: 20,
          locationBias: { circle: { center: { latitude: lat, longitude: lng }, radius: 80000 } }
        }
      );

      console.log('Results for', term, ':', searchData.places?.length || 0, searchData.error?.message || '');

      for (const place of (searchData.places || [])) {
        if (leadsMap.size >= numLeads) break;
        if (leadsMap.has(place.id)) continue;
        if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;

        const addrParts = (place.formattedAddress || '').split(',');
        leadsMap.set(place.id, {
          company: place.displayName?.text || 'Unknown',
          category: term,
          city: addrParts[1]?.trim() || '',
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
    }

    const leads = Array.from(leadsMap.values());
    console.log('Total leads:', leads.length);
    if (leads.length === 0) return res.json({ error: 'No results found. Try a different territory or product.' });
    res.json({ leads, source: 'google' });
  } catch(e) {
    console.error('Places error:', e.message);
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