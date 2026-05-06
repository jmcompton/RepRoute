const express = require('express');
const { pool } = require('../db');
const router = express.Router();
const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;

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
    'siding contractor', 'James Hardie siding installer', 'vinyl siding company',
    'fiber cement siding contractor', 'stucco contractor', 'exterior renovation contractor',
    'hardie board installer', 'LP SmartSide installer', 'soffit fascia contractor',
    'exterior remodeling contractor', 'house siding company', 'home exterior contractor',
    'siding repair company', 'residential siding contractor', 'siding installation company'
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

    // Run all queries in parallel for speed
    const results = await Promise.all(jobs.map(async ({ query, label }) => {
      try {
        const data = await httpsPost(
          'places.googleapis.com',
          '/v1/places:searchText',
          {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': PLACES_KEY,
            'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus'
          },
          { textQuery: query, maxResultCount: 20 }
        );
        return { label, places: data.places || [] };
      } catch(e) {
        console.error('Query error:', label, e.message);
        return { label, places: [] };
      }
    }));
    if (true) {

      for (const { label, places } of results) {
        console.log(label, '->', places.length, 'results | total:', leadsMap.size);
        for (const place of places) {
          if (leadsMap.has(place.id)) continue;
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          if ((place.rating || 0) < 3.5) continue;

          const addr = (place.formattedAddress || '').split(',');
          const name = (place.displayName?.text || '').toLowerCase();
          const rating = place.rating || 0;
          const reviews = place.userRatingCount || 0;

          // Scoring system (0-100)
          let score = 0;

          // 1. Category match (40pts)
          const distributorSignals = ['supply', 'distribution', 'wholesale', 'distributor', 'dealer', 'lumber', 'building material', 'roofing supply', 'siding supply', 'tool', 'equipment'];
          const contractorSignals = ['contractor', 'construction', 'remodel', 'builder', 'install'];
          const p = (product || '').toLowerCase();
          const needsDistributor = p.includes('soudal') || p.includes('shurtape') || p.includes('alum');
          const needsContractor = p.includes('fortress');
          let catScore = 0;
          if (needsDistributor) {
            const hasDistSignal = distributorSignals.some(s => name.includes(s));
            const hasContSignal = contractorSignals.some(s => name.includes(s));
            if (hasDistSignal) catScore = 40;
            else if (!hasContSignal) catScore = 20;
            else catScore = 5;
          } else if (needsContractor) {
            catScore = contractorSignals.some(s => name.includes(s)) ? 40 : 20;
          } else {
            catScore = 25;
          }
          score += catScore;

          // 2. Rating (25pts)
          if (rating >= 4.5) score += 25;
          else if (rating >= 4.2) score += 20;
          else if (rating >= 4.0) score += 15;
          else if (rating >= 3.7) score += 8;
          else score += 2;

          // 3. Review count (20pts)
          if (reviews >= 50) score += 20;
          else if (reviews >= 20) score += 15;
          else if (reviews >= 10) score += 10;
          else if (reviews >= 5) score += 5;

          // 4. Name quality signals (15pts)
          const qualitySignals = ['supply', 'distribution', 'wholesale', 'dealer', 'pro', 'professional', 'commercial'];
          score += qualitySignals.some(s => name.includes(s)) ? 15 : 5;

          const priority = score >= 65 ? 'High' : score >= 40 ? 'Medium' : 'Low';

          leadsMap.set(place.id, {
            company: place.displayName?.text || 'Unknown',
            category: label,
            city: addr[1]?.trim() || '',
            state: addr[2]?.trim().split(' ')[0] || '',
            phone: place.nationalPhoneNumber || null,
            email: null,
            website: place.websiteUri || null,
            contact: null,
            products: product,
            why: 'This ' + label + ' ' + why,
            priority,
            rating,
            reviews,
            score,
            address: place.formattedAddress || null
          });
        }
      }
    }

    // Sort by score, take top numLeads
    let leads = Array.from(leadsMap.values())
      .sort((a, b) => b.score - a.score)
      .slice(0, numLeads);

    // Deduplicate against existing CRM
    try {
      const existing = await pool.query(
        "SELECT LOWER(company) as company FROM prospects WHERE user_id=$1",
        [req.session.user.id]
      );
      const existingNames = new Set(existing.rows.map(r => r.company));
      const before = leads.length;
      leads = leads.filter(l => !existingNames.has((l.company || '').toLowerCase()));
      console.log('Deduped:', before - leads.length, 'already in CRM');
    } catch(e) {
      console.log('Dedup skipped:', e.message);
    }

    console.log('Final:', leads.length, 'scored leads');
    if (leads.length === 0) return res.json({ error: 'No new leads found — all results already in your CRM or low quality. Try a different territory.' });
    res.json({ leads, source: 'google' });
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