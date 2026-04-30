const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;

function getSearchTerms(product, customerType) {
  const p = (product || '').toLowerCase();
  const ct = (customerType || '').toLowerCase();

  if (ct && ct !== 'any building products buyer') {
    return [customerType];
  }

  if (p.includes('alum') || p.includes('scaffolding')) {
    return ['siding contractor', 'James Hardie installer', 'fiber cement siding', 'exterior siding contractor', 'stucco contractor'];
  }
  if (p.includes('soudal') || p.includes('sealant') || p.includes('adhesive')) {
    return ['window installation contractor', 'door installation contractor', 'glazing contractor', 'waterproofing contractor', 'insulation contractor'];
  }
  if (p.includes('shurtape') || p.includes('flashing')) {
    return ['roofing contractor', 'window installer', 'deck contractor', 'home builder', 'remodeling contractor'];
  }
  if (p.includes('framing') || p.includes('fortress') && !p.includes('rail')) {
    return ['deck builder', 'deck contractor', 'remodeling contractor', 'general contractor', 'custom home builder'];
  }
  if (p.includes('railing') || p.includes('fortress')) {
    return ['deck contractor', 'fence contractor', 'railing installer', 'general contractor', 'remodeling contractor'];
  }
  return ['general contractor', 'building contractor', 'construction company', 'remodeling contractor', 'home builder'];
}

function getProductWhy(product, category) {
  const p = (product || '').toLowerCase();
  if (p.includes('alum') || p.includes('scaffolding'))
    return 'needs OSHA-compliant scaffolding for exterior work at height';
  if (p.includes('soudal') || p.includes('sealant'))
    return 'uses high-performance sealants and adhesives on every install';
  if (p.includes('shurtape') || p.includes('flashing'))
    return 'requires code-compliant flashing tape for moisture barriers';
  if (p.includes('framing'))
    return 'needs rot-proof steel framing as a wood alternative for decks';
  if (p.includes('railing'))
    return 'installs railing systems on decks, stairs, and porches';
  return 'purchases building products for construction projects';
}

// Search Google Places for real businesses
router.post('/places-leads', async (req, res) => {
  const { category, territory, count, customer_type } = req.body;
  const user = req.session.user;
  const product = category;
  const numLeads = Math.min(parseInt(count) || 20, 50);
  const searchTerms = getSearchTerms(product, customer_type);
  const why = getProductWhy(product, customer_type);

  // Parse territory into location string
  const loc = territory || user.territory || 'Atlanta, GA';

  try {
    // Step 1: Geocode the territory to get lat/lng
    const geoRes = await fetch(
      `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(loc)}&key=${PLACES_KEY}`
    );
    const geoData = await geoRes.json();
    if (!geoData.results || geoData.results.length === 0) {
      return res.json({ error: 'Could not find that location. Try a city name like "Atlanta, GA"' });
    }
    const { lat, lng } = geoData.results[0].geometry.location;

    // Step 2: Search Places for each search term
    const leadsMap = new Map();
    const perTerm = Math.ceil(numLeads / searchTerms.length);

    for (const term of searchTerms) {
      if (leadsMap.size >= numLeads) break;

      const searchRes = await fetch(
        `https://maps.googleapis.com/maps/api/place/textsearch/json?query=${encodeURIComponent(term)}&location=${lat},${lng}&radius=80000&key=${PLACES_KEY}`
      );
      const searchData = await searchRes.json();
      if (!searchData.results) continue;

      for (const place of searchData.results) {
        if (leadsMap.size >= numLeads) break;
        if (leadsMap.has(place.place_id)) continue;

        // Step 3: Get place details for phone, website
        const detailRes = await fetch(
          `https://maps.googleapis.com/maps/api/place/details/json?place_id=${place.place_id}&fields=name,formatted_phone_number,website,formatted_address,rating,business_status,types&key=${PLACES_KEY}`
        );
        const detailData = await detailRes.json();
        const d = detailData.result || {};

        if (d.business_status === 'CLOSED_PERMANENTLY') continue;

        const addrParts = (place.formatted_address || '').split(',');
        const city = addrParts[1]?.trim() || '';
        const stateZip = addrParts[2]?.trim() || '';
        const state = stateZip.split(' ')[0] || '';

        leadsMap.set(place.place_id, {
          company: d.name || place.name,
          category: term,
          city,
          state,
          phone: d.formatted_phone_number || null,
          email: null,
          website: d.website || null,
          contact: null,
          products: product,
          why: `This ${term} ${why}`,
          priority: (place.rating >= 4.5 && place.user_ratings_total > 20) ? 'High' :
                    (place.rating >= 3.5) ? 'Medium' : 'Low',
          rating: place.rating || null,
          reviews: place.user_ratings_total || 0,
          address: place.formatted_address || null
        });
      }

      // Handle pagination for more results
      if (searchData.next_page_token && leadsMap.size < numLeads) {
        await new Promise(r => setTimeout(r, 2000));
        const page2Res = await fetch(
          `https://maps.googleapis.com/maps/api/place/textsearch/json?pagetoken=${searchData.next_page_token}&key=${PLACES_KEY}`
        );
        const page2Data = await page2Res.json();
        for (const place of (page2Data.results || [])) {
          if (leadsMap.size >= numLeads) break;
          if (leadsMap.has(place.place_id)) continue;
          const addrParts = (place.formatted_address || '').split(',');
          leadsMap.set(place.place_id, {
            company: place.name,
            category: term,
            city: addrParts[1]?.trim() || '',
            state: addrParts[2]?.trim().split(' ')[0] || '',
            phone: null,
            email: null,
            website: null,
            contact: null,
            products: product,
            why: `This ${term} ${why}`,
            priority: (place.rating >= 4.5 && place.user_ratings_total > 20) ? 'High' : 'Medium',
            rating: place.rating || null,
            reviews: place.user_ratings_total || 0,
            address: place.formatted_address || null
          });
        }
      }
    }

    const leads = Array.from(leadsMap.values()).slice(0, numLeads);
    res.json({ leads, source: 'google' });
  } catch(e) {
    console.error('Places API error:', e.message);
    res.json({ error: 'Google Places search failed: ' + e.message });
  }
});

module.exports = router;