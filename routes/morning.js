const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';

// ─── PRODUCT → SEARCH QUERY MAPPING ──────────────────────────────────────────
// Each entry defines what Google Places queries to run and how to score results
const PRODUCT_SEARCH_CONFIG = {
  'BOSS Products': {
    Contractor: [
      { query: 'roofing contractor', score: 10, category: 'Roofing Contractor' },
      { query: 'commercial roofing company', score: 10, category: 'Roofing Contractor' },
      { query: 'residential roofing company', score: 9, category: 'Roofing Contractor' },
      { query: 'roof repair company', score: 9, category: 'Roofing Contractor' },
      { query: 'general contractor roofing', score: 7, category: 'General Contractor' },
    ],
    Dealer: [
      { query: 'roofing supply distributor', score: 10, category: 'Roofing Distributor' },
      { query: 'building materials wholesale', score: 8, category: 'Building Materials' },
      { query: 'roofing wholesale supply', score: 10, category: 'Roofing Distributor' },
      { query: 'ABC Supply roofing', score: 9, category: 'Roofing Distributor' },
      { query: 'Beacon Roofing Supply', score: 9, category: 'Roofing Distributor' },
    ]
  },
  'ShurTape': {
    Contractor: [
      { query: 'roofing contractor', score: 9, category: 'Roofing Contractor' },
      { query: 'deck builder contractor', score: 9, category: 'Deck Contractor' },
      { query: 'siding contractor installation', score: 9, category: 'Siding Contractor' },
      { query: 'window installation contractor', score: 8, category: 'Window Contractor' },
      { query: 'door installation contractor', score: 7, category: 'Door Contractor' },
    ],
    Dealer: [
      { query: 'building products distributor', score: 9, category: 'Building Supply' },
      { query: 'siding supply distributor', score: 10, category: 'Siding Distributor' },
      { query: 'specialty building materials', score: 8, category: 'Building Supply' },
      { query: 'exterior building supply', score: 9, category: 'Building Supply' },
      { query: 'lumber yard building supply', score: 7, category: 'Lumber Yard' },
    ]
  },
  'Alum-A-Pole': {
    Contractor: [
      { query: 'siding contractor installation', score: 10, category: 'Siding Contractor' },
      { query: 'vinyl siding contractor', score: 10, category: 'Siding Contractor' },
      { query: 'James Hardie siding installer', score: 10, category: 'Siding Contractor' },
      { query: 'fiber cement siding company', score: 9, category: 'Siding Contractor' },
      { query: 'cornice contractor soffit fascia', score: 10, category: 'Cornice Contractor' },
      { query: 'exterior siding company', score: 9, category: 'Siding Contractor' },
      { query: 'stucco siding contractor', score: 8, category: 'Siding Contractor' },
    ],
    Dealer: [
      { query: 'siding supply distributor', score: 10, category: 'Siding Distributor' },
      { query: 'building materials distributor siding', score: 9, category: 'Building Supply' },
      { query: 'fastener supply store construction', score: 9, category: 'Fastener Supply' },
      { query: 'construction tool equipment dealer', score: 8, category: 'Equipment Dealer' },
      { query: 'scaffolding rental supply', score: 10, category: 'Scaffolding Dealer' },
      { query: 'exterior building products distributor', score: 9, category: 'Siding Distributor' },
    ]
  }
};

// Google place types that confirm a business is relevant (vs. just having the right name)
const CONTRACTOR_PLACE_TYPES = new Set([
  'roofing_contractor', 'general_contractor',
  'deck_builder', 'siding_contractor', 'window_installation_service',
  'door_supplier', 'masonry_contractor', 'scaffolding_contractor',
  'home_improvement', 'construction_company', 'remodeling_contractor'
]);

const DEALER_PLACE_TYPES = new Set([
  'building_materials_store', 'hardware_store', 'lumber_yard',
  'wholesale_grocer', 'warehouse', 'distribution_center',
  'paint_store', 'home_improvement_store', 'roofing_supply_store'
]);


// Hard block — never return paint-related results for Alum-A-Pole
const PAINT_BLOCKED_KEYWORDS = ['paint', 'painting', 'painter', 'painters'];
function isPaintBlocked(name, brand) {
  if ((brand || '').toLowerCase().includes('alum')) {
    const lower = (name || '').toLowerCase();
    return PAINT_BLOCKED_KEYWORDS.some(kw => lower.includes(kw));
  }
  return false;
}

// Haversine distance in miles
function distanceMiles(lat1, lng1, lat2, lng2) {
  const R = 3958.8;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 +
    Math.cos(lat1 * Math.PI/180) * Math.cos(lat2 * Math.PI/180) * Math.sin(dLng/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

// Calculate opportunity score (1-10) based on multiple signals
function calcOpportunityScore(baseScore, distMi, reviewCount, rating, productMatches, channel) {
  let score = baseScore;

  // Distance bonus/penalty
  if (distMi <= 10) score += 1;
  else if (distMi <= 25) score += 0;
  else if (distMi <= 50) score -= 1;
  else score -= 2;

  // Review count signals — more reviews = more established business = better prospect
  if (reviewCount >= 50) score += 1;
  else if (reviewCount >= 20) score += 0.5;
  else if (reviewCount < 5) score -= 0.5;

  // Multi-product bonus
  if (productMatches >= 3) score += 1;
  else if (productMatches >= 2) score += 0.5;

  // Dealer channel tends to be higher value per call
  if (channel === 'Dealer') score += 0.5;

  return Math.min(10, Math.max(1, Math.round(score)));
}

// ─── MORNING BRIEFING ────────────────────────────────────────────────────────
router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const userName = req.session.user.name || 'Rep';
  try {
    const followUps = await pool.query(
      `SELECT p.id, p.company, p.category, p.city, p.address, p.phone, p.priority,
              c.next_step, c.next_step_date, c.outcome as last_outcome,
              'follow_up' as call_reason,
              (CURRENT_DATE - c.next_step_date) as days_overdue
       FROM calls c JOIN prospects p ON c.prospect_id = p.id
       WHERE c.user_id = $1 AND c.next_step_date IS NOT NULL
         AND c.next_step_date <= CURRENT_DATE
         AND c.next_step IS NOT NULL AND c.next_step != ''
       ORDER BY c.next_step_date ASC LIMIT 10`,
      [uid]
    );
    const neverCalled = await pool.query(
      `SELECT p.id, p.company, p.category, p.city, p.address, p.phone, p.priority,
              'new_prospect' as call_reason, p.created_at
       FROM prospects p
       WHERE p.user_id = $1 AND p.priority IN ('High', 'Medium')
         AND p.id NOT IN (SELECT DISTINCT prospect_id FROM calls WHERE user_id = $1)
       ORDER BY CASE p.priority WHEN 'High' THEN 1 ELSE 2 END, p.created_at ASC LIMIT 10`,
      [uid]
    );
    const todayCalls = await pool.query(
      'SELECT COUNT(*) FROM calls WHERE user_id=$1 AND call_date=CURRENT_DATE', [uid]
    );
    const stats = await pool.query(
      `SELECT COUNT(*) as total, COUNT(CASE WHEN priority='High' THEN 1 END) as hot
       FROM prospects WHERE user_id=$1`, [uid]
    );
    const userRow = await pool.query('SELECT daily_call_goal FROM users WHERE id=$1', [uid]);
    const dailyGoal = (userRow.rows[0] && userRow.rows[0].daily_call_goal) || 10;
    const callsMadeToday = parseInt(todayCalls.rows[0].count);

    const followUpList = followUps.rows;
    const followUpIds = new Set(followUpList.map(r => r.id));
    const freshList = neverCalled.rows.filter(r => !followUpIds.has(r.id));
    const combinedList = [...followUpList, ...freshList].slice(0, 15);

    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });
    const dayOfWeek = new Date().toLocaleDateString('en-US', { weekday: 'long' });

    const prompt = `You are a sales coach for Compton Group LLC, a manufacturer's rep selling BOSS Roofing Sealants, ShurTape, and Alum-A-Pole in the Southeast US. Today is ${today}. Write 3-4 direct, punchy sentences for ${userName}: they've made ${callsMadeToday}/${dailyGoal} calls today, have ${followUpList.length} overdue follow-ups, and ${freshList.length} fresh prospects. Be a motivating coach, not a corporate bot. End with ONE specific action they should take right now.`;

    let aiMessage = '';
    try {
      const aiRes = await fetch(CLAUDE_API, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': process.env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify({ model: MODEL, max_tokens: 250, messages: [{ role: 'user', content: prompt }] })
      });
      const aiData = await aiRes.json();
      aiMessage = (aiData.content || []).filter(b => b.type === 'text').map(b => b.text).join('') || '';
    } catch(e) {
      aiMessage = `Good morning ${userName}. You have ${combinedList.length} prospects today. Start with your overdue follow-ups first.`;
    }

    res.json({
      user: { name: userName },
      date: today,
      dayOfWeek,
      aiMessage,
      stats: {
        callsMadeToday,
        dailyGoal,
        overdueFollowUps: followUpList.length,
        freshProspects: freshList.length,
        hotProspects: parseInt(stats.rows[0].hot)
      },
      callList: combinedList
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── DAILY LEAD FINDER (IMPROVED) ────────────────────────────────────────────
router.post('/daily-leads', async (req, res) => {
  const uid = req.session.user.id;
  const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;
  if (!PLACES_KEY) return res.status(500).json({ error: 'Google Places API key not configured' });

  // Get the rep's territory and home base from their user record
  const userRow = await pool.query('SELECT territory, home_base_lat, home_base_lng, home_base_city FROM users WHERE id=$1', [uid]);
  const userInfo = userRow.rows[0] || {};

  const city = (req.body.city || userInfo.home_base_city || '').trim() || 'Atlanta GA';
  const brands = Array.isArray(req.body.brands) && req.body.brands.length
    ? req.body.brands
    : ['BOSS Products', 'ShurTape', 'Alum-A-Pole'];
  const channel = (req.body.channel || 'Contractor').trim();
  const radiusMiles = parseInt(req.body.radius_miles) || 50; // Default 50mi vs old 5mi
  const radiusMeters = Math.min(radiusMiles * 1609, 80000); // Up to 80km (~50mi)

  try {
    // Get existing prospects to avoid duplicates
    const existing = await pool.query(
      'SELECT LOWER(company) as company, google_place_id FROM prospects WHERE user_id=$1', [uid]
    );
    const existingNames = new Set(existing.rows.map(r => r.company).filter(Boolean));
    const existingPlaceIds = new Set(existing.rows.map(r => r.google_place_id).filter(Boolean));

    // Geocode the city — always use the typed city first, fall back to home base
    let centerCoords = null;
    try {
      const geoRes = await fetch(
        `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(city)}&key=${PLACES_KEY}`
      );
      const geoData = await geoRes.json();
      if (geoData.results && geoData.results[0]) {
        const loc = geoData.results[0].geometry.location;
        centerCoords = { lat: loc.lat, lng: loc.lng };
        console.log(`Geocoded "${city}" → ${loc.lat}, ${loc.lng}`);
      }
    } catch(e) { console.error('Geocode failed:', e.message); }
    // Fall back to stored home base if geocode failed
    if (!centerCoords && userInfo.home_base_lat && userInfo.home_base_lng) {
      centerCoords = { lat: parseFloat(userInfo.home_base_lat), lng: parseFloat(userInfo.home_base_lng) };
      console.log('Using home base coords as fallback');
    }

    if (!centerCoords) {
      return res.status(500).json({ error: `Could not locate city: ${city}. Please check the city name.` });
    }

    // Build all search queries from the brand/channel config
    const searchConfigs = [];
    for (const brand of brands) {
      const config = PRODUCT_SEARCH_CONFIG[brand];
      if (!config) continue;
      const channelConfig = config[channel];
      if (!channelConfig) continue;
      for (const sc of channelConfig) {
        searchConfigs.push({ ...sc, brand });
      }
    }

    // Deduplicate search queries (same query from multiple brands)
    const seenQueries = new Set();
    const uniqueConfigs = searchConfigs.filter(sc => {
      if (seenQueries.has(sc.query)) return false;
      seenQueries.add(sc.query);
      return true;
    });

    const allLeads = [];
    const sessionSeen = new Set();

    for (const config of uniqueConfigs) {
      if (allLeads.length >= 15) break; // Collect 15, return top 10

      try {
        const searchBody = {
          textQuery: `${config.query} near ${city}`,
          maxResultCount: 20,
          locationBias: {
            circle: {
              center: { latitude: centerCoords.lat, longitude: centerCoords.lng },
              radius: radiusMeters
            }
          }
        };

        const placesRes = await fetch('https://places.googleapis.com/v1/places:searchText', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': PLACES_KEY,
            'X-Goog-FieldMask': [
              'places.id',
              'places.displayName',
              'places.formattedAddress',
              'places.nationalPhoneNumber',
              'places.websiteUri',
              'places.rating',
              'places.userRatingCount',
              'places.businessStatus',
              'places.location',
              'places.primaryTypeDisplayName',
              'places.types'
            ].join(',')
          },
          body: JSON.stringify(searchBody)
        });

        const data = await placesRes.json();
        const places = data.places || [];

        for (const place of places) {
          if (allLeads.length >= 15) break;

          const company = place.displayName?.text || '';
          const placeId = place.id || '';
          const companyLower = company.toLowerCase();

          // Basic validity checks
          if (!company) continue;
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          if (placeId && existingPlaceIds.has(placeId)) continue;
          if (existingNames.has(companyLower)) continue;
          if (sessionSeen.has(placeId || companyLower)) continue;

          // Hard block — never serve paint shops/painters as Alum-A-Pole leads
          if (isPaintBlocked(company, config.brand)) continue;
          sessionSeen.add(placeId || companyLower);

          // Distance filter — use the rep's actual radius
          let distMi = null;
          if (place.location?.latitude != null) {
            distMi = Math.round(distanceMiles(
              centerCoords.lat, centerCoords.lng,
              place.location.latitude, place.location.longitude
            ) * 10) / 10;
            if (distMi > radiusMiles) continue;
          }

          // Use Google's place types for accurate classification (not just name matching)
          const placeTypes = new Set(place.types || []);
          const primaryType = place.primaryTypeDisplayName?.text || config.category;

          // Soft channel filter — only block clear mismatches, not ambiguous types
          // Many specialty dealers (siding supply, fastener stores) don't have explicit Google types
          const isClassifiedContractor = [...placeTypes].some(t => CONTRACTOR_PLACE_TYPES.has(t));
          const isClassifiedDealer = [...placeTypes].some(t => DEALER_PLACE_TYPES.has(t));

          // Only hard-exclude if Google is very confident it's the WRONG type
          // Allow anything ambiguous (no classification) through — better to include than miss
          if (channel === 'Dealer' && isClassifiedContractor && !isClassifiedDealer && placeTypes.size > 2) continue;
          if (channel === 'Contractor' && isClassifiedDealer && !isClassifiedContractor && placeTypes.size > 2) continue;

          // Count how many product lines this prospect is relevant to
          const matchingBrands = brands.filter(brand => {
            const bc = PRODUCT_SEARCH_CONFIG[brand]?.[channel];
            return bc?.some(sc => sc.category === config.category || sc.query.split(' ')[0] === config.query.split(' ')[0]);
          });

          const opportunityScore = calcOpportunityScore(
            config.score,
            distMi || 25,
            place.userRatingCount || 0,
            place.rating || 0,
            matchingBrands.length,
            channel
          );

          // Parse city/state from formatted address
          const addrParts = (place.formattedAddress || '').split(',').map(s => s.trim());
          const cityName = addrParts.length >= 2 ? addrParts[addrParts.length - 3] || addrParts[1] : city;
          const stateZip = addrParts.length >= 2 ? addrParts[addrParts.length - 2] || '' : '';
          const stateName = stateZip.split(' ')[0] || 'GA';

          allLeads.push({
            company,
            category: config.category,
            channel,
            city: cityName,
            state: stateName,
            address: place.formattedAddress || '',
            distance_miles: distMi,
            phone: place.nationalPhoneNumber || '',
            website: place.websiteUri || '',
            products: matchingBrands.join(', ') || brands.join(', '),
            place_id: placeId,
            territory: city,
            opportunity_score: opportunityScore,
            priority: opportunityScore >= 8 ? 'High' : opportunityScore >= 6 ? 'Medium' : 'Low',
            rating: place.rating || null,
            reviews: place.userRatingCount || 0,
            primary_type: primaryType,
            matched_query: config.query
          });
        }
      } catch(e) {
        console.error('Places search error for query:', config.query, e.message);
      }
    }

    // Sort: opportunity score DESC, then distance ASC
    allLeads.sort((a, b) => {
      if (b.opportunity_score !== a.opportunity_score) return b.opportunity_score - a.opportunity_score;
      const da = a.distance_miles ?? 999;
      const db = b.distance_miles ?? 999;
      return da - db;
    });

    const topLeads = allLeads.slice(0, 10);

    res.json({
      ok: true,
      leads: topLeads,
      brands_used: brands,
      channel,
      radius_miles: radiusMiles,
      center: { city, coords: centerCoords },
      total_found: allLeads.length,
      excluded_count: existingNames.size
    });

  } catch(e) {
    console.error('daily-leads error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ─── LOG CALL ────────────────────────────────────────────────────────────────
router.post('/log-call', async (req, res) => {
  const uid = req.session.user.id;
  const { prospect_id, outcome, contact_name, next_step, next_step_date } = req.body;
  if (!prospect_id) return res.status(400).json({ error: 'prospect_id required' });
  try {
    const today = new Date().toISOString().split('T')[0];
    await pool.query(
      'INSERT INTO calls (user_id, prospect_id, call_date, outcome, next_step, next_step_date) VALUES ($1,$2,$3,$4,$5,$6)',
      [uid, prospect_id, today, outcome || '', next_step || '', next_step_date || null]
    );
    if (outcome && outcome.includes('Interested') && !outcome.includes('Not')) {
      await pool.query("UPDATE prospects SET status='Warm' WHERE id=$1", [prospect_id]);
    } else if (outcome === 'Ready to Order' || outcome === 'Ready to Buy') {
      await pool.query("UPDATE prospects SET status='Hot' WHERE id=$1", [prospect_id]);
    } else if (outcome && outcome.includes('Not Interested')) {
      await pool.query("UPDATE prospects SET status='Cold' WHERE id=$1", [prospect_id]);
    }
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
