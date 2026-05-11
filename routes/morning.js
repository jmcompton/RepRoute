const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';

// ─── SEGMENT → SEARCH QUERY MAPPING ─────────────────────────────────────────
// 7 named segments replace the old Contractor/Dealer split.
// Each segment has targeted queries + a relevantBrands list for scoring.
const SEGMENT_SEARCH_CONFIG = {
  'Window/Door Installer': {
    relevantBrands: ['ShurTape'],
    queries: [
      { query: 'new construction window installation contractor', score: 10, category: 'Window/Door Installer' },
      { query: 'new construction entry door installer', score: 10, category: 'Window/Door Installer' },
      { query: 'window door installation new build residential', score: 9, category: 'Window/Door Installer' },
      { query: 'window installation company residential', score: 8, category: 'Window/Door Installer' },
      { query: 'exterior entry door installation contractor', score: 8, category: 'Window/Door Installer' },
    ]
  },
  'Deck Contractor': {
    relevantBrands: ['ShurTape'],
    queries: [
      { query: 'deck builder contractor', score: 10, category: 'Deck Contractor' },
      { query: 'deck installation company', score: 10, category: 'Deck Contractor' },
      { query: 'deck construction new build', score: 9, category: 'Deck Contractor' },
      { query: 'composite deck installer', score: 9, category: 'Deck Contractor' },
    ]
  },
  'Roofing Contractor': {
    relevantBrands: ['BOSS Products', 'ShurTape'],
    queries: [
      { query: 'roofing contractor', score: 10, category: 'Roofing Contractor' },
      { query: 'commercial roofing company', score: 10, category: 'Roofing Contractor' },
      { query: 'residential roofing contractor', score: 9, category: 'Roofing Contractor' },
      { query: 'roof replacement contractor', score: 9, category: 'Roofing Contractor' },
      { query: 'roofing company new construction', score: 9, category: 'Roofing Contractor' },
    ]
  },
  'Roofing Distributor': {
    relevantBrands: ['BOSS Products'],
    queries: [
      { query: 'roofing supply distributor', score: 10, category: 'Roofing Distributor' },
      { query: 'roofing wholesale supply', score: 10, category: 'Roofing Distributor' },
      { query: 'ABC Supply roofing materials', score: 9, category: 'Roofing Distributor' },
      { query: 'Beacon Roofing Supply', score: 9, category: 'Roofing Distributor' },
      { query: 'building materials wholesale roofing', score: 8, category: 'Roofing Distributor' },
    ]
  },
  'Siding Contractor': {
    relevantBrands: ['Alum-A-Pole', 'ShurTape'],
    queries: [
      { query: 'siding contractor installation', score: 10, category: 'Siding Contractor' },
      { query: 'vinyl siding contractor', score: 10, category: 'Siding Contractor' },
      { query: 'James Hardie siding installer', score: 10, category: 'Siding Contractor' },
      { query: 'fiber cement siding company', score: 9, category: 'Siding Contractor' },
      { query: 'exterior siding company', score: 9, category: 'Siding Contractor' },
    ]
  },
  'Cornice Contractor': {
    relevantBrands: ['Alum-A-Pole'],
    queries: [
      { query: 'cornice contractor soffit fascia', score: 10, category: 'Cornice Contractor' },
      { query: 'soffit fascia contractor installer', score: 10, category: 'Cornice Contractor' },
      { query: 'fascia board installation contractor', score: 9, category: 'Cornice Contractor' },
      { query: 'exterior trim contractor soffit', score: 9, category: 'Cornice Contractor' },
    ]
  },
  'Fastener/Tool Dealer': {
    relevantBrands: ['Alum-A-Pole'],
    queries: [
      { query: 'fastener supply store construction', score: 10, category: 'Fastener/Tool Dealer' },
      { query: 'construction tool equipment dealer', score: 10, category: 'Fastener/Tool Dealer' },
      { query: 'scaffolding rental supply construction', score: 10, category: 'Fastener/Tool Dealer' },
      { query: 'ladder supply company construction', score: 10, category: 'Fastener/Tool Dealer' },
      { query: 'extension ladder dealer supply store', score: 9, category: 'Fastener/Tool Dealer' },
      { query: 'siding supply distributor tools', score: 9, category: 'Fastener/Tool Dealer' },
      { query: 'building materials distributor siding', score: 8, category: 'Fastener/Tool Dealer' },
    ]
  },
  'Siding Distributor': {
    relevantBrands: ['Alum-A-Pole', 'ShurTape'],
    queries: [
      { query: 'siding supply distributor wholesale', score: 10, category: 'Siding Distributor' },
      { query: 'exterior building products distributor', score: 10, category: 'Siding Distributor' },
      { query: 'siding materials wholesale supplier', score: 9, category: 'Siding Distributor' },
      { query: 'vinyl siding wholesale distributor', score: 9, category: 'Siding Distributor' },
      { query: 'James Hardie siding supply dealer', score: 8, category: 'Siding Distributor' },
    ]
  }
};

// Legacy map kept for any references — maps old channel names to segment groups
const CHANNEL_TO_SEGMENTS = {
  'Contractor': ['Window/Door Installer','Deck Contractor','Roofing Contractor','Siding Contractor','Cornice Contractor'],
  'Dealer': ['Roofing Distributor','Fastener/Tool Dealer']
};

// Shim: build a PRODUCT_SEARCH_CONFIG-compatible lookup for the scoring helper
const PRODUCT_SEARCH_CONFIG = {};

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

// Block remodeling/renovation companies from ShurTape window & door categories
// Window/door tape is for new construction — remodelers use different products
const REMODEL_BLOCK_KEYWORDS = ['remodel', 'remodeling', 'renovation', 'renovations', 'home improvement', 'general contractor', 'general contracting'];
const GARAGE_DOOR_KEYWORDS = ['garage door', 'garage doors', 'overhead door', 'overhead doors'];
const WINDOW_DOOR_CATEGORIES = ['Window Contractor', 'Door Contractor', 'Window/Door Installer'];
function isRemodelBlocked(name, category, brand) {
  const lower = (name || '').toLowerCase();
  // Hard block garage doors from ALL window/door results regardless of brand
  if (WINDOW_DOOR_CATEGORIES.includes(category) || category === 'Window/Door Installer') {
    if (GARAGE_DOOR_KEYWORDS.some(kw => lower.includes(kw))) return true;
  }
  // Block remodelers from ShurTape window/door results
  if ((brand || '').toLowerCase().includes('shurtape') || (brand || '').toLowerCase().includes('shur') || category === 'Window/Door Installer') {
    if (WINDOW_DOOR_CATEGORIES.includes(category)) {
      return REMODEL_BLOCK_KEYWORDS.some(kw => lower.includes(kw));
    }
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

  try {
    // Get ALL team contacts (own + teammates) — no rep should call on another rep's contact
    const existing = await pool.query(
      `SELECT
        LOWER(p.company) as company,
        p.google_place_id,
        LOWER(TRIM(COALESCE(p.address,''))) as address,
        COALESCE(p.phone,'') as phone
       FROM prospects p
       JOIN users u ON p.user_id = u.id
       WHERE u.role IN ('rep','manager','admin')`
    );
    const existingNames    = new Set(existing.rows.map(r => r.company).filter(Boolean));
    const existingPlaceIds = new Set(existing.rows.map(r => r.google_place_id).filter(Boolean));
    const existingAddresses = new Set(existing.rows.map(r => r.address).filter(a => a && a.length > 5));
    // Strip non-digits in JS (avoids REGEXP_REPLACE Postgres version issues)
    const existingPhones   = new Set(
      existing.rows.map(r => (r.phone || '').replace(/[^0-9]/g, '')).filter(p => p.length >= 7)
    );

    // Also exclude leads already shown in this browser session (for refresh)
    const shownPlaceIds = Array.isArray(req.body.shown_place_ids) ? req.body.shown_place_ids : [];
    const shownNames = Array.isArray(req.body.shown_names) ? req.body.shown_names.map(n => n.toLowerCase()) : [];
    for (const id of shownPlaceIds) existingPlaceIds.add(id);
    for (const n of shownNames) existingNames.add(n);

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

    // Build search queries from segment config
    // `channel` is now a segment name (e.g. 'Roofing Contractor', 'Fastener/Tool Dealer')
    // Fall back gracefully if old Contractor/Dealer value passed
    let segmentName = channel;
    if (channel === 'Contractor' || channel === 'Dealer') {
      // Legacy fallback — just use first matching segment
      const fallbackList = CHANNEL_TO_SEGMENTS[channel] || ['Roofing Contractor'];
      segmentName = fallbackList[0];
    }

    const segmentConfig = SEGMENT_SEARCH_CONFIG[segmentName];
    const searchConfigs = segmentConfig
      ? segmentConfig.queries.map(q => ({ ...q, brand: segmentName, segment: segmentName }))
      : [];

    // Deduplicate (shouldn't be needed per-segment but kept for safety)
    const seenQueries = new Set();
    const uniqueConfigs = searchConfigs.filter(sc => {
      if (seenQueries.has(sc.query)) return false;
      seenQueries.add(sc.query);
      return true;
    });

    const allLeads = [];
    const sessionSeen = new Set();

    for (const config of uniqueConfigs) {
      if (allLeads.length >= 40) break; // Collect 40, return top 10 (more headroom after dedup/filter)

      try {
        // City in textQuery for relevance + locationBias circle to pin results locally
        const radiusMeters = Math.min(radiusMiles * 1609.34, 48000);
        const searchBody = {
          textQuery: `${config.query} ${city}`,
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
          if (allLeads.length >= 40) break;

          const company = place.displayName?.text || '';
          const placeId = place.id || '';
          const companyLower = company.toLowerCase();

          // Basic validity checks
          if (!company) continue;
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          if (placeId && existingPlaceIds.has(placeId)) continue;
          if (existingNames.has(companyLower)) continue;
          if (sessionSeen.has(placeId || companyLower)) continue;

          // Skip if address already exists in contacts (catches same location, different name)
          const placeAddr = (place.formattedAddress || '').toLowerCase().trim();
          if (placeAddr.length > 5 && existingAddresses.has(placeAddr)) continue;

          // Skip if phone already exists in contacts
          const placePhone = (place.nationalPhoneNumber || '').replace(/[^0-9]/g, '');
          if (placePhone.length >= 7 && existingPhones.has(placePhone)) continue;

          // Hard block — never serve paint shops/painters as Alum-A-Pole leads
          if (isPaintBlocked(company, config.brand)) continue;
          // Hard block — never serve remodelers as ShurTape window/door leads
          if (isRemodelBlocked(company, config.category, config.brand)) continue;
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
          // For scoring: relevantBrands from segment config
          const matchingBrands = (SEGMENT_SEARCH_CONFIG[segmentName]?.relevantBrands || [segmentName]);

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
            territory: segmentName,
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
