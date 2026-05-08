const express = require('express');
const fetch = require('node-fetch');
const { pool } = require('../db');
const router = express.Router();

const CLAUDE_API = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';

router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const userName = req.session.user.name || 'Rep';
  try {
    const followUps = await pool.query(
      "SELECT p.id, p.company, p.category, p.city, p.address, p.phone, p.priority, c.next_step, c.next_step_date, c.outcome as last_outcome, 'follow_up' as call_reason, (CURRENT_DATE - c.next_step_date) as days_overdue FROM calls c JOIN prospects p ON c.prospect_id = p.id WHERE c.user_id = $1 AND c.next_step_date IS NOT NULL AND c.next_step_date <= CURRENT_DATE AND c.next_step IS NOT NULL AND c.next_step != '' ORDER BY c.next_step_date ASC LIMIT 10",
      [uid]
    );
    const neverCalled = await pool.query(
      "SELECT p.id, p.company, p.category, p.city, p.address, p.phone, p.priority, 'new_prospect' as call_reason, p.created_at FROM prospects p WHERE p.user_id = $1 AND p.priority IN ('High', 'Medium') AND p.id NOT IN (SELECT DISTINCT prospect_id FROM calls WHERE user_id = $1) ORDER BY CASE p.priority WHEN 'High' THEN 1 ELSE 2 END, p.created_at ASC LIMIT 10",
      [uid]
    );
    const todayCalls = await pool.query("SELECT COUNT(*) FROM calls WHERE user_id=$1 AND call_date=CURRENT_DATE", [uid]);
    const stats = await pool.query("SELECT COUNT(*) as total, COUNT(CASE WHEN priority='High' THEN 1 END) as hot FROM prospects WHERE user_id=$1", [uid]);
    const userRow = await pool.query('SELECT daily_call_goal FROM users WHERE id=$1', [uid]);
    const dailyGoal = (userRow.rows[0] && userRow.rows[0].daily_call_goal) || 10;
    const callsMadeToday = parseInt(todayCalls.rows[0].count);

    const followUpList = followUps.rows;
    const followUpIds = new Set(followUpList.map(r => r.id));
    const freshList = neverCalled.rows.filter(r => !followUpIds.has(r.id));
    const combinedList = [...followUpList, ...freshList].slice(0, 15);

    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });
    const dayOfWeek = new Date().toLocaleDateString('en-US', { weekday: 'long' });

    const prompt = "You are a sales coach for Compton Group LLC. Today is " + today + ". Write 3-4 sentences for " + userName + ": " + callsMadeToday + "/" + dailyGoal + " calls today, " + followUpList.length + " overdue follow-ups, " + freshList.length + " fresh prospects. Be direct. End with one specific action.";

    let aiMessage = '';
    try {
      const aiRes = await fetch(CLAUDE_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({ model: MODEL, max_tokens: 250, messages: [{ role: 'user', content: prompt }] })
      });
      const aiData = await aiRes.json();
      const blocks = (aiData.content || []).filter(b => b.type === 'text');
      aiMessage = blocks.map(b => b.text).join('') || '';
    } catch(e) {
      aiMessage = "Good morning " + userName + ". You have " + combinedList.length + " prospects today. Start with overdue follow-ups.";
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

router.post('/daily-leads', async (req, res) => {
  const uid = req.session.user.id;
  const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;
  if (!PLACES_KEY) return res.status(500).json({ error: 'Google Places API key not configured' });

  const city = (req.body.city || '').trim() || 'Atlanta GA';
  const brands = Array.isArray(req.body.brands) && req.body.brands.length ? req.body.brands : ['BOSS Products', 'ShurTape', 'Alum-A-Pole'];
  const channel = (req.body.channel || 'Contractor').trim();

  try {
    let mappingsRes = await pool.query(
      'SELECT brand, channel, customer_types FROM brand_mappings WHERE user_id=$1 AND brand=ANY($2) AND channel=$3',
      [uid, brands, channel]
    );
    if (mappingsRes.rows.length === 0) {
      try {
        const { seedDefaultsForUser } = require('./brand_mappings');
        await seedDefaultsForUser(uid);
        mappingsRes = await pool.query(
          'SELECT brand, channel, customer_types FROM brand_mappings WHERE user_id=$1 AND brand=ANY($2) AND channel=$3',
          [uid, brands, channel]
        );
      } catch(e) { console.error('seed error:', e.message); }
    }

    const customerTypeBrands = {};
    for (const row of mappingsRes.rows) {
      const types = Array.isArray(row.customer_types) ? row.customer_types : JSON.parse(row.customer_types || '[]');
      for (const ct of types) {
        if (!customerTypeBrands[ct]) customerTypeBrands[ct] = new Set();
        customerTypeBrands[ct].add(row.brand);
      }
    }

    const existing = await pool.query('SELECT LOWER(company) as company, google_place_id FROM prospects WHERE user_id=$1', [uid]);
    const existingNames = new Set();
    const existingPlaceIds = new Set();
    for (const row of existing.rows) {
      if (row.company) existingNames.add(row.company);
      if (row.google_place_id) existingPlaceIds.add(row.google_place_id);
    }

    const contractorIndicators = ['roofer','roofing contractor','roofing company','roofing &','roof repair','restoration','home improvement','remodeling','general contractor','siding company','window installation','gutter','painters','painting llc','painting co','painting inc','best roofer','roof experts','roof pros','roof masters','roofing inc','roofing llc','roofing pro','roofing co'];
    const dealerIndicators = ['supply','distribution','distributor','wholesale','building products','building materials','lansing','srs','qxo','alside','abc supply','beacon','lumber','hardware','depot','dealer','mart','warehouse','building supply','pro desk','home center'];

    const allLeads = [];
    const sessionSeen = new Set();
    let cityCoords = null;

    for (const customerType of Object.keys(customerTypeBrands)) {
      if (allLeads.length >= 10) break;
      try {
        // First-time setup: geocode the city to lat/lng once per search session
        if (!cityCoords) {
          try {
            const geoRes = await fetch('https://maps.googleapis.com/maps/api/geocode/json?address=' + encodeURIComponent(city) + '&key=' + PLACES_KEY);
            const geoData = await geoRes.json();
            if (geoData.results && geoData.results[0]) {
              const loc = geoData.results[0].geometry.location;
              cityCoords = { lat: loc.lat, lng: loc.lng };
              console.log('Geocoded', city, '->', cityCoords);
            }
          } catch(e) { console.error('Geocode failed:', e.message); }
        }

        const searchQuery = customerType + ' ' + city;
        const searchBody = {
          textQuery: searchQuery,
          maxResultCount: 20
        };

        // Add tight location restriction if we have coordinates
        if (cityCoords) {
          searchBody.locationBias = {
            circle: {
              center: { latitude: cityCoords.lat, longitude: cityCoords.lng },
              radius: 16000  // 10mi soft bias - we filter to 5mi hard with our own distance calc
            }
          };
        }

        const placesRes = await fetch('https://places.googleapis.com/v1/places:searchText', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': PLACES_KEY,
            'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus,places.location'
          },
          body: JSON.stringify(searchBody)
        });
        const data = await placesRes.json();
        const places = data.places || [];

        for (const place of places) {
          if (allLeads.length >= 10) break;
          const company = (place.displayName && place.displayName.text) || '';
          const placeId = place.id || '';
          const companyLower = company.toLowerCase();

          if (!company) continue;
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          if (placeId && existingPlaceIds.has(placeId)) continue;
          if (existingNames.has(companyLower)) continue;
          if (sessionSeen.has(placeId || companyLower)) continue;
          sessionSeen.add(placeId || companyLower);

          const looksLikeContractor = contractorIndicators.some(function(ind) { return companyLower.indexOf(ind) > -1; });
          const looksLikeDealer = dealerIndicators.some(function(ind) { return companyLower.indexOf(ind) > -1; });

          if (channel === 'Dealer') {
            if (!looksLikeDealer) continue;
          }
          if (channel === 'Contractor') {
            if (looksLikeDealer && !looksLikeContractor) continue;
          }

          const addr = (place.formattedAddress || '').split(',');
          const matchedBrands = Array.from(customerTypeBrands[customerType]).join(', ');

          allLeads.push({
            company,
            category: customerType,
            channel: channel,
            city: (addr[1] || '').trim(),
            address: place.formattedAddress || '',
            phone: place.nationalPhoneNumber || '',
            website: place.websiteUri || '',
            products: matchedBrands,
            place_id: placeId,
            territory: city,
            distance_miles: place._distanceMiles ? Math.round(place._distanceMiles * 10) / 10 : null,
            priority: (place.rating >= 4.5 && place.userRatingCount > 20) ? 'High' : (place.rating >= 3.5) ? 'Medium' : 'Low',
            rating: place.rating || null,
            reviews: place.userRatingCount || 0
          });
        }
      } catch(e) { console.error('Search error for', customerType, e.message); }
    }

    // Sort by distance from city center (closest first)
    allLeads.sort(function(a, b) {
      var da = a.distance_miles == null ? 999 : a.distance_miles;
      var db = b.distance_miles == null ? 999 : b.distance_miles;
      return da - db;
    });

    res.json({
      ok: true,
      leads: allLeads.slice(0, 10),
      brands_used: brands,
      channel: channel,
      excluded_count: existingNames.size,
      city_coords: cityCoords
    });
  } catch(e) {
    console.error('daily-leads error:', e);
    res.status(500).json({ error: e.message });
  }
});

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
    if (outcome && outcome.indexOf('Interested') > -1 && outcome.indexOf('Not') === -1) {
      await pool.query("UPDATE prospects SET status='Warm' WHERE id=$1", [prospect_id]);
    } else if (outcome === 'Ready to Order' || outcome === 'Ready to Buy') {
      await pool.query("UPDATE prospects SET status='Hot' WHERE id=$1", [prospect_id]);
    } else if (outcome && outcome.indexOf('Not Interested') > -1) {
      await pool.query("UPDATE prospects SET status='Cold' WHERE id=$1", [prospect_id]);
    }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

module.exports = router;
