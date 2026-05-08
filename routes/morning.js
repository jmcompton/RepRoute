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
    const followUps = await pool.query(`
      SELECT p.id, p.company, p.category, p.city, p.phone, p.priority,
             c.next_step, c.next_step_date, 'follow_up' as call_reason,
             (CURRENT_DATE - c.next_step_date) as days_overdue
      FROM calls c JOIN prospects p ON c.prospect_id = p.id
      WHERE c.user_id = $1 AND c.next_step_date IS NOT NULL
        AND c.next_step_date <= CURRENT_DATE
        AND c.next_step IS NOT NULL AND c.next_step != ''
      ORDER BY c.next_step_date ASC LIMIT 10
    `, [uid]);

    const neverCalled = await pool.query(`
      SELECT p.id, p.company, p.category, p.city, p.address, p.phone, p.priority,
             'new_prospect' as call_reason, p.created_at
      FROM prospects p
      WHERE p.user_id = $1 AND p.priority IN ('High', 'Medium')
        AND p.id NOT IN (SELECT DISTINCT prospect_id FROM calls WHERE user_id = $1)
      ORDER BY CASE p.priority WHEN 'High' THEN 1 ELSE 2 END, p.created_at ASC LIMIT 10
    `, [uid]);

    const todayCalls = await pool.query(
      'SELECT COUNT(*) FROM calls WHERE user_id=$1 AND call_date=CURRENT_DATE', [uid]);
    const stats = await pool.query(
      'SELECT COUNT(*) as total, COUNT(CASE WHEN priority=\'High\' THEN 1 END) as hot FROM prospects WHERE user_id=$1', [uid]);
    const userRow = await pool.query('SELECT daily_call_goal FROM users WHERE id=$1', [uid]);
    const dailyGoal = userRow.rows[0]?.daily_call_goal || 10;
    const callsMadeToday = parseInt(todayCalls.rows[0].count);

    const followUpList = followUps.rows;
    const followUpIds = new Set(followUpList.map(r => r.id));
    const freshList = neverCalled.rows.filter(r => !followUpIds.has(r.id));
    const combinedList = [...followUpList, ...freshList].slice(0, 15);

    const today = new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' });
    const dayOfWeek = new Date().toLocaleDateString('en-US', { weekday: 'long' });
    const dayFocus = {
      Monday: 'Dealers and distributors today. Start the week with your biggest accounts.',
      Tuesday: 'Deck contractors. Follow up on any samples you dropped last week.',
      Wednesday: 'Window and siding contractors. Mid-week push.',
      Thursday: 'Roofing contractors. Close out your active deals.',
      Friday: 'Wrap up follow-ups and prep next week.'
    };

    const prompt = `You are a sales coach for Compton Group LLC, a manufacturer's rep firm in Atlanta selling Alum-A-Pole, Soudal/Boss sealants, ShurTape, Fortress Steel Framing, and Fortress Railing.

Today is ${today}. Write a short morning message for ${userName}, a sales rep.

Situation: ${callsMadeToday} calls made today out of ${dailyGoal} goal. ${followUpList.length} overdue follow-ups. ${freshList.length} fresh high-priority prospects. Today's focus: ${dayFocus[dayOfWeek] || 'Stay on your hottest prospects.'}

Write 3-4 sentences max. Casual and direct like a coach, not a robot. End with one specific action they should do first. No bullet points, no em dashes.`;

    let aiMessage = '';
    try {
      const aiRes = await fetch(CLAUDE_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
        body: JSON.stringify({ model: MODEL, max_tokens: 250, messages: [{ role: 'user', content: prompt }] })
      });
      const aiData = await aiRes.json();
      aiMessage = aiData.content?.filter(b => b.type === 'text').map(b => b.text).join('') || '';
    } catch(e) {
      aiMessage = `Good morning ${userName}. You've got ${combinedList.length} prospects to hit today. Start with your overdue follow-ups first, then work the fresh leads.`;
    }

    res.json({
      user: { name: userName }, date: today, dayOfWeek, aiMessage,
      stats: { callsMadeToday, dailyGoal, overdueFollowUps: followUpList.length, freshProspects: freshList.length, hotProspects: parseInt(stats.rows[0].hot) },
      callList: combinedList
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

router.post('/log-call', async (req, res) => {
  const uid = req.session.user.id;
  const { prospect_id, outcome, next_step, next_step_date } = req.body;
  if (!prospect_id) return res.status(400).json({ error: 'prospect_id required' });
  try {
    const today = new Date().toISOString().split('T')[0];
    await pool.query(
      'INSERT INTO calls (user_id, prospect_id, call_date, outcome, next_step, next_step_date) VALUES ($1,$2,$3,$4,$5,$6)',
      [uid, prospect_id, today, outcome || '', next_step || '', next_step_date || null]);
    if (outcome === 'Interested') await pool.query("UPDATE prospects SET status='Warm' WHERE id=$1", [prospect_id]);
    else if (outcome === 'Ready to Buy') await pool.query("UPDATE prospects SET status='Hot' WHERE id=$1", [prospect_id]);
    else if (outcome === 'Not Interested') await pool.query("UPDATE prospects SET status='Cold' WHERE id=$1", [prospect_id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});



// ─── DAILY LEADS — uses Google Places API ───────────────────────────────────
const fetchPlaces = require('node-fetch');

router.post('/daily-leads', async (req, res) => {
  const uid = req.session.user.id;
  const fetchPlaces = require('node-fetch');
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

    for (const customerType of Object.keys(customerTypeBrands)) {
      if (allLeads.length >= 10) break;
      try {
        const searchQuery = customerType + ' ' + city;
        const placesRes = await fetchPlaces('https://places.googleapis.com/v1/places:searchText', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': PLACES_KEY,
            'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus'
          },
          body: JSON.stringify({ textQuery: searchQuery, maxResultCount: 20 })
        });
        const data = await placesRes.json();
        const places = data.places || [];

        for (const place of places) {
          if (allLeads.length >= 10) break;
          const company = place.displayName?.text || '';
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
            if (looksLikeContractor && !looksLikeDealer) continue;
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
            city: addr[1]?.trim() || '',
            address: place.formattedAddress || '',
            phone: place.nationalPhoneNumber || '',
            website: place.websiteUri || '',
            products: matchedBrands,
            place_id: placeId,
            territory: city,
            priority: (place.rating >= 4.5 && place.userRatingCount > 20) ? 'High' : (place.rating >= 3.5) ? 'Medium' : 'Low',
            rating: place.rating || null,
            reviews: place.userRatingCount || 0
          });
        }
      } catch(e) { console.error('Search error for', customerType, e.message); }
    }

    res.json({
      ok: true,
      leads: allLeads.slice(0, 10),
      brands_used: brands,
      channel: channel,
      excluded_count: existingNames.size
    });
  } catch(e) {
    console.error('daily-leads error:', e);
    res.status(500).json({ error: e.message });
  }
});


router.post('/log-call', async (req, res) => {
  const uid = req.session.user.id;
  const { prospect_id, outcome, next_step, next_step_date } = req.body;
  if (!prospect_id) return res.status(400).json({ error: 'prospect_id required' });
  try {
    const today = new Date().toISOString().split('T')[0];
    await pool.query(
      'INSERT INTO calls (user_id, prospect_id, call_date, outcome, next_step, next_step_date) VALUES ($1,$2,$3,$4,$5,$6)',
      [uid, prospect_id, today, outcome || '', next_step || '', next_step_date || null]);
    if (outcome === 'Interested') await pool.query("UPDATE prospects SET status='Warm' WHERE id=$1", [prospect_id]);
    else if (outcome === 'Ready to Buy') await pool.query("UPDATE prospects SET status='Hot' WHERE id=$1", [prospect_id]);
    else if (outcome === 'Not Interested') await pool.query("UPDATE prospects SET status='Cold' WHERE id=$1", [prospect_id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});



// ─── DAILY LEADS — uses Google Places API ───────────────────────────────────
const fetchPlaces = require('node-fetch');

router.post('/daily-leads', async (req, res) => {
  const uid = req.session.user.id;

  // Hit our own places-leads endpoint for each combination
  const city = (req.body.city || '').trim() || 'Atlanta GA';

  const targets = [
    { product: 'BOSS Products', customer_type: 'Roofing Contractor', count: 3 },
    { product: 'BOSS Products', customer_type: 'Roofing Distributor', count: 2 },
    { product: 'Alum-A-Pole', customer_type: 'Deck Contractor', count: 2 },
    { product: 'ShurTape', customer_type: 'Lumber Yard', count: 2 },
    { product: 'ShurTape', customer_type: 'Siding Contractor', count: 1 }
  ];

  const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;
  if (!PLACES_KEY) return res.status(500).json({ error: 'Google Places API key not configured' });

  // Reuse the helper functions from places.js
  const placesModule = require('./places');

  try {
    const allLeads = [];
    const seen = new Set();

    // Get existing prospects for this user to filter dupes
    const existing = await pool.query('SELECT LOWER(company) as company FROM prospects WHERE user_id=$1', [uid]);
    existing.rows.forEach(r => seen.add(r.company));

    for (const target of targets) {
      try {
        // Call Google Places directly
        const searchQuery = target.customer_type + ' ' + city;

        const placesRes = await fetchPlaces('https://places.googleapis.com/v1/places:searchText', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': PLACES_KEY,
            'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus'
          },
          body: JSON.stringify({ textQuery: searchQuery, maxResultCount: 10 })
        });

        const data = await placesRes.json();
        const places = data.places || [];

        let added = 0;
        for (const place of places) {
          if (added >= target.count) break;
          const company = place.displayName?.text || '';
          if (!company || seen.has(company.toLowerCase())) continue;
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          seen.add(company.toLowerCase());

          const addr = (place.formattedAddress || '').split(',');
          allLeads.push({
            company,
            category: target.customer_type,
            city: addr[1]?.trim() || '',
            address: place.formattedAddress || '',
            phone: place.nationalPhoneNumber || '',
            website: place.websiteUri || '',
            products: target.product,
            territory: city,
            priority: (place.rating >= 4.5 && place.userRatingCount > 20) ? 'High' : (place.rating >= 3.5) ? 'Medium' : 'Low',
            rating: place.rating || null,
            reviews: place.userRatingCount || 0
          });
          added++;
        }
      } catch(e) {
        console.error('Target error:', target, e.message);
      }
    }

    // If we don't have 10 leads, fill the gap with a generic search
    if (allLeads.length < 10) {
      try {
        const fillRes = await fetchPlaces('https://places.googleapis.com/v1/places:searchText', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': PLACES_KEY,
            'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus'
          },
          body: JSON.stringify({ textQuery: 'building contractors ' + city, maxResultCount: 20 })
        });
        const fillData = await fillRes.json();
        const fillPlaces = fillData.places || [];

        const productCycle = ['BOSS Products', 'ShurTape', 'Alum-A-Pole'];
        let pIdx = 0;

        for (const place of fillPlaces) {
          if (allLeads.length >= 10) break;
          const company = place.displayName?.text || '';
          if (!company || seen.has(company.toLowerCase())) continue;
          if (place.businessStatus === 'CLOSED_PERMANENTLY') continue;
          seen.add(company.toLowerCase());

          const addr = (place.formattedAddress || '').split(',');
          allLeads.push({
            company,
            category: 'Contractor',
            city: addr[1]?.trim() || '',
            address: place.formattedAddress || '',
            phone: place.nationalPhoneNumber || '',
            website: place.websiteUri || '',
            products: productCycle[pIdx % productCycle.length],
            territory: city,
            priority: (place.rating >= 4.5 && place.userRatingCount > 20) ? 'High' : (place.rating >= 3.5) ? 'Medium' : 'Low',
            rating: place.rating || null,
            reviews: place.userRatingCount || 0
          });
          pIdx++;
        }
      } catch(e) { console.error('Fill search error:', e.message); }
    }

    res.json({ ok: true, leads: allLeads.slice(0, 10) });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
