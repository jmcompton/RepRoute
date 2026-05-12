const express = require('express');
const router = express.Router();
const { pool } = require('../db');

// GET all quotes for current user
router.get('/', async (req, res) => {
  try {
    const userId = req.session.user.id;
    const result = await pool.query(
      `SELECT q.*, u.name as rep_name 
       FROM quotes q
       LEFT JOIN users u ON q.rep_id = u.id
       WHERE q.user_id = $1
       ORDER BY q.created_at DESC`,
      [userId]
    );
    res.json({ quotes: result.rows });
  } catch (e) {
    console.error('GET /api/quotes error:', e.message);
    res.json({ quotes: [], error: e.message });
  }
});

// GET single quote
router.get('/:id', async (req, res) => {
  try {
    const userId = req.session.user.id;
    const result = await pool.query(
      `SELECT q.*, u.name as rep_name
       FROM quotes q
       LEFT JOIN users u ON q.rep_id = u.id
       WHERE q.id = $1 AND q.user_id = $2`,
      [req.params.id, userId]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST create quote
router.post('/', async (req, res) => {
  try {
    const userId = req.session.user.id;
    const {
      quote_number, status, account_name, contact_name,
      amount, products, comments, quote_date, follow_up_date,
      pdf_data, pdf_filename
    } = req.body;

    const result = await pool.query(
      `INSERT INTO quotes
       (user_id, rep_id, quote_number, status, account_name, contact_name,
        amount, products, comments, quote_date, follow_up_date, pdf_filename, pdf_data)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
       RETURNING *`,
      [
        userId, userId,
        quote_number || null,
        status || 'Draft',
        account_name,
        contact_name || null,
        amount || null,
        products || null,
        comments || null,
        quote_date || null,
        follow_up_date || null,
        pdf_filename || null,
        pdf_data || null
      ]
    );
    res.json(result.rows[0]);
  } catch (e) {
    console.error('POST /api/quotes error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// PUT update quote
router.put('/:id', async (req, res) => {
  try {
    const userId = req.session.user.id;
    const {
      quote_number, status, account_name, contact_name,
      amount, products, comments, quote_date, follow_up_date,
      pdf_data, pdf_filename
    } = req.body;

    const result = await pool.query(
      `UPDATE quotes SET
        quote_number = $3,
        status = $4,
        account_name = $5,
        contact_name = $6,
        amount = $7,
        products = $8,
        comments = $9,
        quote_date = $10,
        follow_up_date = $11,
        pdf_filename = COALESCE($12, pdf_filename),
        pdf_data = COALESCE($13, pdf_data),
        updated_at = NOW()
       WHERE id = $1 AND user_id = $2
       RETURNING *`,
      [
        req.params.id, userId,
        quote_number || null,
        status || 'Draft',
        account_name,
        contact_name || null,
        amount || null,
        products || null,
        comments || null,
        quote_date || null,
        follow_up_date || null,
        pdf_filename || null,
        pdf_data || null
      ]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (e) {
    console.error('PUT /api/quotes error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// DELETE quote
router.delete('/:id', async (req, res) => {
  try {
    const userId = req.session.user.id;
    await pool.query('DELETE FROM quotes WHERE id = $1 AND user_id = $2', [req.params.id, userId]);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST parse-pdf — send PDF natively to Claude (no extra npm dependencies)
router.post('/parse-pdf', async (req, res) => {
  try {
    const { pdf_data, filename } = req.body;
    if (!pdf_data) return res.json({});

    const fetch = require('node-fetch');

    const apiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'anthropic-beta': 'pdfs-2024-09-25'
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 600,
        messages: [{
          role: 'user',
          content: [
            {
              type: 'document',
              source: {
                type: 'base64',
                media_type: 'application/pdf',
                data: pdf_data
              }
            },
            {
              type: 'text',
              text: 'Extract fields from this sales quote or proposal PDF. Respond ONLY with a valid JSON object, no markdown or explanation:\n{\n  "quote_number": "quote/proposal/estimate number or null",\n  "account_name": "customer/client/bill-to company name or null",\n  "contact_name": "contact person full name or null",\n  "amount": "total dollar amount as number string like \\"1234.56\\" with no dollar sign, or null",\n  "products": "concise summary of all products or line items, max 150 chars, or null",\n  "quote_date": "quote date in YYYY-MM-DD format or null",\n  "follow_up_date": "follow-up or expiry date in YYYY-MM-DD format or null",\n  "comments": "relevant notes, terms, or special instructions max 200 chars or null"\n}\nRules: use null for any field you cannot find with confidence. For amount use the GRAND TOTAL only. Return ONLY the JSON object.'
            }
          ]
        }]
      })
    });

    const aiData = await apiRes.json();

    if (aiData.error) {
      console.error('Claude PDF error:', JSON.stringify(aiData.error));
      return res.json({ _error: aiData.error.message || 'Claude API error' });
    }

    const rawText = (aiData.content || [])
      .filter(b => b.type === 'text')
      .map(b => b.text)
      .join('')
      .trim();

    const jsonMatch = rawText.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      console.error('No JSON in Claude response:', rawText.substring(0, 200));
      return res.json({ _error: 'Could not parse response' });
    }

    const extracted = JSON.parse(jsonMatch[0]);

    // Remove nulls and empty strings
    Object.keys(extracted).forEach(k => {
      if (extracted[k] === null || extracted[k] === '' || extracted[k] === 'null') {
        delete extracted[k];
      }
    });

    res.json(extracted);

  } catch (e) {
    console.error('PDF parse error:', e.message);
    res.json({ _error: e.message });
  }
});

// POST upsert-prospect — create/update account+contact, lookup address+phone via Places
router.post('/upsert-prospect', async (req, res) => {
  try {
    const fetch = require('node-fetch');
    const userId = req.session.user.id;
    const { account_name, contact_name, phone, email, force_update } = req.body;

    if (!account_name || !account_name.trim()) {
      return res.json({ created: false, reason: 'No account name provided' });
    }

    const company = account_name.trim();
    const contact = (contact_name || '').trim() || null;
    const PLACES_KEY = process.env.GOOGLE_PLACES_API_KEY;

    // ── Google Places: findplacefromtext → get place_id, then details ──
    let placesPhone = phone || null;
    let placesEmail = email || null;
    let placesCity = null;
    let placesState = null;
    let placesWebsite = null;
    let placesAddress = null;

    if (PLACES_KEY) {
      try {
        // Step 1: find the place
        const findUrl = `https://maps.googleapis.com/maps/api/place/findplacefromtext/json` +
          `?input=${encodeURIComponent(company)}` +
          `&inputtype=textquery` +
          `&fields=place_id,name` +
          `&key=${PLACES_KEY}`;

        const findRes = await fetch(findUrl);
        const findData = await findRes.json();

        if (findData.candidates && findData.candidates.length > 0) {
          const placeId = findData.candidates[0].place_id;

          // Step 2: get full details including address_components, phone, website
          const detailUrl = `https://maps.googleapis.com/maps/api/place/details/json` +
            `?place_id=${placeId}` +
            `&fields=name,formatted_phone_number,website,formatted_address,address_components` +
            `&key=${PLACES_KEY}`;

          const detailRes = await fetch(detailUrl);
          const detailData = await detailRes.json();

          if (detailData.result) {
            const r = detailData.result;
            if (r.formatted_phone_number) placesPhone = r.formatted_phone_number;
            if (r.website) placesWebsite = r.website;
            if (r.formatted_address) placesAddress = r.formatted_address;
            if (r.address_components) {
              for (const comp of r.address_components) {
                if (comp.types.includes('locality')) placesCity = comp.long_name;
                if (comp.types.includes('administrative_area_level_1')) placesState = comp.short_name;
              }
            }
          }
        }
      } catch (plErr) {
        console.error('Places lookup error:', plErr.message);
      }
    }

    // ── Check if this company already exists for this user ──
    const existing = await pool.query(
      `SELECT id, company, contact, phone, email, city, website FROM prospects
       WHERE user_id = $1 AND LOWER(TRIM(company)) = LOWER($2)
       LIMIT 1`,
      [userId, company]
    );

    if (existing.rows.length > 0) {
      const p = existing.rows[0];

      // Fill in any blanks with new data
      const updates = [];
      const vals = [];
      const add = (col, val) => {
        if (val !== null && val !== undefined && val !== '') {
          vals.push(val);
          updates.push(col + ' = $' + vals.length);
        }
      };

      // Contact: update if blank, or force_update when rep edits quote
      if (contact && (!p.contact || p.contact.trim() === '' || force_update)) add('contact', contact);
      // Phone: fill blank from Places or from what was passed in
      if (placesPhone && (!p.phone || p.phone.trim() === '')) add('phone', placesPhone);
      // Email: fill blank
      if (placesEmail && (!p.email || p.email.trim() === '')) add('email', placesEmail);
      // City/State/Website: fill blank
      if (placesCity && (!p.city || p.city.trim() === '')) add('city', placesCity);
      if (placesState) add('state', placesState);
      if (placesWebsite && (!p.website || p.website.trim() === '')) add('website', placesWebsite);

      if (updates.length > 0) {
        vals.push(p.id);
        await pool.query(
          `UPDATE prospects SET ${updates.join(', ')} WHERE id = $${vals.length}`,
          vals
        );
      }

      return res.json({
        created: false,
        updated: updates.length > 0,
        id: p.id,
        company: p.company,
        phone: placesPhone || p.phone,
        city: placesCity || p.city,
        message: updates.length > 0 ? 'Account updated' : 'Account already exists'
      });
    }

    // ── Create brand new prospect record ──
    const result = await pool.query(
      `INSERT INTO prospects
         (user_id, company, category, contact, phone, email, city, state, website, status, priority, source, notes)
       VALUES ($1, $2, 'Account', $3, $4, $5, $6, $7, $8, 'New', 'Medium', 'Quote', $9)
       RETURNING id, company, contact, phone, city, state, website`,
      [
        userId,
        company,
        contact,
        placesPhone,
        placesEmail,
        placesCity,
        placesState,
        placesWebsite,
        placesAddress ? 'Address: ' + placesAddress : null
      ]
    );

    const created = result.rows[0];
    console.log(`Created account: ${created.company} | phone: ${created.phone} | city: ${created.city}`);

    res.json({
      created: true,
      id: created.id,
      company: created.company,
      phone: created.phone,
      city: created.city,
      state: created.state,
      message: 'Account and contact created'
    });

  } catch (e) {
    console.error('upsert-prospect error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;