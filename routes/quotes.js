const express = require('express');
const router = express.Router();
const { pool } = require('../db');

// GET all quotes — team-wide (all users share the same quote board)
router.get('/', async (req, res) => {
  try {
    // Show all quotes across the whole team so every rep sees the full pipeline
    const result = await pool.query(
      `SELECT q.*, u.name as rep_name
       FROM quotes q
       LEFT JOIN users u ON q.user_id = u.id
       ORDER BY q.created_at DESC`
    );
    res.json({ quotes: result.rows });
  } catch (e) {
    console.error('GET /api/quotes error:', e.message);
    res.json({ quotes: [], error: e.message });
  }
});

// GET /contacts-for-account -- return ranked contact list for a given account name (team-wide)
// Ranks by: (1) most recent call activity, (2) contact frequency, (3) alphabetical
router.get('/contacts-for-account', async (req, res) => {
  try {
    const { account } = req.query;
    if (!account || !account.trim()) return res.json({ contacts: [] });

    // Pull all unique contacts across the whole team for this company
    // Rank by most recent call date attached to that contact's prospect record
    const result = await pool.query(
      `SELECT
         p.contact,
         COUNT(*) AS freq,
         MAX(COALESCE(lc.call_date, p.created_at)) AS last_activity
       FROM prospects p
       LEFT JOIN LATERAL (
         SELECT call_date FROM calls
         WHERE prospect_id = p.id
         ORDER BY call_date DESC, created_at DESC
         LIMIT 1
       ) lc ON true
       WHERE LOWER(TRIM(p.company)) = LOWER(TRIM($1))
         AND p.contact IS NOT NULL
         AND TRIM(p.contact) != ''
       GROUP BY p.contact
       ORDER BY last_activity DESC NULLS LAST, freq DESC, p.contact ASC`,
      [account.trim()]
    );

    const contacts = result.rows.map(r => r.contact);
    res.json({ contacts });
  } catch (e) {
    console.error('contacts-for-account error:', e.message);
    res.status(500).json({ contacts: [], error: e.message });
  }
});

// GET /:id/pdf — serve stored PDF base64 data as an inline PDF for viewing
router.get('/:id/pdf', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT pdf_data, pdf_filename FROM quotes WHERE id = $1`,
      [req.params.id]
    );
    if (!result.rows.length || !result.rows[0].pdf_data) {
      return res.status(404).json({ error: 'No PDF attached to this quote' });
    }
    const { pdf_data, pdf_filename } = result.rows[0];
    // pdf_data is stored as base64 string
    const buf = Buffer.from(pdf_data, 'base64');
    res.set('Content-Type', 'application/pdf');
    res.set('Content-Disposition', 'inline; filename="' + (pdf_filename || 'quote.pdf') + '"');
    res.set('Content-Length', buf.length);
    res.send(buf);
  } catch (e) {
    console.error('GET /api/quotes/:id/pdf error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET single quote — team-wide access
router.get('/:id', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT q.*, u.name as rep_name
       FROM quotes q
       LEFT JOIN users u ON q.user_id = u.id
       WHERE q.id = $1`,
      [req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST create quote — with duplicate prevention (team-wide)
// POST /api/quotes/fix-rep — hardcoded migration: Sean Conroy → Ray Breedlove
// Placed FIRST so /:id route never intercepts it
router.post('/fix-rep', async (req, res) => {
  try {
    const findName    = (req.body && req.body.find)    || 'Sean Conroy';
    const replaceName = (req.body && req.body.replace) || 'Ray Breedlove';

    const result = await pool.query(
      `UPDATE quotes
         SET rep_name = $1, updated_at = NOW()
       WHERE LOWER(COALESCE(rep_name,'')) = LOWER($2)
       RETURNING id, quote_number, account_name, rep_name`,
      [replaceName, findName]
    );

    console.log('[fix-rep] Updated', result.rowCount, 'quotes:', findName, '→', replaceName);
    res.json({
      success: true,
      updated: result.rowCount,
      message: `Updated ${result.rowCount} quotes from "${findName}" to "${replaceName}"`,
      quotes: result.rows.map(r => ({ id: r.id, quote_number: r.quote_number, account: r.account_name }))
    });
  } catch (e) {
    console.error('[fix-rep] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});


router.post('/', async (req, res) => {
  try {
    const userId = req.session.user.id;
    const {
      quote_number, status, account_name, contact_name,
      amount, products, comments, quote_date, follow_up_date,
      pdf_data, pdf_filename, rep_name, force_override
    } = req.body;

    if (!account_name || !account_name.trim()) {
      return res.status(400).json({ error: 'Account name is required' });
    }

    // ── Duplicate checks — skipped if user explicitly chose to proceed (force_override) ──
    if (!force_override) {
      // Check 1: same quote_number (team-wide)
      if (quote_number && quote_number.trim()) {
        const dupNum = await pool.query(
          `SELECT id FROM quotes WHERE LOWER(TRIM(quote_number)) = LOWER($1) LIMIT 1`,
          [quote_number.trim()]
        );
        if (dupNum.rows.length > 0) {
          return res.status(409).json({
            error: 'duplicate',
            message: 'A quote with number "' + quote_number.trim() + '" already exists.',
            existing_id: dupNum.rows[0].id
          });
        }
      }

      // Check 2: same account + amount + quote_date
      if (account_name && amount && quote_date) {
        const dupMatch = await pool.query(
          `SELECT id FROM quotes
           WHERE LOWER(TRIM(account_name)) = LOWER($1)
             AND amount = $2
             AND quote_date::date = $3::date
           LIMIT 1`,
          [account_name.trim(), parseFloat(amount) || 0, quote_date]
        );
        if (dupMatch.rows.length > 0) {
          return res.status(409).json({
            error: 'duplicate',
            message: 'A quote for "' + account_name.trim() + '" with this amount and date already exists.',
            existing_id: dupMatch.rows[0].id
          });
        }
      }
    }

    const result = await pool.query(
      `INSERT INTO quotes
       (user_id, rep_id, quote_number, status, account_name, contact_name,
        amount, products, comments, quote_date, follow_up_date, pdf_filename, pdf_data, rep_name)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
       RETURNING *`,
      [
        userId, userId,
        quote_number ? quote_number.trim() : null,
        status || 'Draft',
        account_name.trim(),
        contact_name || null,
        amount ? parseFloat(amount) : null,
        products || null,
        comments || null,
        quote_date || null,
        follow_up_date || null,
        pdf_filename || null,
        pdf_data || null,
        rep_name || null
      ]
    );
    res.json(result.rows[0]);
  } catch (e) {
    console.error('POST /api/quotes error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// PUT update quote — team-wide edit access, persists rep_name
router.put('/:id', async (req, res) => {
  try {
    const {
      quote_number, status, account_name, contact_name,
      amount, products, comments, quote_date, follow_up_date,
      pdf_data, pdf_filename, rep_name
    } = req.body;

    // ── Duplicate check: if quote_number changed, ensure it doesn't collide ──
    if (quote_number && quote_number.trim()) {
      const dupNum = await pool.query(
        `SELECT id FROM quotes
         WHERE LOWER(TRIM(quote_number)) = LOWER($1) AND id != $2 LIMIT 1`,
        [quote_number.trim(), req.params.id]
      );
      if (dupNum.rows.length > 0) {
        return res.status(409).json({
          error: 'duplicate',
          message: 'A quote with number "' + quote_number.trim() + '" already exists.',
          existing_id: dupNum.rows[0].id
        });
      }
    }

    const result = await pool.query(
      `UPDATE quotes SET
        quote_number = $2,
        status = $3,
        account_name = $4,
        contact_name = $5,
        amount = $6,
        products = $7,
        comments = $8,
        quote_date = $9,
        follow_up_date = $10,
        pdf_filename = COALESCE($11, pdf_filename),
        pdf_data = COALESCE($12, pdf_data),
        rep_name = COALESCE($13, rep_name),
        updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [
        req.params.id,
        quote_number ? quote_number.trim() : null,
        status || 'Draft',
        account_name,
        contact_name || null,
        amount ? parseFloat(amount) : null,
        products || null,
        comments || null,
        quote_date || null,
        follow_up_date || null,
        pdf_filename || null,
        pdf_data || null,
        rep_name || null
      ]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (e) {
    console.error('PUT /api/quotes error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// DELETE quote — team-wide
router.delete('/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM quotes WHERE id = $1', [req.params.id]);
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
    const { account_name, contact_name, phone, email, force_update, dry_run } = req.body;

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

    // ── If dry_run: just return Places data without touching DB ──
    if (dry_run) {
      return res.json({
        dry_run: true,
        company,
        phone: placesPhone,
        email: placesEmail,
        city: placesCity,
        state: placesState,
        website: placesWebsite,
        address: placesAddress
      });
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