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

    // Build dynamic update to handle optional pdf_data
    let setClause = `
      quote_number = $3,
      status = $4,
      account_name = $5,
      contact_name = $6,
      amount = $7,
      products = $8,
      comments = $9,
      quote_date = $10,
      follow_up_date = $11,
      updated_at = NOW()`;

    const params = [
      req.params.id, userId,
      quote_number || null,
      status || 'Draft',
      account_name,
      contact_name || null,
      amount || null,
      products || null,
      comments || null,
      quote_date || null,
      follow_up_date || null
    ];

    // Only update PDF columns if values provided
    if (pdf_filename !== undefined) {
      setClause += `, pdf_filename = $${params.length + 1}`;
      params.push(pdf_filename || null);
    }
    if (pdf_data !== undefined && pdf_data !== null) {
      setClause += `, pdf_data = $${params.length + 1}`;
      params.push(pdf_data);
    }

    const result = await pool.query(
      `UPDATE quotes SET ${setClause} WHERE id = $1 AND user_id = $2 RETURNING *`,
      params
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
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST parse PDF — send PDF to Claude natively (no library needed)
router.post('/parse-pdf', async (req, res) => {
  try {
    const { pdf_data, filename } = req.body;
    if (!pdf_data) return res.json({});

    // Claude supports PDF documents natively via the API
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
              text: `Extract fields from this sales quote/proposal PDF. Respond ONLY with a valid JSON object, no markdown or explanation:
{
  "quote_number": "quote/proposal/estimate number or null",
  "account_name": "customer/client/bill-to company name or null",
  "contact_name": "contact person full name or null",
  "amount": "total dollar amount as number string like \"1234.56\" (no $ sign) or null",
  "products": "concise summary of products/line items max 150 chars or null",
  "quote_date": "quote date in YYYY-MM-DD format or null",
  "follow_up_date": "follow-up or expiry date in YYYY-MM-DD format or null",
  "comments": "relevant notes, terms, or special instructions max 200 chars or null"
}
Rules: use null for fields you cannot find. For amount use TOTAL/GRAND TOTAL only. Return ONLY the JSON.`
            }
          ]
        }]
      })
    });

    const aiData = await apiRes.json();
    if (aiData.error) {
      console.error('Claude PDF error:', aiData.error.message);
      return res.json({ _error: aiData.error.message });
    }

    const rawText = (aiData.content || []).filter(b => b.type === 'text').map(b => b.text).join('').trim();
    const jsonMatch = rawText.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return res.json({ _error: 'No JSON in response' });

    const extracted = JSON.parse(jsonMatch[0]);

    // Remove nulls
    Object.keys(extracted).forEach(k => {
      if (extracted[k] === null || extracted[k] === '' || extracted[k] === 'null') delete extracted[k];
    });

    res.json(extracted);

  } catch (e) {
    console.error('PDF parse error:', e.message);
    res.json({ _error: e.message });
  }
});

// DELETE quote
router.delete('/:id', async (req, res) => {
  try {
    const userId = req.session.user.id;
    await pool.query('DELETE FROM quotes WHERE id = $1 AND user_id = $2', [req.params.id, userId]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST parse PDF — extract text with pdf-parse then use Claude to extract fields
router.post('/parse-pdf', async (req, res) => {
  try {
    const { pdf_data, filename } = req.body;
    if (!pdf_data) return res.json({});

    // Step 1: Extract text from PDF using pdf-parse
    let pdfText = '';
    try {
      const pdfParse = require('pdf-parse');
      const buf = Buffer.from(pdf_data, 'base64');
      const parsed = await pdfParse(buf);
      pdfText = parsed.text || '';
    } catch (parseErr) {
      console.error('pdf-parse error:', parseErr.message);
      // Fallback: raw latin1 text extraction
      const buf = Buffer.from(pdf_data, 'base64');
      pdfText = buf.toString('latin1').replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/g, ' ');
    }

    if (!pdfText || pdfText.trim().length < 20) {
      return res.json({ _error: 'Could not extract text from PDF' });
    }

    // Trim to first 3000 chars to keep Claude prompt fast
    const snippet = pdfText.substring(0, 3000);

    // Step 2: Use Claude Haiku to intelligently extract fields
    const prompt = `You are extracting fields from a sales quote or proposal PDF. Here is the raw text extracted from the PDF:

---
${snippet}
---

Extract the following fields and respond ONLY with a valid JSON object (no markdown, no explanation):
{
  "quote_number": "the quote, proposal, or estimate number/ID (string or null)",
  "account_name": "the customer/client/bill-to company name (string or null)",
  "contact_name": "the contact person's full name (string or null)",
  "amount": "the total dollar amount as a number string like '1234.56' (string or null — no $ sign)",
  "products": "a concise summary of the products or line items, max 120 chars (string or null)",
  "quote_date": "the quote date in YYYY-MM-DD format (string or null)",
  "follow_up_date": "any follow-up or expiry date in YYYY-MM-DD format (string or null)",
  "comments": "any relevant notes, terms, or special instructions, max 200 chars (string or null)"
}

Rules:
- Use null for any field you cannot find with confidence
- For amount: use the TOTAL or GRAND TOTAL, not subtotals
- For products: combine line item descriptions into one summary string
- For dates: convert any date format to YYYY-MM-DD
- Return ONLY the JSON object, nothing else`;

    const apiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 500,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const aiData = await apiRes.json();
    if (!aiData.content) throw new Error('No response from Claude');

    const rawText = aiData.content.filter(b => b.type === 'text').map(b => b.text).join('').trim();

    // Parse the JSON response
    const jsonMatch = rawText.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('Could not parse Claude response as JSON');

    const extracted = JSON.parse(jsonMatch[0]);

    // Clean up nulls and empty strings
    Object.keys(extracted).forEach(k => {
      if (extracted[k] === null || extracted[k] === '' || extracted[k] === 'null') {
        delete extracted[k];
      }
    });

    res.json(extracted);

  } catch (e) {
    console.error('PDF parse error:', e.message);
    res.json({ _error: e.message }); // return empty — user fills manually
  }
});

module.exports = router;
