const express = require('express');
const router = express.Router();
const { pool } = require('./db');

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

// POST parse PDF — extract fields from base64 PDF using simple text extraction
router.post('/parse-pdf', async (req, res) => {
  try {
    const { pdf_data, filename } = req.body;
    if (!pdf_data) return res.json({});

    // Decode base64 PDF and extract raw text using basic buffer parsing
    const buf = Buffer.from(pdf_data, 'base64');
    const pdfText = buf.toString('latin1');

    const extracted = {};

    // --- Extract quote number ---
    const qnMatch = pdfText.match(/(?:quote|quotation|proposal|estimate|order)[\s#:\-]*(?:no\.?|number|num|#)?[\s:]*([A-Z0-9\-]{3,20})/i);
    if (qnMatch) extracted.quote_number = qnMatch[1].trim();

    // --- Extract company/account name ---
    const billMatch = pdfText.match(/(?:bill\s*to|sold\s*to|customer|client|account|company)[\s:]+([^\n\r]{3,60})/i);
    if (billMatch) extracted.account_name = billMatch[1].replace(/[^\w\s&.,'-]/g,'').trim().substring(0,60);

    // --- Extract contact name ---
    const contactMatch = pdfText.match(/(?:attn|attention|contact|prepared\s*for)[\s:]+([A-Za-z][^\n\r]{2,40})/i);
    if (contactMatch) extracted.contact_name = contactMatch[1].replace(/[^\w\s.'-]/g,'').trim().substring(0,40);

    // --- Extract total amount ---
    const amtPatterns = [
      /(?:total|grand\s*total|amount\s*due|subtotal|quote\s*total)[\s:$]*\$?([\d,]+\.?\d{0,2})/i,
      /\$\s*([\d,]{3,}\.?\d{0,2})/
    ];
    for (const p of amtPatterns) {
      const m = pdfText.match(p);
      if (m) {
        const num = parseFloat(m[1].replace(/,/g,''));
        if (!isNaN(num) && num > 0) { extracted.amount = num.toFixed(2); break; }
      }
    }

    // --- Extract date ---
    const datePatterns = [
      /(?:date|quote\s*date|issued|prepared)[\s:]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/i,
      /(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})/
    ];
    for (const p of datePatterns) {
      const m = pdfText.match(p);
      if (m) {
        try {
          const parts = m[1].split(/[\/\-]/);
          if (parts[2] && parts[2].length === 4) {
            extracted.quote_date = parts[2] + '-' + parts[0].padStart(2,'0') + '-' + parts[1].padStart(2,'0');
          }
        } catch(ex) {}
        break;
      }
    }

    // --- Extract products/description (look for line items) ---
    const prodMatch = pdfText.match(/(?:description|item|product|sku|part)[\s:]+([^\n\r]{5,120})/i);
    if (prodMatch) extracted.products = prodMatch[1].trim().substring(0,120);

    res.json(extracted);
  } catch (e) {
    console.error('PDF parse error:', e.message);
    res.json({}); // return empty if parse fails — user fills manually
  }
});

module.exports = router;
