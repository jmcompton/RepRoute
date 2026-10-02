const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const { accountKey, accountKeySql } = require('../lib/account-key');

const DEFAULT_FOLLOWUP_MIN = 5000;

// The rep's "no automatic follow-up under $X" threshold (users.quote_followup_min).
async function followupMin(uid) {
  const r = await pool.query('SELECT quote_followup_min FROM users WHERE id=$1', [uid]);
  const v = r.rows[0] && r.rows[0].quote_followup_min;
  return v == null ? DEFAULT_FOLLOWUP_MIN : Number(v);
}

// follow_up_enabled: true = rep wants a follow-up, false = no follow-up (the date
// is cleared), null/undefined = legacy client → keep whatever date was sent.
function followUpFields(body) {
  const fe = body.follow_up_enabled;
  const enabled = fe === true ? true : fe === false ? false : null;
  return { enabled, date: enabled === false ? null : (body.follow_up_date || null) };
}

// Canonical (most-used) spelling of an existing account matching `name` by
// accountKey — team-wide, since the quote board is shared. null if none.
async function canonicalAccountName(name) {
  const key = accountKey(name);
  if (!key) return null;
  const r = await pool.query(
    `SELECT TRIM(company) AS company, COUNT(*) AS n FROM prospects
      WHERE ${accountKeySql('company')} = $1
      GROUP BY TRIM(company) ORDER BY n DESC, LENGTH(TRIM(company)) ASC LIMIT 1`, [key]);
  return r.rows.length ? r.rows[0].company : null;
}

// GET all quotes — team-wide (all users share the same quote board)
router.get('/', async (req, res) => {
  try {
    const { range } = req.query;
    // PERIOD filter keys on quote_date (the quote's own date), NOT created_at, so
    // a quote belongs to the calendar month of its quote_date only. Boundaries are
    // computed in America/New_York (the app's standard tz — see WEEKLY_REPORT_TZ)
    // so there is no UTC off-by-one at month edges: a Jun 30 quote counts as June,
    // a Jul 1 quote counts as July. quote_date is a DATE, so we compare date-to-date
    // against NY-local month/year boundaries and NY "today" for rolling windows.
    const NY_NOW = `(NOW() AT TIME ZONE 'America/New_York')`;               // naive NY wall-clock time
    const NY_MONTH = `date_trunc('month', ${NY_NOW})`;                       // 1st of current NY month
    const NY_YEAR = `date_trunc('year', ${NY_NOW})`;                         // Jan 1 of current NY year
    const NY_TODAY = `(${NY_NOW})::date`;                                    // current NY calendar date
    const rangeFilters = {
      'this_month':    `q.quote_date >= ${NY_MONTH}::date AND q.quote_date < (${NY_MONTH} + INTERVAL '1 month')::date`,
      'last_month':    `q.quote_date >= (${NY_MONTH} - INTERVAL '1 month')::date AND q.quote_date < ${NY_MONTH}::date`,
      'last_30':       `q.quote_date >= ${NY_TODAY} - 30  AND q.quote_date <= ${NY_TODAY}`,
      'last_90':       `q.quote_date >= ${NY_TODAY} - 90  AND q.quote_date <= ${NY_TODAY}`,
      'last_6_months': `q.quote_date >= ${NY_TODAY} - 180 AND q.quote_date <= ${NY_TODAY}`,
      'this_year':     `q.quote_date >= ${NY_YEAR}::date AND q.quote_date < (${NY_YEAR} + INTERVAL '1 year')::date`
    };
    const whereClause = (range && rangeFilters[range]) ? `WHERE ${rangeFilters[range]}` : '';
    const result = await pool.query(
      `SELECT q.*, COALESCE(q.rep_name, u.name) as rep_name
       FROM quotes q
       LEFT JOIN users u ON q.user_id = u.id
       ${whereClause}
       ORDER BY q.created_at DESC`
    );
    res.json({ quotes: result.rows });
  } catch (e) {
    console.error('GET /api/quotes error:', e.message);
    res.json({ quotes: [], error: e.message });
  }
});

// ── Follow-ups: every OPEN follow-up regardless of quote month ──────
// Open = has a follow_up_date and isn't Won/Lost. Overdue ones stay until the
// rep marks the quote won/lost or dismisses the follow-up.
// ?scope=mine (default) → quotes for this rep; ?scope=all → whole team.
router.get('/followups', async (req, res) => {
  try {
    const uid = req.session.user.id;
    const mine = req.query.scope !== 'all';
    const r = await pool.query(
      `SELECT q.id, q.user_id, q.rep_id, q.quote_number, q.customer_number, q.status, q.account_name,
              q.contact_name, q.amount, q.products, q.comments, q.quote_date, q.follow_up_date,
              q.follow_up_enabled, q.pdf_filename, q.created_at, q.updated_at,
              COALESCE(q.rep_name, u.name) AS rep_name
         FROM quotes q LEFT JOIN users u ON q.user_id = u.id
        WHERE q.follow_up_date IS NOT NULL
          AND q.status NOT IN ('Won','Lost')
          ${mine ? 'AND (q.rep_id = $1 OR q.user_id = $1)' : ''}
        ORDER BY q.follow_up_date ASC, q.id ASC`,
      mine ? [uid] : []);
    res.set('Cache-Control', 'no-store');
    res.json({ followups: r.rows, min_amount: await followupMin(uid) });
  } catch (e) {
    console.error('GET /api/quotes/followups error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /followups/bulk { ids:[], action:'dismiss'|'reschedule', date? }
// dismiss → clears the follow-up (quote stays open, can still be marked won).
router.post('/followups/bulk', async (req, res) => {
  try {
    const ids = (Array.isArray(req.body.ids) ? req.body.ids : []).map(Number).filter(Number.isFinite);
    const { action, date } = req.body;
    if (!ids.length) return res.status(400).json({ error: 'No quotes selected' });
    let r;
    if (action === 'dismiss') {
      r = await pool.query(
        `UPDATE quotes SET follow_up_date = NULL, follow_up_enabled = FALSE,
                status = CASE WHEN status = 'Follow-Up' THEN 'Sent' ELSE status END,
                updated_at = NOW()
          WHERE id = ANY($1::int[]) AND status NOT IN ('Won','Lost')`, [ids]);
    } else if (action === 'reschedule') {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date || '')) return res.status(400).json({ error: 'Pick a follow-up date' });
      r = await pool.query(
        `UPDATE quotes SET follow_up_date = $2, follow_up_enabled = TRUE, updated_at = NOW()
          WHERE id = ANY($1::int[])`, [ids, date]);
    } else {
      return res.status(400).json({ error: 'Unknown action' });
    }
    res.json({ ok: true, updated: r.rowCount });
  } catch (e) {
    console.error('POST /api/quotes/followups/bulk error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET/POST /settings — the rep's no-automatic-follow-up threshold.
router.get('/settings', async (req, res) => {
  try { res.json({ followup_min_amount: await followupMin(req.session.user.id) }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
router.post('/settings', async (req, res) => {
  try {
    const v = Number(req.body.followup_min_amount);
    if (!Number.isFinite(v) || v < 0) return res.status(400).json({ error: 'Enter a dollar amount (0 or more)' });
    await pool.query('UPDATE users SET quote_followup_min=$1 WHERE id=$2', [Math.round(v * 100) / 100, req.session.user.id]);
    res.json({ ok: true, followup_min_amount: v });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /search-accounts -- typeahead: return matching account names from prospects (Fix 2)
router.get('/search-accounts', async (req, res) => {
  try {
    const { q } = req.query;
    if (!q || q.trim().length < 2) return res.json({ accounts: [] });
    // One entry per account (accountKey): "CRS Inc" / "CRS, Inc." / "CRS Inc."
    // show once, as the most-used spelling. City-suffixed locations stay distinct.
    const result = await pool.query(
      `SELECT DISTINCT ON (k) company FROM (
         SELECT TRIM(company) AS company, ${accountKeySql('company')} AS k, COUNT(*) OVER (PARTITION BY TRIM(company)) AS n
           FROM prospects
          WHERE LOWER(TRIM(company)) LIKE LOWER($1)
       ) t
       ORDER BY k, n DESC, LENGTH(company) ASC
       LIMIT 15`,
      ['%' + q.trim() + '%']
    );
    res.json({ accounts: result.rows.map(r => r.company).sort((a, b) => a.localeCompare(b)).slice(0, 10) });
  } catch (e) {
    res.status(500).json({ accounts: [], error: e.message });
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
       WHERE ${accountKeySql('p.company')} = $1
         AND p.contact IS NOT NULL
         AND TRIM(p.contact) != ''
       GROUP BY p.contact
       ORDER BY last_activity DESC NULLS LAST, freq DESC, p.contact ASC`,
      [accountKey(account)]
    );
    // Plus people saved on those accounts' Contacts lists (incl. ones typed on quotes).
    const listed = await pool.query(
      `SELECT c.name, MAX(c.created_at) AS added
         FROM contacts c JOIN prospects p ON p.id = c.prospect_id
        WHERE ${accountKeySql('p.company')} = $1 AND TRIM(c.name) <> ''
        GROUP BY c.name ORDER BY added DESC`,
      [accountKey(account)]
    );
    const seen = new Set();
    const contacts = [];
    for (const n of result.rows.map(r => r.contact).concat(listed.rows.map(r => r.name))) {
      const k = String(n).trim().toLowerCase();
      if (!k || seen.has(k)) continue;
      seen.add(k); contacts.push(String(n).trim());
    }
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
      `SELECT q.*, COALESCE(q.rep_name, u.name) as rep_name
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
router.post('/', async (req, res) => {
  try {
    // Guard: session must exist (requireAuthAPI middleware should catch this first,
    // but defend here too so we always return JSON and never an empty/redirect response)
    if (!req.session || !req.session.user || !req.session.user.id) {
      return res.status(401).json({ error: 'Session expired. Please log in again.' });
    }
    const userId = req.session.user.id;
    const {
      quote_number, customer_number, status, account_name, contact_name,
      amount, products, comments, quote_date, follow_up_date,
      pdf_data, pdf_filename, rep_name, force_override
    } = req.body;
    // "Save anyway" override — accept either `force` (new) or `force_override` (legacy).
    const force = req.body.force === true || force_override === true;

    if (!account_name || !account_name.trim()) {
      return res.status(400).json({ error: 'Account name is required' });
    }

    // Match the account to an existing one (same accountKey) so a new spelling
    // — "Mid-Atlantic Roofing Supply Company" — doesn't start a duplicate.
    const acctName = (await canonicalAccountName(account_name)) || account_name.trim();
    const fu = followUpFields(req.body);

    // Quote number is OPTIONAL; customer number is a wholly separate field and is
    // NEVER read into or compared against the quote number.
    const qnum = quote_number && quote_number.trim() ? quote_number.trim() : null;
    const cnum = customer_number && String(customer_number).trim() ? String(customer_number).trim() : null;

    // ── Duplicate check — PER-REP, QUOTE-NUMBER ONLY ────────────────────────────
    // Runs ONLY when a non-empty quote_number is present (no number → just save).
    // Customer number is intentionally absent here. Manufacturer quote numbers
    // legitimately repeat across reps, so a different rep is never a conflict.
    let existingDup = null;
    if (qnum) {
      const dupNum = await pool.query(
        `SELECT id, account_name, amount, quote_date FROM quotes
          WHERE rep_id = $1 AND LOWER(TRIM(quote_number)) = LOWER($2)
          ORDER BY id DESC LIMIT 1`,
        [userId, qnum]
      );
      if (dupNum.rows.length > 0) existingDup = dupNum.rows[0];
    }

    // Same-rep collision and no override → warn (the UI offers "Save anyway").
    if (existingDup && !force) {
      return res.status(409).json({
        error: 'duplicate',
        message: 'A quote with number "' + qnum + '" already exists.',
        existing_id: existingDup.id,
        existing: { account_name: existingDup.account_name, amount: existingDup.amount, quote_date: existingDup.quote_date }
      });
    }

    // Override confirmed on a real duplicate → UPSERT: update the existing row in
    // place (one saved record) rather than inserting a second duplicate.
    if (existingDup && force) {
      const upd = await pool.query(
        `UPDATE quotes SET
           quote_number = $2, customer_number = $3, status = $4, account_name = $5,
           contact_name = $6, amount = $7, products = $8, comments = $9,
           quote_date = $10, follow_up_date = $11,
           pdf_filename = COALESCE($12, pdf_filename),
           pdf_data = COALESCE($13, pdf_data),
           rep_name = COALESCE($14, rep_name),
           follow_up_enabled = $15,
           updated_at = NOW()
         WHERE id = $1
         RETURNING *`,
        [
          existingDup.id, qnum, cnum, status || 'Draft', acctName,
          contact_name || null, amount ? parseFloat(amount) : null, products || null,
          comments || null, quote_date || null, fu.date,
          pdf_filename || null, pdf_data || null, rep_name || null, fu.enabled
        ]
      );
      const savedQuote = upd.rows[0];
      console.log('[quotes] POST override → updated existing quote id=' + savedQuote.id + ' account=' + savedQuote.account_name);
      return res.json({ success: true, quote: savedQuote, updated_existing: true });
    }

    const result = await pool.query(
      `INSERT INTO quotes
       (user_id, rep_id, quote_number, customer_number, status, account_name, contact_name,
        amount, products, comments, quote_date, follow_up_date, pdf_filename, pdf_data, rep_name,
        follow_up_enabled)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
       RETURNING *`,
      [
        userId, userId,
        qnum,
        cnum,
        status || 'Draft',
        acctName,
        contact_name || null,
        amount ? parseFloat(amount) : null,
        products || null,
        comments || null,
        quote_date || null,
        fu.date,
        pdf_filename || null,
        pdf_data || null,
        rep_name || null,
        fu.enabled
      ]
    );
    const savedQuote = result.rows[0];
    console.log('[quotes] POST created quote id=' + savedQuote.id + ' account=' + savedQuote.account_name + ' rep=' + savedQuote.rep_name);
    res.json({ success: true, quote: savedQuote });
  } catch (e) {
    console.error('[quotes] POST /api/quotes error:', e.message, e.stack);
    res.status(500).json({ error: e.message || 'Failed to save quote' });
  }
});

// PUT update quote — team-wide edit access, persists rep_name
router.put('/:id', async (req, res) => {
  try {
    // Guard: always return JSON even if session somehow missing
    if (!req.session || !req.session.user || !req.session.user.id) {
      return res.status(401).json({ error: 'Session expired. Please log in again.' });
    }
    const {
      quote_number, customer_number, status, account_name, contact_name,
      amount, products, comments, quote_date, follow_up_date,
      pdf_data, pdf_filename, rep_name, force_override
    } = req.body;
    const force = req.body.force === true || force_override === true;
    const cnum = customer_number && String(customer_number).trim() ? String(customer_number).trim() : null;
    const acctName = account_name ? ((await canonicalAccountName(account_name)) || String(account_name).trim()) : account_name;
    const fu = followUpFields(req.body);

    // ── Duplicate check (PER-REP) — never collides with the quote's OWN id ──
    // Scoped to the same rep as the quote being edited and excludes this id, so
    // re-saving a quote while keeping its own number can never self-collide.
    if (!force && quote_number && quote_number.trim()) {
      const dupNum = await pool.query(
        `SELECT id, account_name, amount, quote_date FROM quotes
          WHERE LOWER(TRIM(quote_number)) = LOWER($1)
            AND id != $2
            AND rep_id IS NOT DISTINCT FROM (SELECT rep_id FROM quotes WHERE id = $2)
          ORDER BY id DESC LIMIT 1`,
        [quote_number.trim(), req.params.id]
      );
      if (dupNum.rows.length > 0) {
        const ex = dupNum.rows[0];
        return res.status(409).json({
          error: 'duplicate',
          message: 'A quote with number "' + quote_number.trim() + '" already exists.',
          existing_id: ex.id,
          existing: { account_name: ex.account_name, amount: ex.amount, quote_date: ex.quote_date }
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
        customer_number = $14,
        follow_up_enabled = $15,
        updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [
        req.params.id,
        quote_number ? quote_number.trim() : null,
        status || 'Draft',
        acctName,
        contact_name || null,
        amount ? parseFloat(amount) : null,
        products || null,
        comments || null,
        quote_date || null,
        fu.date,
        pdf_filename || null,
        pdf_data || null,
        rep_name || null,
        cnum,
        fu.enabled
      ]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Quote not found' });
    const savedQuote = result.rows[0];
    console.log('[quotes] PUT updated quote id=' + savedQuote.id + ' account=' + savedQuote.account_name);
    res.json({ success: true, quote: savedQuote });
  } catch (e) {
    console.error('[quotes] PUT /api/quotes/:id error:', e.message, e.stack);
    res.status(500).json({ error: e.message || 'Failed to update quote' });
  }
});

// POST /:id/pdf — attach (or clear) a quote's PDF as a SEPARATE step from the
// quote-field save. The base64 PDF is a multi-MB body; keeping it out of the
// main save JSON means a large/slow/truncated PDF body can never turn the quote
// save itself into a body-parse 400. This endpoint is best-effort: if it fails,
// the quote row is already saved and the client tells the rep to retry the PDF.
router.post('/:id/pdf', async (req, res) => {
  try {
    if (!req.session || !req.session.user || !req.session.user.id) {
      return res.status(401).json({ error: 'Session expired. Please log in again.' });
    }
    const { pdf_data, pdf_filename, remove } = req.body;
    if (remove) {
      const r = await pool.query(
        `UPDATE quotes SET pdf_data = NULL, pdf_filename = NULL, updated_at = NOW()
          WHERE id = $1 RETURNING id, pdf_filename`,
        [req.params.id]
      );
      if (!r.rows.length) return res.status(404).json({ error: 'Quote not found' });
      return res.json({ success: true, quote: r.rows[0] });
    }
    if (!pdf_data) return res.status(400).json({ error: 'No PDF data provided' });
    const r = await pool.query(
      `UPDATE quotes SET pdf_data = $2, pdf_filename = $3, updated_at = NOW()
        WHERE id = $1 RETURNING id, pdf_filename`,
      [req.params.id, pdf_data, pdf_filename || null]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Quote not found' });
    console.log('[quotes] POST /:id/pdf attached pdf to quote id=' + req.params.id);
    res.json({ success: true, quote: r.rows[0] });
  } catch (e) {
    console.error('[quotes] POST /api/quotes/:id/pdf error:', e.message);
    res.status(500).json({ error: e.message || 'Failed to attach PDF' });
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
        max_tokens: 800,
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
              text: 'Extract fields from this sales quote or proposal PDF. Respond ONLY with a valid JSON object, no markdown or explanation:\n{\n  "quote_number": "the QUOTE/PROPOSAL/ESTIMATE number (the seller\'s number for THIS document) or null",\n  "customer_number": "the CUSTOMER/ACCOUNT number (the buyer\'s account/customer ID, often labeled Customer #, Account #, Cust No) or null",\n  "account_name": "the CUSTOMER company the quote is sold/addressed to (Sold To, Bill To, Customer, Quoted To, Attention company) or null — NEVER the job or project name",\n  "job_name": "the job / project / ship-to site name if shown (e.g. a church, school, or building project) or null",\n  "contact_name": "contact person full name or null",\n  "amount": "total dollar amount as number string like \\"1234.56\\" with no dollar sign, or null",\n  "products": "concise summary of all products or line items, max 150 chars, or null",\n  "quote_date": "quote date in YYYY-MM-DD format or null",\n  "follow_up_date": "follow-up or expiry date in YYYY-MM-DD format or null",\n  "comments": "relevant notes, terms, or special instructions max 200 chars or null"\n}\nRules: use null for any field you cannot find with confidence. Quotes often show BOTH a job/project name (e.g. "Johnson Ferry Baptist Church") and the customer company buying the material (e.g. "Mid-Atlantic Roofing Supply") — account_name is ALWAYS the customer company, and the job goes in job_name. If the only company shown is a job/project, still put it in job_name and leave account_name null. For amount use the GRAND TOTAL only. quote_number and customer_number are DIFFERENT fields — never put the same value in both. If the document shows only ONE number and you cannot tell which kind it is, put it in customer_number and leave quote_number null. Return ONLY the JSON object.'
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

    // Customer, not job: if the model returned the job name as the account too,
    // leave the account blank so the rep picks the customer (the job is noted).
    if (extracted.job_name && extracted.account_name &&
        accountKey(extracted.job_name) === accountKey(extracted.account_name)) {
      delete extracted.account_name;
    }
    if (extracted.job_name) {
      const jobNote = 'Job: ' + extracted.job_name;
      extracted.comments = extracted.comments ? jobNote + ' — ' + extracted.comments : jobNote;
    }
    // Snap to an existing account's spelling (same matching rules as duplicates).
    if (extracted.account_name) {
      const canon = await canonicalAccountName(extracted.account_name).catch(() => null);
      if (canon) { extracted.account_name_raw = extracted.account_name; extracted.account_name = canon; }
    }

    res.json(extracted);

  } catch (e) {
    console.error('PDF parse error:', e.message);
    res.json({ _error: e.message });
  }
});

// Save a contact typed on a quote to the account's Contacts list when it's new —
// not the account's main contact and not already listed (case-insensitive).
// Returns true when a contact was added.
async function saveQuoteContact(prospectId, mainContact, name, userId) {
  const n = String(name || '').trim();
  if (!n) return false;
  if (mainContact && mainContact.trim().toLowerCase() === n.toLowerCase()) return false;
  const r = await pool.query(
    `INSERT INTO contacts (prospect_id, name, status, created_by)
     SELECT $1, $2, 'New', $3
      WHERE NOT EXISTS (SELECT 1 FROM contacts WHERE prospect_id=$1 AND LOWER(TRIM(name)) = LOWER($2))`,
    [prospectId, n, userId]);
  return r.rowCount > 0;
}

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

    // ── Check if this company already exists for this user — same account rule
    //    as duplicate review (Inc/LLC/Co/Company/punctuation don't matter; a city
    //    after the name is a separate location). Prefer the most-active record.
    const existing = await pool.query(
      `SELECT id, company, contact, phone, email, city, website FROM prospects
       WHERE user_id = $1 AND ${accountKeySql('company')} = $2
       ORDER BY last_activity_at DESC NULLS LAST, id ASC
       LIMIT 1`,
      [userId, accountKey(company)]
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

      // Contact: fill the account's main contact only if blank. A different person
      // on a quote is added to the account's Contacts list below instead of
      // overwriting the main contact.
      if (contact && (!p.contact || p.contact.trim() === '')) add('contact', contact);
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

      const contactAdded = await saveQuoteContact(p.id, p.contact, contact, userId);
      return res.json({
        created: false,
        updated: updates.length > 0,
        contact_added: contactAdded,
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