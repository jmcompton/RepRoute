const express = require('express');
const { pool } = require('../db');
const router = express.Router();

// ════════════════════════════════════════════════════════════════
// VALID CATEGORIES — 8 specific types only
// No "General Contractor" or vague categories allowed
// ════════════════════════════════════════════════════════════════
const DISTRIBUTOR_CATEGORIES = new Set([
  'Roofing Distributor',
  'Decking Distributor',
  'Siding Distributor',
  'Window & Door Distributor'
]);

const CONTRACTOR_CATEGORIES = new Set([
  'Roofing Contractor',
  'Decking Contractor',
  'Siding Contractor',
  'Window & Door Installer',
  'Cornice Contractor',
  'Construction Fasteners'
]);

function resolveCompanyType(category) {
  if (!category) return 'Contractor';
  const cat = category.trim();
  if (DISTRIBUTOR_CATEGORIES.has(cat)) return 'Distributor';
  // Fuzzy fallback for legacy/imported data
  const lower = cat.toLowerCase();
  if (lower.includes('distributor') || lower.includes('dealer') ||
      lower.includes('supply') || lower.includes('wholesale') ||
      lower.includes('lumber') || lower.includes('building material')) {
    return 'Distributor';
  }
  return 'Contractor';
}

// ── GET /api/prospects/team ──────────────────────────────────────
router.get('/team', async (req, res) => {
  const uid = req.session.user.id;
  try {
    const result = await pool.query(
      `SELECT DISTINCT ON (p.user_id, LOWER(REGEXP_REPLACE(p.company,'[^a-zA-Z0-9]','','g')), LOWER(COALESCE(p.city,'')))
              p.*,
              COALESCE(NULLIF(TRIM(u.name),''), u.email) as rep_name,
              lc.outcome as last_call_outcome,
              lc.notes as last_call_notes,
              lc.call_date as last_call_date
       FROM prospects p
       JOIN users u ON p.user_id = u.id
       LEFT JOIN LATERAL (
         SELECT outcome, notes, call_date
         FROM calls
         WHERE prospect_id = p.id
         ORDER BY call_date DESC, created_at DESC
         LIMIT 1
       ) lc ON true
       WHERE p.user_id != $1
       ORDER BY p.user_id,
                LOWER(REGEXP_REPLACE(p.company,'[^a-zA-Z0-9]','','g')),
                LOWER(COALESCE(p.city,'')),
                p.id ASC`,
      [uid]
    );
    const sorted = result.rows.sort((a, b) => {
      const ra = (a.rep_name||'').toLowerCase();
      const rb = (b.rep_name||'').toLowerCase();
      if (ra < rb) return -1;
      if (ra > rb) return 1;
      return (a.company||'').toLowerCase().localeCompare((b.company||'').toLowerCase());
    });
    res.json(sorted);
  } catch(e) {
    console.error('team contacts error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/prospects/contacts/search?q=&user_id= ───────────────
// Live autocomplete for accounts page search
// Returns up to 8 distinct companies matching the query
router.get('/contacts/search', async (req, res) => {
  const uid = req.session.user.id;
  const { q, rep_user_id } = req.query;
  if (!q || q.trim().length < 2) return res.json([]);
  try {
    // Allow manager to scope search to a specific rep's accounts
    const targetUid = rep_user_id ? parseInt(rep_user_id) : uid;
    const result = await pool.query(
      `SELECT DISTINCT ON (LOWER(TRIM(company)))
              id, company, city, state, category, company_type
       FROM prospects
       WHERE user_id = $1
         AND LOWER(company) ILIKE $2
       ORDER BY LOWER(TRIM(company)), id ASC
       LIMIT 8`,
      [targetUid, '%' + q.trim().toLowerCase() + '%']
    );
    res.json(result.rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/prospects ───────────────────────────────────────────
router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const { category, status, data_status, company_type, search } = req.query;
  let query = 'SELECT * FROM prospects WHERE user_id=$1';
  const params = [uid];
  if (category && category !== 'All') {
    params.push(category); query += ` AND category=$${params.length}`;
  }
  if (status && status !== 'All') {
    params.push(status); query += ` AND status=$${params.length}`;
  }
  if (data_status && data_status !== 'All') {
    // Contacted/Unvetted are computed LIVE from call activity (not just the
    // stored data_status column) so the filter is correct for every rep — the
    // stored column is only reliably set for accounts that were backfilled.
    // 'Verified CRM Data' remains a pure column match (it's a manual flag).
    if (data_status === 'Contacted') {
      query += ` AND (data_status IN ('Contacted','Verified CRM Data')
                      OR EXISTS (SELECT 1 FROM calls c WHERE c.prospect_id = prospects.id AND c.user_id = $1))`;
    } else if (data_status === 'Unvetted') {
      query += ` AND data_status NOT IN ('Contacted','Verified CRM Data')
                 AND NOT EXISTS (SELECT 1 FROM calls c WHERE c.prospect_id = prospects.id AND c.user_id = $1)`;
    } else {
      params.push(data_status); query += ` AND data_status=$${params.length}`;
    }
  }
  if (company_type && company_type !== 'All') {
    params.push(company_type); query += ` AND company_type=$${params.length}`;
  }
  if (search) {
    params.push('%' + search + '%');
    const p = params.length;
    query += ` AND (company ILIKE $${p} OR contact ILIKE $${p} OR phone ILIKE $${p} OR email ILIKE $${p} OR city ILIKE $${p} OR category ILIKE $${p})`;
  }
  query += ' ORDER BY created_at DESC';
  const result = await pool.query(query, params);
  res.json(result.rows);
});

// ── POST /api/prospects ──────────────────────────────────────────
router.post('/', async (req, res) => {
  const uid = req.session.user.id;
  const {
    company, category, city, state, phone, contact, website, products,
    status, priority, notes, source, address, google_place_id,
    data_status, manufacturer_assoc, email,
    title, mobile, zip
  } = req.body;

  const company_type = resolveCompanyType(category);
  const ds = data_status || 'Unvetted';

  const result = await pool.query(
    `INSERT INTO prospects
       (user_id, company, category, company_type, city, state, phone, email,
        contact, website, products, status, priority, notes, source,
        address, google_place_id, data_status, manufacturer_assoc,
        title, mobile, zip, last_activity_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,NOW())
     RETURNING *`,
    [uid, company, category, company_type,
     city || null, state || 'GA', phone || null, email || null,
     contact || null, website || null, products || null,
     status || 'New', priority || 'Medium', notes || null, source || 'Manual',
     address || null, google_place_id || null, ds, manufacturer_assoc || null,
     title || null, mobile || null, zip || null]
  );
  res.json(result.rows[0]);
});

// ── PUT /api/prospects/:id ───────────────────────────────────────
router.put('/:id', async (req, res) => {
  const uid = req.session.user.id;
  const id = req.params.id;
  const {
    company, category, city, state, phone, email, contact, website,
    products, status, priority, notes, pipeline_stage, google_place_id,
    address, data_status, manufacturer_assoc, business_card_image,
    title, mobile, zip
  } = req.body;

  const fields = [];
  const vals = [];
  const add = (col, val) => {
    if (val !== undefined) { vals.push(val); fields.push(col + '=$' + vals.length); }
  };

  add('company', company);
  add('category', category);
  // Auto-resolve company_type when category changes
  if (category !== undefined) {
    vals.push(resolveCompanyType(category));
    fields.push('company_type=$' + vals.length);
  }
  add('city', city);
  add('state', state);
  add('phone', phone);
  add('email', email);
  add('contact', contact);
  add('website', website);
  add('products', products);
  add('status', status);
  add('priority', priority);
  add('notes', notes);
  add('pipeline_stage', pipeline_stage);
  add('google_place_id', google_place_id);
  add('address', address);
  add('manufacturer_assoc', manufacturer_assoc);
  add('business_card_image', business_card_image);
  add('title', title);
  add('mobile', mobile);
  add('zip', zip);

  // data_status transitions
  if (data_status !== undefined) {
    add('data_status', data_status);
    if (data_status === 'Verified CRM Data') {
      vals.push('NOW()');
      fields.push('verified_at=NOW()');
    }
  }

  // Always bump last_activity_at on any update
  fields.push('last_activity_at=NOW()');

  if (fields.length <= 1) return res.json({ error: 'Nothing to update' });

  vals.push(id);
  vals.push(uid);
  const result = await pool.query(
    'UPDATE prospects SET ' + fields.join(', ') +
    ' WHERE id=$' + (vals.length - 1) + ' AND user_id=$' + vals.length + ' RETURNING *',
    vals
  );
  res.json(result.rows[0] || { error: 'Not found' });
});

// ── GET /api/prospects/:id ───────────────────────────────────────
router.get('/:id', async (req, res) => {
  const uid = req.session.user.id;
  try {
    const result = await pool.query(
      'SELECT * FROM prospects WHERE id=$1 AND user_id=$2',
      [req.params.id, uid]
    );
    if (!result.rows[0]) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/prospects/:id/fill-blanks ──────────────────────────
// Update only fields that are currently NULL or empty in the database.
// Used by Voice Logger to safely merge new contact data without overwriting
// existing values. All fields are optional in the request body.
router.post('/:id/fill-blanks', async (req, res) => {
  const uid = req.session.user.id;
  const id  = req.params.id;
  const allowed = ['phone','mobile','email','address','city','state','zip',
                   'website','title','contact','business_card_image'];
  const fields = [];
  const vals   = [];

  allowed.forEach(function(col) {
    const v = req.body[col];
    if (v !== undefined && v !== null && String(v).trim() !== '') {
      vals.push(String(v).trim());
      // COALESCE(NULLIF(TRIM(col),''), $n) — only updates if current value is NULL or blank
      fields.push(`${col} = COALESCE(NULLIF(TRIM(${col}),''), $${vals.length})`);
    }
  });

  if (fields.length === 0) return res.json({ ok: true, updated: 0 });

  vals.push(id);
  vals.push(uid);
  try {
    await pool.query(
      `UPDATE prospects SET ${fields.join(', ')}, last_activity_at=NOW()
       WHERE id=$${vals.length - 1} AND user_id=$${vals.length}`,
      vals
    );
    res.json({ ok: true, updated: fields.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── DELETE /api/prospects/:id ────────────────────────────────────
router.delete('/:id', async (req, res) => {
  await pool.query('DELETE FROM prospects WHERE id=$1 AND user_id=$2',
    [req.params.id, req.session.user.id]);
  res.json({ success: true });
});

// ── PATCH /api/prospects/:id — update contact name ───────────────
router.patch('/:id', async (req, res) => {
  const uid = req.session.user.id;
  const { contact } = req.body;
  if (!contact) return res.status(400).json({ error: 'contact required' });
  try {
    await pool.query(
      'UPDATE prospects SET contact=$1, last_activity_at=NOW() WHERE id=$2 AND user_id=$3',
      [contact, req.params.id, uid]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/prospects/:id/stage ────────────────────────────────
router.post('/:id/stage', async (req, res) => {
  const uid = req.session.user.id;
  const { pipeline_stage } = req.body;
  if (!pipeline_stage) return res.status(400).json({ error: 'pipeline_stage required' });
  try {
    await pool.query(
      'UPDATE prospects SET pipeline_stage=$1, last_activity_at=NOW() WHERE id=$2 AND user_id=$3',
      [pipeline_stage, req.params.id, uid]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/prospects/:id/verify ── mark Verified CRM Data ─────
router.post('/:id/verify', async (req, res) => {
  const uid = req.session.user.id;
  try {
    const result = await pool.query(
      `UPDATE prospects
       SET data_status='Verified CRM Data', verified_at=NOW(), last_activity_at=NOW()
       WHERE id=$1 AND user_id=$2 RETURNING id, company, data_status`,
      [req.params.id, uid]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true, record: result.rows[0] });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/prospects/:id/mark-contacted ────────────────────────
router.post('/:id/mark-contacted', async (req, res) => {
  const uid = req.session.user.id;
  try {
    await pool.query(
      `UPDATE prospects
       SET data_status=CASE WHEN data_status='Unvetted' THEN 'Contacted' ELSE data_status END,
           last_activity_at=NOW()
       WHERE id=$1 AND user_id=$2`,
      [req.params.id, uid]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/prospects/team-calls/:id ────────────────────────────
router.get('/team-calls/:id', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT c.*, u.name as rep_name
       FROM calls c
       JOIN prospects p ON c.prospect_id = p.id
       JOIN users u ON c.user_id = u.id
       WHERE p.id = $1 AND p.user_id != $2
       ORDER BY c.call_date DESC, c.created_at DESC`,
      [req.params.id, req.session.user.id]
    );
    res.json(result.rows);
  } catch(e) {
    console.error('team-calls error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/prospects/:id/manager-note ─────────────────────────
router.post('/:id/manager-note', async (req, res) => {
  const { note } = req.body;
  try {
    const result = await pool.query(
      'UPDATE prospects SET manager_notes=$1 WHERE id=$2 RETURNING id, manager_notes',
      [note || null, req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Not found' });
    res.json({ ok: true, record: result.rows[0] });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/prospects/stats ─────────────────────────────────────
router.get('/stats', async (req, res) => {
  const uid = req.session.user.id;
  try {
    const r = await pool.query(
      `SELECT
         COUNT(*) FILTER (WHERE company_type='Distributor') as distributors,
         COUNT(*) FILTER (WHERE company_type='Contractor') as contractors,
         COUNT(*) FILTER (WHERE data_status='Unvetted') as unvetted,
         COUNT(*) FILTER (WHERE data_status='Contacted') as contacted,
         COUNT(*) FILTER (WHERE data_status='Verified CRM Data') as verified,
         COUNT(*) as total
       FROM prospects WHERE user_id=$1`,
      [uid]
    );
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

module.exports = router;
