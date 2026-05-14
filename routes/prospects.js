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
  'Fastener/Tool Dealer'
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
    params.push(data_status); query += ` AND data_status=$${params.length}`;
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
    data_status, manufacturer_assoc, email
  } = req.body;

  const company_type = resolveCompanyType(category);
  const ds = data_status || 'Unvetted';

  const result = await pool.query(
    `INSERT INTO prospects
       (user_id, company, category, company_type, city, state, phone, email,
        contact, website, products, status, priority, notes, source,
        address, google_place_id, data_status, manufacturer_assoc, last_activity_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NOW())
     RETURNING *`,
    [uid, company, category, company_type,
     city || 'Atlanta', state || 'GA', phone || null, email || null,
     contact || null, website || null, products || null,
     status || 'New', priority || 'Medium', notes || null, source || 'Manual',
     address || null, google_place_id || null, ds, manufacturer_assoc || null]
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
    address, data_status, manufacturer_assoc
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
      `SELECT c.*
       FROM calls c
       JOIN prospects p ON c.prospect_id = p.id
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
