const express = require('express');
const { pool } = require('../db');
const router = express.Router();


// GET /api/prospects/team — all teammates' contacts (read-only)
// Returns all contacts from other users — no role restriction so no one is missed
router.get('/team', async (req, res) => {
  const uid = req.session.user.id;
  try {
    // Use DISTINCT ON to deduplicate: keep the oldest record per (user_id, normalized company name + city)
    // Also pull the latest call outcome/notes via LATERAL join
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
    // Re-sort after dedup: by rep name then company name
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

router.get('/', async (req, res) => {
  const uid = req.session.user.id;
  const { category, status, search } = req.query;
  let query = 'SELECT * FROM prospects WHERE user_id=$1';
  const params = [uid];
  if (category && category !== 'All') { params.push(category); query += ` AND category=$${params.length}`; }
  if (status && status !== 'All') { params.push(status); query += ` AND status=$${params.length}`; }
  if (search) {
    params.push('%' + search + '%');
    const p = params.length;
    query += ` AND (company ILIKE $${p} OR contact ILIKE $${p} OR phone ILIKE $${p} OR email ILIKE $${p} OR city ILIKE $${p})`;
  }
  query += ' ORDER BY created_at DESC';
  const result = await pool.query(query, params);
  res.json(result.rows);
});

router.post('/', async (req, res) => {
  const uid = req.session.user.id;
  const { company, category, city, state, phone, contact, website, products, status, priority, notes, source } = req.body;
  const result = await pool.query(
    `INSERT INTO prospects (user_id, company, category, city, state, phone, contact, website, products, status, priority, notes, source)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *`,
    [uid, company, category, city || 'Atlanta', state || 'GA', phone, contact, website, products, status || 'New', priority || 'Medium', notes, source || 'Manual']
  );
  res.json(result.rows[0]);
});

router.put('/:id', async (req, res) => {
  const uid = req.session.user.id;
  const id = req.params.id;
  const { company, category, city, state, phone, email, contact, website, products, status, priority, notes, pipeline_stage, google_place_id, address } = req.body;

  // Build dynamic update — only update fields that were sent
  const fields = [];
  const vals = [];
  const add = (col, val) => { if (val !== undefined) { vals.push(val); fields.push(col + '=$' + vals.length); } };

  add('company', company);
  add('category', category);
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

  if (fields.length === 0) return res.json({ error: 'Nothing to update' });

  vals.push(id);
  vals.push(uid);
  const result = await pool.query(
    'UPDATE prospects SET ' + fields.join(', ') + ' WHERE id=$' + (vals.length - 1) + ' AND user_id=$' + vals.length + ' RETURNING *',
    vals
  );
  res.json(result.rows[0] || { error: 'Not found' });
});

router.delete('/:id', async (req, res) => {
  await pool.query('DELETE FROM prospects WHERE id=$1 AND user_id=$2', [req.params.id, req.session.user.id]);
  res.json({ success: true });
});


router.patch('/:id', async (req, res) => {
  const uid = req.session.user.id;
  const { contact } = req.body;
  if (!contact) return res.status(400).json({ error: 'contact required' });
  try {
    await pool.query(
      'UPDATE prospects SET contact=$1 WHERE id=$2 AND user_id=$3',
      [contact, req.params.id, uid]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// Update just the pipeline stage of a prospect (used for drag-drop)
router.post('/:id/stage', async (req, res) => {
  const uid = req.session.user.id;
  const { pipeline_stage } = req.body;
  if (!pipeline_stage) return res.status(400).json({ error: 'pipeline_stage required' });
  try {
    await pool.query(
      'UPDATE prospects SET pipeline_stage=$1 WHERE id=$2 AND user_id=$3',
      [pipeline_stage, req.params.id, uid]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// GET /api/prospects/team-calls/:id — all call history for a teammate's prospect
router.get('/team-calls/:id', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT c.*
       FROM calls c
       JOIN prospects p ON c.prospect_id = p.id
       WHERE p.id = $1
         AND p.user_id != $2
       ORDER BY c.call_date DESC, c.created_at DESC`,
      [req.params.id, req.session.user.id]
    );
    res.json(result.rows);
  } catch(e) {
    console.error('team-calls error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;

