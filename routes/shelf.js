// Shelf photos (Kody): a rep snaps the shelf at a dealer, AI reads the brands,
// products and prices it can see, and the read is kept on the account so the
// next visit starts with what was on the shelf last time.
const express = require('express');
const { pool } = require('../db');
const llm = require('../lib/llm');
const router = express.Router();

const MAX_BYTES = 6 * 1024 * 1024;
const TYPES = ['image/jpeg', 'image/png', 'image/webp'];
const OUR_LINES = ['Soudal', 'ShurTape', 'Alum-A-Pole', 'Fortress', 'BOSS'];

let ready = null;
function ensure() {
  if (!ready) ready = pool.query(`
    CREATE TABLE IF NOT EXISTS shelf_photos (
      id SERIAL PRIMARY KEY,
      prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      mime TEXT NOT NULL, size INTEGER NOT NULL, data BYTEA NOT NULL,
      note TEXT, reading JSONB, read_error TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE INDEX IF NOT EXISTS idx_shelf_photos_prospect ON shelf_photos(prospect_id, created_at DESC);`)
    .catch(e => { ready = null; throw e; });
  return ready;
}
router.use(async (req, res, next) => { try { await ensure(); next(); } catch (e) { res.status(500).json({ error: e.message }); } });

const PROMPT = `This is a photo of a store shelf or display at a building supply dealer.
List every product you can actually read. Do not guess at anything you can't read.
Our lines are: ${OUR_LINES.join(', ')}.
Return ONLY a JSON object:
{
  "items": [{"brand": "", "product": "", "size": "", "price": "", "facings": 0, "ours": false}],
  "our_lines_seen": [],
  "competitor_brands": [],
  "empty_spots": "",
  "summary": ""
}
Rules: price only if a tag is readable, as written (e.g. "$8.97"), else "". facings is how many fronts of that item are visible, 0 if unsure. ours is true only for our lines. empty_spots notes any visible gaps or out-of-stock spots, else "". summary is one or two plain sentences a sales rep would want, no fluff.`;

function clean(reading) {
  const s = v => String(v == null ? '' : v).trim().slice(0, 120);
  const items = (Array.isArray(reading.items) ? reading.items : []).slice(0, 80).map(i => ({
    brand: s(i.brand), product: s(i.product), size: s(i.size), price: s(i.price),
    facings: Math.max(0, Math.min(99, parseInt(i.facings) || 0)),
    ours: OUR_LINES.some(l => s(i.brand).toLowerCase().includes(l.toLowerCase()))
  })).filter(i => i.brand || i.product);
  const ours = [...new Set(items.filter(i => i.ours).map(i => OUR_LINES.find(l => i.brand.toLowerCase().includes(l.toLowerCase()))))];
  const comp = [...new Set(items.filter(i => !i.ours && i.brand).map(i => i.brand))];
  return { items, our_lines_seen: ours, competitor_brands: comp, empty_spots: s(reading.empty_spots).slice(0, 300), summary: String(reading.summary || '').trim().slice(0, 500) };
}

async function readPhoto(id) {
  const p = (await pool.query('SELECT mime, data FROM shelf_photos WHERE id=$1', [id])).rows[0];
  try {
    const text = await llm.vision({ image: p.data.toString('base64'), mediaType: p.mime, prompt: PROMPT });
    const reading = clean(llm.parseJson(text));
    if (!reading.items.length && !reading.summary) throw new Error('Nothing readable in the photo');
    await pool.query('UPDATE shelf_photos SET reading=$2, read_error=NULL WHERE id=$1', [id, reading]);
  } catch (e) {
    console.error('[shelf] read failed:', e.message);
    await pool.query('UPDATE shelf_photos SET read_error=$2 WHERE id=$1',
      [id, /Nothing readable/.test(e.message) ? 'Couldn\'t read any products. Try closer, with the price tags in frame.' : 'The photo reader didn\'t answer. Try Read again in a minute.']);
  }
}

const COLS = `s.id, s.prospect_id, s.user_id, s.note, s.reading, s.read_error, s.created_at, s.size,
  COALESCE(NULLIF(TRIM(u.name),''), u.email) AS taken_by`;
async function account(raw) {
  const r = await pool.query(`SELECT p.id FROM prospects p JOIN users u ON u.id=p.user_id WHERE p.id=$1 AND u.disabled_at IS NULL`, [parseInt(raw) || 0]);
  return r.rows.length ? r.rows[0].id : null;
}
async function one(id) {
  return (await pool.query(`SELECT ${COLS} FROM shelf_photos s LEFT JOIN users u ON u.id=s.user_id WHERE s.id=$1`, [id])).rows[0];
}

router.get('/account/:pid', async (req, res) => {
  const pid = await account(req.params.pid);
  if (!pid) return res.status(404).json({ error: 'Account not found' });
  const r = await pool.query(`SELECT ${COLS} FROM shelf_photos s LEFT JOIN users u ON u.id=s.user_id WHERE s.prospect_id=$1 ORDER BY s.created_at DESC LIMIT 30`, [pid]);
  res.json({ photos: r.rows });
});

// { image: base64 or data URL, mime, note }
router.post('/account/:pid', async (req, res) => {
  const pid = await account(req.params.pid);
  if (!pid) return res.status(404).json({ error: 'Account not found' });
  const b = req.body || {};
  let data = String(b.image || '').trim(), mime = b.mime || 'image/jpeg';
  const m = data.match(/^data:([^;]+);base64,(.*)$/s);
  if (m) { mime = m[1]; data = m[2]; }
  if (!TYPES.includes(mime)) return res.status(400).json({ error: 'Use a JPG or PNG photo' });
  const buf = Buffer.from(data, 'base64');
  if (buf.length < 100) return res.status(400).json({ error: 'No photo came through' });
  if (buf.length > MAX_BYTES) return res.status(400).json({ error: 'That photo is too large' });
  const ins = await pool.query(`INSERT INTO shelf_photos (prospect_id, user_id, mime, size, data, note) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
    [pid, req.session.user.id, mime, buf.length, buf, String(b.note || '').trim().slice(0, 500) || null]);
  const id = ins.rows[0].id;
  await readPhoto(id);
  res.json({ photo: await one(id) });
});

router.post('/:id/read', async (req, res) => {
  const p = await one(parseInt(req.params.id) || 0);
  if (!p || !(await account(p.prospect_id))) return res.status(404).json({ error: 'Not found' });
  await readPhoto(p.id);
  res.json({ photo: await one(p.id) });
});

router.get('/:id/image', async (req, res) => {
  const r = await pool.query('SELECT mime, data FROM shelf_photos WHERE id=$1', [parseInt(req.params.id) || 0]);
  if (!r.rows.length) return res.status(404).send('Not found');
  res.setHeader('Content-Type', r.rows[0].mime);
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Cache-Control', 'private, max-age=86400');
  res.end(r.rows[0].data);
});

router.delete('/:id', async (req, res) => {
  const p = await one(parseInt(req.params.id) || 0);
  if (!p) return res.status(404).json({ error: 'Not found' });
  const me = req.session.user;
  if (p.user_id !== me.id && me.role !== 'manager') return res.status(403).json({ error: 'Only the rep who took it or a manager can remove it' });
  await pool.query('DELETE FROM shelf_photos WHERE id=$1', [p.id]);
  res.json({ success: true });
});

module.exports = router;
module.exports._clean = clean;
