// Shared document library (Daniel): spec sheets, price sheets and brochures
// uploaded once and opened by anyone on the team, from the Documents page or
// from any account they're attached to.
const express = require('express');
const { pool } = require('../db');
const router = express.Router();

const MAX_BYTES = 10 * 1024 * 1024;
const TYPES = {
  'application/pdf': 'pdf', 'image/png': 'png', 'image/jpeg': 'jpg', 'image/webp': 'webp',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
  'application/vnd.ms-excel': 'xls', 'application/msword': 'doc', 'text/csv': 'csv'
};
const KINDS = ['Spec sheet', 'Price sheet', 'Brochure', 'Install guide', 'Warranty', 'Other'];
const LINES = ['Soudal', 'ShurTape', 'Alum-A-Pole', 'Fortress', 'BOSS', 'General'];

let ready = null;
function ensure() {
  if (!ready) ready = pool.query(`
    CREATE TABLE IF NOT EXISTS documents (
      id SERIAL PRIMARY KEY, name TEXT NOT NULL, line TEXT, kind TEXT, filename TEXT NOT NULL,
      mime TEXT NOT NULL, size INTEGER NOT NULL, data BYTEA NOT NULL,
      uploaded_by INTEGER REFERENCES users(id) ON DELETE SET NULL, created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS account_documents (
      prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
      added_by INTEGER REFERENCES users(id) ON DELETE SET NULL, created_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (prospect_id, document_id));`).catch(e => { ready = null; throw e; });
  return ready;
}
router.use(async (req, res, next) => { try { await ensure(); next(); } catch (e) { res.status(500).json({ error: e.message }); } });

const LIST_COLS = `d.id, d.name, d.line, d.kind, d.filename, d.mime, d.size, d.created_at, d.uploaded_by,
  COALESCE(NULLIF(TRIM(u.name),''), u.email) AS uploaded_by_name`;

router.get('/', async (req, res) => {
  const r = await pool.query(`SELECT ${LIST_COLS} FROM documents d LEFT JOIN users u ON u.id = d.uploaded_by ORDER BY d.line NULLS LAST, d.name`);
  res.json({ documents: r.rows, kinds: KINDS, lines: LINES });
});

// Upload as JSON: { name, line, kind, filename, mime, data (base64) }
router.post('/', async (req, res) => {
  const b = req.body || {};
  const name = String(b.name || '').trim().slice(0, 200);
  const filename = String(b.filename || '').trim().replace(/[^\x20-\x7e]|[\\/"]/g, '_').slice(0, 200);
  const mime = String(b.mime || '');
  if (!name || !filename) return res.status(400).json({ error: 'Give the document a name and pick a file' });
  if (!TYPES[mime]) return res.status(400).json({ error: 'That file type isn\'t supported. Use PDF, a photo, Excel, Word or CSV.' });
  const buf = Buffer.from(String(b.data || ''), 'base64');
  if (!buf.length) return res.status(400).json({ error: 'The file was empty' });
  if (buf.length > MAX_BYTES) return res.status(400).json({ error: 'That file is over 10 MB' });
  const line = LINES.includes(b.line) ? b.line : 'General';
  const kind = KINDS.includes(b.kind) ? b.kind : 'Other';
  const r = await pool.query(
    `INSERT INTO documents (name, line, kind, filename, mime, size, data, uploaded_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     RETURNING id, name, line, kind, filename, mime, size, created_at`,
    [name, line, kind, filename, mime, buf.length, buf, req.session.user.id]);
  res.json({ document: r.rows[0] });
});

router.get('/:id/file', async (req, res) => {
  const id = parseInt(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).end();
  const r = await pool.query('SELECT filename, mime, data FROM documents WHERE id=$1', [id]);
  if (!r.rows.length) return res.status(404).send('Not found');
  const d = r.rows[0];
  res.setHeader('Content-Type', d.mime);
  res.setHeader('Content-Disposition', `${/^image\/|pdf$/.test(d.mime) ? 'inline' : 'attachment'}; filename="${d.filename}"`);
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Cache-Control', 'private, max-age=300');
  res.end(d.data);
});

router.delete('/:id', async (req, res) => {
  const id = parseInt(req.params.id) || 0;
  const d = (await pool.query('SELECT uploaded_by FROM documents WHERE id=$1', [id])).rows[0];
  if (!d) return res.status(404).json({ error: 'Not found' });
  const me = req.session.user;
  if (d.uploaded_by !== me.id && me.role !== 'manager') return res.status(403).json({ error: 'Only the person who uploaded it or a manager can remove it' });
  await pool.query('DELETE FROM documents WHERE id=$1', [id]);
  res.json({ success: true });
});

// Documents attached to an account (any active rep's account).
async function accountId(raw) {
  const r = await pool.query(`SELECT p.id FROM prospects p JOIN users u ON u.id=p.user_id WHERE p.id=$1 AND u.disabled_at IS NULL`, [parseInt(raw) || 0]);
  return r.rows.length ? r.rows[0].id : null;
}
router.get('/account/:pid', async (req, res) => {
  const pid = await accountId(req.params.pid);
  if (!pid) return res.status(404).json({ error: 'Account not found' });
  const r = await pool.query(
    `SELECT ${LIST_COLS} FROM account_documents a JOIN documents d ON d.id = a.document_id
       LEFT JOIN users u ON u.id = d.uploaded_by WHERE a.prospect_id=$1 ORDER BY a.created_at DESC`, [pid]);
  res.json({ documents: r.rows });
});
router.post('/account/:pid', async (req, res) => {
  const pid = await accountId(req.params.pid);
  if (!pid) return res.status(404).json({ error: 'Account not found' });
  const did = parseInt((req.body || {}).document_id);
  const d = await pool.query('SELECT id FROM documents WHERE id=$1', [did || 0]);
  if (!d.rows.length) return res.status(404).json({ error: 'Document not found' });
  await pool.query(`INSERT INTO account_documents (prospect_id, document_id, added_by) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, [pid, did, req.session.user.id]);
  res.json({ success: true });
});
router.delete('/account/:pid/:did', async (req, res) => {
  const pid = await accountId(req.params.pid);
  if (!pid) return res.status(404).json({ error: 'Account not found' });
  await pool.query('DELETE FROM account_documents WHERE prospect_id=$1 AND document_id=$2', [pid, parseInt(req.params.did) || 0]);
  res.json({ success: true });
});

module.exports = router;
