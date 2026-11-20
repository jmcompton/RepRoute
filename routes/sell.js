// /api/sell: the "What you sell" page. Everyone can read it; managers edit it.
const express = require('express');
const { pool } = require('../db');
const sell = require('../lib/sell');
const { resolveLine } = require('../lib/lines-store');
const router = express.Router();

// The Lead Finder segments a line can be bought by. Kept in step with
// routes/morning.js SEGMENT_SEARCH_CONFIG, which is where the searches live.
const SEGMENTS = Object.keys(require('./morning').SEGMENT_SEARCH_CONFIG || {});

function managerOnly(req, res, next) {
  if (req.session.user.role !== 'manager') return res.status(403).json({ error: 'Only a manager can change what the team sells' });
  next();
}
function clean(b) {
  const t = (v, n) => (v == null ? null : String(v).trim().slice(0, n) || null);
  const segs = (Array.isArray(b.buyer_segments) ? b.buyer_segments : []).filter(s => SEGMENTS.includes(s));
  return { what: t(b.what, 300), competitors: t(b.competitors, 300), match_words: t(b.match_words, 300), buyer_segments: [...new Set(segs)] };
}

router.get('/', async (req, res) => {
  res.json({ lines: await sell.getLines(true), segments: SEGMENTS, can_edit: req.session.user.role === 'manager' });
});

router.post('/', managerOnly, async (req, res) => {
  const name = String((req.body || {}).name || '').trim().slice(0, 80);
  if (!name) return res.status(400).json({ error: 'Give the line a name' });
  const id = await resolveLine(pool, name);
  const c = clean(req.body || {});
  const max = (await pool.query('SELECT COALESCE(MAX(sort_order), 0)::int m FROM lines WHERE featured')).rows[0].m;
  await pool.query(`UPDATE lines SET featured=TRUE, what=$2, competitors=$3, match_words=$4, buyer_segments=$5, sort_order=$6 WHERE id=$1`,
    [id, c.what, c.competitors, c.match_words, c.buyer_segments, max + 1]);
  sell.clearCache();
  res.json({ id });
});

router.put('/:id', managerOnly, async (req, res) => {
  const id = parseInt(req.params.id) || 0;
  const c = clean(req.body || {});
  const r = await pool.query(`UPDATE lines SET what=$2, competitors=$3, match_words=$4, buyer_segments=$5 WHERE id=$1 AND featured RETURNING id`,
    [id, c.what, c.competitors, c.match_words, c.buyer_segments]);
  if (!r.rows.length) return res.status(404).json({ error: 'Line not found' });
  sell.clearCache();
  res.json({ success: true });
});

// Taking a line off the page keeps it (commission history still points at it).
router.delete('/:id', managerOnly, async (req, res) => {
  await pool.query('UPDATE lines SET featured=FALSE WHERE id=$1', [parseInt(req.params.id) || 0]);
  sell.clearCache();
  res.json({ success: true });
});

module.exports = router;
