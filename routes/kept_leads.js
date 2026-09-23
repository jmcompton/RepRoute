const express = require('express');
const { pool } = require('../db');
const router = express.Router();

// ════════════════════════════════════════════════════════════════
// KEPT LEADS — "Keep on my leads": leads a rep chose to keep on the Lead
// Finder after logging a call (shown with a "Called — follow up" badge).
// Stored per user so the list and badges show on every device.
// lead_key = place_id, or the normalized company name when there is none.
// ════════════════════════════════════════════════════════════════

const MAX_KEPT = 500;

function validKey(k) { return typeof k === 'string' && k.length > 0 && k.length <= 300; }

// ── GET /api/kept-leads → { key: leadObj, ... } ──────────────────
router.get('/', async (req, res) => {
  try {
    const r = await pool.query(
      'SELECT lead_key, lead FROM kept_leads WHERE user_id=$1 ORDER BY updated_at DESC LIMIT $2',
      [req.session.user.id, MAX_KEPT]
    );
    const out = {};
    for (const row of r.rows) out[row.lead_key] = row.lead;
    res.set('Cache-Control', 'no-store');
    res.json(out);
  } catch (err) {
    console.error('[kept-leads] GET error:', err.message);
    res.status(500).json({ error: 'Failed to load kept leads' });
  }
});

// ── POST /api/kept-leads/sync ────────────────────────────────────
// Applies a batch of queued changes in order. Body: { ops: [{op:'keep',key,lead} | {op:'remove',key}] }
// Also used for the one-time upload of leads kept in the browser before this existed.
router.post('/sync', async (req, res) => {
  const uid = req.session.user.id;
  const ops = Array.isArray(req.body && req.body.ops) ? req.body.ops.slice(0, MAX_KEPT) : null;
  if (!ops) return res.status(400).json({ error: 'ops must be an array' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const o of ops) {
      if (!o || !validKey(o.key)) continue;
      if (o.op === 'remove') {
        await client.query('DELETE FROM kept_leads WHERE user_id=$1 AND lead_key=$2', [uid, o.key]);
      } else if (o.op === 'keep' && o.lead && typeof o.lead === 'object') {
        await client.query(
          `INSERT INTO kept_leads (user_id, lead_key, lead, updated_at)
           VALUES ($1,$2,$3::jsonb,NOW())
           ON CONFLICT (user_id, lead_key) DO UPDATE SET lead=EXCLUDED.lead, updated_at=NOW()`,
          [uid, o.key, JSON.stringify(o.lead)]
        );
      }
    }
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[kept-leads] sync error:', err.message);
    res.status(500).json({ error: 'Failed to save kept leads' });
  } finally {
    client.release();
  }
});

module.exports = router;
