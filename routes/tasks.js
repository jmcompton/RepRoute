const express = require('express');
const { pool } = require('../db');
const router = express.Router();

// ════════════════════════════════════════════════════════════════
// TASKS — the rep's to-do list (Tasks page). Stored per user so tasks
// created on one device show on every device. Previously localStorage only.
// client_id = the id a task had in the browser before this existed; it makes
// the one-time upload of old browser tasks safe to repeat (no duplicates).
// ════════════════════════════════════════════════════════════════

const TYPES = new Set(['call', 'email', 'sample', 'literature', 'meeting', 'other']);

function shape(row) {
  return {
    id: row.id,
    title: row.title,
    type: row.type,
    contact: row.contact || '',
    due: row.due || '',
    done: !!row.done,
    created: row.created_at,
  };
}

function clean(t) {
  const title = String((t && t.title) || '').trim().slice(0, 500);
  const type = TYPES.has(t && t.type) ? t.type : 'other';
  const contact = String((t && t.contact) || '').trim().slice(0, 200) || null;
  const due = /^\d{4}-\d{2}-\d{2}$/.test((t && t.due) || '') ? t.due : null;
  return { title, type, contact, due, done: !!(t && t.done) };
}

// ── GET /api/tasks ───────────────────────────────────────────────
router.get('/', async (req, res) => {
  try {
    const r = await pool.query(
      'SELECT * FROM tasks WHERE user_id=$1 ORDER BY created_at DESC, id DESC',
      [req.session.user.id]
    );
    res.set('Cache-Control', 'no-store');
    res.json(r.rows.map(shape));
  } catch (err) {
    console.error('[tasks] GET error:', err.message);
    res.status(500).json({ error: 'Failed to load tasks' });
  }
});

// ── POST /api/tasks ──────────────────────────────────────────────
router.post('/', async (req, res) => {
  const t = clean(req.body);
  if (!t.title) return res.status(400).json({ error: 'Task description required' });
  try {
    const r = await pool.query(
      `INSERT INTO tasks (user_id, title, type, contact, due, done)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [req.session.user.id, t.title, t.type, t.contact, t.due, t.done]
    );
    res.json(shape(r.rows[0]));
  } catch (err) {
    console.error('[tasks] POST error:', err.message);
    res.status(500).json({ error: 'Failed to save task' });
  }
});

// ── POST /api/tasks/import ───────────────────────────────────────
// One-time upload of tasks that were saved in the browser. Body: { tasks: [...] }
router.post('/import', async (req, res) => {
  const uid = req.session.user.id;
  const list = Array.isArray(req.body && req.body.tasks) ? req.body.tasks.slice(0, 1000) : null;
  if (!list) return res.status(400).json({ error: 'tasks must be an array' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    let imported = 0;
    for (const raw of list) {
      const t = clean(raw);
      if (!t.title || raw.id == null) continue;
      const created = raw.created && !isNaN(Date.parse(raw.created)) ? raw.created : new Date().toISOString();
      const r = await client.query(
        `INSERT INTO tasks (user_id, client_id, title, type, contact, due, done, created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (user_id, client_id) DO NOTHING`,
        [uid, String(raw.id), t.title, t.type, t.contact, t.due, t.done, created]
      );
      imported += r.rowCount;
    }
    await client.query('COMMIT');
    res.json({ ok: true, imported });
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[tasks] import error:', err.message);
    res.status(500).json({ error: 'Failed to import tasks' });
  } finally {
    client.release();
  }
});

// ── PUT /api/tasks/:id ───────────────────────────────────────────
router.put('/:id', async (req, res) => {
  const id = parseInt(req.params.id);
  if (!id) return res.status(400).json({ error: 'Invalid id' });
  const b = req.body || {};
  const fields = [], vals = [];
  const add = (col, v) => { vals.push(v); fields.push(col + '=$' + vals.length); };
  if (b.done !== undefined) add('done', !!b.done);
  if (b.title !== undefined) {
    const title = String(b.title).trim().slice(0, 500);
    if (!title) return res.status(400).json({ error: 'Task description required' });
    add('title', title);
  }
  if (!fields.length) return res.status(400).json({ error: 'Nothing to update' });
  add('updated_at', new Date());
  vals.push(id, req.session.user.id);
  try {
    const r = await pool.query(
      `UPDATE tasks SET ${fields.join(', ')} WHERE id=$${vals.length - 1} AND user_id=$${vals.length} RETURNING *`,
      vals
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Task not found' });
    res.json(shape(r.rows[0]));
  } catch (err) {
    console.error('[tasks] PUT error:', err.message);
    res.status(500).json({ error: 'Failed to update task' });
  }
});

// ── DELETE /api/tasks/:id ────────────────────────────────────────
router.delete('/:id', async (req, res) => {
  const id = parseInt(req.params.id);
  if (!id) return res.status(400).json({ error: 'Invalid id' });
  try {
    await pool.query('DELETE FROM tasks WHERE id=$1 AND user_id=$2', [id, req.session.user.id]);
    res.json({ ok: true });
  } catch (err) {
    console.error('[tasks] DELETE error:', err.message);
    res.status(500).json({ error: 'Failed to delete task' });
  }
});

module.exports = router;
