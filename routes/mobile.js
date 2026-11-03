// Phone app (/m) data endpoints.
// Each screen loads in ONE request so it stays fast on cell signal. Reads are
// team-wide where the desktop app already shows team data (team accounts,
// quotes); "my" lists are scoped to the signed-in rep.
const express = require('express');
const { pool } = require('../db');
const router = express.Router();

const TZ = 'America/New_York';
const isYmd = s => /^\d{4}-\d{2}-\d{2}$/.test(s || '');
const isManager = u => u && u.role === 'manager';

// The phone's own date (the server clock is UTC, which is "tomorrow" in the evening).
async function todayFor(req) {
  if (isYmd(req.query.date)) return req.query.date;
  const r = await pool.query(`SELECT (NOW() AT TIME ZONE '${TZ}')::date::text AS d`);
  return r.rows[0].d;
}

// ── GET /api/m/me ────────────────────────────────────────────────
router.get('/me', (req, res) => {
  const u = req.session.user;
  res.json({ id: u.id, name: u.name, role: u.role, territory: u.territory });
});

// ── GET /api/m/team ──────────────────────────────────────────────
// Reps on the team, for report and account filters.
router.get('/team', async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT id, COALESCE(NULLIF(TRIM(name),''), email) AS name, role
         FROM users WHERE role IN ('rep','manager') ORDER BY name`);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/m/today?date=YYYY-MM-DD ─────────────────────────────
// Today's planned stops, follow-ups due, sample check-ins and quote follow-ups.
router.get('/today', async (req, res) => {
  const uid = req.session.user.id;
  try {
    const today = await todayFor(req);
    const [stops, followups, samples, quotes, logged] = await Promise.all([
      pool.query(
        `SELECT pi.id, pi.account_id, pi.completed_at, pi.sort_order, pi.item_type, pi.appt_time, pi.note,
                COALESCE(p.company, pi.title) AS company, p.city, p.state, p.phone, p.address, p.lat, p.lng,
                lc.next_step, lc.call_date AS last_call_date
           FROM planner_items pi
           LEFT JOIN prospects p ON p.id = pi.account_id
           LEFT JOIN LATERAL (
             SELECT next_step, call_date FROM calls c
              WHERE c.prospect_id = pi.account_id
              ORDER BY c.call_date DESC, c.id DESC LIMIT 1) lc ON TRUE
          WHERE pi.rep_id = $1 AND pi.planned_date = $2
          ORDER BY pi.sort_order ASC, pi.id ASC`, [uid, today]),
      // Latest call per account where I set a next step that is due, and nothing
      // has been logged on that account since.
      pool.query(
        `SELECT DISTINCT ON (c.prospect_id) c.id, c.prospect_id, c.next_step, c.next_step_date::text AS due,
                c.call_date::text AS call_date, p.company, p.city, p.phone,
                (SELECT name FROM contacts ct WHERE ct.prospect_id = p.id ORDER BY ct.is_primary DESC NULLS LAST, ct.id LIMIT 1) AS contact
           FROM calls c JOIN prospects p ON p.id = c.prospect_id
          WHERE c.user_id = $1 AND c.next_step_date IS NOT NULL AND c.next_step_date <= $2::date + 2
            AND NOT EXISTS (SELECT 1 FROM calls c2 WHERE c2.prospect_id = c.prospect_id
                              AND (c2.call_date > c.call_date OR (c2.call_date = c.call_date AND c2.id > c.id)))
          ORDER BY c.prospect_id, c.call_date DESC, c.id DESC`, [uid, today]),
      pool.query(
        `SELECT s.id, s.prospect_id, s.product_line, s.sent_date::text AS sent_date, s.follow_up_date::text AS due,
                p.company, p.phone
           FROM samples s LEFT JOIN prospects p ON p.id = s.prospect_id
          WHERE s.user_id = $1 AND LOWER(COALESCE(s.status,'pending')) NOT IN ('converted','no_interest','no interest','closed','lost','won')
            AND s.closed_at IS NULL
          ORDER BY s.follow_up_date ASC NULLS LAST`, [uid]),
      pool.query(
        `SELECT q.id, q.account_name, q.amount, q.status, q.follow_up_date::text AS due, q.products
           FROM quotes q
          WHERE (q.user_id = $1 OR q.rep_id = $1) AND q.status NOT IN ('Won','Lost')
            AND q.follow_up_date IS NOT NULL AND q.follow_up_date <= $2::date + 2
          ORDER BY q.follow_up_date ASC`, [uid, today]),
      pool.query(`SELECT COUNT(*)::int AS n FROM calls WHERE user_id=$1 AND call_date=$2`, [uid, today])
    ]);
    const follow = followups.rows.sort((a, b) => (a.due || '').localeCompare(b.due || ''));
    res.json({
      date: today,
      name: req.session.user.name,
      stops: stops.rows,
      followups: follow,
      samples: samples.rows,
      quote_followups: quotes.rows,
      logged_today: logged.rows[0].n,
      counts: {
        stops: stops.rows.length,
        followups: follow.filter(f => f.due <= today).length,
        samples: samples.rows.length
      }
    });
  } catch (e) {
    console.error('[m/today]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/m/accounts?q=&scope=mine|team&limit= ────────────────
router.get('/accounts', async (req, res) => {
  const uid = req.session.user.id;
  const q = String(req.query.q || '').trim();
  const scope = req.query.scope === 'mine' ? 'mine' : 'team';
  const limit = Math.min(parseInt(req.query.limit) || 60, 200);
  try {
    const params = [];
    const where = [];
    if (scope === 'mine') { params.push(uid); where.push(`p.user_id = $${params.length}`); }
    if (q) {
      params.push('%' + q.toLowerCase() + '%');
      const n = params.length;
      where.push(`(LOWER(p.company) LIKE $${n} OR LOWER(COALESCE(p.city,'')) LIKE $${n}
                   OR EXISTS (SELECT 1 FROM contacts ct WHERE ct.prospect_id = p.id AND LOWER(ct.name) LIKE $${n}))`);
    }
    params.push(limit);
    const r = await pool.query(
      `SELECT p.id, p.company, p.city, p.state, p.phone, p.status, p.category, p.user_id,
              COALESCE(NULLIF(TRIM(u.name),''), u.email) AS owner,
              lc.call_date::text AS last_visit
         FROM prospects p
         LEFT JOIN users u ON u.id = p.user_id
         LEFT JOIN LATERAL (SELECT call_date FROM calls c WHERE c.prospect_id = p.id
                            ORDER BY call_date DESC, id DESC LIMIT 1) lc ON TRUE
        ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
        ORDER BY (p.user_id = ${parseInt(uid)}) DESC, lc.call_date DESC NULLS LAST, p.company ASC
        LIMIT $${params.length}`, params);
    res.json(r.rows);
  } catch (e) {
    console.error('[m/accounts]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/m/account/:id ───────────────────────────────────────
// Everything the account screen shows, team-wide.
router.get('/account/:id', async (req, res) => {
  const id = parseInt(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'Bad id' });
  try {
    const a = await pool.query(
      `SELECT p.*, COALESCE(NULLIF(TRIM(u.name),''), u.email) AS owner
         FROM prospects p LEFT JOIN users u ON u.id = p.user_id WHERE p.id = $1`, [id]);
    if (!a.rows.length) return res.status(404).json({ error: 'Account not found' });
    const acct = a.rows[0];
    delete acct.business_card_image; delete acct.enrich_candidates;
    const [contacts, calls, quotes, samples, lines] = await Promise.all([
      pool.query(`SELECT id, name, title, phone, email, status, is_primary, created_by
                    FROM contacts WHERE prospect_id=$1 ORDER BY is_primary DESC NULLS LAST, created_at ASC, id ASC`, [id]),
      pool.query(`SELECT c.id, c.call_date::text AS call_date, c.call_type, c.outcome, c.products_discussed,
                         c.next_step, c.next_step_date::text AS next_step_date, c.notes,
                         COALESCE(NULLIF(TRIM(u.name),''), u.email) AS rep
                    FROM calls c LEFT JOIN users u ON u.id = c.user_id
                   WHERE c.prospect_id=$1 ORDER BY c.call_date DESC, c.id DESC LIMIT 15`, [id]),
      pool.query(`SELECT id, user_id, rep_id, account_name, quote_number, status, amount, products, quote_date::text AS quote_date,
                         follow_up_date::text AS follow_up_date, rep_name
                    FROM quotes
                   WHERE LOWER(REGEXP_REPLACE(account_name,'[^a-zA-Z0-9]','','g'))
                       = LOWER(REGEXP_REPLACE($1,'[^a-zA-Z0-9]','','g'))
                   ORDER BY quote_date DESC NULLS LAST, id DESC LIMIT 10`, [acct.company]),
      pool.query(`SELECT id, user_id, product_line, quantity, sent_date::text AS sent_date, follow_up_date::text AS follow_up_date,
                         status, closed_at
                    FROM samples WHERE prospect_id=$1 ORDER BY sent_date DESC LIMIT 10`, [id]),
      pool.query(`SELECT l.name, al.total_sales
                    FROM account_lines al JOIN lines l ON l.id = al.line_id
                   WHERE al.account_id=$1 ORDER BY al.total_sales DESC NULLS LAST`, [id]).catch(() => ({ rows: [] }))
    ]);
    res.json({
      account: acct,
      mine: acct.user_id === req.session.user.id,
      can_delete_contacts: acct.user_id === req.session.user.id || isManager(req.session.user),
      contacts: contacts.rows, calls: calls.rows, quotes: quotes.rows,
      samples: samples.rows,
      // Commission $ stay manager-only; reps see which lines the account buys.
      lines: lines.rows.map(l => isManager(req.session.user) ? l : { name: l.name })
    });
  } catch (e) {
    console.error('[m/account]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/m/pipeline?scope=mine|team ──────────────────────────
router.get('/pipeline', async (req, res) => {
  const uid = req.session.user.id;
  const team = req.query.scope === 'team';
  try {
    const [quotes, samples] = await Promise.all([
      pool.query(
        `SELECT q.id, q.user_id, q.rep_id, q.account_name, q.amount, q.status, q.products, q.quote_date::text AS quote_date,
                q.follow_up_date::text AS follow_up_date, COALESCE(q.rep_name, u.name) AS rep
           FROM quotes q LEFT JOIN users u ON u.id = q.user_id
          WHERE ($2 OR q.user_id = $1 OR q.rep_id = $1)
            AND (q.status NOT IN ('Won','Lost') OR q.quote_date >= (NOW() AT TIME ZONE '${TZ}')::date - 60)
          ORDER BY q.quote_date DESC NULLS LAST, q.id DESC LIMIT 200`, [uid, team]),
      pool.query(
        `SELECT s.id, s.user_id, s.prospect_id, s.product_line, s.quantity, s.sent_date::text AS sent_date,
                s.follow_up_date::text AS follow_up_date, s.status, p.company,
                COALESCE(NULLIF(TRIM(u.name),''), u.email) AS rep
           FROM samples s LEFT JOIN prospects p ON p.id = s.prospect_id LEFT JOIN users u ON u.id = s.user_id
          WHERE ($2 OR s.user_id = $1) AND s.closed_at IS NULL
            AND LOWER(COALESCE(s.status,'pending')) NOT IN ('converted','no_interest','no interest','closed','lost','won')
          ORDER BY s.follow_up_date ASC NULLS LAST LIMIT 200`, [uid, team])
    ]);
    res.json({ quotes: quotes.rows, samples: samples.rows });
  } catch (e) {
    console.error('[m/pipeline]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/m/accounts ─────────────────────────────────────────
// Quick-add an account from the phone (rep becomes the owner).
router.post('/accounts', async (req, res) => {
  const uid = req.session.user.id;
  const b = req.body || {};
  const company = String(b.company || '').trim();
  if (!company) return res.status(400).json({ error: 'Company name is required' });
  try {
    const dup = await pool.query(
      `SELECT id, company FROM prospects
        WHERE LOWER(REGEXP_REPLACE(company,'[^a-zA-Z0-9]','','g')) = LOWER(REGEXP_REPLACE($1,'[^a-zA-Z0-9]','','g'))
          AND LOWER(COALESCE(city,'')) = LOWER($2) LIMIT 1`, [company, String(b.city || '').trim()]);
    if (dup.rows.length) return res.status(409).json({ error: 'That account already exists', id: dup.rows[0].id });
    const r = await pool.query(
      `INSERT INTO prospects (user_id, company, category, city, state, phone, source, status, data_status)
       VALUES ($1,$2,$3,$4,$5,$6,'Mobile','New','Unvetted') RETURNING id`,
      [uid, company, String(b.category || 'Other').trim() || 'Other', String(b.city || '').trim() || null,
       String(b.state || 'GA').trim() || 'GA', String(b.phone || '').trim() || null]);
    res.json({ id: r.rows[0].id });
  } catch (e) {
    console.error('[m/accounts POST]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/m/stops/:id/done ───────────────────────────────────
// The rep logged a visit from a Today stop: tick the stop off. The visit itself
// was already saved through /api/calls, so this only stamps completed_at.
router.post('/stops/:id/done', async (req, res) => {
  try {
    const r = await pool.query(
      'UPDATE planner_items SET completed_at = COALESCE(completed_at, NOW()) WHERE id=$1 AND rep_id=$2 RETURNING id',
      [parseInt(req.params.id), req.session.user.id]);
    if (!r.rows.length) return res.status(404).json({ error: 'Stop not found' });
    res.json({ ok: true });
  } catch (e) {
    console.error('[m/stops/done]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Ownership helper for quick edits from the phone ──────────────
const canEdit = (me, ownerIds) => me.role === 'manager' || ownerIds.some(id => id && Number(id) === Number(me.id));
const isYmdOrNull = v => v === null || v === '' || isYmd(v);

// ── PATCH /api/m/quotes/:id { status?, follow_up_date? } ─────────
// Only touches status / follow-up. The full quote editor stays on desktop.
router.patch('/quotes/:id', async (req, res) => {
  const me = req.session.user;
  const id = parseInt(req.params.id);
  const b = req.body || {};
  const STATUSES = ['Draft', 'Sent', 'Follow-Up', 'Won', 'Lost'];
  try {
    const q = await pool.query('SELECT id, user_id, rep_id FROM quotes WHERE id=$1', [id]);
    if (!q.rows.length) return res.status(404).json({ error: 'Quote not found' });
    if (!canEdit(me, [q.rows[0].user_id, q.rows[0].rep_id])) return res.status(403).json({ error: 'Only the quote\'s rep or a manager can change it' });
    const sets = [], vals = [];
    if (b.status !== undefined) {
      if (!STATUSES.includes(b.status)) return res.status(400).json({ error: 'Unknown status' });
      vals.push(b.status); sets.push(`status=$${vals.length}`);
    }
    if (b.follow_up_date !== undefined) {
      if (!isYmdOrNull(b.follow_up_date)) return res.status(400).json({ error: 'Bad date' });
      vals.push(b.follow_up_date || null); sets.push(`follow_up_date=$${vals.length}`);
      sets.push(`follow_up_enabled=${b.follow_up_date ? 'TRUE' : 'FALSE'}`);
    }
    if (!sets.length) return res.status(400).json({ error: 'Nothing to change' });
    vals.push(id);
    const r = await pool.query(
      `UPDATE quotes SET ${sets.join(', ')}, updated_at=NOW() WHERE id=$${vals.length}
       RETURNING id, status, follow_up_date::text AS follow_up_date`, vals);
    res.json(r.rows[0]);
  } catch (e) {
    console.error('[m/quotes PATCH]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── PATCH /api/m/samples/:id { outcome: converted|no_interest|pending, follow_up_date? }
router.patch('/samples/:id', async (req, res) => {
  const me = req.session.user;
  const id = parseInt(req.params.id);
  const b = req.body || {};
  try {
    const q = await pool.query('SELECT id, user_id FROM samples WHERE id=$1', [id]);
    if (!q.rows.length) return res.status(404).json({ error: 'Sample not found' });
    if (!canEdit(me, [q.rows[0].user_id])) return res.status(403).json({ error: 'Only the rep who left it or a manager can change it' });
    if (b.outcome !== undefined) {
      if (!['converted', 'no_interest', 'pending'].includes(b.outcome)) return res.status(400).json({ error: 'Unknown outcome' });
      const closed = b.outcome !== 'pending';
      await pool.query(
        `UPDATE samples SET status=$1, outcome_notes=COALESCE($2, outcome_notes), closed_at=${closed ? 'NOW()' : 'NULL'} WHERE id=$3`,
        [b.outcome, b.notes ? String(b.notes).slice(0, 500) : null, id]);
    }
    if (b.follow_up_date !== undefined) {
      if (!isYmdOrNull(b.follow_up_date)) return res.status(400).json({ error: 'Bad date' });
      await pool.query('UPDATE samples SET follow_up_date=$1 WHERE id=$2', [b.follow_up_date || null, id]);
    }
    res.json({ ok: true });
  } catch (e) {
    console.error('[m/samples PATCH]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/m/followups/:callId { action: 'done' | 'snooze', days? } ──
// Close out or push back the next step a visit set, without logging a visit.
router.post('/followups/:callId', async (req, res) => {
  const me = req.session.user;
  const id = parseInt(req.params.callId);
  const action = (req.body || {}).action;
  try {
    const c = await pool.query('SELECT id, user_id FROM calls WHERE id=$1', [id]);
    if (!c.rows.length) return res.status(404).json({ error: 'Follow-up not found' });
    if (!canEdit(me, [c.rows[0].user_id])) return res.status(403).json({ error: 'Not your follow-up' });
    if (action === 'done') {
      await pool.query('UPDATE calls SET next_step_date=NULL WHERE id=$1', [id]);
    } else if (action === 'snooze') {
      const days = Math.min(Math.max(parseInt((req.body || {}).days) || 7, 1), 90);
      const base = isYmd((req.body || {}).today) ? req.body.today : (await todayFor(req));
      await pool.query(`UPDATE calls SET next_step_date = $1::date + $2::int WHERE id=$3`, [base, days, id]);
    } else {
      return res.status(400).json({ error: 'Unknown action' });
    }
    res.json({ ok: true });
  } catch (e) {
    console.error('[m/followups]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── PATCH /api/m/account/:id ─────────────────────────────────────
// Reps fix contact details on any team account they're standing in.
// Changing the account's name or type stays with the owner or a manager.
router.patch('/account/:id', async (req, res) => {
  const me = req.session.user;
  const id = parseInt(req.params.id);
  const b = req.body || {};
  try {
    const a = await pool.query('SELECT id, user_id FROM prospects WHERE id=$1', [id]);
    if (!a.rows.length) return res.status(404).json({ error: 'Account not found' });
    const owner = canEdit(me, [a.rows[0].user_id]);
    const allowed = ['phone', 'address', 'city', 'state', 'zip', 'website'].concat(owner ? ['company', 'category'] : []);
    const sets = [], vals = [];
    allowed.forEach(k => {
      if (b[k] === undefined) return;
      const v = String(b[k] == null ? '' : b[k]).trim().slice(0, 200);
      if ((k === 'company' || k === 'category') && !v) return;
      vals.push(v || null); sets.push(`${k}=$${vals.length}`);
    });
    if (!sets.length) return res.status(400).json({ error: 'Nothing to change' });
    // A moved address needs a fresh map pin.
    if (b.address !== undefined || b.city !== undefined) sets.push('lat=NULL, lng=NULL, geocoded_at=NULL');
    vals.push(id);
    await pool.query(`UPDATE prospects SET ${sets.join(', ')} WHERE id=$${vals.length}`, vals);
    res.json({ ok: true });
  } catch (e) {
    console.error('[m/account PATCH]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/m/activity?date=YYYY-MM-DD&days=1|7 (managers) ──────
// Who logged what: per-rep counts plus the latest visits across the team.
router.get('/activity', async (req, res) => {
  if (!isManager(req.session.user)) return res.status(403).json({ error: 'Managers only' });
  try {
    const today = await todayFor(req);
    const days = parseInt(req.query.days) === 7 ? 7 : 1;
    const from = (await pool.query('SELECT ($1::date - $2::int + 1)::text AS d', [today, days])).rows[0].d;
    const [reps, recent] = await Promise.all([
      pool.query(
        `SELECT u.id, COALESCE(NULLIF(TRIM(u.name),''), u.email) AS rep,
                COUNT(c.id)::int AS visits, COUNT(DISTINCT c.prospect_id)::int AS accounts,
                MAX(c.created_at) AS last_at
           FROM users u
           LEFT JOIN calls c ON c.user_id = u.id AND c.call_date BETWEEN $1 AND $2
          WHERE u.role IN ('rep','manager')
          GROUP BY u.id, 2 ORDER BY 3 DESC, 2`, [from, today]),
      pool.query(
        `SELECT c.id, c.call_date::text AS call_date, c.outcome, c.products_discussed, c.next_step,
                LEFT(c.notes, 160) AS notes, p.id AS prospect_id, p.company, p.city,
                COALESCE(NULLIF(TRIM(u.name),''), u.email) AS rep
           FROM calls c JOIN prospects p ON p.id = c.prospect_id LEFT JOIN users u ON u.id = c.user_id
          WHERE c.call_date BETWEEN $1 AND $2
          ORDER BY c.call_date DESC, c.created_at DESC NULLS LAST, c.id DESC LIMIT 40`, [from, today])
    ]);
    res.json({ from, to: today, reps: reps.rows, recent: recent.rows });
  } catch (e) {
    console.error('[m/activity]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/m/route/insert { account_id, after_stop_number, date } ──
// Put an account on today's route right after the stop it's near (stop numbers
// are 1-based positions in today's planned order), renumbering the rest.
// Already planned today → returns that stop unchanged.
router.post('/route/insert', async (req, res) => {
  const uid = req.session.user.id;
  const b = req.body || {};
  const accountId = parseInt(b.account_id);
  if (!Number.isFinite(accountId)) return res.status(400).json({ error: 'account_id required' });
  const client = await pool.connect();
  try {
    const date = isYmd(b.date) ? b.date : await todayFor(req);
    await client.query('BEGIN');
    const items = (await client.query(
      `SELECT id, account_id FROM planner_items WHERE rep_id=$1 AND planned_date=$2
        ORDER BY sort_order ASC, id ASC FOR UPDATE`, [uid, date])).rows;
    const dup = items.find(i => i.account_id === accountId);
    if (dup) { await client.query('ROLLBACK'); return res.json({ id: dup.id, duplicate: true }); }
    const after = Math.min(Math.max(parseInt(b.after_stop_number) || items.length, 0), items.length);
    const ins = await client.query(
      `INSERT INTO planner_items (rep_id, planned_date, item_type, account_id, sort_order, source)
       VALUES ($1,$2,'stop',$3,0,'manual') RETURNING id`, [uid, date, accountId]);
    const order = items.slice(0, after).map(i => i.id).concat([ins.rows[0].id], items.slice(after).map(i => i.id));
    for (let k = 0; k < order.length; k++) {
      await client.query('UPDATE planner_items SET sort_order=$1 WHERE id=$2', [k, order[k]]);
    }
    await client.query('COMMIT');
    res.json({ id: ins.rows[0].id, stop_number: after + 1 });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('[m/route/insert]', e.message);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

module.exports = router;
