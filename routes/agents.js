// Agents: AI drafts, the rep approves. Nothing here sends anything.
//
// POST /api/agents/followup { call_id }
//   After a visit is logged, draft the follow-up email to the buyer plus a
//   suggested next step, from the rep's own notes. Returns
//   { subject, body, to_email, to_name, next_step, next_step_date, ai }.
//   If the AI is unavailable it returns a plain template draft (ai:false) so the
//   rep is never left with nothing.
const express = require('express');
const { pool } = require('../db');
const llm = require('../lib/llm');
const router = express.Router();

function addDays(ymd, n) {
  const d = new Date(ymd + 'T12:00:00Z'); d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
const firstName = n => String(n || '').trim().split(/\s+/)[0] || '';

function templateDraft(ctx) {
  const hi = ctx.contactName ? `Hi ${firstName(ctx.contactName)},` : 'Hi,';
  const lines = ctx.lines ? ` on ${ctx.lines}` : '';
  const body = [
    hi, '',
    `Thanks for your time ${ctx.callType === 'Phone' ? 'on the phone' : 'today'}. Following up on our conversation${lines}.`,
    ctx.nextStep ? `\nNext step on my end: ${ctx.nextStep}.` : '',
    '', 'Let me know if you need anything in the meantime.', '',
    'Thanks,', ctx.repName || ''
  ].filter(x => x !== null).join('\n').replace(/\n{3,}/g, '\n\n');
  return {
    subject: `Following up${ctx.lines ? ' on ' + ctx.lines : ''} | ${ctx.company}`,
    body,
    next_step: ctx.nextStep || 'Follow up',
    next_step_date: ctx.nextStepDate || addDays(ctx.callDate, 7)
  };
}

router.post('/followup', async (req, res) => {
  const me = req.session.user;
  const callId = parseInt((req.body || {}).call_id);
  if (!Number.isFinite(callId)) return res.status(400).json({ error: 'call_id required' });
  try {
    const c = await pool.query(
      `SELECT c.*, c.call_date::text AS call_day, c.next_step_date::text AS next_day,
              p.company, p.city, p.category
         FROM calls c JOIN prospects p ON p.id = c.prospect_id WHERE c.id = $1`, [callId]);
    if (!c.rows.length) return res.status(404).json({ error: 'Visit not found' });
    const call = c.rows[0];
    if (call.user_id !== me.id && me.role !== 'manager') return res.status(403).json({ error: 'Not your visit' });

    // Who to write to: the person named in the notes, else the main contact.
    const contacts = (await pool.query(
      `SELECT name, title, email FROM contacts WHERE prospect_id=$1 ORDER BY is_primary DESC NULLS LAST, id`, [call.prospect_id])).rows;
    const spoke = /\(Spoke with ([^)]+)\)/.exec(call.notes || '');
    const contact = (spoke && contacts.find(x => x.name === spoke[1].trim())) || contacts.find(x => x.email) || contacts[0] || null;
    const rep = (await pool.query('SELECT name, email FROM users WHERE id=$1', [call.user_id])).rows[0] || {};

    const ctx = {
      company: call.company, city: call.city, callType: call.call_type, callDate: call.call_day,
      outcome: call.outcome, lines: call.products_discussed || '', notes: String(call.notes || '').replace(/\(Spoke with [^)]+\)/, '').trim(),
      nextStep: call.next_step, nextStepDate: call.next_day,
      contactName: contact && contact.name, contactTitle: contact && contact.title, repName: rep.name
    };

    let draft, ai = true;
    try {
      const system = 'You write short, plain follow-up emails for a building-products manufacturer\'s rep. ' +
        'Sound like a real salesperson texting from the truck: friendly, direct, no fluff, no buzzwords, no em dashes, no exclamation-point spam. ' +
        'Never invent prices, quantities, discounts, dates or promises that are not in the notes.';
      const prompt = `Rep: ${ctx.repName || 'the rep'} (Compton Sales)
Account: ${ctx.company}${ctx.city ? ', ' + ctx.city : ''}
Person: ${ctx.contactName || 'unknown'}${ctx.contactTitle ? ' (' + ctx.contactTitle + ')' : ''}
Visit: ${ctx.callType || 'In-Person'} on ${ctx.callDate}
Outcome: ${ctx.outcome || 'n/a'}
Lines discussed: ${ctx.lines || 'n/a'}
Rep's notes: ${ctx.notes || 'none'}
Next step the rep set: ${ctx.nextStep || 'none'}${ctx.nextStepDate ? ' by ' + ctx.nextStepDate : ''}

Write the follow-up email (under 120 words, greet by first name if known, sign off with the rep's first name).
Also suggest the single best next step and a date (YYYY-MM-DD) within 14 days of ${ctx.callDate}, keeping the rep's own next step if set.
Return ONLY JSON: {"subject":"...","body":"...","next_step":"...","next_step_date":"YYYY-MM-DD"}`;
      const out = llm.parseJson(await llm.complete({ system, prompt, maxTokens: 600 }));
      if (!out.subject || !out.body) throw new Error('Incomplete draft');
      draft = {
        subject: String(out.subject).replace(/[ \t]*[\u2014\u2013][ \t]*/g, ', ').slice(0, 160),
        body: String(out.body).replace(/[ \t]*[\u2014\u2013][ \t]*/g, ', ').slice(0, 3000),
        next_step: String(out.next_step || ctx.nextStep || 'Follow up').slice(0, 200),
        next_step_date: /^\d{4}-\d{2}-\d{2}$/.test(out.next_step_date || '') ? out.next_step_date : (ctx.nextStepDate || addDays(ctx.callDate, 7))
      };
    } catch (e) {
      console.warn('[agents/followup] AI unavailable, using template:', e.message);
      draft = templateDraft(ctx); ai = false;
    }
    res.json(Object.assign(draft, { to_email: contact && contact.email || '', to_name: contact && contact.name || '', ai }));
  } catch (e) {
    console.error('[agents/followup]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ════════════════════════════════════════════════════════════════
// Shared bits for the round-4 agents
// ════════════════════════════════════════════════════════════════
const TZ = 'America/New_York';
const isYmd = s => /^\d{4}-\d{2}-\d{2}$/.test(s || '');
const STYLE = 'You write for a building-products manufacturer\'s rep at Compton Sales (lines: Soudal, ShurTape, Alum-A-Pole, Fortress Railing). ' +
  'Sound like a real salesperson: friendly, direct, short, no fluff, no buzzwords, no em dashes, no exclamation-point spam. ' +
  'Never invent prices, quantities, discounts, dates, products or promises that are not in the facts given.';
const niceDate = d => new Date(d + 'T12:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
// Dashes from the model become commas (house style) without leaving " ,  " gaps.
const clean = (t, n) => String(t || '').replace(/[ \t]*[\u2014\u2013][ \t]*/g, ', ').replace(/, ,/g, ',').trim().slice(0, n);
async function todayFor(req) {
  const q = req.query.date || (req.body && req.body.date);
  if (isYmd(q)) return q;
  return (await pool.query(`SELECT (NOW() AT TIME ZONE '${TZ}')::date::text AS d`)).rows[0].d;
}
async function repName(id) { return ((await pool.query('SELECT name FROM users WHERE id=$1', [id])).rows[0] || {}).name || ''; }
async function mainContact(prospectId, preferName) {
  const cs = (await pool.query(
    `SELECT name, title, email, phone FROM contacts WHERE prospect_id=$1 ORDER BY is_primary DESC NULLS LAST, id`, [prospectId])).rows;
  const byName = preferName && cs.find(c => c.name && c.name.toLowerCase() === String(preferName).toLowerCase());
  return byName || cs.find(c => c.email) || cs[0] || null;
}
async function draftOrTemplate(tag, system, prompt, fallback) {
  try {
    const out = llm.parseJson(await llm.complete({ system, prompt, maxTokens: 600 }));
    if (!out.body) throw new Error('Incomplete draft');
    return { subject: clean(out.subject, 160) || fallback.subject, body: clean(out.body, 3000), ai: true };
  } catch (e) {
    console.warn(`[agents/${tag}] AI unavailable, using template:`, e.message);
    return Object.assign({}, fallback, { ai: false });
  }
}

// Accounts that have gone cold: visited before, nothing in `days`+ days.
// Bigger buyers (from commission data) first, then longest gone.
async function coldAccounts({ userId, team, days = 60, limit = 15 }) {
  const r = await pool.query(
    `SELECT p.id, p.company, p.city, p.phone, p.user_id,
            COALESCE(NULLIF(TRIM(u.name),''), u.email) AS owner,
            lc.call_date::text AS last_visit, lc.outcome AS last_outcome, LEFT(lc.notes, 200) AS last_notes,
            lc.products_discussed AS last_lines,
            ((NOW() AT TIME ZONE '${TZ}')::date - lc.call_date) AS days_cold,
            COALESCE(al.sales, 0)::float AS sales, al.lines
       FROM prospects p
       JOIN LATERAL (SELECT call_date, outcome, notes, products_discussed FROM calls c
                      WHERE c.prospect_id = p.id ORDER BY call_date DESC, id DESC LIMIT 1) lc ON TRUE
       LEFT JOIN users u ON u.id = p.user_id
       LEFT JOIN LATERAL (SELECT SUM(x.total_sales) AS sales, STRING_AGG(l.name, ', ' ORDER BY x.total_sales DESC NULLS LAST) AS lines
                            FROM account_lines x JOIN lines l ON l.id = x.line_id WHERE x.account_id = p.id) al ON TRUE
      WHERE lc.call_date <= (NOW() AT TIME ZONE '${TZ}')::date - $1::int
        AND COALESCE(p.reconnect_dismissed, FALSE) = FALSE
        AND (p.reconnect_snoozed_until IS NULL OR p.reconnect_snoozed_until < NOW())
        ${team ? '' : 'AND p.user_id = $3'}
      ORDER BY COALESCE(al.sales, 0) DESC, lc.call_date ASC
      LIMIT $2`, team ? [days, limit] : [days, limit, userId]);
  return r.rows;
}

// ── GET /api/agents/brief?date=&refresh=1 ────────────────────────
// The rep's morning plan: 3-5 things that matter most today, in order.
// Cached per rep per day so it costs one AI call a day; Refresh rebuilds it.
router.get('/brief', async (req, res) => {
  const me = req.session.user;
  try {
    const today = await todayFor(req);
    if (req.query.refresh !== '1') {
      const c = await pool.query('SELECT content FROM agent_briefs WHERE user_id=$1 AND brief_date=$2', [me.id, today]);
      if (c.rows.length) return res.json(Object.assign({ cached: true }, c.rows[0].content));
    }
    const [stops, follow, quotes, samples, yday] = await Promise.all([
      pool.query(`SELECT pi.account_id, COALESCE(p.company, pi.title) AS company, p.city, pi.appt_time
                    FROM planner_items pi LEFT JOIN prospects p ON p.id = pi.account_id
                   WHERE pi.rep_id=$1 AND pi.planned_date=$2 AND pi.completed_at IS NULL ORDER BY pi.sort_order, pi.id`, [me.id, today]),
      pool.query(`SELECT * FROM (SELECT DISTINCT ON (c.prospect_id) c.prospect_id AS account_id, p.company, c.next_step,
                         c.next_step_date::text AS due, c.user_id
                    FROM calls c JOIN prospects p ON p.id = c.prospect_id
                   ORDER BY c.prospect_id, c.call_date DESC, c.id DESC) x
                  WHERE x.user_id=$1 AND x.due IS NOT NULL AND x.due <= $2 ORDER BY x.due LIMIT 8`, [me.id, today]),
      pool.query(`SELECT q.id, q.account_name AS company, q.amount::float AS amount, q.products, q.follow_up_date::text AS due,
                         (SELECT id FROM prospects p WHERE LOWER(REGEXP_REPLACE(p.company,'[^a-zA-Z0-9]','','g')) =
                                 LOWER(REGEXP_REPLACE(q.account_name,'[^a-zA-Z0-9]','','g')) LIMIT 1) AS account_id
                    FROM quotes q WHERE (q.user_id=$1 OR q.rep_id=$1) AND q.status NOT IN ('Won','Lost')
                     AND q.follow_up_date IS NOT NULL AND q.follow_up_date <= $2
                   ORDER BY q.amount DESC NULLS LAST LIMIT 6`, [me.id, today]),
      pool.query(`SELECT s.prospect_id AS account_id, p.company, s.product_line, s.follow_up_date::text AS due
                    FROM samples s LEFT JOIN prospects p ON p.id = s.prospect_id
                   WHERE s.user_id=$1 AND s.closed_at IS NULL AND s.follow_up_date <= $2
                     AND LOWER(COALESCE(s.status,'pending')) NOT IN ('converted','no_interest','no interest')
                   ORDER BY s.follow_up_date LIMIT 6`, [me.id, today]),
      pool.query(`SELECT COUNT(*)::int AS n FROM calls WHERE user_id=$1 AND call_date = $2::date - 1`, [me.id, today])
    ]);
    const cold = await coldAccounts({ userId: me.id, team: false, days: 60, limit: 3 });

    // Rule-based plan: always available, and the AI's guard rails.
    const items = [];
    quotes.rows.forEach(q => items.push({ kind: 'quote', account_id: q.account_id, quote_id: q.id,
      text: `Chase the ${q.products ? q.products + ' ' : ''}quote at ${q.company}${q.amount ? ' ($' + Math.round(q.amount).toLocaleString('en-US') + ')' : ''}` }));
    follow.rows.forEach(f => items.push({ kind: 'followup', account_id: f.account_id,
      text: `${f.next_step || 'Follow up'} with ${f.company}${f.due < today ? ' (overdue)' : ''}` }));
    samples.rows.forEach(s => items.push({ kind: 'sample', account_id: s.account_id,
      text: `Check in on the ${s.product_line} samples at ${s.company || 'the account'}` }));
    if (stops.rows.length) items.push({ kind: 'stops', account_id: null,
      text: `${stops.rows.length} stop${stops.rows.length === 1 ? '' : 's'} planned: ${stops.rows.slice(0, 3).map(s => s.company).join(', ')}${stops.rows.length > 3 ? '…' : ''}` });
    cold.forEach(c => items.push({ kind: 'cold', account_id: c.id,
      text: `${c.company} hasn't had a visit in ${c.days_cold} days` }));
    const facts = items.slice(0, 12);

    let brief = {
      headline: facts.length ? `${facts.length} thing${facts.length === 1 ? '' : 's'} worth your time today.` : 'Nothing urgent today. Good day to see new accounts.',
      items: facts.slice(0, 5), ai: false
    };
    if (facts.length) {
      try {
        const prompt = `Today is ${today}. Rep: ${me.name}. Yesterday they logged ${yday.rows[0].n} visits.
Here are today's facts, numbered:
${facts.map((f, i) => `${i + 1}. [${f.kind}] ${f.text}`).join('\n')}

Pick the 3 to 5 that matter most for making money today (overdue quotes and follow-ups with real dollars first, then samples, then cold accounts) and put them in order.
Rewrite each as a short instruction (under 14 words). Add a one-sentence headline.
Return ONLY JSON: {"headline":"...","items":[{"n":<fact number>,"text":"..."}]}`;
        const out = llm.parseJson(await llm.complete({ system: STYLE, prompt, maxTokens: 500 }));
        const picked = (out.items || []).map(x => {
          const f = facts[(parseInt(x.n) || 0) - 1];
          return f ? Object.assign({}, f, { text: clean(x.text, 140) || f.text }) : null;
        }).filter(Boolean).slice(0, 5);
        if (picked.length) brief = { headline: clean(out.headline, 160) || brief.headline, items: picked, ai: true };
      } catch (e) {
        console.warn('[agents/brief] AI unavailable, using rule-based plan:', e.message);
      }
    }
    brief.date = today;
    // Cache the AI plan for the day. A fallback plan isn't cached, so the next
    // open retries the AI instead of keeping the plain version all day.
    if (brief.ai || !facts.length) await pool.query(
      `INSERT INTO agent_briefs (user_id, brief_date, content) VALUES ($1,$2,$3)
       ON CONFLICT (user_id, brief_date) DO UPDATE SET content = EXCLUDED.content, created_at = NOW()`,
      [me.id, today, JSON.stringify(brief)]);
    res.json(brief);
  } catch (e) {
    console.error('[agents/brief]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/agents/winback?scope=mine|team&days=60 ──────────────
router.get('/winback', async (req, res) => {
  try {
    const days = Math.min(Math.max(parseInt(req.query.days) || 60, 21), 365);
    const rows = await coldAccounts({ userId: req.session.user.id, team: req.query.scope === 'team', days, limit: 25 });
    res.json({ days, accounts: rows });
  } catch (e) {
    console.error('[agents/winback]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/agents/winback/draft { account_id, channel: 'email'|'text' } ──
// Reach-out that acknowledges the gap and asks for a few minutes. No hard pitch.
router.post('/winback/draft', async (req, res) => {
  const me = req.session.user;
  const id = parseInt((req.body || {}).account_id);
  const text = (req.body || {}).channel === 'text';
  try {
    const a = (await pool.query(
      `SELECT p.id, p.company, p.city,
              lc.call_date::text AS last_visit, lc.outcome, lc.notes, lc.products_discussed,
              ((NOW() AT TIME ZONE '${TZ}')::date - lc.call_date) AS days_cold,
              (SELECT STRING_AGG(l.name, ', ') FROM account_lines x JOIN lines l ON l.id = x.line_id WHERE x.account_id = p.id) AS lines
         FROM prospects p
         LEFT JOIN LATERAL (SELECT call_date, outcome, notes, products_discussed FROM calls c WHERE c.prospect_id = p.id
                             ORDER BY call_date DESC, id DESC LIMIT 1) lc ON TRUE
        WHERE p.id = $1`, [id])).rows[0];
    if (!a) return res.status(404).json({ error: 'Account not found' });
    const contact = await mainContact(id);
    const rep = await repName(me.id);
    const hi = contact && contact.name ? `Hi ${firstName(contact.name)},` : 'Hi,';
    const fallback = text
      ? { subject: '', body: `${hi.replace(',', '')}, it's ${firstName(rep)} with Compton Sales. It's been a while since I came by ${a.company}. Mind if I stop in this week to catch up?` }
      : { subject: `Checking in | ${a.company}`,
          body: `${hi}\n\nIt's been a while since I've been by ${a.company}, and I wanted to check in. Anything you need on ${a.lines || a.products_discussed || 'our lines'}, or anything coming up I can help with?\n\nI'll be in the area this week and can stop in for a few minutes if that works.\n\nThanks,\n${firstName(rep)}` };
    const prompt = `Write a ${text ? 'text message (under 45 words, no subject)' : 'short email (under 90 words)'} from ${rep} at Compton Sales to ${contact && contact.name ? contact.name + (contact.title ? ' (' + contact.title + ')' : '') : 'the buyer'} at ${a.company}${a.city ? ' in ' + a.city : ''}.
Nobody has visited in ${a.days_cold == null ? 'a long time' : a.days_cold + ' days'}.
Last visit: ${a.last_visit || 'unknown'}, outcome "${a.outcome || 'n/a'}", notes: ${clean(a.notes, 300) || 'none'}.
Lines they buy: ${a.lines || a.products_discussed || 'unknown'}.
Goal: reconnect and ask for a few minutes this week. Acknowledge it's been a while without apologizing at length. Don't pitch a specific product unless it's in their lines.
Return ONLY JSON: {"subject":"${text ? '' : '...'}","body":"..."}`;
    const d = await draftOrTemplate('winback', STYLE, prompt, fallback);
    res.json(Object.assign(d, { channel: text ? 'text' : 'email', to_email: contact && contact.email || '', to_phone: contact && contact.phone || '', to_name: contact && contact.name || '' }));
  } catch (e) {
    console.error('[agents/winback/draft]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/agents/quote-nudge { quote_id } ────────────────────
router.post('/quote-nudge', async (req, res) => {
  const me = req.session.user;
  const id = parseInt((req.body || {}).quote_id);
  try {
    const q = (await pool.query(
      `SELECT q.*, q.quote_date::text AS qd, q.follow_up_date::text AS fu,
              (SELECT id FROM prospects p WHERE LOWER(REGEXP_REPLACE(p.company,'[^a-zA-Z0-9]','','g')) =
                      LOWER(REGEXP_REPLACE(q.account_name,'[^a-zA-Z0-9]','','g')) LIMIT 1) AS account_id
         FROM quotes q WHERE q.id = $1`, [id])).rows[0];
    if (!q) return res.status(404).json({ error: 'Quote not found' });
    if (me.role !== 'manager' && q.user_id !== me.id && q.rep_id !== me.id) return res.status(403).json({ error: 'Not your quote' });
    const contact = q.account_id ? await mainContact(q.account_id, q.contact_name) : null;
    const who = (contact && contact.name) || q.contact_name;
    const rep = await repName(q.rep_id || q.user_id);
    const what = [q.products, q.quote_number ? '#' + q.quote_number : ''].filter(Boolean).join(' quote ') || 'quote';
    const fallback = {
      subject: `Following up on the ${q.products ? q.products + ' ' : ''}quote${q.quote_number ? ' #' + q.quote_number : ''} | ${q.account_name}`,
      body: `${who ? 'Hi ' + firstName(who) + ',' : 'Hi,'}\n\nWanted to follow up on the ${what}${q.qd ? ' I sent over on ' + niceDate(q.qd) : ''}. Any questions, or anything I can adjust to make it work?\n\nHappy to stop by or jump on a quick call.\n\nThanks,\n${firstName(rep)}`
    };
    const prompt = `Write a short follow-up email (under 80 words) from ${rep} at Compton Sales to ${who || 'the buyer'} at ${q.account_name}.
It's about a quote: ${q.products || 'products'}${q.amount ? ', $' + Number(q.amount).toLocaleString('en-US') : ''}${q.quote_number ? ', quote #' + q.quote_number : ''}, sent ${q.qd ? niceDate(q.qd) : 'recently'}.
Quote notes: ${clean(q.comments, 300) || 'none'}.
Goal: ask if they have questions or are ready to move forward, offer to adjust or stop by. Mention the quote number if there is one. Don't restate the price.
Return ONLY JSON: {"subject":"...","body":"..."}`;
    const d = await draftOrTemplate('quote-nudge', STYLE, prompt, fallback);
    res.json(Object.assign(d, { to_email: contact && contact.email || '', to_name: who || '' }));
  } catch (e) {
    console.error('[agents/quote-nudge]', e.message);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;
