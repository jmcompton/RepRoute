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
        subject: String(out.subject).slice(0, 160),
        body: String(out.body).replace(/—/g, ', ').slice(0, 3000),
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

module.exports = router;
