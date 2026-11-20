// After a visit is saved, work out the next steps the rep might want, from
// what they logged. Nothing happens until the rep approves each one.
// Rules, not AI, so it's instant and predictable:
//   • a sample was left or promised      → track it (with a follow-up date)
//   • pricing or a quote was asked for   → start a draft quote
//   • a next step with no date           → put a follow-up on the calendar
//   • always                             → draft the follow-up email
const LINES = [
  ['Soudal', /\bsoudal|fix ?all|\bsealant/i],
  ['ShurTape', /shur ?tape|flashing tape|\btape\b/i],
  ['Alum-A-Pole', /alum.?a.?pole|pump ?jack|\bladder/i],
  ['Fortress', /fortress|railing/i],
  ['BOSS', /\bboss\b/i]
];

function linesIn(text, logged) {
  const out = [];
  String(logged || '').split(/\s*,\s*/).forEach(l => { if (l && !out.includes(l)) out.push(l); });
  for (const [name, rx] of LINES) if (rx.test(text || '') && !out.includes(name)) out.push(name);
  return out;
}

// The line the sample was most likely of: the one mentioned closest to the
// word "sample" ("left a sample of the flashing tape, wants Fix All pricing"
// is a ShurTape sample, not Soudal).
function sampleLine(text, lines, sampleRx) {
  if (!lines.length) return '';
  const m = sampleRx.exec(text || '');
  if (!m) return lines[0];
  let best = lines[0], bestD = Infinity;
  for (const [name, rx] of LINES) {
    if (!lines.includes(name)) continue;
    const g = new RegExp(rx.source, 'gi'); let x;
    while ((x = g.exec(text))) { const d = Math.abs(x.index - m.index); if (d < bestD) { bestD = d; best = name; } }
  }
  return best;
}

function addBusinessDays(ymd, n) {
  const d = new Date(ymd + 'T12:00:00Z');
  while (n > 0) { d.setUTCDate(d.getUTCDate() + 1); const w = d.getUTCDay(); if (w !== 0 && w !== 6) n--; }
  return d.toISOString().slice(0, 10);
}

// call: { outcome, notes, next_step, next_step_date, products_discussed, call_day }
// state: { sampleToday: bool, quoteSince: bool }
function suggest(call, state = {}) {
  const text = [call.notes, call.next_step].filter(Boolean).join(' ');
  const lines = linesIn(text, call.products_discussed);
  const day = call.call_day;
  const out = [];
  const sampleWords = /\bsamples?\b|left (him|her|them|a|some)\b|drop(ped)? off/i;
  if (!state.sampleToday && (/left sample/i.test(call.outcome || '') || sampleWords.test(text))) {
    const sl = sampleLine(text, lines, sampleWords);
    out.push({ kind: 'sample', line: sl,
      label: `Track the ${sl ? sl + ' ' : ''}sample`, detail: 'Adds it to Samples with a check-in a week out.' });
  }
  if (!state.quoteSince && /\b(pric(e|es|ing)|quote|quoted|bid|how much|cost)\b/i.test(text)) {
    out.push({ kind: 'quote', products: lines.join(', '),
      label: `Start a quote${lines.length ? ' for ' + lines.join(', ') : ''}`, detail: 'Creates a draft quote on this account with your notes. Finish it on Quotes.' });
  }
  if (call.next_step && !call.next_step_date) {
    out.push({ kind: 'followup_date', date: addBusinessDays(day, 5),
      label: 'Put the follow-up on your calendar', detail: `"${String(call.next_step).slice(0, 80)}". A week out, change the date if you need to.` });
  }
  out.push({ kind: 'email', label: 'Draft the follow-up email', detail: 'Written from your notes. You review and send it from your own email.' });
  return out;
}

module.exports = { suggest, linesIn, addBusinessDays, sampleLine };
