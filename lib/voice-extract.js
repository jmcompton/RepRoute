// Turns a rep's spoken visit note into the fields the voice logger saves.
// Kept separate from the route so it can be tested without a server or an AI.
//
// What it guards against (all seen in real November logs):
//  • a recording that caught only a couple of words being saved as a visit
//  • the AI's own "insufficient information" text being saved as call notes
//  • "Magbee ball ground": the city spoken as part of the account name
//  • "come back early December" never becoming a follow-up date
//  • summaries that shrink a visit to two sentences of "Rep said..."

const MIN_WORDS = 6;
const LINES = ['Soudal', 'ShurTape', 'Alum-A-Pole', 'Fortress', 'BOSS'];
const OUTCOMES = ['Left Voicemail', 'Dropped In - Interested', 'Dropped In - Not Interested', 'Left Sample',
  'Ready to Order', 'Follow-up Needed', 'Not Available', 'Phone - Interested', 'Phone - Not Interested',
  'Order Placed', 'Demo Scheduled', 'Other'];

function wordCount(s) {
  return String(s || '').trim().split(/\s+/).filter(w => /[a-z0-9]/i.test(w)).length;
}

// Today as YYYY-MM-DD in the team's time zone.
function todayYMD(tz = 'America/New_York', now = new Date()) {
  return new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
}
function weekdayName(ymd) {
  return new Date(ymd + 'T12:00:00Z').toLocaleDateString('en-US', { weekday: 'long', timeZone: 'UTC' });
}
function addDays(ymd, n) {
  const d = new Date(ymd + 'T12:00:00Z'); d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

function buildPrompt(transcript, today) {
  return `You turn a building-products sales rep's spoken visit note into CRM fields. The rep works for Compton Sales, a manufacturer's rep firm selling ${LINES.join(', ')}.

Today is ${weekdayName(today)}, ${today}.

TRANSCRIPT:
"${String(transcript).trim().substring(0, 4000)}"

Return ONLY a JSON object with these fields:
- insufficient: true if the transcript does not describe a visit or call with a business (for example it is only a few stray words). Otherwise false.
- account_name: the business name only, as spoken. Do NOT include the city or words like "in Douglasville". null if not said.
- account_city: the city if the rep said where the business is, else null.
- contact_name: the person's first name (and last name if said). null if not said.
- contact_title: their role if said (e.g. "yard manager", "buyer"), else null.
- summary: the rep's visit notes, cleaned up. Remove filler words and false starts, fix obvious speech-to-text mistakes, but KEEP every fact: names, products, competitor products, quantities, prices, dates, objections, what they carry now, and what was promised. Write it like the rep's own notes in plain sentences ("Mike wants pricing on 20 cases"), never "The rep said" or "Rep attempted". As long as it needs to be.
- outcome: exactly one of ${OUTCOMES.map(o => '"' + o + '"').join(' | ')}.
- next_steps: the next action, close to how the rep said it. null if none.
- next_step_date: the date for that next action as YYYY-MM-DD, worked out from today ("next week" = 7 days out, "in a month" = 30 days, "early December" = the 5th, "mid December" = the 15th, "late December" = the 25th, a weekday = the next one). null if no timing was said.
- products: the lines discussed, from this list only: ${LINES.join(', ')}. Comma-separated, or null.

JSON only, no markdown:
{"insufficient":false,"account_name":null,"account_city":null,"contact_name":null,"contact_title":null,"summary":"...","outcome":"Other","next_steps":null,"next_step_date":null,"products":null}`;
}

const MONTHS = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'];
const DAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
const NUM = { a: 1, an: 1, one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, couple: 2, few: 3 };

// Backup for when the AI leaves the date blank: handles the phrasings reps use.
function parseFollowUpDate(text, today) {
  const t = ' ' + String(text || '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ') + ' ';
  if (!t.trim()) return null;
  if (/ tomorrow /.test(t)) return addDays(today, 1);
  if (/ next week /.test(t)) return addDays(today, 7);
  if (/ next month /.test(t)) return addDays(today, 30);
  let m = t.match(/ in (\d+|a|an|one|two|three|four|five|six|couple|few)(?: of)? (day|days|week|weeks|month|months) /);
  if (m) {
    const n = /^\d+$/.test(m[1]) ? parseInt(m[1], 10) : NUM[m[1]];
    const unit = m[2].startsWith('day') ? 1 : m[2].startsWith('week') ? 7 : 30;
    return addDays(today, n * unit);
  }
  m = t.match(/ (early|mid|middle of|late|end of|beginning of|first of)? ?(january|february|march|april|may|june|july|august|september|october|november|december) /);
  if (m) {
    const mon = MONTHS.indexOf(m[2]);
    const day = /late|end/.test(m[1] || '') ? 25 : /mid|middle/.test(m[1] || '') ? 15 : /early|beginning|first/.test(m[1] || '') ? 5 : 1;
    const [y, cm] = today.split('-').map(Number);
    const year = mon + 1 < cm ? y + 1 : y;
    const ymd = `${year}-${String(mon + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    return ymd < today ? `${year + 1}${ymd.slice(4)}` : ymd;
  }
  m = t.match(/ (next )?(monday|tuesday|wednesday|thursday|friday) /);
  if (m) {
    const target = DAYS.indexOf(m[2]);
    const cur = new Date(today + 'T12:00:00Z').getUTCDay();
    let diff = (target - cur + 7) % 7; if (diff === 0) diff = 7;
    return addDays(today, diff);
  }
  return null;
}

function validDate(v, today) {
  if (typeof v !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(v)) return null;
  const d = new Date(v + 'T12:00:00Z');
  if (isNaN(d) || d.toISOString().slice(0, 10) !== v) return null;
  if (v < today || v > addDays(today, 400)) return null;
  return v;
}

function clean(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return !s || /^(null|none|n\/a|unknown)$/i.test(s) ? null : s;
}

// "Building material liquidators in Douglasville" → name + city.
function splitCityFromName(name, city) {
  let n = clean(name); let c = clean(city);
  if (!n) return { name: null, city: c };
  const inCity = n.match(/^(.*\S)\s+(?:in|at|over in|out in)\s+([A-Za-z .'-]{3,30})$/i);
  if (inCity) { n = inCity[1]; c = c || inCity[2].trim(); }
  if (c) {
    const rx = new RegExp('[\\s,-]+' + c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '$', 'i');
    if (rx.test(n) && n.replace(rx, '').trim().length >= 3) n = n.replace(rx, '').trim();
  }
  return { name: n, city: c ? c.replace(/\b\w/g, ch => ch.toUpperCase()) : null };
}

// Normalize whatever the AI returned into the shape the app saves.
function finalize(raw, transcript, today) {
  const r = raw || {};
  const tooShort = wordCount(transcript) < MIN_WORDS;
  const insufficient = tooShort || r.insufficient === true ||
    /insufficient information|does not contain (enough )?details|not enough information/i.test(String(r.summary || ''));
  const { name, city } = splitCityFromName(r.account_name, r.account_city);
  const nextSteps = clean(r.next_steps);
  const date = validDate(r.next_step_date, today) ||
    parseFollowUpDate(nextSteps, today) || (nextSteps ? null : parseFollowUpDate(transcript, today));
  const products = clean(r.products);
  return {
    too_short: insufficient,
    account_name: name,
    account_city: city,
    contact_name: clean(r.contact_name),
    contact_title: clean(r.contact_title),
    summary: insufficient ? String(transcript || '').trim().slice(0, 1000) : (clean(r.summary) || String(transcript || '').trim().slice(0, 1500)),
    outcome: OUTCOMES.includes(r.outcome) ? r.outcome : 'Other',
    next_steps: nextSteps,
    next_step_date: date,
    products: products ? products.split(/\s*,\s*/).filter(p => LINES.includes(p)).join(', ') || null : null
  };
}

module.exports = { buildPrompt, finalize, parseFollowUpDate, splitCityFromName, todayYMD, wordCount, MIN_WORDS, OUTCOMES };
