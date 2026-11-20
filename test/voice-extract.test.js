// Voice logger field cleanup. Run: node test/voice-extract.test.js
const assert = require('assert');
const vx = require('../lib/voice-extract');

const today = '2026-11-20'; // a Friday

// Follow-up dates from how reps talk
assert.strictEqual(vx.parseFollowUpDate('come back early December after their inventory count', today), '2026-12-05');
assert.strictEqual(vx.parseFollowUpDate('follow up mid December', today), '2026-12-15');
assert.strictEqual(vx.parseFollowUpDate('check back late January', today), '2027-01-25');
assert.strictEqual(vx.parseFollowUpDate('follow up in a month', today), '2026-12-20');
assert.strictEqual(vx.parseFollowUpDate('call him in two weeks', today), '2026-12-04');
assert.strictEqual(vx.parseFollowUpDate('stop back next week', today), '2026-11-27');
assert.strictEqual(vx.parseFollowUpDate('call Tuesday', today), '2026-11-24');
assert.strictEqual(vx.parseFollowUpDate('tomorrow', today), '2026-11-21');
assert.strictEqual(vx.parseFollowUpDate('send samples', today), null);
assert.strictEqual(vx.parseFollowUpDate('early November', today), '2027-11-05'); // already past this year

// City spoken as part of the name
assert.deepStrictEqual(vx.splitCityFromName('Building material liquidators in Douglasville', null),
  { name: 'Building material liquidators', city: 'Douglasville' });
assert.deepStrictEqual(vx.splitCityFromName('Magbee ball ground', 'Ball Ground'), { name: 'Magbee', city: 'Ball Ground' });
assert.deepStrictEqual(vx.splitCityFromName('Adams Building Supply', null), { name: 'Adams Building Supply', city: null });

// A recording that caught two words is never a visit
let r = vx.finalize({ summary: 'x', account_name: 'Trinity custom homes' }, 'Using it', today);
assert.strictEqual(r.too_short, true);
// The AI's own "insufficient information" text never becomes call notes
r = vx.finalize({ insufficient: false, account_name: 'Trinity custom homes',
  summary: "Insufficient information provided. Only the phrase 'Using it' was captured in the transcript." },
  'okay so I was using it over at the place today you know', today);
assert.strictEqual(r.too_short, true);

// A normal visit keeps its fields; a bad date from the AI falls back to the words
r = vx.finalize({ account_name: 'Adams Building Supply', account_city: 'toccoa', contact_name: 'Mike',
  contact_title: 'yard manager', summary: 'Mike carries MoistureShield now and wants ShurTape pricing on 20 cases.',
  outcome: 'Dropped In - Interested', next_steps: 'Return early December after inventory', next_step_date: '2025-01-01',
  products: 'ShurTape, Soudal, Duct Tape' },
  'Just left Adams Building Supply in Toccoa talked to Mike the yard manager about tape', today);
assert.strictEqual(r.too_short, false);
assert.strictEqual(r.account_city, 'Toccoa');
assert.strictEqual(r.next_step_date, '2026-12-05');
assert.strictEqual(r.products, 'ShurTape, Soudal');
assert.strictEqual(r.outcome, 'Dropped In - Interested');

// An outcome the app doesn't know becomes Other; "null" strings become null
r = vx.finalize({ account_name: 'null', outcome: 'Great visit', summary: 'Talked shop about flashing tape for an hour.' },
  'talked shop about flashing tape for an hour with the guys', today);
assert.strictEqual(r.account_name, null);
assert.strictEqual(r.outcome, 'Other');

// The prompt carries today's date so relative dates resolve
assert(vx.buildPrompt('hello there', today).includes('Friday, 2026-11-20'));

console.log('voice-extract: all tests passed');
