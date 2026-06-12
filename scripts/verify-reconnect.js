'use strict';

// Ad-hoc verification for the Reconnect recency fix. Runs getReconnect against
// the live DB (DATABASE_URL) for a manager (firm-wide), prints the resulting
// count + total_at_risk and a few sample rows so we can confirm March-quiet
// high-value accounts flag while April-active ones don't.
//
//   DATABASE_URL=... node scripts/verify-reconnect.js
//
// Read-only; writes nothing.

const { pool } = require('../db');
const { getReconnect } = require('../lib/reconnect-store');

(async () => {
  try {
    const res = await getReconnect(pool, { scope: 'manager', filter: 'customers' });
    const today = new Date().toISOString().slice(0, 10);
    console.log('Reconnect verification (manager / customers) — today ' + today);
    console.log('count        :', res.summary.count);
    console.log('total_at_risk: $' + res.summary.total_at_risk.toLocaleString('en-US'));
    console.log('');
    console.log('Top flagged accounts:');
    res.accounts.slice(0, 15).forEach(a => {
      console.log(
        '  ' + (a.company || '').padEnd(34).slice(0, 34) +
        ' $' + String(a.trailing_commission).padStart(9) +
        '  ' + String(a.days_quiet).padStart(4) + 'd quiet' +
        '  tier=' + a.tier_label.padEnd(10) +
        '  months=' + a.months_loaded +
        '  last=' + (a.last_activity ? new Date(a.last_activity).toISOString().slice(0, 10) : 'n/a')
      );
    });
  } catch (e) {
    console.error('VERIFY FAILED:', e.message);
    process.exitCode = 1;
  } finally {
    await pool.end().catch(() => {});
  }
})();
