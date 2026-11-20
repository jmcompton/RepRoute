// Lead Finder: Kody's search fixes and Near Me, against a stub Google.
// Run: node test/e2e-leads.test.js
const assert = require('assert');
const http = require('http');
const { start } = require('./helpers/harness');

// Results Google really returned on Nov 20 (names from the live app), placed
// around Gainesville GA (34.30, -83.82) unless noted.
const P = (name, lat, lng, types) => ({ id: 'pid-' + name.replace(/\W/g, ''), displayName: { text: name },
  formattedAddress: '1 Main St, Gainesville, GA 30501', location: { latitude: lat, longitude: lng },
  types: types || ['establishment'], rating: 4.6, userRatingCount: 40, businessStatus: 'OPERATIONAL', websiteUri: 'https://x.test' });
const WINDOW = [
  P('Bullard Exteriors', 34.30, -83.82, ['general_contractor']),
  P('Door4Life', 34.31, -83.83),
  P('Top Notch Dock & Door', 34.20, -83.86),
  P('Michael James Remodeling', 34.33, -83.80, ['general_contractor']),
  P('Gainesville Window & Door Showroom', 34.29, -83.81, ['store']),
  P('Atlanta Highway Glass', 34.28, -83.84)
];
const FASTENERS = [
  P('Hilti Store - Atlanta Norcross', 34.30, -83.82, ['hardware_store']),
  P('Fastenal Fulfillment Center', 34.30, -83.82, ['warehouse']),
  P('Bolt Expo', 34.30, -83.82, ['hardware_store']),
  P('American Home Liquidation', 34.30, -83.82, ['store']),
  P('SouthernCarlson', 34.30, -83.82, ['hardware_store']),
  P('Far Away Tool Supply', 35.30, -83.82, ['hardware_store'])   // ~69 mi north
];
const REBAR = [
  P('Rebar Supply of Georgia', 34.30, -83.82, ['store']),
  P('Smith Ready Mix Concrete', 34.30, -83.82, ['store']),
  P('Concrete Leveling Pros', 34.30, -83.82, ['general_contractor']),
  P('North GA Masonry Supply', 34.30, -83.82, ['building_materials_store'])
];

const seen = [];
function startStubGoogle(port) {
  return new Promise(resolve => {
    const srv = http.createServer((req, res) => {
      let body = ''; req.on('data', d => body += d);
      req.on('end', () => {
        res.setHeader('Content-Type', 'application/json');
        if (req.url.includes('geocode')) return res.end(JSON.stringify({ status: 'OK', results: [{ geometry: { location: { lat: 34.30, lng: -83.82 } } }] }));
        const b = JSON.parse(body || '{}'); seen.push(b);
        const q = (b.textQuery || '').toLowerCase();
        const places = /window|door|millwork|pella/.test(q) ? WINDOW
          : /rebar|concrete|masonry/.test(q) ? REBAR : FASTENERS;
        res.end(JSON.stringify({ places }));
      });
    }).listen(port, () => resolve(srv));
  });
}

(async () => {
  const gport = 4500 + Math.floor(Math.random() * 300);
  const g = await startStubGoogle(gport);
  const h = await start({ GOOGLE_PLACES_API_KEY: 'stub', PLACES_BASE_URL: `http://127.0.0.1:${gport}`, GEOCODE_BASE_URL: `http://127.0.0.1:${gport}` });
  try {
    const kody = await h.login('kody');
    const names = r => (r.body.leads || []).filter(l => !l.already_contacted).map(l => l.company);

    // ShurTape window/door: no garage/dock door shops, no remodelers; dealers kept
    let r = await kody('POST', '/api/morning/daily-leads', { city: 'Gainesville GA', brands: ['ShurTape'], channel: 'Window/Door Installer' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    let n = names(r);
    assert.ok(!n.includes('Door4Life') && !n.includes('Top Notch Dock & Door') && !n.includes('Michael James Remodeling'), 'junk dropped: ' + n);
    assert.ok(n.includes('Gainesville Window & Door Showroom') && n.includes('Bullard Exteriors'), 'good ones kept: ' + n);
    assert.ok(seen.some(b => /window and door dealer/i.test(b.textQuery)), 'dealer searches run');

    // Alum-A-Pole fasteners: no bolt sellers, fulfillment centers, liquidators
    seen.length = 0;
    r = await kody('POST', '/api/morning/daily-leads', { city: 'Gainesville GA', brands: ['Alum-A-Pole'], channel: 'Construction Fasteners' });
    n = names(r);
    assert.ok(!n.some(x => /Fastenal Fulfillment|Bolt Expo|Liquidation/.test(x)), 'junk dropped: ' + n);
    assert.ok(n.includes('Hilti Store - Atlanta Norcross') && n.includes('SouthernCarlson'), 'tool stores kept: ' + n);
    assert.ok(seen.some(b => /ladder/i.test(b.textQuery)) && seen.some(b => /siding supply/i.test(b.textQuery)), 'Kody\'s searches run');

    // Rebar / concrete: new segment works and drops ready-mix and concrete contractors
    r = await kody('POST', '/api/morning/daily-leads', { city: 'Gainesville GA', brands: ['Soudal'], channel: 'Rebar / Concrete Supply' });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    n = names(r);
    assert.deepStrictEqual(n.sort(), ['North GA Masonry Supply', 'Rebar Supply of Georgia'], 'rebar results: ' + n);

    // Near me: no city typed, GPS + 15 miles. Searches carry no city text and
    // are biased around the point; a store 69 miles away is dropped.
    seen.length = 0;
    r = await kody('POST', '/api/morning/daily-leads', { brands: ['Fortress'], channel: 'Construction Fasteners', lat: 34.30, lng: -83.82, radius_miles: 15 });
    assert.strictEqual(r.status, 200, JSON.stringify(r.body));
    assert.strictEqual(r.body.mode, 'near');
    assert.ok(seen.length && seen.every(b => !/atlanta|gainesville/i.test(b.textQuery)), 'no city in near-me queries');
    const bias = seen[0].locationBias.circle;
    assert.ok(Math.abs(bias.center.latitude - 34.30) < 1e-9 && bias.radius > 20000 && bias.radius < 25000, 'bias at the rep, ~15 mi');
    assert.ok(!names(r).includes('Far Away Tool Supply'), 'outside the distance is dropped');

    console.log('e2e-leads: all tests passed');
  } catch (e) {
    console.error(e); console.error(h.log().slice(-1500)); process.exitCode = 1;
  } finally { await h.stop(); g.close(); }
})();
