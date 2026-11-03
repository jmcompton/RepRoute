// RepRoute Service Worker
// Strategy:
//   • Navigations / HTML (the app shell): NETWORK-FIRST (10s dead-zone fallback),
//     so online users get the latest app.html; cache is an offline fallback.
//   • /api/* : NETWORK-ONLY — never served from cache (stale API data = bug).
//   • Static, stable assets (icons/manifest/css/svg/png): cache-first, stored
//     under the versioned cache below.
//   • On activate: delete every cache whose name !== current version, then claim.
//   • skipWaiting + clients.claim so a new SW takes over existing clients on
//     their next visit instead of waiting for all tabs to close.
const CACHE_VERSION = 'reproute-v4';

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

// Belt-and-suspenders: the page can ask a waiting worker to activate now.
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k !== CACHE_VERSION).map(k => caches.delete(k))
    )).then(() => self.clients.claim())
  );
});

// Is this an app-shell navigation (the HTML document itself)?
function isNavigationRequest(request) {
  return request.mode === 'navigate' ||
    (request.method === 'GET' &&
     (request.headers.get('accept') || '').includes('text/html'));
}

// Network-first. When the phone has signal, wait for the current app.html:
// the old 3-second cutoff served reps an outdated app against the new server,
// which broke buttons in the field. Only if the network fails, or gives nothing
// for 10 seconds (dead-zone signal), fall back to the last saved copy. The app
// is gzipped (~225 KB), so 10 s is plenty on any real connection.
const NAV_FALLBACK_MS = 10000;
function networkFirst(request) {
  return new Promise((resolve) => {
    let settled = false;
    const done = (r) => { if (!settled) { settled = true; resolve(r); } };
    const timer = setTimeout(() => {
      caches.match(request).then(cached => { if (cached) done(cached); });
    }, NAV_FALLBACK_MS);
    fetch(request)
      .then(response => {
        clearTimeout(timer);
        if (response && response.ok) {
          const clone = response.clone();
          caches.open(CACHE_VERSION).then(cache => cache.put(request, clone));
        }
        done(response);
      })
      .catch(() => {
        clearTimeout(timer);
        caches.match(request).then(cached => done(cached || Response.error()));
      });
  });
}

// Cache-first for stable static assets.
function cacheFirst(request) {
  return caches.match(request).then(cached => {
    if (cached) return cached;
    return fetch(request).then(response => {
      if (response && response.ok) {
        const clone = response.clone();
        caches.open(CACHE_VERSION).then(cache => cache.put(request, clone));
      }
      return response;
    });
  });
}

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET') return;

  // /api/* — NETWORK-ONLY. Bypass the SW entirely so responses are never cached.
  if (request.url.includes('/api/')) return;

  // App-shell navigations / HTML documents — network-first.
  if (isNavigationRequest(request)) {
    event.respondWith(networkFirst(request));
    return;
  }

  // Static, stable assets — cache-first under the versioned cache.
  if (/\.(?:css|js|svg|png|jpg|jpeg|gif|webp|ico|woff2?|ttf)$/i.test(request.url) ||
      request.url.endsWith('manifest.json')) {
    event.respondWith(cacheFirst(request));
    return;
  }

  // Everything else: just hit the network (no caching).
});
