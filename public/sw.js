// RepRoute Service Worker
// Strategy:
//   • Navigations / HTML (the app shell): NETWORK-FIRST with a 3s timeout, so
//     online users always get the latest app.html; cache is offline fallback only.
//   • /api/* : NETWORK-ONLY — never served from cache (stale API data = bug).
//   • Static, stable assets (icons/manifest/css/svg/png): cache-first, stored
//     under the versioned cache below.
//   • On activate: delete every cache whose name !== current version, then claim.
//   • skipWaiting + clients.claim so a new SW takes over existing clients on
//     their next visit instead of waiting for all tabs to close.
const CACHE_VERSION = 'reproute-v3';
const NAV_TIMEOUT_MS = 3000;

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

// Network-first with a timeout: race the network against a timer; if the network
// is too slow or fails, fall back to whatever we cached last (offline support).
function networkFirst(request) {
  return new Promise((resolve) => {
    let settled = false;
    const done = (r) => { if (!settled) { settled = true; resolve(r); } };

    const timer = setTimeout(() => {
      caches.match(request).then(cached => { if (cached) done(cached); });
    }, NAV_TIMEOUT_MS);

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
