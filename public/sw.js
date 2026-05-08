// RepRoute Service Worker - minimal v1
const CACHE_VERSION = 'reproute-v1';

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k !== CACHE_VERSION).map(k => caches.delete(k))
    )).then(() => self.clients.claim())
  );
});

// Network-first strategy - always try fresh, fall back to cache only if offline
self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  if (event.request.url.includes('/api/')) return; // never cache API calls

  event.respondWith(
    fetch(event.request)
      .then(response => {
        // Cache successful responses for shell files only
        if (response.ok && (event.request.url.endsWith('.html') || event.request.url.endsWith('.css') || event.request.url.endsWith('.svg') || event.request.url.endsWith('manifest.json'))) {
          const clone = response.clone();
          caches.open(CACHE_VERSION).then(cache => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => caches.match(event.request))
  );
});
