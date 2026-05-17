/* TJF V9 SAFE SERVICE WORKER: bypass Firebase live channels; handle FCM background pushes */
const TJF_SW_VERSION = 'v9-safe-sw';
const CACHE_NAME = 'tjfweather-static-v9';
const FIREBASE_CONFIG = { apiKey: "AIzaSyDGujuIMmbcaxTVf2NOHBoR7Swzg2D2VUA", authDomain: "tjfwx-2b9b7.firebaseapp.com", projectId: "tjfwx-2b9b7", storageBucket: "tjfwx-2b9b7.firebasestorage.app", messagingSenderId: "665398157561", appId: "1:665398157561:web:a019e1b0e17ac4e74b4228", measurementId: "G-MLFVBH7KDH" };
try {
  importScripts('https://www.gstatic.com/firebasejs/10.14.1/firebase-app-compat.js');
  importScripts('https://www.gstatic.com/firebasejs/10.14.1/firebase-messaging-compat.js');
  firebase.initializeApp(FIREBASE_CONFIG);
  const messaging = firebase.messaging();
  messaging.onBackgroundMessage((payload) => {
    const n = payload.notification || {}, d = payload.data || {};
    self.registration.showNotification(n.title || d.title || 'TJFWeather Alert', { body: n.body || d.body || d.event || 'New weather notification', icon: d.icon || './icon-192.png', badge: d.badge || './icon-192.png', tag: d.tag || d.alertId || n.title || 'tjfweather-alert', renotify: true, vibrate: [200, 100, 200], data: { url: d.url || './#radar', ...d } });
  });
} catch (err) { console.warn('Firebase messaging failed to initialize in service worker', err); }
function shouldBypassFetch(request) {
  try {
    const url = new URL(request.url), host = url.hostname, path = url.pathname;
    if (request.method !== 'GET') return true;
    if (host.includes('firestore.googleapis.com')) return true;
    if (host.includes('firebase') || host.includes('googleapis.com') || host.includes('gstatic.com')) return true;
    if (host.includes('identitytoolkit.googleapis.com') || host.includes('securetoken.googleapis.com')) return true;
    if (host.includes('weather.gov') || host.includes('spc.noaa.gov') || host.includes('aviationweather.gov')) return true;
    if (host.includes('rainviewer.com') || host.includes('tilecache')) return true;
    if (host.includes('openstreetmap.org') || host.includes('tile.openstreetmap.org')) return true;
    if (host.includes('jsdelivr.net') || host.includes('unpkg.com') || host.includes('d3js.org')) return true;
    if (path.includes('/google.firestore.v1.Firestore/Listen/')) return true;
    return false;
  } catch (e) { return true; }
}
self.addEventListener('install', event => { self.skipWaiting(); event.waitUntil(caches.open(CACHE_NAME)); });
self.addEventListener('activate', event => { event.waitUntil((async () => { const keys = await caches.keys(); await Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))); await self.clients.claim(); })()); });
self.addEventListener('fetch', event => {
  if (shouldBypassFetch(event.request)) return;
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname === '/' || url.pathname.endsWith('/index.html') || url.pathname.endsWith('.js') || url.pathname.endsWith('.css') || url.pathname.includes('design_overrides')) {
    event.respondWith(fetch(event.request, { cache: 'no-store' }).catch(() => caches.match(event.request)));
    return;
  }
  event.respondWith((async () => { const cache = await caches.open(CACHE_NAME); const cached = await cache.match(event.request); const fetchPromise = fetch(event.request).then(response => { if (response && response.ok) cache.put(event.request, response.clone()).catch(() => {}); return response; }).catch(() => cached); return cached || fetchPromise; })());
});
self.addEventListener('notificationclick', event => {
  event.notification.close();
  const targetUrl = new URL(event.notification.data?.url || './#radar', self.location.origin).href;
  event.waitUntil((async () => { const clientsArr = await clients.matchAll({ type: 'window', includeUncontrolled: true }); for (const client of clientsArr) { if ('focus' in client) { try { await client.navigate(targetUrl); } catch (e) {} return client.focus(); } } return clients.openWindow(targetUrl); })());
});
