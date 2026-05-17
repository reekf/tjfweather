/* TJFWeather clean service worker: app shell + Firebase Cloud Messaging background notifications.
   This worker intentionally bypasses Firebase/Firestore/Google API/NWS/RainViewer requests so it cannot break live data streams. */
const TJF_SW_VERSION = 'tjfweather-clean-v10-2026-05-16';
const CACHE_NAME = TJF_SW_VERSION;
const APP_SHELL = [
  './',
  './index.html',
  './manifest.webmanifest',
  './icon-192.png',
  './icon-512.png'
];

function shouldBypassFetch(url) {
  try {
    const u = new URL(url);
    const host = u.hostname;
    return (
      host.includes('firestore.googleapis.com') ||
      host.includes('firebaseio.com') ||
      host.includes('firebaseapp.com') ||
      host.includes('googleapis.com') ||
      host.includes('gstatic.com') ||
      host.includes('weather.gov') ||
      host.includes('spc.noaa.gov') ||
      host.includes('aviationweather.gov') ||
      host.includes('rainviewer.com') ||
      host.includes('openstreetmap.org') ||
      host.includes('tile.openstreetmap.org') ||
      host.includes('unpkg.com') ||
      host.includes('cdn.jsdelivr.net') ||
      host.includes('fonts.googleapis.com') ||
      host.includes('fonts.gstatic.com')
    );
  } catch (_) {
    return true;
  }
}

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(APP_SHELL).catch(() => undefined))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

async function networkFirst(request, fallbackUrl = './index.html') {
  try {
    const response = await fetch(request);
    if (response && response.ok && request.method === 'GET') {
      const url = new URL(request.url);
      if (url.origin === self.location.origin) {
        const cache = await caches.open(CACHE_NAME);
        cache.put(request, response.clone()).catch(() => undefined);
      }
    }
    return response;
  } catch (_) {
    const cached = await caches.match(request);
    if (cached) return cached;
    return caches.match(fallbackUrl);
  }
}

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET') return;
  if (shouldBypassFetch(request.url)) return;

  const url = new URL(request.url);

  // Always network-first for navigations and HTML so users do not get trapped on old patched pages.
  if (request.mode === 'navigate' || request.destination === 'document' || url.pathname.endsWith('/index.html')) {
    event.respondWith(networkFirst(request, './index.html'));
    return;
  }

  // Same-origin static assets: cache-first with network fill.
  if (url.origin === self.location.origin) {
    event.respondWith(
      caches.match(request).then((cached) => {
        if (cached) return cached;
        return fetch(request).then((response) => {
          if (response && response.ok) {
            const copy = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(request, copy)).catch(() => undefined);
          }
          return response;
        });
      })
    );
  }
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetUrl = event.notification?.data?.url || './#dashboard';
  event.waitUntil((async () => {
    const allClients = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const client of allClients) {
      if ('focus' in client) {
        await client.focus();
        if ('navigate' in client) return client.navigate(targetUrl);
        return;
      }
    }
    if (clients.openWindow) return clients.openWindow(targetUrl);
  })());
});

try {
  importScripts('https://www.gstatic.com/firebasejs/11.6.1/firebase-app-compat.js');
  importScripts('https://www.gstatic.com/firebasejs/11.6.1/firebase-messaging-compat.js');

  firebase.initializeApp({
    apiKey: 'AIzaSyDGujuIMmbcaxTVf2NOHBoR7Swzg2D2VUA',
    authDomain: 'tjfwx-2b9b7.firebaseapp.com',
    projectId: 'tjfwx-2b9b7',
    storageBucket: 'tjfwx-2b9b7.firebasestorage.app',
    messagingSenderId: '665398157561',
    appId: '1:665398157561:web:a019e1b0e17ac4e74b4228',
    measurementId: 'G-MLFVBH7KDH'
  });

  const messaging = firebase.messaging();
  messaging.onBackgroundMessage((payload) => {
    const data = payload.data || {};
    const notification = payload.notification || {};
    const title = data.title || notification.title || 'TJFWeather Alert';
    return self.registration.showNotification(title, {
      body: data.body || notification.body || 'New TJFWeather notification.',
      icon: data.icon || notification.icon || './icon-192.png',
      badge: data.badge || './icon-192.png',
      tag: data.tag || 'tjfweather-alert',
      renotify: true,
      requireInteraction: data.requireInteraction === 'true',
      data: {
        url: data.url || './#dashboard',
        kind: data.kind || 'generic'
      },
      actions: [{ action: 'open', title: 'Open TJFWeather' }]
    });
  });
} catch (error) {
  console.error('Firebase messaging failed to initialize in service worker:', error);
}
