/* TJFWeather service worker with Firebase Cloud Messaging background support. */
const CACHE_NAME = 'tjfweather-shell-v4';
const APP_SHELL = [
  './',
  './index.html',
  './manifest.webmanifest',
  './icon-192.png',
  './icon-512.png'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(APP_SHELL))
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

self.addEventListener('fetch', (event) => {
  const request = event.request;
  if (request.method !== 'GET') return;

  event.respondWith(
    caches.match(request).then((cached) => {
      if (cached) return cached;
      return fetch(request).then((response) => {
        const copy = response.clone();
        try {
          if (response.ok && new URL(request.url).origin === self.location.origin) {
            caches.open(CACHE_NAME).then((cache) => cache.put(request, copy));
          }
        } catch (_) {}
        return response;
      }).catch(() => caches.match('./index.html'));
    })
  );
});

// Firebase notes that custom notificationclick handling should be registered before importing FCM.
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

// Firebase Cloud Messaging compat SDK is used because it works in non-bundled service workers.
importScripts('https://www.gstatic.com/firebasejs/11.6.1/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/11.6.1/firebase-messaging-compat.js');

firebase.initializeApp({
  apiKey: "AIzaSyDGujuIMmbcaxTVf2NOHBoR7Swzg2D2VUA",
  authDomain: "tjfwx-2b9b7.firebaseapp.com",
  projectId: "tjfwx-2b9b7",
  storageBucket: "tjfwx-2b9b7.firebasestorage.app",
  messagingSenderId: "665398157561",
  appId: "1:665398157561:web:a019e1b0e17ac4e74b4228",
  measurementId: "G-MLFVBH7KDH"
});

try {
  const messaging = firebase.messaging();
  messaging.onBackgroundMessage((payload) => {
    const data = payload.data || {};
    const notification = payload.notification || {};
    const title = data.title || notification.title || 'TJFWeather Alert';
    const options = {
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
      actions: [
        { action: 'open', title: 'Open TJFWeather' }
      ]
    };
    return self.registration.showNotification(title, options);
  });
} catch (error) {
  console.error('Firebase messaging failed to initialize in service worker:', error);
}
