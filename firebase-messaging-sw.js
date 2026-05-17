
/* TJF V7 combined service worker */
importScripts("https://www.gstatic.com/firebasejs/11.6.1/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/11.6.1/firebase-messaging-compat.js");

firebase.initializeApp({
  apiKey: "AIzaSyDGujuIMmbcaxTVf2NOHBoR7Swzg2D2VUA",
  authDomain: "tjfwx-2b9b7.firebaseapp.com",
  projectId: "tjfwx-2b9b7",
  storageBucket: "tjfwx-2b9b7.firebasestorage.app",
  messagingSenderId: "665398157561",
  appId: "1:665398157561:web:a019e1b0e17ac4e74b4228",
  measurementId: "G-MLFVBH7KDH"
});

const messaging = firebase.messaging();

self.addEventListener('install', event => {
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(self.clients.claim());
});

messaging.onBackgroundMessage((payload) => {
  const title = payload?.notification?.title || payload?.data?.title || "TJFWeather";
  const options = {
    body: payload?.notification?.body || payload?.data?.body || "",
    icon: payload?.notification?.icon || "./icon-192.png",
    badge: "./icon-192.png",
    data: {
      url: payload?.data?.url || "./#radar",
      ...payload?.data
    },
    vibrate: [200, 100, 200]
  };
  self.registration.showNotification(title, options);
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  const url = event.notification?.data?.url || './';
  event.waitUntil((async () => {
    const list = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const client of list) {
      if ('focus' in client) {
        client.navigate(url);
        return client.focus();
      }
    }
    return clients.openWindow(url);
  })());
});
