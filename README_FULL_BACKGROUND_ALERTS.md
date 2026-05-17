# TJFWeather fully background alert patch

This patch turns mobile weather alerts into true server-driven Web Push alerts using your existing Firebase project.

## What this adds

- `index.html` now enrolls a logged-in device for Firebase Cloud Messaging.
- `sw.js` receives FCM background messages and displays notifications even when the app is closed.
- `functions/index.js` runs every 5 minutes, checks active NWS alerts for each user's pinned cities, and sends one push per new alert per device.
- Firestore stores each device token under:

```text
artifacts/tjf-weather-app/users/{uid}/pushTokens/{tokenDocId}
```

Your existing pinned-city preference path is preserved:

```text
artifacts/tjf-weather-app/users/{uid}/preferences/weather
```

## Required setup

1. Copy these root files into your GitHub Pages repo:

```text
index.html
sw.js
manifest.webmanifest
icon-192.png
icon-512.png
```

2. In Firebase Console, open:

```text
Project Settings -> Cloud Messaging -> Web Push certificates
```

Generate a key pair and copy the **public key** into `index.html`:

```js
const FCM_VAPID_PUBLIC_KEY = "REPLACE_WITH_FIREBASE_WEB_PUSH_CERTIFICATE_KEY_PAIR_PUBLIC_KEY";
```

3. Edit `functions/index.js`:

```js
const SITE_URL = (process.env.TJF_SITE_URL || 'https://YOUR_GITHUB_PAGES_URL/').replace(/\/$/, '');
const NWS_USER_AGENT = process.env.NWS_USER_AGENT || 'TJFWeather/1.0 (contact@example.com)';
```

Use your real GitHub Pages URL and contact email.

4. Deploy the Cloud Function from the folder that contains `firebase.json`:

```bash
npm --prefix functions install
firebase login
firebase use tjfwx-2b9b7
firebase deploy --only functions
```

5. Merge the Firestore rules additions in `firestore.rules`, then deploy rules:

```bash
firebase deploy --only firestore:rules
```

## User flow

1. User logs in.
2. User pins one or more cities.
3. User opens About -> App Features & Notifications.
4. User taps **Enable Background Alerts**.
5. On iPhone/iPad, the user must install the site to the Home Screen first, then open it from the Home Screen icon.

## Notes

- GitHub Pages still hosts the app. Firebase only handles push tokens and scheduled alert delivery.
- The scheduled function checks NWS point alerts directly with `api.weather.gov/alerts/active?point=lat,lon`, so alerts work even when NWS alert polygons are county/zone based.
- The function de-duplicates by NWS alert ID and stores the latest seen IDs per device.
- A local test notification only verifies device notification display. A real background alert is sent by the scheduled Firebase function.
