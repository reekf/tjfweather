# TJFWeather v2 patch: pixel solid theme + true background notifications

This patch does two things:

1. Replaces the translucent/glass styling with a solid midnight-blue, pixel-panel theme.
2. Moves daily/evening forecast briefings into Firebase Cloud Functions, so they can fire when the app is closed.

## Why the current daily/evening notifications do not fire

The older frontend used `setInterval(...)` in `index.html` to check briefing times and local NWS alerts. That can only run while the page/PWA is active. The local test notification works because the page is open and calls the Notification API directly. Fully background alerts require the scheduled Firebase Function to send FCM/Web Push messages to the saved device token.

## Files in this patch

- `apply_tjfweather_v2_patch.py` patches your existing `index.html` without overwriting your VAPID key.
- `design_overrides/tjf_pixel_solid_theme.css` is the solid pixel/midnight-blue theme inserted into `index.html`.
- `functions/index.js` adds/updates scheduled alert and daily/evening briefing senders.
- `functions/package.json` updates the function package metadata.
- `functions/.env.example` shows the environment variables to use before deploy.
- `sw.js` updates the service worker notification click handling and background FCM handling.
- `firebase-messaging-sw.js` is a compatibility entrypoint for Firebase flows that look for the default worker name.

## How to apply in GitHub Codespaces

From the root of your repo:

```bash
# 1) Unzip/copy the patch files into the repo root.
# 2) Patch index.html while preserving your current VAPID key.
python3 apply_tjfweather_v2_patch.py

# 3) Copy/edit function env vars. Do not put secrets here; these are not secrets.
cp functions/.env.example functions/.env
nano functions/.env

# 4) Install and deploy Firebase Functions + rules.
npm --prefix functions install
firebase use tjfwx-2b9b7
firebase deploy --only functions
firebase deploy --only firestore:rules

# 5) Commit/push frontend and function source.
git add index.html sw.js firebase-messaging-sw.js manifest.webmanifest icon-192.png icon-512.png firebase.json firestore.rules functions/ design_overrides/ apply_tjfweather_v2_patch.py
git commit -m "Add pixel solid theme and background brief notifications"
git push
```

## After deploy

On the live site:

1. Hard refresh or reopen the installed PWA.
2. Log in.
3. Check **Alerts for Pinned Cities** and/or **Daily Forecast Briefings**.
4. Set times.
5. Tap **Grant Permission**.
6. Pin at least one city for NWS watch/warning/advisory alerts.

In Firestore, your user should have a document like:

`artifacts/tjf-weather-app/users/<uid>/pushTokens/<tokenId>`

That document should include:

- `token`
- `alertsEnabled`
- `briefsEnabled`
- `morningTime`
- `eveningTime`
- `timezone`
- `homeLocation`
- `lastAlertCheckAt` after the alert function runs
- `lastBriefCheckAt` after the briefing function runs

## Logs to check

```bash
firebase functions:log --only checkPinnedCityAlerts
firebase functions:log --only sendDailyBriefings
```

If there are no logs, the functions are not deployed or the scheduler has not run yet. If logs say no enabled push tokens, the phone/browser has not successfully saved an FCM token to Firestore.
