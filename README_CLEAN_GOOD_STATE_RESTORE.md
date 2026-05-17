# TJFWeather clean good-state restore

This restores the site to the solid pixel-panel version from the point where the design was working well, then adds only the necessary fixes:

- FCM VAPID key already filled in.
- `tjfweather.com` used as the notification link/site URL default.
- Safe service worker that bypasses Firebase/Firestore/API requests instead of intercepting them.
- Full background alerts/briefings Cloud Functions.
- SPC/risk cards filled via the clean v5 dynamic fixer, without v7/v8/v9 emergency overlays.
- RainViewer radar `maxNativeZoom: 7` so deep zooms upscale instead of disappearing.
- Warning cards/polygons click to center the radar map.
- Mobile radar controls stay on-screen.
- Mobile Support Me button is centered.

## Apply

From the repo root:

```bash
unzip tjfweather_clean_good_state_restore.zip -d .
python3 apply_clean_good_state_restore.py
```

Then check:

```bash
grep -n "__TJF_CLEAN_GOOD_STATE\|FCM_VAPID_PUBLIC_KEY\|maxNativeZoom\|centerMapOnAlert" index.html
grep -n "shouldBypassFetch\|firestore.googleapis.com\|TJF_SW_VERSION" sw.js
grep -R "TJF_V7\|TJF_V8\|TJF_V9\|tjf_v7\|tjf_v8\|tjf_v9" -n index.html design_overrides 2>/dev/null || true
```

Expected: first two commands show matches; the last command should show nothing.

Commit and push:

```bash
git add -A
git commit -m "Restore clean pixel panel site and notification fixes"
git push
```

Deploy Firebase if needed:

```bash
npm --prefix functions install
firebase use tjfwx-2b9b7
firebase deploy --only functions
firebase deploy --only firestore:rules
```

If you use `functions/.env`, set:

```bash
TJF_SITE_URL=https://tjfweather.com
NWS_USER_AGENT=TJFWeather/1.0 (your-email@example.com)
TJF_APP_ID=tjf-weather-app
TJF_DEFAULT_TZ=America/Chicago
```

## Clear the old broken service worker once

Open `https://tjfweather.com`, open the browser console, and run:

```js
(async () => {
  for (const reg of await navigator.serviceWorker.getRegistrations()) await reg.unregister();
  for (const key of await caches.keys()) await caches.delete(key);
  location.reload();
})();
```

Then verify on the live site console:

```js
window.__TJF_CLEAN_GOOD_STATE
window.TJF_FCM_VAPID_PUBLIC_KEY
```

Expected:

```js
"clean-good-state-v10-2026-05-16"
"BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg"
```
