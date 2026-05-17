# TJFWeather V9 Direct Runtime Fix

This patch is for the case where the FCM key is present in `index.html`, but the runtime visual/radar/notification fixes are not loading.

Apply from the repo root:

```bash
unzip /path/to/tjfweather_v9_direct_runtime_fix.zip -d .
python3 apply_tjfweather_v9_direct_runtime_fix.py
```

Verify before committing:

```bash
grep -n "TJF_V9\|tjf_v9_direct_runtime_fix\|FCM_VAPID_PUBLIC_KEY" index.html
grep -n "shouldBypassFetch\|firestore.googleapis.com\|TJF_SW_VERSION" sw.js
```

Commit:

```bash
git add index.html sw.js firebase-messaging-sw.js design_overrides/ apply_tjfweather_v9_direct_runtime_fix.py README_TJF_V9_DIRECT_RUNTIME_FIX.md
git commit -m "Direct runtime fix for FCM, SPC fills, and radar mobile UI"
git push
```

After pushing, unregister the old service worker once, or delete/reinstall the mobile Home Screen app.

Expected live console values:

```js
window.__TJF_V9_INDEX_MARKER
window.__TJF_V9_PATCH_MARKER
window.TJF_FCM_VAPID_PUBLIC_KEY
window.TJF_V9_DIRECT_RUNTIME_FIX
```

Expected:

```js
"index patched v9 direct runtime fix"
"TJF V9 DIRECT RUNTIME FIX"
"BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg"
true
```
