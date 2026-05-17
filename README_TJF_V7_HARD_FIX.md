# TJFWeather V7 hard fix patch

This patch fixes the problems that can survive CSS-only overrides:

- The notification button still saying the FCM key is missing.
- SPC cards not filling because they are generated dynamically from inline template strings.
- RainViewer tiles disappearing when zooming beyond native radar tile zoom.
- Warning/watch/advisory cards and polygons not centering the radar map.
- Mobile radar controls and warning panel running off-screen.
- Mobile Support Me alignment.

## Apply

From the root of your repo in Codespaces:

```bash
unzip tjfweather_v7_hard_fix_patch.zip -d .
python3 apply_tjfweather_v7_hard_fix.py
```

## Verify before committing

Run:

```bash
grep -n "TJF V7\|FCM_VAPID_PUBLIC_KEY\|tjf_v7\|maxNativeZoom" index.html design_overrides/*.js
git diff -- index.html design_overrides/tjf_v7_hard_override.css design_overrides/tjf_v7_hard_fix.js design_overrides/tjf_v7_fcm_hard_fix.js sw.js firebase-messaging-sw.js
```

You should see:

- Your VAPID public key: `BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg`
- `TJF V7 HARD FIX`
- `tjf_v7_hard_fix.js`
- `tjf_v7_fcm_hard_fix.js`
- `maxNativeZoom = 7` or `finalOpts.maxNativeZoom = 7` in the runtime JS

## Commit and push

```bash
git add index.html sw.js firebase-messaging-sw.js design_overrides/ apply_tjfweather_v7_hard_fix.py README_TJF_V7_HARD_FIX.md
git commit -m "Hard fix mobile notifications, SPC fills, and radar controls"
git push
```

## Important PWA cache note

After pushing, uninstall/reinstall the Home Screen app on iPhone/Android, or unregister the service worker in desktop DevTools. The site can keep serving an old `index.html`/`sw.js` from service worker cache even when GitHub has the new file.
