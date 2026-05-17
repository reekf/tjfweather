# TJFWeather final retro visual cleanup

This is a clean restore package, not another stacked overlay patch. It overwrites the current site files with one consolidated retro/pixel visual system and removes the old v3/v4/v5/v6/v7/v8/v9 patch artifacts.

## Apply

From the root of your `tjfweather` repo in Codespaces:

```bash
unzip /path/to/tjfweather_final_retro_cleanup.zip -d .
python3 apply_tjfweather_final_retro_cleanup.py
```

## Verify

```bash
grep -n "final-retro-clean\|FCM_VAPID_PUBLIC_KEY\|maxNativeZoom\|centerMapOnAlert" index.html
grep -R "TJF_V7\|TJF_V8\|TJF_V9\|tjf_v7\|tjf_v8\|tjf_v9" -n . 2>/dev/null || true
```

The second command should return nothing or only old git history references, not active site files.

## Commit

```bash
git status
git add -A
git commit -m "Clean up retro visual system"
git push
```

After deploy, clear the old service worker once on `https://tjfweather.com`:

```js
(async () => {
  for (const reg of await navigator.serviceWorker.getRegistrations()) await reg.unregister();
  for (const key of await caches.keys()) await caches.delete(key);
  location.reload();
})();
```
