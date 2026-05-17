# TJFWeather v6 force patch

This patch does two things:

1. Forces the provided Firebase Cloud Messaging VAPID public key into `index.html`.
2. Re-applies the solid-panel/radar visual fixes with explicit verification output.

Run from the root of the GitHub repo:

```bash
unzip tjfweather_v6_force_key_visual_patch.zip -d .
python3 apply_tjfweather_v6_force_patch.py

grep -n "FCM_VAPID_PUBLIC_KEY\|TJF V6\|maxNativeZoom" index.html

git diff -- index.html
git add index.html design_overrides/tjf_pixel_solid_theme_v6_force.css apply_tjfweather_v6_force_patch.py README_V6_FORCE_PATCH.md
git commit -m "Force FCM key and solid panel visual fixes"
git push
```

After GitHub Pages updates, clear the PWA/service-worker cache if the old page still appears.
