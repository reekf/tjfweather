# TJFWeather v5 Visual + Radar Patch

This patch assumes the v4 solid-panel patch has already been applied.

## What it changes

- Makes the SPC categorical row and Tornado/Wind/Hail probability cards use solid fills.
- Lightens the blue theme slightly.
- Tightens the dashboard into a compact two-column layout and removes forced stretch/blank panel height.
- Fixes RainViewer radar disappearing at deep zoom by setting Leaflet `maxNativeZoom: 7` for radar tiles.
- Lets users click a warning badge or polygon to switch to the radar tab and fit the map to the warning geometry.
- Centers the mobile dropdown `SUPPORT ME` button.

## Apply

From the root of the GitHub repo in Codespaces:

```bash
unzip /path/to/tjfweather_v5_visual_radar_patch.zip -d .
python3 apply_tjfweather_v5_visual_radar_patch.py
```

Then:

```bash
git add index.html design_overrides/tjf_pixel_solid_theme_v5.css apply_tjfweather_v5_visual_radar_patch.py README_V5_VISUAL_RADAR_PATCH.md
git commit -m "Refine filled panels and radar interactions"
git push
```

## FCM VAPID key reminder

Firebase Console → Project settings → Cloud Messaging → Web configuration → Web Push certificates → Generate key pair / copy public key.

Only paste the **public** key into the frontend.
