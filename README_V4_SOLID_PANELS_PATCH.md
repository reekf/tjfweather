# TJFWeather v4 solid panels patch

This patch is meant to be applied after the v3 visual patch.

## What it changes

- Makes **all nested panels/cards** opaque, not just SPC risk blocks.
- Adds a reusable `.tjf-panel-depth-1` / `.tjf-panel-depth-2` system so dynamically generated forecast cards, alert cards, mini cards, model panels, and environmental panels stand out from their parent panels.
- Fills NWS alert badges with their hazard color instead of relying mostly on outlines.
- Lightens the blue theme again so the site is less dark overall.
- Rebalances the dashboard into two cleaner rows: main forecast + alerts, then SPC + radar.
- Keeps radar/map tiles functional; only the UI panels over the map are forced solid.

## Apply in GitHub Codespaces

From the root of your repo:

```bash
unzip /path/to/tjfweather_v4_solid_nested_panels_patch.zip -d .
python3 apply_tjfweather_v4_solid_panels_patch.py
```

Then commit/push:

```bash
git add index.html design_overrides/tjf_pixel_solid_theme_v4.css apply_tjfweather_v4_solid_panels_patch.py README_V4_SOLID_PANELS_PATCH.md
git commit -m "Make all nested panels solid filled blocks"
git push
```

## Notes

- The script creates `index.html.before_tjf_v4_solid_panels_patch` locally as a backup the first time it runs.
- This patch does not touch Firebase notification settings, VAPID keys, or Cloud Functions.
