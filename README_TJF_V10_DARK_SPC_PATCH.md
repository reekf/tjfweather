# TJFWeather v10 dark SPC patch

This patch is intended to be applied on top of the clean good-state restore.

It changes only `index.html` and focuses on:

- darker midnight-blue background and panels
- darker nested panels while keeping solid block styling
- SPC categorical risk cards filled with the risk color
- Tornado/Wind/Hail probability blocks filled with their probability color
- removing the oversized long panel around the "View SPC Outlook" link

## Apply

From the root of your repo in Codespaces:

```bash
python3 apply_tjfweather_v10_dark_spc_patch.py
```

## Verify before commit

```bash
grep -n "TJF_V10\|spc-risk-card\|spc-outlook-link\|clean-good-state-v10-dark-spc" index.html
```

## Commit

```bash
git add index.html apply_tjfweather_v10_dark_spc_patch.py README_TJF_V10_DARK_SPC_PATCH.md
git commit -m "Darken pixel panels and fix filled SPC cards"
git push
```

After deploy, clear service-worker/cache once on `https://tjfweather.com` if needed.
