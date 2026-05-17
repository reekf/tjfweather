#!/usr/bin/env python3
from pathlib import Path
import shutil

ROOT = Path.cwd()
SRC = Path(__file__).resolve().parent / 'final_clean_site'
if not SRC.exists():
    raise SystemExit('final_clean_site/ not found next to this script')

# Remove known stacked patch/backup artifacts that were causing visual duplication.
patterns = [
    'design_overrides',
    'clean_good_state_files',
    'backup_before_clean_good_state_restore',
    'index.html.before_tjf_v5_visual_radar_patch',
    'index.html.before_tjf_v6_force_patch',
    'index.html.pre-v8.bak',
    'apply_tjfweather_v*.py',
    'README_TJF_V*.md',
    'README_V*.md',
    'README_CLEAN_GOOD_STATE_RESTORE.md',
    'apply_clean_good_state_restore.py',
    'tjfweather_*_patch.zip',
]
for pat in patterns:
    for path in ROOT.glob(pat):
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()

# Copy final clean site files into repo root.
for src in SRC.rglob('*'):
    if src.is_dir():
        continue
    rel = src.relative_to(SRC)
    dst = ROOT / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)

print('Applied TJFWeather final retro cleanup.')
print('Next: git status && git add -A && git commit -m "Clean up retro visual system" && git push')
