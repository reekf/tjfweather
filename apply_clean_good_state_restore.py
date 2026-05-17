#!/usr/bin/env python3
"""
Restore TJFWeather to the clean solid-panel state and remove stacked emergency/runtime overlays.

Run from the root of the tjfweather GitHub repo after unzipping this package:
    python3 apply_clean_good_state_restore.py
"""
from __future__ import annotations
from pathlib import Path
import shutil

ROOT = Path.cwd()
PKG = ROOT / "clean_good_state_files"
if not PKG.exists():
    raise SystemExit("clean_good_state_files/ not found. Unzip the package into the repo root first.")

# Make one backup of current files before overwriting.
backup_dir = ROOT / "backup_before_clean_good_state_restore"
backup_dir.mkdir(exist_ok=True)
for rel in ["index.html", "sw.js", "firebase-messaging-sw.js", "manifest.webmanifest", "firebase.json", "firestore.rules"]:
    src = ROOT / rel
    if src.exists() and not (backup_dir / rel).exists():
        (backup_dir / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, backup_dir / rel)

for rel in [
    "index.html",
    "sw.js",
    "firebase-messaging-sw.js",
    "manifest.webmanifest",
    "icon-192.png",
    "icon-512.png",
    "firebase.json",
    "firestore.rules",
    "functions/index.js",
    "functions/package.json",
    "functions/.env.example",
]:
    src = PKG / rel
    dst = ROOT / rel
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

# Remove stale layered/runtime patches that caused duplicated panels and old branches to keep running.
stale_files = [
    "apply_tjfweather_v6_force_patch.py",
    "README_V6_FORCE_PATCH.md",
    "apply_tjfweather_v7_hard_fix.py",
    "README_TJF_V7_HARD_FIX.md",
    "apply_tjfweather_v8_emergency_fix.py",
    "README_TJF_V8_EMERGENCY_FIX.md",
    "apply_tjfweather_v9_direct_runtime_fix.py",
    "README_TJF_V9_DIRECT_RUNTIME_FIX.md",
]
for name in stale_files:
    p = ROOT / name
    if p.exists():
        p.unlink()

# Stale design_overrides are no longer needed because the clean CSS/JS are integrated into index.html.
design = ROOT / "design_overrides"
if design.exists():
    for p in design.glob("tjf_v*.js"):
        p.unlink()
    for p in design.glob("tjf_*v6*.css"):
        p.unlink()
    for p in design.glob("tjf_*v7*.css"):
        p.unlink()
    for p in design.glob("tjf_*v8*.css"):
        p.unlink()
    for p in design.glob("tjf_*v9*.css"):
        p.unlink()

print("Restored clean TJFWeather good-state files.")
print("Now run: git status")
