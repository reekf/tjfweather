#!/usr/bin/env python3
"""
Apply TJFWeather v4 solid nested-panel refinements.

This patch is intended to run after the v2/v3 pixel theme patches, but it is
safe to run as a standalone override because it appends CSS and a runtime tagger
without touching Firebase/VAPID configuration.

Run from the root of the GitHub repo:
  python3 apply_tjfweather_v4_solid_panels_patch.py
"""
from __future__ import annotations

from pathlib import Path
import re
import shutil

ROOT = Path.cwd()
INDEX = ROOT / "index.html"
THEME_FILE = ROOT / "design_overrides" / "tjf_pixel_solid_theme_v4.css"

if not INDEX.exists():
    raise SystemExit("index.html not found. Run this from the root of your GitHub repo.")
if not THEME_FILE.exists():
    raise SystemExit("Missing design_overrides/tjf_pixel_solid_theme_v4.css")

html = INDEX.read_text(encoding="utf-8")
backup = ROOT / "index.html.before_tjf_v4_solid_panels_patch"
if not backup.exists():
    shutil.copy2(INDEX, backup)

# Ensure readable pixel body font remains available.
font_href = "https://fonts.googleapis.com/css2?family=Pixelify+Sans:wght@400;500;600;700&family=VT323&display=swap"
html = re.sub(
    r'<link href="https://fonts\.googleapis\.com/css2\?family=Pixelify\+Sans:[^"\n]+" rel="stylesheet">',
    f'<link href="{font_href}" rel="stylesheet">',
    html,
)
if "family=VT323" not in html:
    html = html.replace(
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n'
        f'    <link href="{font_href}" rel="stylesheet">',
        1,
    )

# Add/replace v4 CSS override block. This deliberately sits after v3 so it wins.
css = THEME_FILE.read_text(encoding="utf-8")
marker_start = "/* === TJF PIXEL SOLID PANELS V4 START === */"
marker_end = "/* === TJF PIXEL SOLID PANELS V4 END === */"
css_block = f"\n        {marker_start}\n{css}\n        {marker_end}\n"
if marker_start in html and marker_end in html:
    html = re.sub(re.escape(marker_start) + r".*?" + re.escape(marker_end), css_block.strip(), html, flags=re.S)
else:
    # Insert immediately after the v3 block when present, otherwise before </style>.
    v3_end = "/* === TJF PIXEL SOLID THEME OVERRIDES END === */"
    if v3_end in html:
        html = html.replace(v3_end, v3_end + css_block, 1)
    else:
        html = html.replace("    </style>", css_block + "    </style>", 1)

# Make dashboard classes available even if v3 was not run or missed one of these replacements.
html = html.replace(
    '<div class="glass" style="padding: 30px; flex: 2; min-width: 300px;">',
    '<div class="glass dashboard-main-panel" style="padding: 30px; flex: 2; min-width: 300px;">',
)
html = html.replace(
    '<div class="glass" style="padding: 25px; flex: 1; min-width: 300px; display: flex; flex-direction: column; gap: 15px; order: 3;">',
    '<div class="glass dashboard-spc-panel" style="padding: 25px; flex: 1; min-width: 300px; display: flex; flex-direction: column; gap: 15px; order: 3;">',
)
html = html.replace(
    '<div class="glass" style="padding: 20px; flex: 1; min-width: 300px; cursor: pointer; transition: 0.3s; order: 3;" onclick="document.getElementById(\'navRadarBtn\').click()">',
    '<div class="glass dashboard-radar-panel" style="padding: 20px; flex: 1; min-width: 300px; cursor: pointer; transition: 0.3s; order: 3;" onclick="document.getElementById(\'navRadarBtn\').click()">',
)

# If the previous SPC-filled patch wasn't applied, add a light class bridge where the template strings are easy to patch.
html = html.replace(
    '<div style="display: flex; flex-direction: column; gap: 15px; margin-bottom: 12px;">',
    '<div class="spc-risk-wrap tjf-panel-depth-1" style="display: flex; flex-direction: column; gap: 15px; margin-bottom: 12px;">',
)
html = html.replace(
    '<div style="padding: 10px 14px; background: rgba(255,255,255,0.05); border-radius: 8px; font-size: 15px; opacity: 0.7; font-weight: 500; margin-bottom: 12px; color:#fff;">No Severe Weather Expected</div>',
    '<div class="spc-no-severe-card tjf-panel-depth-1" style="padding: 12px 14px; font-size: 16px; font-weight: 700; margin-bottom: 12px; color:#fff;">No Severe Weather Expected</div>',
)

# Runtime tagger: catches dynamically-created cards produced by JS template strings.
js_marker_start = "<!-- TJF SOLID PANEL TAGGER V4 START -->"
js_marker_end = "<!-- TJF SOLID PANEL TAGGER V4 END -->"
js_block = r'''
    <!-- TJF SOLID PANEL TAGGER V4 START -->
    <script>
    (function tjfSolidPanelTaggerV4() {
        function isHTMLElement(el) { return el && el.nodeType === 1 && el instanceof HTMLElement; }
        function styleLooksPanelish(style) {
            if (!style) return false;
            const s = style.toLowerCase();
            const painted = s.includes('background:') || s.includes('border:') || s.includes('border-radius') || s.includes('box-shadow');
            const sized = s.includes('padding:') || s.includes('min-width') || s.includes('min-height') || s.includes('height:') || s.includes('display:flex') || s.includes('display: flex') || s.includes('display:grid') || s.includes('display: grid');
            return painted && sized;
        }
        function classLooksPanelish(el) {
            const cls = String(el.className || '');
            return /(^|\s)(glass|lf-hour-col|lf-daily-col|alert-badge|spc-risk-summary|spc-hazard-card|spc-no-severe-card|placeholder-banner|asos-panel|nws-panel)(\s|$)/.test(cls);
        }
        function idLooksPanelish(el) {
            const id = String(el.id || '');
            return /^(homeLocalWeather|homeForecastFootnote|homeEnviroContainer|homeNextTemps|radarAlertsList|radarAlertsPanel|lf-banner-container|lf-alerts-container|lf-hourly-container|lf-daily-container|lf-mos-container|lf-enviro-container|lf-spc-container|spcTodayHome|spcTomHome)$/.test(id);
        }
        function insidePanelRegion(el) {
            return !!el.closest('.glass, #dashboard, #localforecast, #radarAlertsPanel, #radarAlertsList, #datasources, #about, #feedback');
        }
        function shouldTag(el) {
            if (!isHTMLElement(el)) return false;
            if (el.matches('script, style, svg, path, canvas, img, i, span')) return false;
            if (el.classList.contains('tjf-panel-depth-1') || el.classList.contains('tjf-panel-depth-2')) return false;
            if (classLooksPanelish(el) || idLooksPanelish(el)) return true;
            if (!insidePanelRegion(el)) return false;
            const style = el.getAttribute('style') || '';
            if (!styleLooksPanelish(style)) return false;
            const textLen = (el.textContent || '').trim().length;
            const hasContent = textLen > 0 || el.children.length > 0;
            const isTinyInline = el.children.length === 0 && textLen < 12 && !style.toLowerCase().includes('min-width') && !style.toLowerCase().includes('height');
            return hasContent && !isTinyInline;
        }
        function tag(root) {
            const scope = root && root.querySelectorAll ? root : document;
            scope.querySelectorAll('div, section, article, aside, label').forEach(el => {
                if (shouldTag(el)) el.classList.add('tjf-panel-depth-1');
            });
            scope.querySelectorAll('.tjf-panel-depth-1 .tjf-panel-depth-1').forEach(el => {
                el.classList.add('tjf-panel-depth-2');
            });
        }
        function scheduleTag(root) {
            requestAnimationFrame(() => tag(root || document));
        }
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => scheduleTag(document));
        } else {
            scheduleTag(document);
        }
        const observer = new MutationObserver(mutations => {
            let root = document;
            for (const m of mutations) {
                if (m.addedNodes && m.addedNodes.length) {
                    root = m.target && m.target.querySelectorAll ? m.target : document;
                    break;
                }
            }
            scheduleTag(root);
        });
        observer.observe(document.documentElement, { childList: true, subtree: true });
    })();
    </script>
    <!-- TJF SOLID PANEL TAGGER V4 END -->
'''
if js_marker_start in html and js_marker_end in html:
    html = re.sub(re.escape(js_marker_start) + r".*?" + re.escape(js_marker_end), js_block.strip(), html, flags=re.S)
else:
    html = html.replace("</body>", js_block + "\n</body>", 1)

INDEX.write_text(html, encoding="utf-8")
print("Applied TJFWeather v4 solid nested-panel patch. Backup saved as index.html.before_tjf_v4_solid_panels_patch")
