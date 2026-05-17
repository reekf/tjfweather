#!/usr/bin/env python3
"""
Apply TJFWeather v5 visual/radar refinements.

Run from the root of the GitHub repo after the v4 patch:
  python3 apply_tjfweather_v5_visual_radar_patch.py
"""
from __future__ import annotations

from pathlib import Path
import re
import shutil

ROOT = Path.cwd()
INDEX = ROOT / "index.html"
THEME_FILE = ROOT / "design_overrides" / "tjf_pixel_solid_theme_v5.css"

if not INDEX.exists():
    raise SystemExit("index.html not found. Run this from the root of your GitHub repo.")
if not THEME_FILE.exists():
    raise SystemExit("Missing design_overrides/tjf_pixel_solid_theme_v5.css")

html = INDEX.read_text(encoding="utf-8")
backup = ROOT / "index.html.before_tjf_v5_visual_radar_patch"
if not backup.exists():
    shutil.copy2(INDEX, backup)

css = THEME_FILE.read_text(encoding="utf-8")
marker_start = "/* === TJF PIXEL SOLID THEME V5 START === */"
marker_end = "/* === TJF PIXEL SOLID THEME V5 END === */"
css_block = f"\n        {marker_start}\n{css}\n        {marker_end}\n"
if marker_start in html and marker_end in html:
    html = re.sub(re.escape(marker_start) + r".*?" + re.escape(marker_end), css_block.strip(), html, flags=re.S)
else:
    # Put v5 at the very end of <style> so it wins over v2/v3/v4 and inline-ish CSS selectors.
    html = html.replace("    </style>", css_block + "    </style>", 1)

# Ensure dashboard classes exist even if earlier patches missed the exact replacements.
html = html.replace(
    '<div class="glass" style="padding: 30px; flex: 2; min-width: 300px;">',
    '<div class="glass dashboard-main-panel" style="padding: 30px; flex: 2; min-width: 300px;">',
)
html = html.replace(
    '<div class="glass dashboard-main-panel dashboard-main-panel"',
    '<div class="glass dashboard-main-panel"',
)
html = html.replace(
    '<div class="glass" style="padding: 25px; flex: 1; min-width: 300px; display: flex; flex-direction: column; gap: 15px; order: 3;">',
    '<div class="glass dashboard-spc-panel" style="padding: 25px; flex: 1; min-width: 300px; display: flex; flex-direction: column; gap: 15px; order: 3;">',
)
html = html.replace(
    '<div class="glass dashboard-spc-panel dashboard-spc-panel"',
    '<div class="glass dashboard-spc-panel"',
)
html = html.replace(
    '<div class="glass" style="padding: 20px; flex: 1; min-width: 300px; cursor: pointer; transition: 0.3s; order: 3;" onclick="document.getElementById(\'navRadarBtn\').click()">',
    '<div class="glass dashboard-radar-panel" style="padding: 20px; flex: 1; min-width: 300px; cursor: pointer; transition: 0.3s; order: 3;" onclick="document.getElementById(\'navRadarBtn\').click()">',
)
html = html.replace(
    '<div class="glass dashboard-radar-panel dashboard-radar-panel"',
    '<div class="glass dashboard-radar-panel"',
)

# RainViewer only provides native XYZ tiles through zoom 7. Tell Leaflet to upscale z7 tiles at deeper zooms.
html = re.sub(
    r"let\s+radarOpts\s*=\s*\{\s*opacity:\s*0,\s*pane:\s*'radarPane',\s*keepBuffer:\s*8,\s*updateWhenZooming:\s*false(?:,\s*maxNativeZoom:\s*\d+)?(?:,\s*maxZoom:\s*\d+)?\s*\};",
    "let radarOpts = { opacity: 0, pane: 'radarPane', keepBuffer: 8, updateWhenZooming: false, maxNativeZoom: 7, maxZoom: 12 };",
    html,
)

# Add center-on-alert helper if missing.
center_marker = "window.centerMapOnAlert = function"
center_fn = r'''
        window.centerMapOnAlert = function(alertIdEncoded) {
            try {
                const alertId = decodeURIComponent(alertIdEncoded || '');
                const alerts = window.nwsAlerts || [];
                const target = alerts.find(f => {
                    const props = f.properties || {};
                    const fallback = `${props.event || ''}|${props.areaDesc || ''}|${props.expires || ''}`;
                    return String(props.id || f.id || fallback) === alertId;
                });
                if (!target || !target.geometry || !window.L) return;
                const targetLayer = L.geoJSON(target);
                const bounds = targetLayer.getBounds();
                if (!bounds || !bounds.isValid()) return;

                const go = () => {
                    if (!window._mapFull) return;
                    window._mapFull.fitBounds(bounds, { padding: [42, 42], maxZoom: 8 });
                    if (window.innerWidth <= 768) {
                        const panel = document.getElementById('radarAlertsPanel');
                        if (panel) panel.classList.remove('open');
                    }
                };

                if (!document.getElementById('radar')?.classList.contains('active')) {
                    const navBtn = document.getElementById('navRadarBtn');
                    if (window.showTab) window.showTab('radar', navBtn || null);
                    setTimeout(go, 220);
                } else {
                    go();
                }
            } catch (e) {
                console.warn('Could not center alert on map', e);
            }
        };
'''
if center_marker not in html:
    html = html.replace("        window.renderAlertPanels = function() {", center_fn + "\n        window.renderAlertPanels = function() {", 1)

# Make list alert badges clickable. Avoid duplicating if a previous run already added data-alert-id.
if "data-alert-id=\"${alertId}\"" not in html:
    html = re.sub(
        r"(let\s+icon\s*=\s*\"ph-warning\";\s*\n\s*if\(cls === 'alert-tornado'\).*?else if\(cls === 'alert-winter'\) icon = \"ph-snowflake\";\s*)\n\s*return `<div class=\"alert-badge \$\{cls\}\">",
        r"\1\n            const alertId = encodeURIComponent((f.properties && (f.properties.id || `${f.properties.event || ''}|${f.properties.areaDesc || ''}|${f.properties.expires || ''}`)) || f.id || '');\n            return `<div class=\"alert-badge ${cls}\" role=\"button\" tabindex=\"0\" data-alert-id=\"${alertId}\" onclick=\"window.centerMapOnAlert && window.centerMapOnAlert('${alertId}')\" onkeydown=\"if(event.key==='Enter'||event.key===' '){event.preventDefault(); window.centerMapOnAlert && window.centerMapOnAlert('${alertId}');}\">",
        html,
        flags=re.S,
    )

# Make warning polygons clickable too.
if "window.centerMapOnAlert && window.centerMapOnAlert(alertId);" not in html:
    html = re.sub(
        r"const\s+onEachFn\s*=\s*\(feature,\s*layer\)\s*=>\s*\{\s*layer\.bindTooltip\(`(<b>\$\{feature\.properties\.event\}</b><br/><small>\$\{\(feature\.properties\.areaDesc \|\| ''\)\.split\(';'\)\[0\]\}</small>)`,\s*\{\s*sticky:\s*true\s*\}\);\s*\};",
        """const onEachFn = (feature, layer) => {
                layer.bindTooltip(`<b>${feature.properties.event}</b><br/><small>${(feature.properties.areaDesc || '').split(';')[0]}</small>`, { sticky: true });
                layer.on('click', () => {
                    const props = feature.properties || {};
                    const alertId = encodeURIComponent(props.id || feature.id || `${props.event || ''}|${props.areaDesc || ''}|${props.expires || ''}`);
                    if (window.centerMapOnAlert) window.centerMapOnAlert(alertId);
                    else if (window._mapFull && layer.getBounds) window._mapFull.fitBounds(layer.getBounds(), { padding: [42, 42], maxZoom: 8 });
                });
            };""",
        html,
        flags=re.S,
    )

# Runtime v5: classify dashboard cards and fill dynamic SPC cards using their actual risk colors.
js_marker_start = "<!-- TJF V5 DYNAMIC VISUAL FIXES START -->"
js_marker_end = "<!-- TJF V5 DYNAMIC VISUAL FIXES END -->"
js_block = r'''
    <!-- TJF V5 DYNAMIC VISUAL FIXES START -->
    <script>
    (function tjfV5DynamicVisualFixes() {
        const panelBlue = '#5b9fe0';
        const darkTextColors = ['rgb(0, 0, 0)', '#000', '#000000', 'black'];

        function isTransparent(color) {
            if (!color) return true;
            const c = String(color).trim().toLowerCase();
            return c === 'transparent' || c === 'rgba(0, 0, 0, 0)' || c === 'rgba(0,0,0,0)';
        }
        function readableTextFor(bg, fallback) {
            if (!bg || isTransparent(bg)) return fallback || '#ffffff';
            const m = bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
            if (!m) return fallback || '#ffffff';
            const r = Number(m[1]), g = Number(m[2]), b = Number(m[3]);
            const yiq = (r * 299 + g * 587 + b * 114) / 1000;
            return yiq >= 150 ? '#061a33' : '#ffffff';
        }
        function tagDashboardCards() {
            const container = document.querySelector('.home-panels-container');
            if (!container) return;
            [...container.children].forEach(el => {
                if (!(el instanceof HTMLElement)) return;
                const text = (el.textContent || '').toLowerCase();
                if (el.classList.contains('home-alerts-panel') || text.includes('active weather alerts')) return;
                if (el.querySelector('#homeLocalWeather') || text.includes('national weather service dashboard')) el.classList.add('dashboard-main-panel');
                if (text.includes('storm prediction center outlook') || el.querySelector('#spcTodayHome') || el.querySelector('#spcTomHome')) el.classList.add('dashboard-spc-panel');
                if (el.querySelector('#miniRadarMap') || text.includes('local composite radar')) el.classList.add('dashboard-radar-panel');
            });
        }
        function directDivChildren(el) {
            return [...(el?.children || [])].filter(child => child instanceof HTMLElement && child.tagName === 'DIV');
        }
        function fillSPC(root) {
            if (!root) return;

            // Category row: contains the level badge and the category name.
            root.querySelectorAll('div').forEach(row => {
                if (!(row instanceof HTMLElement)) return;
                const kids = directDivChildren(row);
                if (kids.length < 2) return;
                const text = (row.textContent || '').toLowerCase();
                const looksCategory = /marginal|slight|enhanced|moderate|high risk|general thunderstorm|no severe/.test(text) && !/tornado\s*wind\s*hail/.test(text);
                if (!looksCategory) return;
                const badge = kids[0];
                const badgeStyle = getComputedStyle(badge);
                let fill = badgeStyle.backgroundColor;
                let textColor = badgeStyle.color;
                if (isTransparent(fill)) fill = panelBlue;
                if (!textColor || isTransparent(textColor)) textColor = readableTextFor(fill, '#ffffff');
                row.classList.add('spc-category-filled');
                row.style.setProperty('--tjf-spc-fill', fill);
                row.style.setProperty('--tjf-spc-text', textColor);
            });

            // Hazard cards: first child label says Tornado, Wind, or Hail; second significant child is usually the probability chip.
            root.querySelectorAll('div').forEach(card => {
                if (!(card instanceof HTMLElement)) return;
                const kids = directDivChildren(card);
                if (kids.length < 2) return;
                const label = (kids[0].textContent || '').trim().toLowerCase();
                if (!['tornado', 'wind', 'hail'].includes(label)) return;
                const chip = kids.find(k => /^(\d+%|sig|0%)$/i.test((k.textContent || '').trim())) || kids[1];
                const chipStyle = getComputedStyle(chip);
                let fill = chipStyle.backgroundColor;
                let textColor = chipStyle.color;
                if (isTransparent(fill)) {
                    fill = label === 'tornado' ? '#328158' : '#5b83b9';
                    textColor = '#ffffff';
                }
                card.classList.add('spc-hazard-filled', `spc-hazard-${label}`);
                chip.classList.add('spc-hazard-chip-filled');
                card.style.setProperty('--tjf-spc-fill', fill);
                card.style.setProperty('--tjf-spc-text', textColor || readableTextFor(fill, '#ffffff'));
            });
        }
        function run() {
            tagDashboardCards();
            ['#spcTodayHome', '#spcTomHome', '#lfSpcToday', '#lfSpcTom', '#lf-spc-container'].forEach(sel => {
                document.querySelectorAll(sel).forEach(fillSPC);
            });
        }
        let scheduled = false;
        function schedule() {
            if (scheduled) return;
            scheduled = true;
            requestAnimationFrame(() => { scheduled = false; run(); });
        }
        if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', schedule);
        else schedule();
        new MutationObserver(schedule).observe(document.documentElement, { childList: true, subtree: true });
    })();
    </script>
    <!-- TJF V5 DYNAMIC VISUAL FIXES END -->
'''
if js_marker_start in html and js_marker_end in html:
    html = re.sub(re.escape(js_marker_start) + r".*?" + re.escape(js_marker_end), js_block.strip(), html, flags=re.S)
else:
    html = html.replace("</body>", js_block + "\n</body>", 1)

INDEX.write_text(html, encoding="utf-8")
print("Applied TJFWeather v5 visual/radar patch. Backup saved as index.html.before_tjf_v5_visual_radar_patch")
