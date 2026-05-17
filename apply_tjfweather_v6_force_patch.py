#!/usr/bin/env python3
"""
Force-apply TJFWeather v6: inject FCM VAPID public key and re-apply the v5 visual/radar fixes.
Run from the root of the GitHub repo:
  python3 apply_tjfweather_v6_force_patch.py
"""
from __future__ import annotations
from pathlib import Path
import re, shutil, sys

FCM_VAPID_PUBLIC_KEY = "BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg"
ROOT = Path.cwd()
INDEX = ROOT / "index.html"
THEME = ROOT / "design_overrides" / "tjf_pixel_solid_theme_v6_force.css"

if not INDEX.exists():
    raise SystemExit("ERROR: index.html not found. Run this from the root of your GitHub repo.")
if not THEME.exists():
    raise SystemExit("ERROR: design_overrides/tjf_pixel_solid_theme_v6_force.css missing. Unzip the patch first.")

html = INDEX.read_text(encoding="utf-8")
backup = ROOT / "index.html.before_tjf_v6_force_patch"
if not backup.exists():
    shutil.copy2(INDEX, backup)

changed = []

# 1) Force-inject the FCM VAPID public key, regardless of what placeholder/value is present.
pattern = r"const\s+FCM_VAPID_PUBLIC_KEY\s*=\s*(['\"])(.*?)\1\s*;"
replacement = f'const FCM_VAPID_PUBLIC_KEY = "{FCM_VAPID_PUBLIC_KEY}";'
html2, n = re.subn(pattern, replacement, html, count=1)
if n:
    html = html2
    changed.append("Set FCM_VAPID_PUBLIC_KEY")
else:
    # If the FCM code exists but the constant is missing, insert it before the first getToken call.
    if "getToken(" in html or "firebase/messaging" in html or "getMessaging" in html:
        html = html.replace("getToken(", replacement + "\n                getToken(", 1)
        changed.append("Inserted missing FCM_VAPID_PUBLIC_KEY before getToken")
    else:
        print("WARNING: Could not find FCM web messaging code in index.html. The VAPID key was not inserted because the notification patch may not be present.")

# 2) Force-inject the solid visual CSS near the end of <head> so it is not dependent on old style markers.
css = THEME.read_text(encoding="utf-8")
css_marker_start = "<!-- TJF V6 FORCE SOLID THEME START -->"
css_marker_end = "<!-- TJF V6 FORCE SOLID THEME END -->"
css_block = f"""
    {css_marker_start}
    <style id=\"tjf-v6-force-solid-theme\">
{css}

/* Extra v6 force overrides: make nested cards solid even when generated from inline JS. */
#radarAlertsPanel,
#radarAlertsList,
.radar-controls,
#homeLocalWeather,
#homeNextTemps .lf-hour-col,
#lf-hourly-container .lf-hour-col,
#lf-daily-container .lf-daily-col,
#lf-asos-container,
#lf-nws-container,
#lf-enviro-container .glass,
#lf-mos-container .glass,
#lf-spc-container .glass,
#homePinnedCards .glass {{
  background-color: #174d83 !important;
  background-image: none !important;
  backdrop-filter: none !important;
  -webkit-backdrop-filter: none !important;
  border-color: #6bb7ff !important;
}}
#dashboard .home-panels-container {{
  display: grid !important;
  grid-template-columns: minmax(320px, 1.45fr) minmax(280px, .8fr) !important;
  grid-auto-flow: dense !important;
  align-items: start !important;
  gap: 18px !important;
}}
#dashboard .home-panels-container > * {{
  width: 100% !important;
  max-width: none !important;
  min-width: 0 !important;
  margin: 0 !important;
}}
#dashboard .home-alerts-panel {{ grid-column: 2 !important; grid-row: 1 !important; max-height: 360px !important; }}
#dashboard .dashboard-main-panel {{ grid-column: 1 !important; grid-row: 1 / span 2 !important; }}
#dashboard .dashboard-spc-panel {{ grid-column: 1 !important; grid-row: 3 !important; }}
#dashboard .dashboard-radar-panel {{ grid-column: 2 !important; grid-row: 2 / span 2 !important; }}
#homePinnedSection {{ grid-column: 1 / -1 !important; order: 99 !important; }}
@media (max-width: 900px) {{
  #dashboard .home-panels-container {{ grid-template-columns: 1fr !important; }}
  #dashboard .home-panels-container > * {{ grid-column: auto !important; grid-row: auto !important; }}
}}
.spc-category-filled,
.spc-hazard-filled {{
  background: var(--tjf-spc-fill, #3283c7) !important;
  color: var(--tjf-spc-text, #fff) !important;
  border-color: color-mix(in srgb, var(--tjf-spc-fill, #3283c7), #fff 28%) !important;
  box-shadow: 6px 6px 0 #061a33 !important;
}}
.spc-category-filled *,
.spc-hazard-filled * {{ color: inherit !important; }}
.spc-hazard-chip-filled {{
  background: rgba(255,255,255,0.22) !important;
  border: 2px solid rgba(255,255,255,0.35) !important;
}}
    </style>
    {css_marker_end}
"""
if css_marker_start in html and css_marker_end in html:
    html = re.sub(re.escape(css_marker_start) + r".*?" + re.escape(css_marker_end), css_block.strip(), html, flags=re.S)
else:
    html = html.replace("</head>", css_block + "\n</head>", 1)
changed.append("Injected v6 force CSS")

# 3) Add dashboard classes if current HTML does not have them.
replacements = [
    ('<div class="glass" style="padding: 30px; flex: 2; min-width: 300px;">', '<div class="glass dashboard-main-panel" style="padding: 30px; flex: 2; min-width: 300px;">'),
    ('<div class="glass" style="padding: 25px; flex: 1; min-width: 300px; display: flex; flex-direction: column; gap: 15px; order: 3;">', '<div class="glass dashboard-spc-panel" style="padding: 25px; flex: 1; min-width: 300px; display: flex; flex-direction: column; gap: 15px; order: 3;">'),
    ('<div class="glass" style="padding: 20px; flex: 1; min-width: 300px; cursor: pointer; transition: 0.3s; order: 3;" onclick="document.getElementById(\'navRadarBtn\').click()">', '<div class="glass dashboard-radar-panel" style="padding: 20px; flex: 1; min-width: 300px; cursor: pointer; transition: 0.3s; order: 3;" onclick="document.getElementById(\'navRadarBtn\').click()">'),
]
for old, new in replacements:
    if old in html:
        html = html.replace(old, new, 1)
        changed.append("Tagged dashboard panel classes")
html = html.replace('dashboard-main-panel dashboard-main-panel', 'dashboard-main-panel')
html = html.replace('dashboard-spc-panel dashboard-spc-panel', 'dashboard-spc-panel')
html = html.replace('dashboard-radar-panel dashboard-radar-panel', 'dashboard-radar-panel')

# 4) RainViewer zoom fix.
html2, n = re.subn(
    r"let\s+radarOpts\s*=\s*\{\s*opacity:\s*0,\s*pane:\s*'radarPane',\s*keepBuffer:\s*8,\s*updateWhenZooming:\s*false(?:,\s*maxNativeZoom:\s*\d+)?(?:,\s*maxZoom:\s*\d+)?\s*\};",
    "let radarOpts = { opacity: 0, pane: 'radarPane', keepBuffer: 8, updateWhenZooming: false, maxNativeZoom: 7, maxZoom: 12 };",
    html,
    count=1,
)
if n:
    html = html2
    changed.append("Set RainViewer maxNativeZoom/maxZoom")

# 5) Center on warning from alert panels/polygons.
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
                } else go();
            } catch (e) { console.warn('Could not center alert on map', e); }
        };
'''
if "window.centerMapOnAlert = function" not in html and "window.renderAlertPanels = function()" in html:
    html = html.replace("        window.renderAlertPanels = function() {", center_fn + "\n        window.renderAlertPanels = function() {", 1)
    changed.append("Added centerMapOnAlert helper")

if "data-alert-id=\"${alertId}\"" not in html:
    html2, n = re.subn(
        r"(let\s+icon\s*=\s*\"ph-warning\";\s*\n\s*if\(cls === 'alert-tornado'\).*?else if\(cls === 'alert-winter'\) icon = \"ph-snowflake\";\s*)\n\s*return `<div class=\"alert-badge \$\{cls\}\">",
        r"\1\n            const alertId = encodeURIComponent((f.properties && (f.properties.id || `${f.properties.event || ''}|${f.properties.areaDesc || ''}|${f.properties.expires || ''}`)) || f.id || '');\n            return `<div class=\"alert-badge ${cls}\" role=\"button\" tabindex=\"0\" data-alert-id=\"${alertId}\" onclick=\"window.centerMapOnAlert && window.centerMapOnAlert('${alertId}')\" onkeydown=\"if(event.key==='Enter'||event.key===' '){event.preventDefault(); window.centerMapOnAlert && window.centerMapOnAlert('${alertId}');}\">",
        html,
        count=1,
        flags=re.S,
    )
    if n:
        html = html2
        changed.append("Made alert badges clickable")

# 6) Runtime dynamic SPC/card fill tagger.
js_marker_start = "<!-- TJF V6 FORCE DYNAMIC VISUAL FIXES START -->"
js_marker_end = "<!-- TJF V6 FORCE DYNAMIC VISUAL FIXES END -->"
js_block = r'''
    <!-- TJF V6 FORCE DYNAMIC VISUAL FIXES START -->
    <script>
    (function tjfV6ForceDynamicVisualFixes() {
        function isTransparent(color) {
            if (!color) return true;
            const c = String(color).trim().toLowerCase();
            return c === 'transparent' || c === 'rgba(0, 0, 0, 0)' || c === 'rgba(0,0,0,0)';
        }
        function children(el) { return [...(el?.children || [])].filter(x => x instanceof HTMLElement && x.tagName === 'DIV'); }
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
        function fillSPC(root) {
            if (!root) return;
            root.querySelectorAll('div').forEach(row => {
                if (!(row instanceof HTMLElement)) return;
                const kids = children(row);
                if (kids.length < 2) return;
                const text = (row.textContent || '').toLowerCase();
                const looksCategory = /marginal|slight|enhanced|moderate|high risk|general thunderstorm|no severe/.test(text) && !/tornado\s*wind\s*hail/.test(text);
                if (!looksCategory) return;
                const badge = kids[0];
                let fill = getComputedStyle(badge).backgroundColor;
                let textColor = getComputedStyle(badge).color || '#fff';
                if (isTransparent(fill)) fill = '#3f8ed0';
                row.classList.add('spc-category-filled');
                row.style.setProperty('--tjf-spc-fill', fill);
                row.style.setProperty('--tjf-spc-text', textColor);
            });
            root.querySelectorAll('div').forEach(card => {
                if (!(card instanceof HTMLElement)) return;
                const kids = children(card);
                if (kids.length < 2) return;
                const label = (kids[0].textContent || '').trim().toLowerCase();
                if (!['tornado', 'wind', 'hail'].includes(label)) return;
                const chip = kids.find(k => /^(\d+%|sig|0%)$/i.test((k.textContent || '').trim())) || kids[1];
                let fill = getComputedStyle(chip).backgroundColor;
                let textColor = getComputedStyle(chip).color || '#fff';
                if (isTransparent(fill)) { fill = label === 'tornado' ? '#328158' : '#4d8fc8'; textColor = '#fff'; }
                card.classList.add('spc-hazard-filled', `spc-hazard-${label}`);
                chip.classList.add('spc-hazard-chip-filled');
                card.style.setProperty('--tjf-spc-fill', fill);
                card.style.setProperty('--tjf-spc-text', textColor);
            });
        }
        function run() {
            tagDashboardCards();
            ['#spcTodayHome', '#spcTomHome', '#lfSpcToday', '#lfSpcTom', '#lf-spc-container'].forEach(sel => document.querySelectorAll(sel).forEach(fillSPC));
        }
        let scheduled = false;
        function schedule() {
            if (scheduled) return;
            scheduled = true;
            requestAnimationFrame(() => { scheduled = false; run(); });
        }
        if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', schedule); else schedule();
        new MutationObserver(schedule).observe(document.documentElement, { childList: true, subtree: true });
    })();
    </script>
    <!-- TJF V6 FORCE DYNAMIC VISUAL FIXES END -->
'''
if js_marker_start in html and js_marker_end in html:
    html = re.sub(re.escape(js_marker_start) + r".*?" + re.escape(js_marker_end), js_block.strip(), html, flags=re.S)
else:
    html = html.replace("</body>", js_block + "\n</body>", 1)
changed.append("Injected v6 dynamic visual JS")

INDEX.write_text(html, encoding="utf-8")

print("\nTJFWeather v6 force patch complete.")
for item in changed:
    print(f"  ✓ {item}")
print("\nVerification:")
text = INDEX.read_text(encoding="utf-8")
checks = {
    "FCM key present": FCM_VAPID_PUBLIC_KEY in text,
    "No obvious FCM placeholder": not any(s in text for s in ["PASTE_YOUR_PUBLIC_KEY_HERE", "REPLACE_WITH_FIREBASE_WEB_PUSH_CERTIFICATE_KEY_PAIR_PUBLIC_KEY"]),
    "V6 CSS marker present": css_marker_start in text,
    "V6 JS marker present": js_marker_start in text,
    "RainViewer maxNativeZoom present": "maxNativeZoom: 7" in text,
}
failed = False
for name, ok in checks.items():
    print(f"  {'✓' if ok else '✗'} {name}")
    failed = failed or not ok
if failed:
    print("\nOne or more checks failed. Run: grep -n \"FCM_VAPID_PUBLIC_KEY\\|TJF V6\\|maxNativeZoom\" index.html")
    sys.exit(1)
print("\nNext: git diff -- index.html, then git add/commit/push.")
