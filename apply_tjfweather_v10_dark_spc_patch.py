#!/usr/bin/env python3
"""
TJFWeather v10: darker pixel panels + clean filled SPC cards.
Run from the root of the TJFWeather repo.
"""
from pathlib import Path
import re
import shutil
import time

ROOT = Path.cwd()
INDEX = ROOT / "index.html"
if not INDEX.exists():
    raise SystemExit("index.html not found. Run this from the root of your TJFWeather repo.")

html = INDEX.read_text(encoding="utf-8")
backup = ROOT / f"index.before-v10-dark-spc.{int(time.time())}.html"
shutil.copy2(INDEX, backup)
print(f"Backed up index.html -> {backup.name}")

# Remove older v10 blocks if re-running.
html = re.sub(r"\n\s*/\* === TJF V10 DARK SPC PATCH START === \*/.*?/\* === TJF V10 DARK SPC PATCH END === \*/\s*\n", "\n", html, flags=re.S)

css = r'''
        /* === TJF V10 DARK SPC PATCH START === */
        :root {
            --tjf-clean-version: clean-good-state-v10-dark-spc;
            --bg: #050d1b !important;
            --panel: #0a1a33 !important;
            --text: #f5fbff !important;
            --accent: #38bdf8 !important;
            --border: #23527f !important;
            --danger: #ff4b5c !important;
            --warning: #f6c343 !important;
            --success: #31d07a !important;
            --info: #65a9ff !important;
            --tjf-panel-0: #050d1b;
            --tjf-panel-1: #0a1a33;
            --tjf-panel-2: #0d2342;
            --tjf-panel-3: #123057;
            --tjf-panel-4: #183e6d;
            --tjf-border-1: #23527f;
            --tjf-border-2: #3476ad;
            --tjf-shadow: #020714;
        }

        html, body {
            background-color: var(--tjf-panel-0) !important;
        }

        .bg-overlay,
        .bg-overlay[style] {
            background: rgba(5, 13, 27, 0.88) !important;
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
        }

        nav,
        nav[style] {
            background: var(--tjf-panel-1) !important;
            border-bottom: 3px solid var(--tjf-border-1) !important;
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
            box-shadow: 0 5px 0 var(--tjf-shadow) !important;
        }

        .glass,
        .glass[style] {
            background: var(--tjf-panel-1) !important;
            border: 3px solid var(--tjf-border-1) !important;
            border-radius: 0 !important;
            box-shadow: 7px 7px 0 var(--tjf-shadow) !important;
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
        }

        .glass .glass,
        .lf-hour-col,
        .lf-daily-col,
        .lf-search-wrap input,
        #radarAlertsList,
        #homeLocalWeather,
        #homeWeatherContent,
        #homeNextTemps > div,
        .current-detailed-wrapper > div,
        .home-panels-container .glass .glass,
        .alert-badge {
            background: var(--tjf-panel-2) !important;
            border: 3px solid var(--tjf-border-2) !important;
            border-radius: 0 !important;
            box-shadow: 5px 5px 0 var(--tjf-shadow) !important;
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
        }

        .lf-hour-col,
        .lf-daily-col {
            background: var(--tjf-panel-2) !important;
        }

        .nav-links,
        .nav-links[style] {
            background: var(--tjf-panel-1) !important;
            border: 3px solid var(--tjf-border-1) !important;
            border-radius: 0 !important;
            box-shadow: 7px 7px 0 var(--tjf-shadow) !important;
        }

        .nav-item,
        .glass-btn,
        .mobile-menu-btn,
        .lf-search-wrap button,
        .spc-outlook-link {
            border-radius: 0 !important;
            box-shadow: 4px 4px 0 var(--tjf-shadow) !important;
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
        }

        .glass-btn,
        .glass-btn[style] {
            background: #0f335d !important;
            color: #f5fbff !important;
            border: 3px solid #4aa3dc !important;
        }

        .glass-btn:hover,
        .spc-outlook-link:hover {
            background: #164979 !important;
            transform: translate(-1px, -1px) !important;
            box-shadow: 5px 5px 0 var(--tjf-shadow) !important;
        }

        .radar-controls,
        .radar-controls[style],
        #radarAlertsPanel,
        #radarAlertsPanel[style] {
            background: var(--tjf-panel-1) !important;
            border: 3px solid var(--tjf-border-1) !important;
            border-radius: 0 !important;
            box-shadow: 7px 7px 0 var(--tjf-shadow) !important;
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
        }

        .map-container,
        .leaflet-container {
            background: #071121 !important;
        }

        /* SPC: direct filled cards, no long wrapper panel around the official link. */
        .spc-risk-card {
            background: var(--spc-risk-fill, #123057) !important;
            color: var(--spc-risk-text, #ffffff) !important;
            border: 3px solid rgba(255,255,255,0.55) !important;
            border-radius: 0 !important;
            box-shadow: 5px 5px 0 var(--tjf-shadow) !important;
            padding: 14px !important;
            margin: 0 0 10px 0 !important;
        }

        .spc-risk-summary,
        .spc-risk-summary[style] {
            background: rgba(0,0,0,0.18) !important;
            color: inherit !important;
            border: 3px solid rgba(255,255,255,0.45) !important;
            border-radius: 0 !important;
            padding: 12px !important;
            box-shadow: none !important;
        }

        .spc-hazard-card,
        .spc-hazard-card[style] {
            background: var(--spc-hazard-fill, #0d2342) !important;
            color: var(--spc-hazard-text, #ffffff) !important;
            border: 3px solid rgba(255,255,255,0.50) !important;
            border-radius: 0 !important;
            box-shadow: 4px 4px 0 rgba(2,7,20,0.70) !important;
        }

        .spc-hazard-chip,
        .spc-hazard-chip[style] {
            background: rgba(0,0,0,0.25) !important;
            color: inherit !important;
            border: 2px solid rgba(255,255,255,0.45) !important;
            border-radius: 0 !important;
        }

        .spc-no-severe-card,
        .spc-no-severe-card[style] {
            background: #0d2342 !important;
            color: #f5fbff !important;
            border: 3px solid #3476ad !important;
            border-radius: 0 !important;
            box-shadow: 4px 4px 0 var(--tjf-shadow) !important;
        }

        .spc-outlook-link,
        .spc-outlook-link[style] {
            display: inline-flex !important;
            width: auto !important;
            max-width: max-content !important;
            margin: 4px 0 0 0 !important;
            padding: 7px 10px !important;
            font-size: 11px !important;
            line-height: 1.1 !important;
            align-items: center !important;
            gap: 5px !important;
            background: #102a4c !important;
            color: #f6c343 !important;
            border: 3px solid #b98716 !important;
            text-decoration: none !important;
            font-family: var(--font-display) !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
        }

        .spc-outlook-row,
        .spc-outlook-row[style] {
            display: block !important;
            background: transparent !important;
            border: 0 !important;
            box-shadow: none !important;
            padding: 0 !important;
            margin: 0 !important;
            width: auto !important;
            min-height: 0 !important;
        }

        /* Override older runtime SPC fill classes if any remain from the clean restore. */
        .spc-category-filled,
        .spc-hazard-filled {
            background: unset !important;
        }

        @media (max-width: 768px) {
            .spc-risk-card { padding: 10px !important; }
            .spc-risk-summary,
            .spc-risk-summary[style] {
                flex-direction: column !important;
                align-items: flex-start !important;
                gap: 6px !important;
            }
        }
        /* === TJF V10 DARK SPC PATCH END === */
'''

# Insert before </style> near end of head.
if "</style>" not in html:
    raise SystemExit("Could not find </style> in index.html")
html = html.replace("    </style>", css + "\n    </style>", 1)

new_func = r'''        function renderSPCHTML(spc) {
            const outlookLink = `<div class="spc-outlook-row"><a href="https://www.spc.noaa.gov" target="_blank" rel="noopener noreferrer" class="spc-outlook-link">View SPC Outlook <i class="ph ph-arrow-up-right"></i></a></div>`;

            if (spc.rawLvl === 0) return `
                <div class="spc-no-severe-card" style="padding: 12px 14px; font-size: 16px; font-weight: 700; margin-bottom: 8px;">No Severe Weather Expected</div>
                ${outlookLink}`;
            
            let riskBadge = spc.rawLvl === 1 ? "Thunderstorm" : `<span style="font-size: 2.5em; font-weight: 700; line-height: 0.8;">${spc.level}</span><span style="font-size: 1.2em; opacity: 0.7; margin-left: 2px;">/5</span>`;
            
            let getRiskStyle = (val, isCig, type) => {
                let b = '#0d2342', t = '#ffffff', c = '#3476ad';
                if (type === 'tor') {
                    if (val >= 60)      { b = '#104e8b'; t = '#ffffff'; c = b; }
                    else if (val >= 45) { b = '#912cee'; t = '#ffffff'; c = b; }
                    else if (val >= 30) { b = '#ff00ff'; t = '#ffffff'; c = b; }
                    else if (val >= 15) { b = '#ff0000'; t = '#ffffff'; c = b; }
                    else if (val >= 10) { b = '#ffc800'; t = '#000000'; c = b; }
                    else if (val >= 5)  { b = '#8b4726'; t = '#ffffff'; c = b; }
                    else if (val >= 2)  { b = '#008b00'; t = '#ffffff'; c = b; }
                } else {
                    if (val >= 60)      { b = '#912cee'; t = '#ffffff'; c = b; }
                    else if (val >= 45) { b = '#ff00ff'; t = '#ffffff'; c = b; }
                    else if (val >= 30) { b = '#ff0000'; t = '#ffffff'; c = b; }
                    else if (val >= 15) { b = '#ffc800'; t = '#000000'; c = b; }
                    else if (val >= 5)  { b = '#8b4726'; t = '#ffffff'; c = b; }
                }
                if (isCig && val === 0) { b = '#b98716'; t = '#000000'; c = b; }
                return { bg: b, text: t, border: c };
            };

            let formatVal = (v) => {
                let s = String(v || '').replace(/\s*\((CIG|SIG).*\)/, '').replace(/%/g, '');
                if (s.includes('CIG') || s.includes('SIG')) return "SIG";
                return s ? s : "0";
            };

            let torVal = formatVal(spc.tor);
            let windVal = formatVal(spc.wind);
            let hailVal = formatVal(spc.hail);

            let torCigMatch = String(spc.tor || '').match(/(CIG\d*(?:\.\d+)?|SIG)/);
            let windCigMatch = String(spc.wind || '').match(/(CIG\d*(?:\.\d+)?|SIG)/);
            let hailCigMatch = String(spc.hail || '').match(/(CIG\d*(?:\.\d+)?|SIG)/);

            let torCig = torCigMatch ? torCigMatch[0].replace('SIG', 'CIG') : null;
            let windCig = windCigMatch ? windCigMatch[0].replace('SIG', 'CIG') : null;
            let hailCig = hailCigMatch ? hailCigMatch[0].replace('SIG', 'CIG') : null;

            let torNum = parseInt(torVal) || 0;
            let windNum = parseInt(windVal) || 0;
            let hailNum = parseInt(hailVal) || 0;

            let torStyle = getRiskStyle(torNum, torCig, 'tor');
            let windStyle = getRiskStyle(windNum, windCig, 'wind');
            let hailStyle = getRiskStyle(hailNum, hailCig, 'hail');

            let displayString = (val) => val === 'SIG' ? 'SIG' : val + '%';

            let getDescText = (type, val, cigTag) => {
                let text = "";
                if (type === 'tor') {
                    if (val >= 15) text = "Numerous tornadoes possible";
                    else if (val >= 5) text = "A few tornadoes possible";
                    else if (val >= 2) text = "Isolated tornado possible";
                    if (cigTag) text += (text ? "<br>" : "") + `<span style="font-weight:700; display:flex; text-align:center; align-items:center; justify-content:center; gap:4px; margin-top:6px;"><i class="ph-fill ph-warning"></i> Significant: Up to EF2+ tornadoes possible</span>`;
                } else if (type === 'wind') {
                    if (val >= 30) text = "Widespread damaging winds possible";
                    else if (val >= 15) text = "Scattered damaging winds possible";
                    else if (val >= 5) text = "Isolated damaging winds possible";
                    if (cigTag) text += (text ? "<br>" : "") + `<span style="font-weight:700; display:flex; text-align:center; align-items:center; justify-content:center; gap:4px; margin-top:6px;"><i class="ph-fill ph-warning"></i> Significant: Up to 75+ mph winds possible</span>`;
                } else if (type === 'hail') {
                    if (val >= 30) text = "Widespread large hail possible";
                    else if (val >= 15) text = "Scattered large hail possible";
                    else if (val >= 5) text = "Isolated large hail possible";
                    if (cigTag) {
                        let match = cigTag.match(/(\d+(?:\.\d+)?)/);
                        let sizeNum = (match && match[1] !== "1") ? match[1] : "2.0";
                        let sizeStr = window.getHailTranslation ? window.getHailTranslation(sizeNum) : sizeNum + '"';
                        let objMatch = sizeStr.match(/\((.*?)\)/);
                        let objName = objMatch ? objMatch[1].toLowerCase() : sizeStr;
                        text += (text ? "<br>" : "") + `<span style="font-weight:700; display:flex; text-align:center; align-items:center; justify-content:center; gap:4px; margin-top:6px;"><i class="ph-fill ph-warning"></i> Significant: Up to ${objName} sized hail possible</span>`;
                    }
                }
                if (!text && !cigTag) text = "<span style='opacity:0.75'>No significant risk</span>";
                return text;
            };

            let torDesc = getDescText('tor', torNum, torCig);
            let windDesc = getDescText('wind', windNum, windCig);
            let hailDesc = getDescText('hail', hailNum, hailCig);

            const hazardCard = (label, val, num, cig, style, desc) => `
                <div class="spc-hazard-card" style="--spc-hazard-fill:${style.bg}; --spc-hazard-text:${style.text}; padding: 16px 10px; text-align: center; display: flex; flex-direction: column; justify-content: flex-start;">
                    <div style="font-size: 12px; text-transform: uppercase; letter-spacing: 1px; opacity: 0.95; margin-bottom: 8px; font-weight: bold; color: inherit;">${label}</div>
                    <div class="spc-hazard-chip" style="font-size: 24px; font-weight: 700; padding: 4px 8px; display: inline-block; margin: 0 auto 8px auto;">${displayString(val)}</div>
                    <div style="font-size: 11px; line-height: 1.4; color: inherit;">${desc}</div>
                </div>`;

            return `
                <div class="spc-risk-card" style="--spc-risk-fill:${spc.bgColor}; --spc-risk-text:${spc.textColor};">
                    <div class="spc-risk-summary" style="display: flex; align-items: center; gap: 12px;">
                        <div style="font-weight: 700; font-size: 15px; display: flex; align-items: baseline; gap: 2px;">${riskBadge}</div>
                        <div style="font-weight: 700; font-size: 16px; color: inherit;">${spc.name}</div>
                    </div>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(92px, 1fr)); gap: 10px; margin-top: 10px;">
                        ${hazardCard('Tornado', torVal, torNum, torCig, torStyle, torDesc)}
                        ${hazardCard('Wind', windVal, windNum, windCig, windStyle, windDesc)}
                        ${hazardCard('Hail', hailVal, hailNum, hailCig, hailStyle, hailDesc)}
                    </div>
                </div>
                ${outlookLink}`;
        }
'''

pattern = r"        function renderSPCHTML\(spc\) \{.*?\n        window\.updateHomeSPC = function"
replacement = new_func + "\n        window.updateHomeSPC = function"
html2, n = re.subn(pattern, lambda m: replacement, html, count=1, flags=re.S)
if n != 1:
    raise SystemExit("Could not replace renderSPCHTML(spc). index.html may have drifted from the expected version.")
html = html2

# Keep a marker that is easy to grep from the live page.
marker = '<script>window.__TJF_V10_DARK_SPC_PATCH = "TJF v10 dark SPC patch";</script>'
html = html.replace(marker, '')
html = html.replace('</body>', f'    {marker}\n</body>')

INDEX.write_text(html, encoding="utf-8")
print("Applied v10 dark SPC patch to index.html")
print("Verify with:")
print(r'  grep -n "TJF_V10\|spc-risk-card\|spc-outlook-link\|clean-good-state-v10-dark-spc" index.html')
