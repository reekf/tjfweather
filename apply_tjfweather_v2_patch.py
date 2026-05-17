#!/usr/bin/env python3
"""
Apply TJFWeather v2 frontend changes to the current index.html without overwriting
private/local config such as your Firebase VAPID public key.

Run from the root of the GitHub repo:
  python3 apply_tjfweather_v2_patch.py
"""
from __future__ import annotations

from pathlib import Path
import re
import shutil

ROOT = Path.cwd()
INDEX = ROOT / "index.html"
THEME_FILE = ROOT / "design_overrides" / "tjf_pixel_solid_theme.css"

if not INDEX.exists():
    raise SystemExit("index.html not found. Run this from the root of your GitHub repo.")

html = INDEX.read_text(encoding="utf-8")
backup = ROOT / "index.html.before_tjf_v2_patch"
if not backup.exists():
    shutil.copy2(INDEX, backup)

# 1) Font import: keep Pixelify for headers, add VT323 for readable pixel body text.
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
    )

# 2) Solid pixel theme overrides. Insert before </style> so it wins over earlier CSS.
try:
    theme_css = THEME_FILE.read_text(encoding="utf-8")
except FileNotFoundError:
    # Allow running after copying only this script and not the design_overrides folder.
    theme_css = """
/* TJFWeather Pixel Solid Theme v2: midnight-blue solid panels, no glass blur. */
:root { --bg:#050a18!important; --panel:#0d1834!important; --text:#f4f8ff!important; --accent:#35d8ff!important; --border:#2d4b82!important; --danger:#ff4d6d!important; --warning:#ffd166!important; --success:#54e39f!important; --info:#73b7ff!important; }
html,body{font-family:'VT323','Pixelify Sans',monospace!important;font-size:20px!important;background:#050a18!important;background-image:linear-gradient(rgba(53,216,255,.045) 1px,transparent 1px),linear-gradient(90deg,rgba(53,216,255,.045) 1px,transparent 1px),linear-gradient(180deg,#050a18,#071126 46%,#030716)!important;background-size:18px 18px,18px 18px,auto!important;color:var(--text)!important;}
h1,h2,h3,h4,h5,h6,.logo,.glass-btn,.nav-item{font-family:'Pixelify Sans','VT323',monospace!important}.bg-overlay{background:rgba(2,6,18,.18)!important;backdrop-filter:none!important;-webkit-backdrop-filter:none!important}.glass,.glass[style]{background:#0d1834!important;backdrop-filter:none!important;-webkit-backdrop-filter:none!important;border:2px solid #2d4b82!important;border-radius:8px!important;box-shadow:7px 7px 0 rgba(0,0,0,.55)!important}.glass-btn,.glass-btn[style],.nav-item,.nav-item[style]{background:#12306c!important;border:2px solid #35d8ff!important;border-radius:5px!important;box-shadow:5px 5px 0 #02040d!important;text-shadow:2px 2px 0 #000!important}.glass-btn:hover,.nav-item:hover,.nav-item.active{background:#17418d!important;transform:translate(2px,2px)!important}input,textarea,select{background:#050a18!important;color:#f4f8ff!important;border:2px solid #2d4b82!important;border-radius:4px!important}nav{background:#071126!important;backdrop-filter:none!important;-webkit-backdrop-filter:none!important;border-bottom:3px solid #4c78c8!important;box-shadow:0 5px 0 rgba(0,0,0,.5)!important}
""".strip()

marker_start = "/* === TJF PIXEL SOLID THEME OVERRIDES START === */"
marker_end = "/* === TJF PIXEL SOLID THEME OVERRIDES END === */"
theme_block = f"\n        {marker_start}\n{theme_css}\n        {marker_end}\n"
if marker_start in html:
    html = re.sub(
        re.escape(marker_start) + r".*?" + re.escape(marker_end),
        theme_block.strip(),
        html,
        flags=re.S,
    )
else:
    html = html.replace("    </style>", theme_block + "    </style>", 1)

# 3) Make local notification settings sync to Firestore/FCM token docs too.
new_update_notif = """window.updateNotifSettings = function() {
            window.notifSettings.alertsEnabled = !!document.getElementById('settingAlerts')?.checked;
            window.notifSettings.briefsEnabled = !!document.getElementById('settingBriefs')?.checked;
            window.notifSettings.morningTime = document.getElementById('settingMorning')?.value || '07:00';
            window.notifSettings.eveningTime = document.getElementById('settingEvening')?.value || '18:00';
            localStorage.setItem('tjf_notif_settings', JSON.stringify(window.notifSettings));
            if (window.syncPushSettingsToServer) {
                window.syncPushSettingsToServer({ silent: false, requestIfNeeded: true });
            }
        };"""
html = re.sub(
    r"window\.updateNotifSettings\s*=\s*function\(\)\s*\{.*?\n\s*\};",
    new_update_notif,
    html,
    count=1,
    flags=re.S,
)

# 4) Clarify the About text so users know these are true server push notifications.
html = re.sub(
    r'<p style="font-size: 14px; opacity: 0\.8; margin-bottom: 15px;">Configure daily briefings.*?</p>',
    '<p style="font-size: 14px; opacity: 0.8; margin-bottom: 15px;">Configure true background push notifications for pinned-city NWS watches/warnings/advisories and daily forecast briefings. On iPhone/iPad, install TJFWeather to the Home Screen first, then open the installed app, log in, choose your settings, and grant notification permission.</p>',
    html,
    count=1,
    flags=re.S,
)

# 5) Patch Firebase module token saving to include briefing prefs, times, timezone, and current location.
new_save_push_token = r'''async function savePushToken(token, alertsEnabled = null) {
            if (!auth || !auth.currentUser || auth.currentUser.isAnonymous || !token) return;
            const uid = auth.currentUser.uid;
            const tokenRef = doc(db, 'artifacts', appId, 'users', uid, 'pushTokens', tokenDocId(token));
            const settings = window.notifSettings || {};
            const homeLabelEl = document.getElementById('homeLocationName');
            const homeDisplay = (homeLabelEl ? homeLabelEl.innerText : '')
                .replace('Official National Weather Service Forecast — ', '')
                .replace('Official National Weather Service Forecast - ', '')
                .trim() || 'Current Location';
            const homeLat = Number(window.currentHomeLat || 42.03);
            const homeLon = Number(window.currentHomeLon || -93.62);
            const payload = {
                token,
                alertsEnabled: alertsEnabled === null ? !!settings.alertsEnabled : !!alertsEnabled,
                briefsEnabled: !!settings.briefsEnabled,
                morningTime: settings.morningTime || '07:00',
                eveningTime: settings.eveningTime || '18:00',
                timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'America/Chicago',
                homeLocation: { display: homeDisplay, lat: homeLat, lon: homeLon },
                userAgent: navigator.userAgent,
                standalonePWA: window.isStandalonePWA ? window.isStandalonePWA() : false,
                platform: navigator.platform || 'unknown',
                updatedAt: serverTimestamp()
            };
            await setDoc(tokenRef, payload, { merge: true });
            localStorage.setItem('tjf_fcm_token', token);
        }

        window.syncPushSettingsToServer = async function(options = {}) {
            const silent = !!options.silent;
            const requestIfNeeded = !!options.requestIfNeeded;
            if (!auth || !auth.currentUser || auth.currentUser.isAnonymous) {
                if (!silent && (window.notifSettings?.alertsEnabled || window.notifSettings?.briefsEnabled)) {
                    window.openAuthModal('Please log in or register before enabling background alerts or briefings.');
                }
                return false;
            }

            let token = localStorage.getItem('tjf_fcm_token');
            const wantsPush = !!(window.notifSettings?.alertsEnabled || window.notifSettings?.briefsEnabled);
            if (!token) {
                if (requestIfNeeded && wantsPush) {
                    return window.enablePushNotifications({ silent });
                }
                return false;
            }

            try {
                await savePushToken(token, null);
                if (!silent && wantsPush) window.showSysMessage('Background notification settings saved for this device.');
                return true;
            } catch (e) {
                console.error('Failed to sync push settings:', e);
                if (!silent) window.showSysMessage('Could not save background notification settings. Check Firestore rules and sign-in status.');
                return false;
            }
        };
'''
html = re.sub(
    r"async function savePushToken\(token, alertsEnabled = true\) \{.*?\n\s*\}\n\n\s*window\.enableBackgroundAlerts",
    lambda _m: new_save_push_token + "\n        window.enableBackgroundAlerts",
    html,
    count=1,
    flags=re.S,
)

# 6) Replace enableBackgroundAlerts with a more general push enrollment function.
new_enable_block = r'''window.enablePushNotifications = async function(options = {}) {
            const silent = !!options.silent;
            if (!('Notification' in window)) {
                if (!silent) window.showSysMessage("This browser does not support notifications.");
                return false;
            }
            if (!window.isSecureContext && location.hostname !== 'localhost') {
                if (!silent) window.showSysMessage("Background notifications require HTTPS. GitHub Pages is OK when loaded with https://");
                return false;
            }
            if (window.isIOSDevice && window.isIOSDevice() && window.isStandalonePWA && !window.isStandalonePWA()) {
                if (!silent) window.showSysMessage("iPhone/iPad setup:\n\n1) Open this site in Safari.\n2) Tap Share.\n3) Tap Add to Home Screen.\n4) Open TJFWeather from the Home Screen icon.\n5) Log in, choose notification settings, and tap Grant Permission again.");
                return false;
            }
            if (!auth || !auth.currentUser || auth.currentUser.isAnonymous) {
                if (!silent) window.openAuthModal("Please log in or register before enabling background notifications. The server needs your saved cities and device token.");
                return false;
            }
            if (!FCM_VAPID_PUBLIC_KEY || FCM_VAPID_PUBLIC_KEY.startsWith('REPLACE_')) {
                if (!silent) window.showSysMessage("Firebase Cloud Messaging is not fully configured yet. Replace FCM_VAPID_PUBLIC_KEY in index.html with your Firebase Web Push certificate public key.");
                return false;
            }

            const reg = await window.registerServiceWorker();
            if (!reg) {
                if (!silent) window.showSysMessage("The service worker could not be registered. Make sure sw.js is committed next to index.html.");
                return false;
            }

            let permission = Notification.permission;
            if (permission !== 'granted') permission = await Notification.requestPermission();
            if (permission !== 'granted') {
                if (!silent) window.showSysMessage("Notification permission was not granted. You can enable it later in browser/system settings.");
                return false;
            }

            try {
                const token = await getCurrentPushToken();
                if (!token) throw new Error('No FCM token returned.');
                await savePushToken(token, null);
                if (window.loadNotifSettingsUI) window.loadNotifSettingsUI();
                if (!silent) window.showSysMessage("This device is enrolled for TJFWeather background push. The Alerts and Daily Briefing checkboxes control what gets sent.");
                return true;
            } catch (e) {
                console.error('Background notification enrollment failed:', e);
                if (!silent) window.showSysMessage("Background notification setup failed. Check the VAPID key, FCM Registration API, Firestore rules, and service worker file.");
                return false;
            }
        };

        window.enableBackgroundAlerts = async function(options = {}) {
            window.notifSettings.alertsEnabled = true;
            localStorage.setItem('tjf_notif_settings', JSON.stringify(window.notifSettings));
            if (window.loadNotifSettingsUI) window.loadNotifSettingsUI();
            return window.enablePushNotifications(options);
        };
'''
html = re.sub(
    r"window\.enableBackgroundAlerts\s*=\s*async function\(options = \{\}\) \{.*?\n\s*\};\n\n\s*window\.disableBackgroundAlerts",
    lambda _m: new_enable_block + "\n        window.disableBackgroundAlerts",
    html,
    count=1,
    flags=re.S,
)

# 7) Make the Grant Permission button call the general enrollment function, not force only alert enrollment.
html = re.sub(
    r"// The old button now means true Web Push enrollment, not only local Notification permission\.\s*window\.requestNotificationPermission\s*=\s*function\(\)\s*\{\s*return window\.enableBackgroundAlerts\(\{ silent: false \}\);\s*\};",
    lambda _m: "// The Grant Permission button enrolls the device; checkboxes decide alert/briefing types.\n        window.requestNotificationPermission = function() {\n            return window.enablePushNotifications({ silent: false });\n        };",
    html,
    count=1,
    flags=re.S,
)

# 8) Ensure auth-state refresh syncs existing tokens with changed home location/settings.
html = html.replace(
    "setTimeout(() => window.enableBackgroundAlerts({ silent: true }), 500);",
    "setTimeout(() => window.syncPushSettingsToServer({ silent: true, requestIfNeeded: false }), 500);",
)

INDEX.write_text(html, encoding="utf-8")
print("Patched index.html. Backup saved as index.html.before_tjf_v2_patch")
