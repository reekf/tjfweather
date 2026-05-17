#!/usr/bin/env python3
from pathlib import Path
import re

ROOT = Path.cwd()
KEY = "BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg"

index_path = ROOT / "index.html"
if not index_path.exists():
    raise SystemExit("index.html not found. Run this from the root of the tjfweather repo.")

index = index_path.read_text(encoding="utf-8")

# Force the public VAPID key into every known/frontend location.
index = re.sub(
    r'const\s+FCM_VAPID_PUBLIC_KEY\s*=\s*["\'][^"\']*["\'];',
    f'const FCM_VAPID_PUBLIC_KEY = "{KEY}";',
    index,
)
index = re.sub(
    r'window\.TJF_FCM_VAPID_PUBLIC_KEY\s*=\s*["\'][^"\']*["\'];',
    f'window.TJF_FCM_VAPID_PUBLIC_KEY = "{KEY}";',
    index,
)
index = re.sub(
    r'window\.FCM_VAPID_PUBLIC_KEY\s*=\s*["\'][^"\']*["\'];',
    f'window.FCM_VAPID_PUBLIC_KEY = "{KEY}";',
    index,
)

marker_script = f'''
<!-- TJF V9 DIRECT RUNTIME FIX -->
<script>
  window.__TJF_V9_INDEX_MARKER = "index patched v9 direct runtime fix";
  window.TJF_FCM_VAPID_PUBLIC_KEY = "{KEY}";
  window.FCM_VAPID_PUBLIC_KEY = "{KEY}";
</script>
<link rel="stylesheet" href="design_overrides/tjf_v9_direct_runtime_fix.css?v=9">
<script src="design_overrides/tjf_v9_direct_runtime_fix.js?v=9"></script>
<!-- /TJF V9 DIRECT RUNTIME FIX -->
'''

# Remove stale v7/v8/v9 runtime insertion blocks so only one hard-fix runtime is active.
index = re.sub(r'\n?<!-- TJF V[789].*?-->(?:.|\n)*?<!-- /TJF V[789].*?-->\n?', '\n', index)
index = re.sub(r'\n?<!-- TJF V9 DIRECT RUNTIME FIX -->(?:.|\n)*?<!-- /TJF V9 DIRECT RUNTIME FIX -->\n?', '\n', index)
if "</body>" not in index:
    raise SystemExit("Could not find </body> in index.html")
index = index.replace("</body>", marker_script + "\n</body>")
index_path.write_text(index, encoding="utf-8")

odir = ROOT / "design_overrides"
odir.mkdir(exist_ok=True)
(odir / "tjf_v9_direct_runtime_fix.css").write_text(r'''/* TJF V9 DIRECT RUNTIME FIX: hard solid-panel/mobile/radar overrides */
:root {
  --bg: #102a4d !important;
  --panel: #173d68 !important;
  --panel2: #1f4f82 !important;
  --panel3: #2a659e !important;
  --text: #f8fbff !important;
  --muted: #c8d9ef !important;
  --accent: #45d7ff !important;
  --border: #75c7ff !important;
  --block-shadow: 6px 6px 0 rgba(3, 12, 28, 0.85) !important;
}
html, body { background-color: var(--bg) !important; color: var(--text) !important; }
.bg-overlay { background: #102a4d !important; backdrop-filter: none !important; -webkit-backdrop-filter: none !important; }
.glass, .alert-badge, .lf-hour-col, .lf-daily-col, #radarAlertsPanel, .radar-controls, #radarAlertsList, #homeLocalWeather, #supportModal .glass, #sysModal .glass, #authModal .glass {
  background: var(--panel) !important; backdrop-filter: none !important; -webkit-backdrop-filter: none !important; border: 2px solid var(--border) !important; border-radius: 10px !important; box-shadow: var(--block-shadow) !important;
}
.glass .glass, .glass .lf-hour-col, .glass .lf-daily-col, .current-detailed-wrapper > .glass, #homeAlertsList > .alert-badge, #radarAlertsList > .alert-badge, #lf-alerts-container .alert-badge { background: var(--panel2) !important; border-color: #9bd9ff !important; }
#fullRadarMap, #miniRadarMap, .map-container, .leaflet-container { background: #112d52 !important; }
.radar-controls { background: #1e5187 !important; color: #fff !important; }
#radarAlertsPanel, #radarAlertsList { background: #163c67 !important; }
.nav-links .donate-btn { justify-content: center !important; text-align: center !important; }
#dashboard > div { padding: 18px !important; padding-bottom: 88px !important; }
.home-panels-container { display: grid !important; grid-template-columns: minmax(310px, 1.15fr) minmax(300px, 0.85fr) !important; grid-auto-flow: dense !important; align-items: start !important; gap: 18px !important; width: 100% !important; }
.home-panels-container > .glass, .home-panels-container > div { min-width: 0 !important; max-width: none !important; width: 100% !important; align-self: start !important; }
.home-alerts-panel { grid-column: 2 !important; grid-row: 1 !important; max-height: 360px !important; overflow-y: auto !important; }
.home-panels-container > .glass:nth-of-type(2) { grid-column: 1 !important; grid-row: 1 !important; }
#homePinnedSection { grid-column: 1 / -1 !important; order: 99 !important; margin-bottom: 0 !important; }
#homePinnedCards { align-items: stretch !important; }
.tjf-warning-clickable { cursor: pointer !important; }
.tjf-warning-clickable:hover { filter: brightness(1.14) !important; }
.tjf-spc-filled-card { box-shadow: 4px 4px 0 rgba(0,0,0,0.65) !important; border-width: 2px !important; }
@media (max-width: 1000px) {
  .nav-links .donate-btn, .nav-links .nav-item { justify-content: center !important; text-align: center !important; align-items: center !important; }
}
@media (max-width: 768px) {
  #dashboard > div { padding: 12px !important; padding-bottom: 120px !important; }
  .home-panels-container { display: flex !important; flex-direction: column !important; gap: 14px !important; }
  .home-alerts-panel { max-height: none !important; }
  #radar .radar-controls, .radar-controls {
    position: fixed !important; left: 50% !important; right: auto !important; bottom: max(10px, env(safe-area-inset-bottom)) !important; transform: translateX(-50%) !important; width: calc(100vw - 18px) !important; max-width: calc(100vw - 18px) !important; max-height: 44vh !important; overflow-y: auto !important; flex-direction: column !important; align-items: stretch !important; gap: 8px !important; padding: 10px !important; z-index: 8500 !important;
  }
  .radar-controls label, .radar-controls > div, .radar-controls button { width: 100% !important; justify-content: center !important; border-left: 0 !important; padding-left: 0 !important; text-align: center !important; }
  #radarAlertsPanel {
    position: fixed !important; top: 0 !important; left: 0 !important; right: auto !important; bottom: 0 !important; width: min(88vw, 350px) !important; max-width: min(88vw, 350px) !important; height: 100vh !important; max-height: 100vh !important; overflow-y: auto !important; transform: translateX(-105%) !important; z-index: 9000 !important; padding: 64px 14px 120px 14px !important; border-radius: 0 10px 10px 0 !important;
  }
  #radarAlertsPanel.open { transform: translateX(0) !important; }
  #mobileSettingsBtn { display: flex; right: 12px !important; top: 78px !important; z-index: 9100 !important; }
}
''', encoding="utf-8")

(odir / "tjf_v9_direct_runtime_fix.js").write_text(r'''/* TJF V9 DIRECT RUNTIME FIX */
(function () {
  'use strict';
  const VAPID_KEY = "BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg";
  const FIREBASE_CONFIG = { apiKey: "AIzaSyDGujuIMmbcaxTVf2NOHBoR7Swzg2D2VUA", authDomain: "tjfwx-2b9b7.firebaseapp.com", projectId: "tjfwx-2b9b7", storageBucket: "tjfwx-2b9b7.firebasestorage.app", messagingSenderId: "665398157561", appId: "1:665398157561:web:a019e1b0e17ac4e74b4228", measurementId: "G-MLFVBH7KDH" };
  window.__TJF_V9_PATCH_MARKER = "TJF V9 DIRECT RUNTIME FIX";
  window.TJF_V9_DIRECT_RUNTIME_FIX = true;
  window.TJF_FCM_VAPID_PUBLIC_KEY = VAPID_KEY;
  window.FCM_VAPID_PUBLIC_KEY = VAPID_KEY;
  console.info("%cTJF V9 direct runtime fix loaded", "background:#45d7ff;color:#061629;padding:2px 6px;font-weight:bold");

  function safeTokenId(token) { return btoa(token).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '').slice(0, 180); }
  function readNotifSettings() {
    let settings = {}; try { settings = JSON.parse(localStorage.getItem('tjf_notif_settings') || '{}') || {}; } catch (e) {}
    const alertsEl = document.getElementById('settingAlerts'), briefsEl = document.getElementById('settingBriefs'), morningEl = document.getElementById('settingMorning'), eveningEl = document.getElementById('settingEvening');
    return { alertsEnabled: alertsEl ? !!alertsEl.checked : !!settings.alertsEnabled, briefsEnabled: briefsEl ? !!briefsEl.checked : !!settings.briefsEnabled, morningTime: (morningEl && morningEl.value) || settings.morningTime || '07:00', eveningTime: (eveningEl && eveningEl.value) || settings.eveningTime || '18:00', timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'America/Chicago', homeLocation: { lat: Number(window.currentHomeLat || 42.03), lon: Number(window.currentHomeLon || -93.62), label: (document.getElementById('homeLocationName')?.innerText || 'Ames, IA').replace(/^Official National Weather Service Forecast\s*[—-]\s*/i, '') } };
  }
  async function getFirebaseModules() {
    const [{ initializeApp, getApps, getApp }, { getMessaging, getToken, isSupported }, { getFirestore, doc, setDoc, serverTimestamp }] = await Promise.all([import('https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js'), import('https://www.gstatic.com/firebasejs/11.6.1/firebase-messaging.js'), import('https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js')]);
    const app = getApps().length ? getApp() : initializeApp(FIREBASE_CONFIG);
    return { app, getMessaging, getToken, isSupported, getFirestore, doc, setDoc, serverTimestamp };
  }
  async function registerPushToken(silent) {
    if (!('Notification' in window)) { if (!silent) window.showSysMessage?.("This browser does not support notifications."); return null; }
    if (!('serviceWorker' in navigator)) { if (!silent) window.showSysMessage?.("This browser does not support service workers, so background notifications cannot work."); return null; }
    const perm = Notification.permission === 'granted' ? 'granted' : await Notification.requestPermission();
    if (perm !== 'granted') { if (!silent) window.showSysMessage?.("Notification permission was not granted."); return null; }
    const auth = window._firebaseAuth, user = auth && auth.currentUser;
    if (!user || user.isAnonymous) { if (!silent) window.showSysMessage?.("Please login or register before enabling background alerts, so TJFWeather can save this device's push token to your account."); return null; }
    const mods = await getFirebaseModules();
    if (!(await mods.isSupported())) { if (!silent) window.showSysMessage?.("Firebase Cloud Messaging is not supported in this browser. On iPhone/iPad, install the site to your Home Screen first."); return null; }
    const reg = await navigator.serviceWorker.register('./sw.js?v=9', { scope: './' });
    await navigator.serviceWorker.ready;
    const messaging = mods.getMessaging(mods.app);
    const token = await mods.getToken(messaging, { vapidKey: VAPID_KEY, serviceWorkerRegistration: reg });
    if (!token) { if (!silent) window.showSysMessage?.("No push token was returned. Try removing/reinstalling the Home Screen app or clearing the old service worker."); return null; }
    const db = mods.getFirestore(mods.app), appId = 'tjf-weather-app';
    const tokenDoc = mods.doc(db, 'artifacts', appId, 'users', user.uid, 'pushTokens', safeTokenId(token));
    await mods.setDoc(tokenDoc, { token, uid: user.uid, platform: navigator.userAgent, updatedAt: mods.serverTimestamp(), createdAt: mods.serverTimestamp(), appId, ...readNotifSettings() }, { merge: true });
    localStorage.setItem('tjf_fcm_token', token); localStorage.setItem('tjf_fcm_token_saved_at', new Date().toISOString());
    if (!silent) window.showSysMessage?.("Background notifications are enabled for this device.");
    return token;
  }
  window.tjfV9RegisterPushToken = registerPushToken;
  window.requestNotificationPermission = function () { return registerPushToken(false); };
  const oldUpdate = window.updateNotifSettings;
  window.updateNotifSettings = function () { try { if (typeof oldUpdate === 'function') oldUpdate.apply(this, arguments); } catch (e) { console.warn(e); } if (Notification?.permission === 'granted' && window._firebaseAuth?.currentUser && !window._firebaseAuth.currentUser.isAnonymous) registerPushToken(true).catch(err => console.warn('TJF v9 silent token sync failed', err)); };

  function patchLeafletTileLayer() { if (!window.L || !L.tileLayer || L.tileLayer.__tjfV9Patched) return; const orig = L.tileLayer; L.tileLayer = function (url, options) { const opts = Object.assign({}, options || {}); if (String(url || '').includes('rainviewer') || String(url || '').includes('tilecache')) { opts.maxNativeZoom = 7; opts.maxZoom = Math.max(Number(opts.maxZoom || 12), 12); opts.updateWhenZooming = false; opts.keepBuffer = Math.max(Number(opts.keepBuffer || 8), 8); } return orig.call(this, url, opts); }; Object.assign(L.tileLayer, orig); L.tileLayer.__tjfV9Patched = true; }
  function boundsForFeature(feature) { try { const layer = L.geoJSON(feature), b = layer.getBounds(); return b && b.isValid && b.isValid() ? b : null; } catch (e) { return null; } }
  function centerOnFeature(feature) { if (!feature || !window.L) return; const b = boundsForFeature(feature); if (!b) return; try { window.showTab?.('radar', document.getElementById('navRadarBtn')); } catch (e) {} setTimeout(() => { if (window._mapFull) { window._mapFull.fitBounds(b.pad(0.22), { animate: true, maxZoom: 8 }); if (window.innerWidth <= 768) document.getElementById('radarAlertsPanel')?.classList.remove('open'); } }, 180); }
  function findFeatureForAlertBadge(el) { const text = (el.innerText || '').toLowerCase(); if (!window.nwsAlerts) return null; return window.nwsAlerts.find(f => { const event = String(f.properties?.event || '').toLowerCase(); const area0 = String(f.properties?.areaDesc || '').split(';')[0].toLowerCase(); return event && text.includes(event) && (!area0 || text.includes(area0.slice(0, Math.min(18, area0.length)))); }) || window.nwsAlerts.find(f => text.includes(String(f.properties?.event || '').toLowerCase())); }
  function wireAlertBadges() { document.querySelectorAll('.alert-badge').forEach(el => { if (el.__tjfV9ClickWired) return; const f = findFeatureForAlertBadge(el); if (!f || !f.geometry) return; el.__tjfV9ClickWired = true; el.classList.add('tjf-warning-clickable'); el.title = 'Click to center this warning on the radar map'; el.addEventListener('click', () => centerOnFeature(f)); }); }
  function wirePolygonLayer(group) { if (!group || !group.eachLayer) return; group.eachLayer(layer => { if (layer.eachLayer) return wirePolygonLayer(layer); if (!layer.feature || layer.__tjfV9ClickWired) return; layer.__tjfV9ClickWired = true; layer.on('click', () => centerOnFeature(layer.feature)); try { layer.setStyle && layer.setStyle({ interactive: true }); } catch (e) {} }); }
  function patchRenderAlertPolygons() { if (!window.renderAlertPolygons || window.renderAlertPolygons.__tjfV9Patched) return; const orig = window.renderAlertPolygons; window.renderAlertPolygons = function () { const out = orig.apply(this, arguments); setTimeout(() => { wirePolygonLayer(window.alertPolygonLayer); wirePolygonLayer(window.alertPolygonLayerMini); wireAlertBadges(); }, 80); return out; }; window.renderAlertPolygons.__tjfV9Patched = true; }
  function readableTextColor(bg) { const m = String(bg || '').match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i); if (!m) return '#fff'; const [r,g,b] = [Number(m[1]), Number(m[2]), Number(m[3])]; return (0.299*r + 0.587*g + 0.114*b) > 150 ? '#061629' : '#fff'; }
  function fillSpcCards() {
    document.querySelectorAll('div').forEach(el => { const txt = (el.innerText || '').trim(); if (!/^(Tornado|Wind|Hail)\s*\n/i.test(txt) || el.__tjfV9SpcFilled) return; const prob = Array.from(el.querySelectorAll('div')).find(d => /(%|SIG)/.test(d.textContent || '') && parseFloat(getComputedStyle(d).fontSize) >= 18); if (!prob) return; const bg = getComputedStyle(prob).backgroundColor; if (!bg || bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent') return; const color = readableTextColor(bg); el.style.setProperty('background', bg, 'important'); el.style.setProperty('border-color', bg, 'important'); el.style.setProperty('color', color, 'important'); el.classList.add('tjf-spc-filled-card'); el.querySelectorAll('*').forEach(kid => kid.style.setProperty('color', color, 'important')); prob.style.setProperty('background', 'rgba(255,255,255,0.22)', 'important'); prob.style.setProperty('border', '2px solid rgba(255,255,255,0.55)', 'important'); el.__tjfV9SpcFilled = true; });
    document.querySelectorAll('div').forEach(el => { if (el.__tjfV9RiskFilled) return; const txt = el.innerText || ''; if (!/(Risk of Severe Weather|General Thunderstorms|No Severe Weather Expected)/i.test(txt)) return; const badge = Array.from(el.querySelectorAll('div')).find(d => { const bg = getComputedStyle(d).backgroundColor; return bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent' && /(\/5|Thunderstorm|Risk|No Severe)/i.test(d.innerText || ''); }); if (!badge) return; const bg = getComputedStyle(badge).backgroundColor, color = readableTextColor(bg); el.style.setProperty('background', bg, 'important'); el.style.setProperty('border', '2px solid rgba(255,255,255,0.65)', 'important'); el.style.setProperty('border-radius', '10px', 'important'); el.style.setProperty('padding', '12px', 'important'); el.style.setProperty('color', color, 'important'); Array.from(el.children).forEach(kid => kid.style.setProperty('color', color, 'important')); el.__tjfV9RiskFilled = true; });
  }
  function solidifyNestedPanels() { const selectors = ['.glass .glass', '.lf-hour-col', '.lf-daily-col', '.alert-badge', '#radarAlertsPanel', '#radarAlertsList', '.radar-controls', '#lf-asos-container', '#lf-nws-container']; document.querySelectorAll(selectors.join(',')).forEach((el, i) => { if (el.classList.contains('alert-badge') || el.__tjfV9Solid) return; el.style.setProperty('backdrop-filter', 'none', 'important'); el.style.setProperty('-webkit-backdrop-filter', 'none', 'important'); if (!/spc/i.test(el.id || '')) el.style.setProperty('background-color', i % 2 ? '#1f4f82' : '#173d68', 'important'); el.style.setProperty('border', '2px solid #75c7ff', 'important'); el.__tjfV9Solid = true; }); }
  function tightenDashboard() { const cont = document.querySelector('.home-panels-container'); if (!cont || cont.__tjfV9Tightened) return; cont.style.setProperty('display', window.innerWidth <= 768 ? 'flex' : 'grid', 'important'); if (window.innerWidth > 768) { cont.style.setProperty('grid-template-columns', 'minmax(310px, 1.15fr) minmax(300px, 0.85fr)', 'important'); cont.style.setProperty('align-items', 'start', 'important'); } else cont.style.setProperty('flex-direction', 'column', 'important'); cont.style.setProperty('gap', '18px', 'important'); cont.__tjfV9Tightened = true; }
  function tick() { patchLeafletTileLayer(); patchRenderAlertPolygons(); wireAlertBadges(); wirePolygonLayer(window.alertPolygonLayer); fillSpcCards(); solidifyNestedPanels(); tightenDashboard(); }
  const observer = new MutationObserver(() => tick());
  function start() { tick(); observer.observe(document.documentElement, { childList: true, subtree: true }); setInterval(tick, 1500); window.addEventListener('resize', () => { const c = document.querySelector('.home-panels-container'); if (c) c.__tjfV9Tightened = false; tightenDashboard(); }); if ('serviceWorker' in navigator) navigator.serviceWorker.register('./sw.js?v=9', { scope: './' }).catch(err => console.warn('TJF v9 sw register failed', err)); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start, { once: true }); else start();
})();
''', encoding="utf-8")

sw_code = r'''/* TJF V9 SAFE SERVICE WORKER: bypass Firebase live channels; handle FCM background pushes */
const TJF_SW_VERSION = 'v9-safe-sw';
const CACHE_NAME = 'tjfweather-static-v9';
const FIREBASE_CONFIG = { apiKey: "AIzaSyDGujuIMmbcaxTVf2NOHBoR7Swzg2D2VUA", authDomain: "tjfwx-2b9b7.firebaseapp.com", projectId: "tjfwx-2b9b7", storageBucket: "tjfwx-2b9b7.firebasestorage.app", messagingSenderId: "665398157561", appId: "1:665398157561:web:a019e1b0e17ac4e74b4228", measurementId: "G-MLFVBH7KDH" };
try {
  importScripts('https://www.gstatic.com/firebasejs/10.14.1/firebase-app-compat.js');
  importScripts('https://www.gstatic.com/firebasejs/10.14.1/firebase-messaging-compat.js');
  firebase.initializeApp(FIREBASE_CONFIG);
  const messaging = firebase.messaging();
  messaging.onBackgroundMessage((payload) => {
    const n = payload.notification || {}, d = payload.data || {};
    self.registration.showNotification(n.title || d.title || 'TJFWeather Alert', { body: n.body || d.body || d.event || 'New weather notification', icon: d.icon || './icon-192.png', badge: d.badge || './icon-192.png', tag: d.tag || d.alertId || n.title || 'tjfweather-alert', renotify: true, vibrate: [200, 100, 200], data: { url: d.url || './#radar', ...d } });
  });
} catch (err) { console.warn('Firebase messaging failed to initialize in service worker', err); }
function shouldBypassFetch(request) {
  try {
    const url = new URL(request.url), host = url.hostname, path = url.pathname;
    if (request.method !== 'GET') return true;
    if (host.includes('firestore.googleapis.com')) return true;
    if (host.includes('firebase') || host.includes('googleapis.com') || host.includes('gstatic.com')) return true;
    if (host.includes('identitytoolkit.googleapis.com') || host.includes('securetoken.googleapis.com')) return true;
    if (host.includes('weather.gov') || host.includes('spc.noaa.gov') || host.includes('aviationweather.gov')) return true;
    if (host.includes('rainviewer.com') || host.includes('tilecache')) return true;
    if (host.includes('openstreetmap.org') || host.includes('tile.openstreetmap.org')) return true;
    if (host.includes('jsdelivr.net') || host.includes('unpkg.com') || host.includes('d3js.org')) return true;
    if (path.includes('/google.firestore.v1.Firestore/Listen/')) return true;
    return false;
  } catch (e) { return true; }
}
self.addEventListener('install', event => { self.skipWaiting(); event.waitUntil(caches.open(CACHE_NAME)); });
self.addEventListener('activate', event => { event.waitUntil((async () => { const keys = await caches.keys(); await Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))); await self.clients.claim(); })()); });
self.addEventListener('fetch', event => {
  if (shouldBypassFetch(event.request)) return;
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname === '/' || url.pathname.endsWith('/index.html') || url.pathname.endsWith('.js') || url.pathname.endsWith('.css') || url.pathname.includes('design_overrides')) {
    event.respondWith(fetch(event.request, { cache: 'no-store' }).catch(() => caches.match(event.request)));
    return;
  }
  event.respondWith((async () => { const cache = await caches.open(CACHE_NAME); const cached = await cache.match(event.request); const fetchPromise = fetch(event.request).then(response => { if (response && response.ok) cache.put(event.request, response.clone()).catch(() => {}); return response; }).catch(() => cached); return cached || fetchPromise; })());
});
self.addEventListener('notificationclick', event => {
  event.notification.close();
  const targetUrl = new URL(event.notification.data?.url || './#radar', self.location.origin).href;
  event.waitUntil((async () => { const clientsArr = await clients.matchAll({ type: 'window', includeUncontrolled: true }); for (const client of clientsArr) { if ('focus' in client) { try { await client.navigate(targetUrl); } catch (e) {} return client.focus(); } } return clients.openWindow(targetUrl); })());
});
'''
(ROOT / "sw.js").write_text(sw_code, encoding="utf-8")
(ROOT / "firebase-messaging-sw.js").write_text(sw_code, encoding="utf-8")

print("Applied TJFWeather v9 direct runtime fix.")
print("Verify with:")
print('  grep -n "TJF_V9\\|tjf_v9_direct_runtime_fix\\|FCM_VAPID_PUBLIC_KEY" index.html')
print('  grep -n "shouldBypassFetch\\|firestore.googleapis.com\\|TJF_SW_VERSION" sw.js')
