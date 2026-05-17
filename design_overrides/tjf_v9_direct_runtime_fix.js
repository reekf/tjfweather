/* TJF V9 DIRECT RUNTIME FIX */
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
