/* TJF V8 EMERGENCY FIX
   This file intentionally patches runtime paths that were being overridden by inline JS/styles. */
(function () {
  'use strict';

  const VAPID_KEY = 'BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg';
  window.__TJF_V8_PATCH_MARKER = 'TJF V8 EMERGENCY FIX';
  window.__TJF_V7_PATCH_MARKER = 'TJF V8 supersedes V7';
  window.TJF_V7_HARD_FIX = true;
  window.TJF_V8_EMERGENCY_FIX = true;
  window.TJF_FCM_VAPID_PUBLIC_KEY = VAPID_KEY;

  console.info('[TJF V8] Emergency runtime patch loaded.');

  function appRootPath() {
    const p = window.location.pathname;
    if (p.endsWith('/')) return p;
    return p.slice(0, p.lastIndexOf('/') + 1) || '/';
  }

  function swUrl(file) {
    return new URL(file, window.location.origin + appRootPath()).toString();
  }

  async function ensureServiceWorkers() {
    if (!('serviceWorker' in navigator)) return null;
    const scope = appRootPath();
    try {
      await navigator.serviceWorker.register(swUrl('sw.js'), { scope, updateViaCache: 'none' });
    } catch (err) {
      console.warn('[TJF V8] sw.js registration failed:', err);
    }
    try {
      const reg = await navigator.serviceWorker.register(swUrl('firebase-messaging-sw.js'), { scope, updateViaCache: 'none' });
      await navigator.serviceWorker.ready;
      if (reg.update) reg.update().catch(() => {});
      return reg;
    } catch (err) {
      console.warn('[TJF V8] firebase-messaging-sw.js registration failed; falling back to sw.js:', err);
      try { return await navigator.serviceWorker.ready; } catch (_) { return null; }
    }
  }

  // Make the old button path robust: set the key globally and avoid old placeholder checks.
  const oldRequestPermission = window.requestNotificationPermission;
  window.requestNotificationPermission = async function tjfV8RequestNotificationPermission() {
    if (!('Notification' in window)) {
      if (window.showSysMessage) window.showSysMessage("Your browser doesn't support notifications.");
      return;
    }

    const isiOS = /iPad|iPhone|iPod/.test(navigator.userAgent) ||
      (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
    const isStandalone = window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;
    if (isiOS && !isStandalone) {
      if (window.showSysMessage) {
        window.showSysMessage('For iPhone/iPad background alerts, install TJFWeather to your Home Screen first, then reopen it and grant permission.');
      }
      return;
    }

    let permission = Notification.permission;
    if (permission !== 'granted') {
      permission = await Notification.requestPermission();
    }
    if (permission !== 'granted') {
      if (window.showSysMessage) window.showSysMessage('Notification permission was not granted.');
      return;
    }

    const reg = await ensureServiceWorkers();
    window.TJF_V8_LAST_SW_REG = !!reg;

    // Let the app's original Firebase enrollment run after the key is definitely populated.
    if (typeof oldRequestPermission === 'function') {
      try {
        await oldRequestPermission.call(window);
      } catch (err) {
        console.warn('[TJF V8] Original permission/enrollment path failed; permission itself is granted.', err);
      }
    }

    if (window.showSysMessage) {
      window.showSysMessage('Notifications are enabled. If this device is logged in and has pinned cities/briefings enabled, background alerts can be sent by Firebase.');
    }
  };

  // Patch RainViewer tiles before initLiveRadar creates them.
  function patchLeafletTileLayer() {
    if (!window.L || !L.tileLayer || L.tileLayer.__tjfV8Patched) return;
    const original = L.tileLayer;
    const patched = function tjfV8TileLayer(url, options) {
      const opts = Object.assign({}, options || {});
      if (typeof url === 'string' && url.includes('rainviewer')) {
        opts.maxNativeZoom = 7;
        opts.maxZoom = Math.max(opts.maxZoom || 12, 12);
        opts.updateWhenZooming = false;
        opts.keepBuffer = Math.max(opts.keepBuffer || 8, 8);
      }
      return original.call(this, url, opts);
    };
    Object.keys(original).forEach(k => { try { patched[k] = original[k]; } catch (_) {} });
    patched.__tjfV8Patched = true;
    L.tileLayer = patched;
  }

  function coordsToLatLngs(coords, out) {
    if (!Array.isArray(coords)) return out;
    if (coords.length >= 2 && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
      out.push([coords[1], coords[0]]);
    } else {
      coords.forEach(c => coordsToLatLngs(c, out));
    }
    return out;
  }

  function boundsForFeature(feature) {
    const pts = coordsToLatLngs(feature && feature.geometry && feature.geometry.coordinates, []);
    if (!pts.length || !window.L) return null;
    return L.latLngBounds(pts);
  }

  function centerOnAlertFeature(feature) {
    const map = window._mapFull;
    if (!map || !feature) return;
    if (window.showTab) {
      try { window.showTab('radar', document.getElementById('navRadarBtn'), true); } catch (_) {}
    }
    setTimeout(() => {
      const b = boundsForFeature(feature);
      if (b && b.isValid && b.isValid()) {
        map.fitBounds(b.pad(0.25), { maxZoom: 8, animate: true });
      }
    }, 120);
  }

  function currentSortedAlerts() {
    const mapSev = { Extreme: 4, Severe: 3, Moderate: 2, Minor: 1 };
    return [...(window.nwsAlerts || [])].sort((a, b) => (mapSev[b.properties && b.properties.severity] || 0) - (mapSev[a.properties && a.properties.severity] || 0));
  }

  function makeAlertCardsClickable() {
    const list = document.getElementById('radarAlertsList');
    if (!list || list.__tjfV8ClickBound) return;
    list.__tjfV8ClickBound = true;
    list.addEventListener('click', function (event) {
      const badge = event.target.closest('.alert-badge');
      if (!badge) return;
      const badges = Array.from(list.querySelectorAll('.alert-badge'));
      const idx = badges.indexOf(badge);
      const feature = currentSortedAlerts()[idx];
      if (feature) centerOnAlertFeature(feature);
    });
  }

  function attachPolygonClicks(layer) {
    if (!layer || !layer.eachLayer) return;
    layer.eachLayer(function (child) {
      if (child.feature && !child.__tjfV8ClickBound) {
        child.__tjfV8ClickBound = true;
        child.on('click', function () { centerOnAlertFeature(child.feature); });
        try { child.setStyle && child.setStyle({ interactive: true }); } catch (_) {}
      }
      attachPolygonClicks(child);
    });
  }

  function patchAlertPolygonRenderer() {
    if (typeof window.renderAlertPolygons !== 'function' || window.renderAlertPolygons.__tjfV8Patched) return;
    const original = window.renderAlertPolygons;
    window.renderAlertPolygons = function tjfV8RenderAlertPolygons() {
      const result = original.apply(this, arguments);
      setTimeout(() => {
        attachPolygonClicks(window.alertPolygonLayer);
        attachPolygonClicks(window.alertPolygonLayerMini);
        makeAlertCardsClickable();
      }, 50);
      return result;
    };
    window.renderAlertPolygons.__tjfV8Patched = true;
  }

  function riskColorFromText(text) {
    const t = (text || '').toUpperCase();
    if (t.includes('HIGH RISK')) return ['#ff40ff', '#ffffff'];
    if (t.includes('MODERATE RISK')) return ['#d93737', '#ffffff'];
    if (t.includes('ENHANCED RISK')) return ['#ec7f28', '#000000'];
    if (t.includes('SLIGHT RISK')) return ['#f0d530', '#000000'];
    if (t.includes('MARGINAL RISK')) return ['#168a42', '#ffffff'];
    if (t.includes('GENERAL THUNDER')) return ['#8bc34a', '#000000'];
    return null;
  }

  function probColor(num, type) {
    if (type === 'tor') {
      if (num >= 60) return ['#104e8b', '#fff'];
      if (num >= 45) return ['#912cee', '#fff'];
      if (num >= 30) return ['#ff00ff', '#fff'];
      if (num >= 15) return ['#ff0000', '#fff'];
      if (num >= 10) return ['#ffc800', '#000'];
      if (num >= 5) return ['#8b4726', '#fff'];
      if (num >= 2) return ['#008b00', '#fff'];
    } else {
      if (num >= 60) return ['#912cee', '#fff'];
      if (num >= 45) return ['#ff00ff', '#fff'];
      if (num >= 30) return ['#ff0000', '#fff'];
      if (num >= 15) return ['#ffc800', '#000'];
      if (num >= 5) return ['#8b4726', '#fff'];
    }
    return ['#2a6395', '#fff'];
  }

  function fillSPCBlocks() {
    const roots = ['spcTodayHome', 'spcTomHome', 'lf-spc-container']
      .map(id => document.getElementById(id)).filter(Boolean);
    roots.forEach(root => {
      const divs = Array.from(root.querySelectorAll('div'));
      divs.forEach(el => {
        const txt = (el.innerText || '').trim();
        if (!txt) return;

        const risk = riskColorFromText(txt);
        const looksLikeRiskHeader = risk && (txt.includes('Risk') || txt.includes('Thunderstorm')) && el.children.length <= 4;
        if (looksLikeRiskHeader) {
          el.classList.add('tjf-spc-filled');
          el.style.setProperty('--tjf-spc-bg', risk[0]);
          el.style.setProperty('--tjf-spc-fg', risk[1]);
          el.style.setProperty('--tjf-spc-border', '#ffffff');
        }

        const isTor = /TORNADO/i.test(txt);
        const isWind = /\bWIND\b/i.test(txt);
        const isHail = /\bHAIL\b/i.test(txt);
        if ((isTor || isWind || isHail) && el.querySelector && el.querySelector('div')) {
          const m = txt.match(/(SIG|\d+)\s*%?/i);
          const num = m && m[1].toUpperCase() === 'SIG' ? 30 : (m ? parseInt(m[1], 10) : 0);
          const c = probColor(num, isTor ? 'tor' : 'other');
          el.classList.add('tjf-spc-filled');
          el.style.setProperty('--tjf-spc-bg', c[0]);
          el.style.setProperty('--tjf-spc-fg', c[1]);
          el.style.setProperty('--tjf-spc-border', '#ffffff');
        }
      });
    });
  }

  function fixMobileAlignment() {
    document.querySelectorAll('.donate-btn').forEach(el => {
      el.style.justifyContent = 'center';
      el.style.textAlign = 'center';
      el.style.alignItems = 'center';
    });
  }

  function repeatedRuntimePass() {
    patchLeafletTileLayer();
    patchAlertPolygonRenderer();
    makeAlertCardsClickable();
    fillSPCBlocks();
    fixMobileAlignment();
  }

  document.addEventListener('DOMContentLoaded', () => {
    repeatedRuntimePass();
    ensureServiceWorkers();
    const mo = new MutationObserver(() => repeatedRuntimePass());
    mo.observe(document.body, { childList: true, subtree: true });
    setInterval(repeatedRuntimePass, 2500);
  });

  // Also run early for scripts loaded near end of body.
  repeatedRuntimePass();
})();
