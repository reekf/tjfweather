
/* TJF V7 HARD FIX runtime overrides */
(function () {
  'use strict';

  const V7 = 'TJF V7 HARD FIX loaded';
  window.TJF_V7_HARD_FIX = true;
  console.info(V7);

  function luminanceTextColor(rgb) {
    const m = String(rgb || '').match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
    if (!m) return '#ffffff';
    const r = +m[1], g = +m[2], b = +m[3];
    const yiq = ((r * 299) + (g * 587) + (b * 114)) / 1000;
    return yiq >= 150 ? '#06182c' : '#ffffff';
  }

  function isTransparentColor(c) {
    if (!c) return true;
    c = String(c).trim().toLowerCase();
    return c === 'transparent' || c === 'rgba(0, 0, 0, 0)' || c === 'rgba(0,0,0,0)';
  }

  function hardFillSPC(root) {
    const roots = root ? [root] : [
      document.getElementById('spcTodayHome'),
      document.getElementById('spcTomHome'),
      document.getElementById('lf-spc-container'),
      document.getElementById('lfSpcToday'),
      document.getElementById('lfSpcTom')
    ].filter(Boolean);

    roots.forEach(container => {
      container.querySelectorAll('div').forEach(el => {
        const txt = (el.textContent || '').trim();
        const hasRiskWords = /(Marginal|Slight|Enhanced|Moderate|High|Thunderstorm|No Severe)/i.test(txt);
        const hasHazards = /(Tornado|Wind|Hail)/i.test(txt);
        if (!hasRiskWords && !hasHazards) return;

        const hazardTitle = Array.from(el.children || []).find(ch => /^(Tornado|Wind|Hail)$/i.test((ch.textContent || '').trim()));
        if (hazardTitle) {
          const badge = Array.from(el.querySelectorAll('div')).find(ch => {
            const t = (ch.textContent || '').trim();
            const bg = getComputedStyle(ch).backgroundColor;
            return (/^(\d+%?|SIG)$/i.test(t) || /%/.test(t)) && !isTransparentColor(bg);
          });
          let fill = badge ? getComputedStyle(badge).backgroundColor : getComputedStyle(el).borderTopColor;
          if (isTransparentColor(fill) || fill === 'rgba(255, 255, 255, 0.1)') fill = '#2c5e92';
          el.classList.add('tjf-spc-card-filled');
          el.style.setProperty('background', fill, 'important');
          el.style.setProperty('color', luminanceTextColor(fill), 'important');
          el.querySelectorAll('*').forEach(child => {
            child.style.setProperty('color', luminanceTextColor(fill), 'important');
          });
        }

        if (hasRiskWords && !hasHazards && el.children && el.children.length >= 2) {
          const colored = Array.from(el.querySelectorAll('div')).find(ch => {
            const bg = getComputedStyle(ch).backgroundColor;
            return !isTransparentColor(bg) && bg !== getComputedStyle(container).backgroundColor;
          });
          if (colored) {
            const fill = getComputedStyle(colored).backgroundColor;
            el.classList.add('tjf-spc-risk-banner');
            el.style.setProperty('background', fill, 'important');
            el.style.setProperty('color', luminanceTextColor(fill), 'important');
            el.style.setProperty('padding', '12px', 'important');
            el.querySelectorAll('*').forEach(child => child.style.setProperty('color', luminanceTextColor(fill), 'important'));
          }
        }

        if (/No Severe Weather Expected/i.test(txt) && el.children.length === 0) {
          el.classList.add('tjf-spc-card-filled');
          el.style.setProperty('background', '#3b4c60', 'important');
          el.style.setProperty('color', '#ffffff', 'important');
          el.style.setProperty('border', '2px solid #ffffff', 'important');
        }
      });
    });
  }

  function classifyDashboardPanels() {
    const wrap = document.querySelector('.home-panels-container');
    if (!wrap) return;
    Array.from(wrap.children).forEach(child => {
      if (!(child instanceof HTMLElement)) return;
      if (child.querySelector('#homeAlertsList')) child.classList.add('tjf-dashboard-alerts');
      if (child.querySelector('#homeLocalWeather')) child.classList.add('tjf-dashboard-forecast');
      if (child.querySelector('#spcTodayHome') || child.querySelector('#spcTomHome')) child.classList.add('tjf-dashboard-spc');
      if (child.querySelector('#miniRadarMap')) child.classList.add('tjf-dashboard-radar');
    });
  }

  function alertBounds(feature) {
    if (!feature || !feature.geometry || !window.L) return null;
    try {
      const layer = L.geoJSON(feature);
      const b = layer.getBounds();
      return b && b.isValid && b.isValid() ? b : null;
    } catch (e) {
      console.warn('Could not compute alert bounds', e);
      return null;
    }
  }

  window.centerRadarOnAlert = function(feature) {
    if (!feature || !window._mapFull || !window.L) return;
    const go = () => {
      const b = alertBounds(feature);
      if (b) {
        window._mapFull.fitBounds(b.pad(0.15), { maxZoom: 8, animate: true, padding: [30, 30] });
      }
      if (window.innerWidth <= 768) {
        const p = document.getElementById('radarAlertsPanel');
        if (p) p.classList.remove('open');
      }
    };
    if (!document.getElementById('radar')?.classList.contains('active')) {
      const nav = document.getElementById('navRadarBtn');
      if (window.showTab) window.showTab('radar', nav || null);
      setTimeout(go, 250);
    } else {
      go();
    }
  };

  function attachAlertClicks() {
    const sortedAlerts = [...(window.nwsAlerts || [])].sort((a, b) => {
      const mapSev = { Extreme: 4, Severe: 3, Moderate: 2, Minor: 1 };
      return (mapSev[b.properties?.severity] || 0) - (mapSev[a.properties?.severity] || 0);
    });

    const radarBadges = document.querySelectorAll('#radarAlertsList .alert-badge');
    radarBadges.forEach((el, idx) => {
      const f = sortedAlerts[idx];
      if (!f || el.dataset.tjfClickBound === '1') return;
      el.dataset.tjfClickBound = '1';
      el.title = 'Click to center the radar map on this warning/watch/advisory.';
      el.addEventListener('click', ev => {
        ev.preventDefault();
        ev.stopPropagation();
        window.centerRadarOnAlert(f);
      });
    });
  }

  function attachLayerClicks(layerGroup) {
    if (!layerGroup || !layerGroup.eachLayer) return;
    layerGroup.eachLayer(layer => {
      if (layer.feature && layer.on && !layer._tjfClickBound) {
        layer._tjfClickBound = true;
        layer.on('click', () => window.centerRadarOnAlert(layer.feature));
      }
      if (layer.eachLayer) attachLayerClicks(layer);
    });
  }

  function patchAlertRenderers() {
    if (window.renderAlertPanels && !window.renderAlertPanels._tjfV7Patched) {
      const oldPanels = window.renderAlertPanels;
      window.renderAlertPanels = function() {
        const out = oldPanels.apply(this, arguments);
        setTimeout(attachAlertClicks, 0);
        return out;
      };
      window.renderAlertPanels._tjfV7Patched = true;
    }

    if (window.renderAlertPolygons && !window.renderAlertPolygons._tjfV7Patched) {
      const oldPolys = window.renderAlertPolygons;
      window.renderAlertPolygons = function() {
        const out = oldPolys.apply(this, arguments);
        setTimeout(() => {
          attachLayerClicks(window.alertPolygonLayer);
          attachLayerClicks(window.alertPolygonLayerMini);
        }, 0);
        return out;
      };
      window.renderAlertPolygons._tjfV7Patched = true;
    }
  }

  function patchRainViewerTiles() {
    if (!window.L || !L.tileLayer || L.tileLayer._tjfV7Patched) return;
    const oldTileLayer = L.tileLayer;
    L.tileLayer = function(url, opts) {
      const finalOpts = Object.assign({}, opts || {});
      if (String(url || '').includes('rainviewer')) {
        finalOpts.maxNativeZoom = 7;
        finalOpts.maxZoom = Math.max(finalOpts.maxZoom || 12, 12);
        finalOpts.updateWhenZooming = false;
        finalOpts.keepBuffer = Math.max(finalOpts.keepBuffer || 8, 8);
      }
      return oldTileLayer.call(this, url, finalOpts);
    };
    Object.assign(L.tileLayer, oldTileLayer);
    L.tileLayer._tjfV7Patched = true;
  }

  function runAll() {
    classifyDashboardPanels();
    hardFillSPC();
    patchAlertRenderers();
    attachAlertClicks();
    attachLayerClicks(window.alertPolygonLayer);
    attachLayerClicks(window.alertPolygonLayerMini);
    patchRainViewerTiles();

    document.querySelectorAll('.donate-btn').forEach(btn => {
      btn.style.setProperty('justify-content', 'center', 'important');
      btn.style.setProperty('text-align', 'center', 'important');
      btn.style.setProperty('align-items', 'center', 'important');
    });
  }

  patchRainViewerTiles();
  patchAlertRenderers();

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', runAll);
  } else {
    runAll();
  }
  window.addEventListener('load', runAll);
  window.addEventListener('hashchange', () => setTimeout(runAll, 150));

  const observer = new MutationObserver(() => {
    clearTimeout(window.__tjfV7MutationTimer);
    window.__tjfV7MutationTimer = setTimeout(runAll, 60);
  });
  observer.observe(document.documentElement, { childList: true, subtree: true });
})();
