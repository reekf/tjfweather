const { onSchedule } = require('firebase-functions/v2/scheduler');
const { logger } = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();
const db = admin.firestore();

// Match the appId used in index.html.
const APP_ID = process.env.TJF_APP_ID || 'tjf-weather-app';
const SITE_URL = (process.env.TJF_SITE_URL || 'https://tjfweather.com').replace(/\/$/, '');
const NWS_USER_AGENT = process.env.NWS_USER_AGENT || 'TJFWeather/1.0 (contact@example.com)';
const DEFAULT_TZ = process.env.TJF_DEFAULT_TZ || 'America/Chicago';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cityKey(city) {
  return `${Number(city.lat).toFixed(3)},${Number(city.lon).toFixed(3)}`;
}

function alertId(alert) {
  const p = alert?.properties || {};
  return String(
    alert?.id ||
    p.id ||
    [p.event, p.sent, p.effective, p.expires, p.areaDesc].filter(Boolean).join('|') ||
    Math.random()
  );
}

function alertEvent(alert) {
  return String(alert?.properties?.event || 'Weather Alert');
}

function cleanOneLine(value, maxLen = 220) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, maxLen);
}

function alertBody(alert) {
  const p = alert.properties || {};
  const headline = cleanOneLine(p.headline || p.description || p.instruction || '', 300);
  const area = p.areaDesc ? `Area: ${String(p.areaDesc).split(';').slice(0, 2).join(', ')}` : '';
  const expires = p.expires ? `Expires: ${new Date(p.expires).toLocaleString('en-US', { timeZone: DEFAULT_TZ })}` : '';
  return [headline, area, expires].filter(Boolean).join('\n').slice(0, 900);
}

async function fetchJson(url, purpose) {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, {
        headers: {
          'User-Agent': NWS_USER_AGENT,
          'Accept': 'application/geo+json'
        }
      });
      if (!res.ok) throw new Error(`${purpose || 'fetch'} returned HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      logger.warn(`${purpose || 'fetch'} failed on attempt ${attempt}: ${url}`, err);
      await sleep(500 * attempt);
    }
  }
  return null;
}

async function fetchPointAlerts(city) {
  const lat = Number(city.lat).toFixed(4);
  const lon = Number(city.lon).toFixed(4);
  const url = `https://api.weather.gov/alerts/active?status=actual&point=${lat},${lon}`;
  const json = await fetchJson(url, `NWS alert fetch for ${city.display || city.id || lat + ',' + lon}`);
  return Array.isArray(json?.features) ? json.features : [];
}

function shouldSendAlert(alert) {
  const p = alert.properties || {};
  if (p.status && p.status !== 'Actual') return false;
  if (p.messageType && !['Alert', 'Update'].includes(p.messageType)) return false;
  if (p.expires && new Date(p.expires).getTime() < Date.now()) return false;
  return true;
}

function urgentAlert(event) {
  return /warning|tornado|severe thunderstorm|flash flood|emergency/i.test(event || '');
}

async function sendWebPush(token, payload) {
  const url = payload.url || `${SITE_URL}/#dashboard`;
  const message = {
    token,
    data: {
      title: payload.title,
      body: payload.body || '',
      url,
      tag: payload.tag || 'tjfweather-notification',
      icon: `${SITE_URL}/icon-192.png`,
      badge: `${SITE_URL}/icon-192.png`,
      requireInteraction: payload.requireInteraction ? 'true' : 'false',
      kind: payload.kind || 'generic'
    },
    webpush: {
      headers: {
        Urgency: payload.urgency || 'normal',
        TTL: String(payload.ttl || 3600)
      },
      fcmOptions: { link: url },
      notification: {
        title: payload.title,
        body: payload.body || '',
        icon: `${SITE_URL}/icon-192.png`,
        badge: `${SITE_URL}/icon-192.png`,
        tag: payload.tag || 'tjfweather-notification',
        requireInteraction: !!payload.requireInteraction
      }
    }
  };

  return admin.messaging().send(message);
}

async function markInvalidToken(tokenDoc, err) {
  const code = err?.errorInfo?.code || err?.code || '';
  if (code.includes('registration-token-not-registered') || code.includes('invalid-registration-token')) {
    await tokenDoc.ref.delete();
    return true;
  }
  return false;
}

exports.checkPinnedCityAlerts = onSchedule({
  schedule: 'every 5 minutes',
  timeZone: DEFAULT_TZ,
  region: 'us-central1',
  timeoutSeconds: 540,
  memory: '512MiB'
}, async () => {
  const tokenSnap = await db.collectionGroup('pushTokens').where('alertsEnabled', '==', true).get();
  if (tokenSnap.empty) {
    logger.info('No enabled alert push tokens.');
    return;
  }

  const alertCache = new Map();
  let sentCount = 0;
  let deletedTokens = 0;
  let checkedTokens = 0;

  for (const tokenDoc of tokenSnap.docs) {
    checkedTokens += 1;
    const tokenData = tokenDoc.data() || {};
    const token = tokenData.token;
    if (!token) continue;

    const userRef = tokenDoc.ref.parent.parent;
    if (!userRef) continue;

    const prefSnap = await userRef.collection('preferences').doc('weather').get();
    const pinnedCities = prefSnap.exists ? (prefSnap.data().cities || []) : [];
    const cities = Array.isArray(pinnedCities) ? [...pinnedCities] : [];

    // Optional fallback: if a user has no pinned cities yet, use the device's saved current location.
    if (cities.length === 0 && tokenData.homeLocation && typeof tokenData.homeLocation.lat === 'number') {
      cities.push({
        id: 'current_location',
        display: tokenData.homeLocation.display || 'Current Location',
        lat: tokenData.homeLocation.lat,
        lon: tokenData.homeLocation.lon
      });
    }

    if (cities.length === 0) {
      await tokenDoc.ref.set({
        lastAlertCheckAt: admin.firestore.FieldValue.serverTimestamp(),
        lastAlertCheckStatus: 'No pinned cities or saved current location.'
      }, { merge: true });
      continue;
    }

    const seen = Array.isArray(tokenData.seenAlertIds) ? [...tokenData.seenAlertIds] : [];
    const seenSet = new Set(seen);
    const newlySeen = [];

    for (const city of cities) {
      if (typeof city.lat !== 'number' || typeof city.lon !== 'number') continue;
      const key = cityKey(city);
      if (!alertCache.has(key)) {
        alertCache.set(key, await fetchPointAlerts(city));
        await sleep(150);
      }

      const alerts = alertCache.get(key).filter(shouldSendAlert);
      for (const alert of alerts) {
        const id = alertId(alert);
        if (seenSet.has(id)) continue;

        const event = alertEvent(alert);
        const isUrgent = urgentAlert(event);
        const title = `⚠️ ${event} — ${city.display || city.id || 'Pinned City'}`;
        const url = `${SITE_URL}/#localforecast?city=${encodeURIComponent(city.id || city.display || '')}`;

        try {
          await sendWebPush(token, {
            title,
            body: alertBody(alert),
            url,
            tag: `alert-${id}`,
            requireInteraction: isUrgent,
            urgency: isUrgent ? 'high' : 'normal',
            ttl: 3600,
            kind: 'alert'
          });
          sentCount += 1;
          seenSet.add(id);
          newlySeen.push(id);
        } catch (err) {
          if (await markInvalidToken(tokenDoc, err)) {
            deletedTokens += 1;
            break;
          }
          logger.error(`Failed to send alert ${id}`, err);
        }
      }
    }

    const update = {
      lastAlertCheckAt: admin.firestore.FieldValue.serverTimestamp(),
      lastAlertCheckStatus: `Checked ${cities.length} location(s). Sent ${newlySeen.length}.`,
      lastAlertCheckCityCount: cities.length
    };
    if (newlySeen.length > 0) {
      update.seenAlertIds = Array.from(seenSet).slice(-200);
      update.lastAlertSentAt = admin.firestore.FieldValue.serverTimestamp();
    }
    await tokenDoc.ref.set(update, { merge: true });
  }

  logger.info(`Background alert check complete. Checked ${checkedTokens}; sent ${sentCount}; deleted ${deletedTokens} invalid token(s).`);
});

function localParts(date, timeZone) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  }).formatToParts(date).reduce((acc, p) => {
    if (p.type !== 'literal') acc[p.type] = p.value;
    return acc;
  }, {});

  return {
    dateKey: `${parts.year}-${parts.month}-${parts.day}`,
    minuteOfDay: Number(parts.hour) * 60 + Number(parts.minute),
    hhmm: `${parts.hour}:${parts.minute}`
  };
}

function parseHHMM(value, fallback) {
  const raw = String(value || fallback || '07:00');
  const match = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return parseHHMM(fallback || '07:00', '07:00');
  const hour = Math.max(0, Math.min(23, Number(match[1])));
  const minute = Math.max(0, Math.min(59, Number(match[2])));
  return hour * 60 + minute;
}

function isDue(nowMinute, targetMinute) {
  // The scheduler runs every 5 minutes, but it may not execute exactly on the minute.
  // Send once when the function runs at or within 6 minutes after the requested time.
  const diff = nowMinute - targetMinute;
  return diff >= 0 && diff < 6;
}

async function fetchForecastForLocation(location) {
  const lat = Number(location.lat).toFixed(4);
  const lon = Number(location.lon).toFixed(4);
  const point = await fetchJson(`https://api.weather.gov/points/${lat},${lon}`, `NWS point fetch for ${lat},${lon}`);
  const forecastUrl = point?.properties?.forecast;
  if (!forecastUrl) throw new Error('NWS point response did not include a forecast URL.');
  const forecast = await fetchJson(forecastUrl, `NWS daily forecast fetch for ${lat},${lon}`);
  const periods = forecast?.properties?.periods || [];
  if (!periods.length) throw new Error('NWS forecast response had no periods.');
  return { point, periods };
}

function briefLocation(tokenData) {
  const loc = tokenData.homeLocation || {};
  if (typeof loc.lat === 'number' && typeof loc.lon === 'number') return loc;
  return { lat: 42.03, lon: -93.62, display: 'Ames, IA' };
}

async function buildMorningBrief(tokenData) {
  const loc = briefLocation(tokenData);
  const { periods } = await fetchForecastForLocation(loc);
  const today = periods.find((p) => p.isDaytime) || periods[0];
  const cityName = loc.display || 'Current Location';
  return {
    title: `Morning Forecast: ${cityName}`,
    body: `${today.name}: ${cleanOneLine(today.shortForecast || today.detailedForecast, 220)}, High ${today.temperature}°F.`,
    url: `${SITE_URL}/#dashboard`,
    tag: `brief-morning-${tokenData.uid || 'device'}`,
    kind: 'brief'
  };
}

async function buildEveningBrief(tokenData) {
  const loc = briefLocation(tokenData);
  const { periods } = await fetchForecastForLocation(loc);
  const tonight = periods.find((p) => !p.isDaytime) || periods[0];
  const tomorrow = periods.find((p, idx) => idx > periods.indexOf(tonight) && p.isDaytime) || periods.find((p) => p.isDaytime) || periods[0];
  const cityName = loc.display || 'Current Location';
  return {
    title: `Evening Forecast: ${cityName}`,
    body: `${tonight.name}: ${cleanOneLine(tonight.shortForecast || tonight.detailedForecast, 140)}, Low ${tonight.temperature}°F. ${tomorrow.name}: High ${tomorrow.temperature}°F.`,
    url: `${SITE_URL}/#dashboard`,
    tag: `brief-evening-${tokenData.uid || 'device'}`,
    kind: 'brief'
  };
}

exports.sendDailyBriefings = onSchedule({
  schedule: 'every 5 minutes',
  timeZone: DEFAULT_TZ,
  region: 'us-central1',
  timeoutSeconds: 540,
  memory: '512MiB'
}, async () => {
  const tokenSnap = await db.collectionGroup('pushTokens').where('briefsEnabled', '==', true).get();
  if (tokenSnap.empty) {
    logger.info('No enabled briefing push tokens.');
    return;
  }

  let sentCount = 0;
  let deletedTokens = 0;
  const now = new Date();

  for (const tokenDoc of tokenSnap.docs) {
    const tokenData = tokenDoc.data() || {};
    const token = tokenData.token;
    if (!token) continue;

    const timeZone = tokenData.timezone || DEFAULT_TZ;
    const parts = localParts(now, timeZone);
    const morningTarget = parseHHMM(tokenData.morningTime, '07:00');
    const eveningTarget = parseHHMM(tokenData.eveningTime, '18:00');

    const jobs = [];
    if (isDue(parts.minuteOfDay, morningTarget) && tokenData.lastMorningBriefDate !== parts.dateKey) {
      jobs.push({ type: 'morning', build: buildMorningBrief, dateField: 'lastMorningBriefDate', sentField: 'lastMorningBriefSentAt' });
    }
    if (isDue(parts.minuteOfDay, eveningTarget) && tokenData.lastEveningBriefDate !== parts.dateKey) {
      jobs.push({ type: 'evening', build: buildEveningBrief, dateField: 'lastEveningBriefDate', sentField: 'lastEveningBriefSentAt' });
    }

    if (jobs.length === 0) {
      await tokenDoc.ref.set({
        lastBriefCheckAt: admin.firestore.FieldValue.serverTimestamp(),
        lastBriefCheckLocalTime: parts.hhmm,
        lastBriefCheckStatus: 'Not due.'
      }, { merge: true });
      continue;
    }

    for (const job of jobs) {
      try {
        const payload = await job.build({ ...tokenData, uid: tokenDoc.ref.parent.parent?.id || '' });
        await sendWebPush(token, {
          ...payload,
          urgency: 'normal',
          ttl: 7200,
          requireInteraction: false
        });
        sentCount += 1;
        await tokenDoc.ref.set({
          [job.dateField]: parts.dateKey,
          [job.sentField]: admin.firestore.FieldValue.serverTimestamp(),
          lastBriefCheckAt: admin.firestore.FieldValue.serverTimestamp(),
          lastBriefCheckLocalTime: parts.hhmm,
          lastBriefCheckStatus: `Sent ${job.type} brief.`
        }, { merge: true });
        await sleep(150);
      } catch (err) {
        if (await markInvalidToken(tokenDoc, err)) {
          deletedTokens += 1;
          break;
        }
        logger.error(`Failed to send ${job.type} brief to token ${tokenDoc.id}`, err);
        await tokenDoc.ref.set({
          lastBriefCheckAt: admin.firestore.FieldValue.serverTimestamp(),
          lastBriefCheckLocalTime: parts.hhmm,
          lastBriefCheckStatus: `Failed ${job.type} brief: ${String(err.message || err).slice(0, 180)}`
        }, { merge: true });
      }
    }
  }

  logger.info(`Daily/evening briefing check complete. Sent ${sentCount}; deleted ${deletedTokens} invalid token(s).`);
});
