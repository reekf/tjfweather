const { onSchedule } = require('firebase-functions/v2/scheduler');
const { logger } = require('firebase-functions');
const admin = require('firebase-admin');

admin.initializeApp();
const db = admin.firestore();

// Match the appId used in index.html.
const APP_ID = process.env.TJF_APP_ID || 'tjf-weather-app';

// Replace this with your real GitHub Pages URL before deploying functions.
// Example: https://tyreekfrazier.github.io/TJFWeather/
const SITE_URL = (process.env.TJF_SITE_URL || 'https://tjfweather.com/').replace(/\/$/, '');

// NWS asks API clients to send a descriptive User-Agent with contact information.
const NWS_USER_AGENT = process.env.NWS_USER_AGENT || 'TJFWeather/1.0 (tyreekfrazier5@gmail.com)';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cityKey(city) {
  return `${Number(city.lat).toFixed(3)},${Number(city.lon).toFixed(3)}`;
}

function encodeFirestoreId(raw) {
  return Buffer.from(String(raw)).toString('base64url');
}

function alertId(alert) {
  return String(alert?.id || alert?.properties?.id || alert?.properties?.event || Math.random());
}

function alertEvent(alert) {
  return String(alert?.properties?.event || 'Weather Alert');
}

function alertBody(alert) {
  const p = alert.properties || {};
  const headline = p.headline || p.description || '';
  const area = p.areaDesc ? `Area: ${String(p.areaDesc).split(';').slice(0, 2).join(', ')}` : '';
  const expires = p.expires ? `Expires: ${new Date(p.expires).toLocaleString('en-US', { timeZone: 'America/Chicago' })}` : '';
  return [headline, area, expires].filter(Boolean).join('\n').slice(0, 900);
}

async function fetchPointAlerts(city) {
  const lat = Number(city.lat).toFixed(4);
  const lon = Number(city.lon).toFixed(4);
  const url = `https://api.weather.gov/alerts/active?status=actual&point=${lat},${lon}`;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, {
        headers: {
          'User-Agent': NWS_USER_AGENT,
          'Accept': 'application/geo+json'
        }
      });
      if (!res.ok) throw new Error(`NWS ${res.status}`);
      const json = await res.json();
      return Array.isArray(json.features) ? json.features : [];
    } catch (err) {
      logger.warn(`NWS alert fetch failed for ${city.display || city.id || lat + ',' + lon}, attempt ${attempt}`, err);
      await sleep(500 * attempt);
    }
  }
  return [];
}

function shouldSend(alert) {
  const p = alert.properties || {};
  if (p.status && p.status !== 'Actual') return false;
  if (p.messageType && !['Alert', 'Update'].includes(p.messageType)) return false;
  if (p.expires && new Date(p.expires).getTime() < Date.now()) return false;
  return true;
}

async function sendAlert(token, city, alert) {
  const id = alertId(alert);
  const event = alertEvent(alert);
  const title = `⚠️ ${event} — ${city.display || city.id || 'Pinned City'}`;
  const url = `${SITE_URL}/#localforecast?city=${encodeURIComponent(city.id || city.display || '')}`;

  const message = {
    token,
    data: {
      title,
      body: alertBody(alert),
      url,
      tag: id,
      alertId: id,
      city: city.display || city.id || '',
      icon: `${SITE_URL}/icon-192.png`,
      badge: `${SITE_URL}/icon-192.png`,
      requireInteraction: /warning|tornado|severe thunderstorm|flash flood/i.test(event) ? 'true' : 'false'
    },
    webpush: {
      headers: {
        Urgency: /warning|tornado|severe thunderstorm|flash flood/i.test(event) ? 'high' : 'normal',
        TTL: '3600'
      },
      fcmOptions: { link: url }
    }
  };

  return admin.messaging().send(message);
}

exports.checkPinnedCityAlerts = onSchedule({
  schedule: 'every 5 minutes',
  timeZone: 'America/Chicago',
  region: 'us-central1',
  timeoutSeconds: 540,
  memory: '512MiB'
}, async () => {
  const tokenSnap = await db.collectionGroup('pushTokens').where('alertsEnabled', '==', true).get();
  if (tokenSnap.empty) {
    logger.info('No enabled push tokens.');
    return;
  }

  const alertCache = new Map();
  let sentCount = 0;
  let deletedTokens = 0;

  for (const tokenDoc of tokenSnap.docs) {
    const tokenData = tokenDoc.data() || {};
    const token = tokenData.token;
    if (!token) continue;

    const userRef = tokenDoc.ref.parent.parent;
    if (!userRef) continue;

    const prefSnap = await userRef.collection('preferences').doc('weather').get();
    const cities = prefSnap.exists ? (prefSnap.data().cities || []) : [];
    if (!Array.isArray(cities) || cities.length === 0) continue;

    const seen = Array.isArray(tokenData.seenAlertIds) ? [...tokenData.seenAlertIds] : [];
    const seenSet = new Set(seen);
    const newlySeen = [];

    for (const city of cities) {
      if (typeof city.lat !== 'number' || typeof city.lon !== 'number') continue;
      const key = cityKey(city);
      if (!alertCache.has(key)) {
        alertCache.set(key, await fetchPointAlerts(city));
        await sleep(150); // Be kind to api.weather.gov when many cities are pinned.
      }

      const alerts = alertCache.get(key).filter(shouldSend);
      for (const alert of alerts) {
        const id = alertId(alert);
        if (seenSet.has(id)) continue;

        try {
          await sendAlert(token, city, alert);
          sentCount += 1;
          seenSet.add(id);
          newlySeen.push(id);
        } catch (err) {
          const code = err?.errorInfo?.code || err?.code || '';
          if (code.includes('registration-token-not-registered') || code.includes('invalid-registration-token')) {
            await tokenDoc.ref.delete();
            deletedTokens += 1;
            break;
          }
          logger.error(`Failed to send alert ${id}`, err);
        }
      }
    }

    if (newlySeen.length > 0) {
      const nextSeen = Array.from(seenSet).slice(-150);
      await tokenDoc.ref.set({ seenAlertIds: nextSeen, lastAlertSentAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    }
  }

  logger.info(`Background alert check complete. Sent ${sentCount}; deleted ${deletedTokens} invalid token(s).`);
});
