
/* TJF V7 FCM hard fix: explicit VAPID key + no placeholder branch */
import { initializeApp, getApps, getApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, onAuthStateChanged } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, doc, setDoc, serverTimestamp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";
import { getMessaging, getToken, isSupported, onMessage } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-messaging.js";

const VAPID_KEY = "BAivtsZBFQ-lH4pntR3ooI1SEvuuH25UxK8pCnw10vtnQnzfQNkZxHKcaYkJQgS2RcrGIpcO0ULXiBif3Cem6zg";
window.TJF_FCM_VAPID_PUBLIC_KEY = VAPID_KEY;
window.FCM_VAPID_PUBLIC_KEY = VAPID_KEY;
window.__TJF_FCM_KEY_SOURCE = "TJF V7 hard-coded public VAPID key";

const firebaseConfig = {
  apiKey: "AIzaSyDGujuIMmbcaxTVf2NOHBoR7Swzg2D2VUA",
  authDomain: "tjfwx-2b9b7.firebaseapp.com",
  projectId: "tjfwx-2b9b7",
  storageBucket: "tjfwx-2b9b7.firebasestorage.app",
  messagingSenderId: "665398157561",
  appId: "1:665398157561:web:a019e1b0e17ac4e74b4228",
  measurementId: "G-MLFVBH7KDH"
};

const app = getApps().length ? getApp() : initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const appId = "tjf-weather-app";

function b64url(str) {
  try {
    return btoa(str).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_').slice(0, 120);
  } catch (e) {
    return String(Date.now());
  }
}

async function getReadyServiceWorker() {
  if (!('serviceWorker' in navigator)) throw new Error("Service workers are not supported in this browser.");
  let reg = await navigator.serviceWorker.getRegistration();
  if (!reg) {
    reg = await navigator.serviceWorker.register('./sw.js?v=7', { scope: './' });
  }
  await navigator.serviceWorker.ready;
  return reg;
}

function getHomeLocation() {
  const lat = Number(window.currentHomeLat || 42.03);
  const lon = Number(window.currentHomeLon || -93.62);
  let label = "Ames, IA";
  try {
    label = document.getElementById('homeLocationName')?.innerText?.replace(/^Official National Weather Service Forecast\s*[—-]\s*/i, '').trim() || label;
  } catch(e) {}
  return { lat, lon, label };
}

async function writePushToken(token) {
  const user = auth.currentUser || window._firebaseAuth?.currentUser;
  if (!user || user.isAnonymous) {
    window.showSysMessage?.("Please log in or register before enabling background alerts. The test notification can work without login, but Firebase needs your account to know which pinned cities and briefing times to use.");
    return false;
  }

  const s = window.notifSettings || {};
  const tokenId = b64url(token);
  const ref = doc(db, 'artifacts', appId, 'users', user.uid, 'pushTokens', tokenId);
  await setDoc(ref, {
    token,
    tokenId,
    alertsEnabled: !!s.alertsEnabled,
    briefsEnabled: !!s.briefsEnabled,
    morningTime: s.morningTime || '07:00',
    eveningTime: s.eveningTime || '18:00',
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'America/Chicago',
    homeLocation: getHomeLocation(),
    pinnedCities: Array.isArray(window.pinnedCities) ? window.pinnedCities : [],
    userAgent: navigator.userAgent,
    updatedAt: serverTimestamp(),
    v: "tjf-v7"
  }, { merge: true });
  return true;
}

window.tjfRegisterPushNotifications = async function() {
  try {
    if (!('Notification' in window)) {
      window.showSysMessage?.("This browser does not support notifications.");
      return null;
    }

    const supported = await isSupported().catch(() => false);
    if (!supported) {
      window.showSysMessage?.("Firebase Cloud Messaging is not supported in this browser. On iPhone/iPad, install TJFWeather to the Home Screen first, then open it from the Home Screen icon.");
      return null;
    }

    let permission = Notification.permission;
    if (permission !== 'granted') permission = await Notification.requestPermission();
    if (permission !== 'granted') {
      window.showSysMessage?.("Notification permission was not granted.");
      return null;
    }

    const reg = await getReadyServiceWorker();
    const messaging = getMessaging(app);
    const token = await getToken(messaging, {
      vapidKey: VAPID_KEY,
      serviceWorkerRegistration: reg
    });

    if (!token) {
      window.showSysMessage?.("Firebase did not return a push token. Try closing/reopening the PWA and granting notification permission again.");
      return null;
    }

    await writePushToken(token);
    localStorage.setItem('tjf_fcm_token', token);
    window.showSysMessage?.("Background notifications are enabled for this device.");
    return token;
  } catch (err) {
    console.error("TJF V7 push registration failed", err);
    window.showSysMessage?.("Background notification setup failed: " + (err?.message || err));
    return null;
  }
};

const oldUpdate = window.updateNotifSettings;
window.updateNotifSettings = function() {
  if (typeof oldUpdate === 'function') oldUpdate.apply(this, arguments);
  try {
    window.notifSettings.alertsEnabled = document.getElementById('settingAlerts')?.checked ?? window.notifSettings.alertsEnabled;
    window.notifSettings.briefsEnabled = document.getElementById('settingBriefs')?.checked ?? window.notifSettings.briefsEnabled;
    window.notifSettings.morningTime = document.getElementById('settingMorning')?.value || window.notifSettings.morningTime || '07:00';
    window.notifSettings.eveningTime = document.getElementById('settingEvening')?.value || window.notifSettings.eveningTime || '18:00';
    localStorage.setItem('tjf_notif_settings', JSON.stringify(window.notifSettings));
  } catch(e) {}
  if (Notification.permission === 'granted') window.tjfRegisterPushNotifications();
};

window.requestNotificationPermission = window.tjfRegisterPushNotifications;

try {
  const messaging = getMessaging(app);
  onMessage(messaging, (payload) => {
    const title = payload?.notification?.title || payload?.data?.title || "TJFWeather";
    const body = payload?.notification?.body || payload?.data?.body || "";
    window.fireNotification?.(title, body);
  });
} catch(e) {
  console.warn("TJF V7 foreground FCM listener unavailable", e);
}

onAuthStateChanged(auth, () => {
  if (Notification.permission === 'granted') {
    setTimeout(() => window.tjfRegisterPushNotifications(), 750);
  }
});

console.info("TJF V7 FCM hard fix loaded", VAPID_KEY.slice(0, 12) + "...");
