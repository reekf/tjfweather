// Compatibility entrypoint for browsers/FCM flows that look for the default FCM worker name.
// The real worker is sw.js, which the app registers explicitly.
importScripts('./sw.js');
