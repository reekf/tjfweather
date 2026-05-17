// Compatibility entrypoint for browsers/FCM flows that look for the default FCM worker name.
// The app registers sw.js explicitly; this imports the same clean worker.
importScripts('./sw.js');
