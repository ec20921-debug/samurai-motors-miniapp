/**
 * sw.js — 現場アプリ v2 の画面を端末に保存する（2026-10-08 / A-2）
 *
 *   - 画面本体（ナビゲーション）: 通信優先・3秒で諦めて端末の版を出す（壊れた版を出しても戻せる）
 *   - そのほかの画面部品: 端末の版を優先し、裏で更新
 *   - telegram-web-app.js: 端末の版を優先し、裏で更新（圏外でも window.Telegram が立つように）
 *   - GAS（script.google.com / script.googleusercontent.com）への通信は一切触らない（保存しない）
 *   - BUILD は同期時に stamp_build.py が書き換える。版が変わると新しい保存場所に入れ直す
 */
var BUILD = 'field-20261008-202811';
var CACHE = 'sm-field-' + BUILD;
var SHELL = ['./', './index.html', './outbox.js', './manifest.webmanifest', './icon-192.png', './icon-512.png',
             './apple-touch-icon.png', '../tg-auth.js', '../logo.png'];
var TG_SDK = 'https://telegram.org/js/telegram-web-app.js';

self.addEventListener('install', function (e) {
  e.waitUntil(caches.open(CACHE).then(function (c) {
    return Promise.all(SHELL.map(function (u) {
      // GitHub Pages の HTTP キャッシュ（10分）を通さず最新を取る
      return fetch(new Request(u + (u.indexOf('?') < 0 ? '?' : '&') + 'v=' + BUILD, { cache: 'reload' }))
        .then(function (r) { if (r.ok) return c.put(u, r); })
        .catch(function () {});
    })).then(function () {
      return fetch(TG_SDK, { mode: 'no-cors' }).then(function (r) { return c.put(TG_SDK, r); }).catch(function () {});
    });
  }));
});

self.addEventListener('activate', function (e) {
  e.waitUntil(caches.keys().then(function (keys) {
    return Promise.all(keys.filter(function (k) { return k.indexOf('sm-field-') === 0 && k !== CACHE; })
      .map(function (k) { return caches.delete(k); }));
  }).then(function () { return self.clients.claim(); }));
});

// 画面側の「新しい版にする」ボタン
self.addEventListener('message', function (e) {
  if (e.data === 'skipWaiting') self.skipWaiting();
});

function fromCache(req) {
  return caches.open(CACHE).then(function (c) { return c.match(req, { ignoreSearch: true }); });
}

function staleWhileRevalidate(req) {
  return caches.open(CACHE).then(function (c) {
    return c.match(req, { ignoreSearch: true }).then(function (hit) {
      var net = fetch(req).then(function (r) {
        if (r && (r.ok || r.type === 'opaque')) c.put(req, r.clone());
        return r;
      }).catch(function () { return hit; });
      return hit || net;
    });
  });
}

function networkFirst(req) {
  return new Promise(function (resolve) {
    var done = false;
    var t = setTimeout(function () {
      if (done) return;
      fromCache('./index.html').then(function (hit) { if (hit && !done) { done = true; resolve(hit); } });
    }, 3000);
    fetch(req).then(function (r) {
      if (done) return;
      done = true; clearTimeout(t);
      if (r && r.ok) caches.open(CACHE).then(function (c) { c.put('./index.html', r.clone()); });
      resolve(r);
    }).catch(function () {
      if (done) return;
      clearTimeout(t);
      fromCache('./index.html').then(function (hit) { done = true; resolve(hit || new Response('offline', { status: 503 })); });
    });
  });
}

self.addEventListener('fetch', function (e) {
  var req = e.request;
  if (req.method !== 'GET') return;
  var url = new URL(req.url);
  if (/(^|\.)script\.google(usercontent)?\.com$/.test(url.hostname)) return;   // GAS は素通し
  if (req.url.indexOf(TG_SDK) === 0) { e.respondWith(staleWhileRevalidate(req)); return; }
  if (url.origin !== self.location.origin) return;
  if (req.mode === 'navigate') { e.respondWith(networkFirst(req)); return; }
  var scopePath = new URL('./', self.registration.scope).pathname;
  var parentPath = new URL('../', self.registration.scope).pathname;
  var inScope = url.pathname.indexOf(scopePath) === 0 ||
                url.pathname === parentPath + 'tg-auth.js' || url.pathname === parentPath + 'logo.png';
  if (inScope) e.respondWith(staleWhileRevalidate(req));
});
