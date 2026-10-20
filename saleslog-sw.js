// SAMURAI SCOUT（saleslog-internal.html）の service worker — 2026-10-20 オフライン対策
// アプリの画面・地図のライブラリ・見た地図をスマホに保存し、電波が弱い所でも開きやすくする。
// - 登録は saleslog-internal.html から scope './saleslog-internal.html' で行う（同じ配信リポジトリの他のミニアプリには効かない）
// - 記録の送信（GAS への POST）は触らない。未送信の記録は画面側の「未送信の箱」（localStorage）が持つ
// - Telegram の端末によっては service worker 自体が動かない（iPhone の Telegram 等）。その場合は画面側の対策だけが効く
const CACHE = 'scout-v1';
const TILE_CACHE = 'scout-tiles-v1';
const TILE_MAX = 400; // 地図タイルは見た所だけ・最大 400 枚
const SHELL = ['./saleslog-internal.html', './tg-auth.js'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).catch(() => {}).then(() => self.skipWaiting()));
});
self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then((ks) => Promise.all(
    ks.filter((k) => k.startsWith('scout-') && k !== CACHE && k !== TILE_CACHE).map((k) => caches.delete(k))
  )).then(() => self.clients.claim()));
});

function trimTiles() {
  return caches.open(TILE_CACHE).then((c) => c.keys().then((ks) => {
    if (ks.length <= TILE_MAX) return;
    return Promise.all(ks.slice(0, ks.length - TILE_MAX).map((k) => c.delete(k)));
  }));
}

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return; // GAS への POST などは素通し
  const u = new URL(req.url);

  // 地図タイル: ネット優先 → 圏外なら保存済みのタイル
  if (u.hostname.endsWith('tile.openstreetmap.org') || u.hostname.endsWith('arcgisonline.com')) {
    e.respondWith(fetch(req).then((r) => {
      if (r.ok || r.type === 'opaque') { const copy = r.clone(); caches.open(TILE_CACHE).then((c) => c.put(req, copy)).then(trimTiles); }
      return r;
    }).catch(() => caches.match(req).then((r) => r || Response.error())));
    return;
  }
  // 地図ライブラリ・Telegram の script・フォント: 保存済みを先に使い、裏で取り直す
  if (u.hostname === 'unpkg.com' || u.hostname === 'telegram.org' || u.hostname.endsWith('googleapis.com') || u.hostname.endsWith('gstatic.com')) {
    e.respondWith(caches.match(req).then((hit) => {
      const net = fetch(req).then((r) => { if (r.ok || r.type === 'opaque') { const copy = r.clone(); caches.open(CACHE).then((c) => c.put(req, copy)); } return r; });
      return hit || net;
    }));
    return;
  }
  // 同じサイト（画面本体など）: ネット優先（更新をすぐ反映）→ 圏外なら保存済み
  if (u.origin === location.origin) {
    e.respondWith(fetch(req).then((r) => {
      if (r.ok) { const copy = r.clone(); caches.open(CACHE).then((c) => c.put(req, copy)); }
      return r;
    }).catch(() => caches.match(req, { ignoreSearch: true }).then((r) => r || caches.match('./saleslog-internal.html', { ignoreSearch: true }))));
  }
});
