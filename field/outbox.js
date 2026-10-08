/**
 * outbox.js — 現場アプリ v2 の「送信待ち箱」（2026-10-08 / FieldApp_v2_Production_Plan_v0.2 A-2）
 *
 * 記録はまず端末の IndexedDB に保存し（＝「保存しました」）、電波があるときに順に送る。
 * サーバーが client_id を返したときだけ「送信済み」にする（中身の無い ok は失敗扱い）。
 *
 *   - 送る順番: 同じ仕事（job_client_id）の中だけ「開始 → 終了」を守る。仕事どうしは独立
 *     （1件が失敗し続けても、ほかの仕事の記録は送れる）
 *   - 再送: 5秒 → 15秒 → 1分 → 5分（以後5分おき）。8回失敗したら「要確認」にして本人に見せる
 *   - サーバーが retryable:false を返したら「要確認」（例: 不正なID）
 *   - 送るきっかけ: 起動時 / 電波が戻った時 / 画面に戻った時 / 未送信がある間は1分おき / 手動
 *   - 送信済みの記録は写真を消して軽くし、7日で削除
 */
(function (global) {
  'use strict';

  var DB_NAME = 'sm_field_v1';
  var DB_VER = 1;
  var BACKOFF_MS = [5000, 15000, 60000, 300000];
  var MAX_TRIES = 8;
  var TIMEOUT_MS = 90000;
  var KEEP_SENT_MS = 7 * 24 * 3600 * 1000;

  var dbp = null;
  function db() {
    if (dbp) return dbp;
    dbp = new Promise(function (resolve, reject) {
      var req = indexedDB.open(DB_NAME, DB_VER);
      req.onupgradeneeded = function () {
        var d = req.result;
        if (!d.objectStoreNames.contains('outbox')) d.createObjectStore('outbox', { keyPath: 'client_id' });
        if (!d.objectStoreNames.contains('kv')) d.createObjectStore('kv', { keyPath: 'k' });
      };
      req.onsuccess = function () { resolve(req.result); };
      req.onerror = function () { reject(req.error); };
    });
    return dbp;
  }

  function tx(store, mode, fn) {
    return db().then(function (d) {
      return new Promise(function (resolve, reject) {
        var t = d.transaction(store, mode);
        var s = t.objectStore(store);
        var out = fn(s);
        t.oncomplete = function () { resolve(out && out.result !== undefined ? out.result : out); };
        t.onerror = function () { reject(t.error); };
        t.onabort = function () { reject(t.error); };
      });
    });
  }

  function getAll() {
    return db().then(function (d) {
      return new Promise(function (resolve, reject) {
        var r = d.transaction('outbox', 'readonly').objectStore('outbox').getAll();
        r.onsuccess = function () { resolve(r.result || []); };
        r.onerror = function () { reject(r.error); };
      });
    });
  }
  function put(rec) { return tx('outbox', 'readwrite', function (s) { s.put(rec); }); }
  function del(id) { return tx('outbox', 'readwrite', function (s) { s.delete(id); }); }

  // ── 小さな保存場所（下書き・一覧のキャッシュ等） ──
  function kvGet(k) {
    return db().then(function (d) {
      return new Promise(function (resolve, reject) {
        var r = d.transaction('kv', 'readonly').objectStore('kv').get(k);
        r.onsuccess = function () { resolve(r.result ? r.result.v : undefined); };
        r.onerror = function () { reject(r.error); };
      });
    });
  }
  function kvSet(k, v) { return tx('kv', 'readwrite', function (s) { s.put({ k: k, v: v }); }); }
  function kvDel(k) { return tx('kv', 'readwrite', function (s) { s.delete(k); }); }

  function uuid() {
    if (global.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); (global.crypto || {}).getRandomValues ? crypto.getRandomValues(b) : b.forEach(function (_, i) { b[i] = Math.random() * 256 | 0; });
    b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return ('0' + x.toString(16)).slice(-2); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }

  var seqCounter = 0;
  var cfg = { url: '', onChange: null, appVersion: '' };
  var sending = false;
  var timer = null;

  /** 記録を箱に入れる（保存できたら resolve ＝「保存しました」を出してよい） */
  function enqueue(kind, payload) {
    var rec = {
      client_id: payload.client_id,
      job_client_id: payload.job_client_id || payload.client_id,
      kind: kind,
      seq: kind === 'job_end' ? 2 : 1,
      seq_local: Date.now() * 1000 + (seqCounter++ % 1000),
      created_at: new Date().toISOString(),
      status: 'local',
      attempts: 0,
      next_at: 0,
      last_error: '',
      payload: payload
    };
    return put(rec).then(function () {
      notify();
      setTimeout(flush, 50);
      return rec;
    });
  }

  function counts(list) {
    var c = { local: 0, sending: 0, review: 0, sent: 0 };
    list.forEach(function (r) { c[r.status] = (c[r.status] || 0) + 1; });
    c.pending = c.local + c.sending;
    return c;
  }

  function notify() {
    if (!cfg.onChange) return;
    getAll().then(function (list) { cfg.onChange(counts(list), list); }).catch(function () {});
  }

  function postOnce(rec) {
    var body = Object.assign({}, rec.payload, { action: rec.kind, sent_at: new Date().toISOString(), app_version: cfg.appVersion });
    var ctrl = global.AbortController ? new AbortController() : null;
    var to = setTimeout(function () { if (ctrl) ctrl.abort(); }, TIMEOUT_MS);
    return fetch(cfg.url, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify(body),
      signal: ctrl ? ctrl.signal : undefined
    }).then(function (r) { return r.json(); })
      .then(function (res) { clearTimeout(to); return res; },
            function (err) { clearTimeout(to); throw err; });
  }

  /** 1回分の送信ループ（同時に1本だけ） */
  function flush() {
    if (sending || !cfg.url) return Promise.resolve();
    sending = true;
    return getAll().then(function (list) {
      var now = Date.now();
      // 送信済みの古い記録を掃除
      list.filter(function (r) { return r.status === 'sent' && now - Date.parse(r.sent_at || r.created_at) > KEEP_SENT_MS; })
        .forEach(function (r) { del(r.client_id); });
      // 送信中のまま止まった記録（アプリ強制終了など）は送り直し対象へ
      list.forEach(function (r) { if (r.status === 'sending') r.status = 'local'; });

      var sentStart = {};
      list.forEach(function (r) { if (r.kind === 'job_start' && r.status === 'sent') sentStart[r.job_client_id] = true; });
      var startKnown = {};
      list.forEach(function (r) { if (r.kind === 'job_start') startKnown[r.job_client_id] = true; });

      var due = list.filter(function (r) {
        if (r.status !== 'local') return false;
        if (r.next_at && r.next_at > now) return false;
        // 終了は、同じ仕事の開始が送信済みになってから（開始が箱に無い＝送信済みで掃除された場合は送る）
        if (r.kind === 'job_end' && startKnown[r.job_client_id] && !sentStart[r.job_client_id]) return false;
        return true;
      }).sort(function (a, b) { return a.seq_local - b.seq_local; });

      var chain = Promise.resolve();
      due.forEach(function (rec) {
        chain = chain.then(function () {
          rec.status = 'sending'; rec.attempts++;
          return put(rec).then(function () { notify(); return postOnce(rec); }).then(function (res) {
            var okEcho = res && String(res.client_id || '') === String(rec.client_id) &&
                         (res.status === 'ok' || res.status === 'duplicate');
            if (okEcho) {
              rec.status = 'sent';
              rec.sent_at = new Date().toISOString();
              rec.server = { jobId: res.jobId || '', status: res.status, warning: res.warning || '' };
              // 写真を消して軽くする（記録のメタ情報だけ残す）
              var p = Object.assign({}, rec.payload); delete p.beforePhotos; delete p.afterPhotos; rec.payload = p;
              if (rec.kind === 'job_start') sentStart[rec.job_client_id] = true;
            } else if (res && res.retryable === false) {
              rec.status = 'review';
              rec.last_error = (res.error || res.message || 'rejected');
            } else {
              throw new Error((res && (res.error || res.message)) || 'no echo');
            }
            return put(rec);
          }).catch(function (err) {
            if (rec.status === 'sent' || rec.status === 'review') return put(rec);
            rec.last_error = String((err && err.message) || err || 'error').slice(0, 200);
            if (rec.attempts >= MAX_TRIES) {
              rec.status = 'review';
            } else {
              rec.status = 'local';
              rec.next_at = Date.now() + BACKOFF_MS[Math.min(rec.attempts - 1, BACKOFF_MS.length - 1)];
            }
            return put(rec);
          });
        });
      });
      return chain;
    }).then(function () {
      sending = false; notify(); schedule();
    }, function () {
      sending = false; notify(); schedule();
    });
  }

  function schedule() {
    if (timer) clearTimeout(timer);
    getAll().then(function (list) {
      var pend = list.filter(function (r) { return r.status === 'local'; });
      if (!pend.length) return;
      var now = Date.now();
      var wait = Math.min.apply(null, pend.map(function (r) { return Math.max(1000, (r.next_at || 0) - now); }));
      timer = setTimeout(flush, Math.min(Math.max(wait, 1000), 60000));
    }).catch(function () {});
  }

  /** 「要確認」も含めて今すぐ送り直す（手動） */
  function retryAll() {
    return getAll().then(function (list) {
      return Promise.all(list.filter(function (r) { return r.status === 'review' || r.status === 'local'; }).map(function (r) {
        r.status = 'local'; r.next_at = 0; if (r.attempts >= MAX_TRIES) r.attempts = MAX_TRIES - 3;
        return put(r);
      }));
    }).then(function () { return flush(); });
  }

  function start(options) {
    cfg.url = options.url;
    cfg.onChange = options.onChange || null;
    cfg.appVersion = options.appVersion || '';
    global.addEventListener('online', function () { flush(); });
    document.addEventListener('visibilitychange', function () { if (document.visibilityState === 'visible') flush(); });
    try {
      if (global.Telegram && Telegram.WebApp && Telegram.WebApp.onEvent) Telegram.WebApp.onEvent('activated', function () { flush(); });
    } catch (e) {}
    // 端末の保存を消されにくくする（対応端末のみ）
    try { if (navigator.storage && navigator.storage.persist) navigator.storage.persist(); } catch (e) {}
    notify();
    return flush();
  }

  global.Outbox = {
    start: start, enqueue: enqueue, flush: flush, retryAll: retryAll, all: getAll,
    kvGet: kvGet, kvSet: kvSet, kvDel: kvDel, uuid: uuid
  };
})(window);
