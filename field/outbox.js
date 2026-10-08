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

  /** 記録を箱に入れる（保存できたら resolve ＝「保存しました」を出してよい）
   *  opts.url: 送り先（既定は現場の GAS。勤怠・日報は勤務Bot側の GAS） */
  function enqueue(kind, payload, opts) {
    var rec = {
      url: (opts && opts.url) || '',
      after: (opts && opts.after) || '',   // この記録より先に送り終えるべき記録の client_id（例: 退勤 → 出勤）
      client_id: payload.client_id,
      job_client_id: payload.job_client_id || payload.client_id,
      kind: kind,
      seq: kind === 'job_end' ? 2 : 1,
      seq_local: Date.now() * 1000 + (seqCounter++ % 1000),
      created_at: new Date().toISOString(),
      status: 'local',
      attempts: 0,
      fails: 0,          // サーバーが応答したうえでの失敗回数（圏外は数えない）
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
    return fetch(rec.url || cfg.url, {
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

      var byId = {};
      list.forEach(function (r) { byId[r.client_id] = r; });
      var startRec = {};
      list.forEach(function (r) { if (r.kind === 'job_start') startRec[r.job_client_id] = r; });
      // 先に送るべき記録が「要確認」なら、待っている記録も「要確認」に出す（黙って保留し続けない）
      function blockedBy(r) {
        if (r.kind === 'job_end' && startKnown[r.job_client_id] && !sentStart[r.job_client_id]) return startRec[r.job_client_id];
        if (r.after && byId[r.after] && byId[r.after].status !== 'sent') return byId[r.after];
        return null;
      }
      list.forEach(function (r) {
        var b = r.status === 'local' && blockedBy(r);
        if (b && b.status === 'review') { r.status = 'review'; r.last_error = 'waiting: ' + b.kind; put(r); }
      });

      var due = list.filter(function (r) {
        if (r.status !== 'local') return false;
        if (r.next_at && r.next_at > now) return false;
        // 終了は同じ仕事の開始の後、退勤は出勤の後（先の記録が箱に無い＝送信済みで掃除された場合は送る）
        if (blockedBy(r)) return false;
        return true;
      }).sort(function (a, b) { return a.seq_local - b.seq_local; });

      var chain = Promise.resolve();
      due.forEach(function (rec) {
        chain = chain.then(function () {
          // 同じ回の中で先の記録が失敗していたら、今回は送らない（順番の逆転を防ぐ）
          if (blockedBy(rec)) return;
          rec.status = 'sending'; rec.attempts++;
          return put(rec).then(function () { notify(); return postOnce(rec); }).then(function (res) {
            // 現場の GAS は status ok/duplicate、勤務Bot側の GAS は ok:true（重複は status duplicate）
            var okEcho = res && String(res.client_id || '') === String(rec.client_id) &&
                         (res.status === 'ok' || res.status === 'duplicate' || res.ok === true);
            if (okEcho) {
              rec.status = 'sent';
              rec.sent_at = new Date().toISOString();
              rec.server = { jobId: res.jobId || '', status: res.status, warning: res.warning || '' };
              // 写真を消して軽くする（記録のメタ情報だけ残す）
              var p = Object.assign({}, rec.payload); delete p.beforePhotos; delete p.afterPhotos; delete p.photoBase64; rec.payload = p;
              if (rec.kind === 'job_start') sentStart[rec.job_client_id] = true;
            } else if (res && res.retryable === false) {
              rec.status = 'review';
              rec.last_error = (res.error || res.message || 'rejected');
            } else {
              var se = new Error((res && (res.error || res.message)) || 'no echo'); se.server = true;
              throw se;
            }
            return put(rec);
          }).catch(function (err) {
            if (rec.status === 'sent' || rec.status === 'review') return put(rec);
            rec.last_error = String((err && err.message) || err || 'error').slice(0, 200);
            // 圏外・タイムアウト（サーバーの応答なし）は回数に数えない＝電波が戻るまで待ち続ける
            if (err && err.server) rec.fails = (rec.fails || 0) + 1;
            if ((rec.fails || 0) >= MAX_TRIES) {
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

  /** まだ送っていない同種の古い記録を消す（送信中のものは残し、その client_id を返す＝新しい記録はその後に送る） */
  function supersede(kind, match) {
    return getAll().then(function (list) {
      var keep = '';
      return Promise.all(list.filter(function (r) { return r.kind === kind && r.status !== 'sent' && match(r.payload); }).map(function (r) {
        if (r.status === 'sending') { keep = r.client_id; return null; }
        return del(r.client_id);
      })).then(function () { return keep; });
    });
  }

  /** 「要確認」も含めて今すぐ送り直す（手動） */
  function retryAll() {
    return getAll().then(function (list) {
      return Promise.all(list.filter(function (r) { return r.status === 'review' || r.status === 'local'; }).map(function (r) {
        r.status = 'local'; r.next_at = 0; r.attempts = 0; if ((r.fails || 0) >= MAX_TRIES) r.fails = MAX_TRIES - 3;
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
    start: start, enqueue: enqueue, flush: flush, retryAll: retryAll, all: getAll, supersede: supersede,
    kvGet: kvGet, kvSet: kvSet, kvDel: kvDel, uuid: uuid
  };
})(window);
