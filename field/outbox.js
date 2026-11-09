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
  var CANCEL_WAIT_MS = 95000;   // 取り消しが送信ループの終わりを待つ上限（1件の送信の上限 TIMEOUT_MS より少し長く）

  // 2026-11-09 現場の「⚠ IndexedDB」（Run Kosal・お店の仕事の「正しい・送る」で保存できない）:
  //   ① スマホがアプリを裏に回す・保存場所の更新などで、開いたままの接続が切れることがある → 切れた接続を捨てて開き直す
  //   ② 開くのに一度失敗すると、その失敗を覚えたまま再起動まで全部の保存が失敗していた → 失敗は覚えない
  //   ③ 容量いっぱい（QuotaExceededError）は、送信済みの記録を消して1回だけやり直す
  //   どの保存も put / delete なので、やり直しても記録が二重にはならない
  var dbp = null;
  function resetDb() {
    var p = dbp; dbp = null;
    if (p) p.then(function (d) { try { d.close(); } catch (e) {} }, function () {});
  }
  function db() {
    if (dbp) return dbp;
    var p = dbp = new Promise(function (resolve, reject) {
      var req;
      try { req = indexedDB.open(DB_NAME, DB_VER); } catch (e) { reject(e); return; }
      req.onupgradeneeded = function () {
        var d = req.result;
        if (!d.objectStoreNames.contains('outbox')) d.createObjectStore('outbox', { keyPath: 'client_id' });
        if (!d.objectStoreNames.contains('kv')) d.createObjectStore('kv', { keyPath: 'k' });
      };
      req.onsuccess = function () {
        var d = req.result;
        d.onversionchange = function () { try { d.close(); } catch (e) {} if (dbp === p) dbp = null; };
        d.onclose = function () { if (dbp === p) dbp = null; };   // ブラウザ側が接続を閉じた
        resolve(d);
      };
      req.onerror = function () { reject(req.error); };
    });
    p.catch(function () { if (dbp === p) dbp = null; });   // 次の保存で開き直す
    return p;
  }

  function errName(e) { return (e && (e.name || e.message)) || String(e || 'error'); }
  var lastError = '';

  /** 1回分の処理（読み書き）。fn(store) が返したリクエストの結果を返す */
  function runOnce(store, mode, fn) {
    return db().then(function (d) {
      return new Promise(function (resolve, reject) {
        var t = d.transaction(store, mode);   // 切れた接続ではここで投げる → reject
        var out = fn(t.objectStore(store));
        t.oncomplete = function () { resolve(out && out.result !== undefined ? out.result : undefined); };
        t.onerror = function () { reject(t.error || (out && out.error)); };
        t.onabort = function () { reject(t.error || new Error('AbortError')); };
      });
    });
  }
  function purgeSent() {   // 容量いっぱいの時だけ: 送信済みの記録（写真は既に外してある）を消す
    return runOnce('outbox', 'readonly', function (s) { return s.getAll(); }).then(function (list) {
      var ids = (list || []).filter(function (r) { return r.status === 'sent'; }).map(function (r) { return r.client_id; });
      if (!ids.length) return;
      return runOnce('outbox', 'readwrite', function (s) { ids.forEach(function (id) { s.delete(id); }); });
    }).catch(function () {});
  }
  function run(store, mode, fn) {
    return runOnce(store, mode, fn).catch(function (e1) {
      resetDb();
      var quota = /quota/i.test(errName(e1));
      return (quota && mode === 'readwrite' ? purgeSent() : Promise.resolve()).then(function () {
        return runOnce(store, mode, fn);
      }).catch(function (e2) {
        lastError = errName(e2 || e1);
        try { console.warn('[outbox] IndexedDB', store, mode, e1, e2); } catch (e) {}
        throw e2 || e1;
      });
    });
  }
  function tx(store, mode, fn) { return run(store, mode, fn); }

  function getAll() { return run('outbox', 'readonly', function (s) { return s.getAll(); }).then(function (r) { return r || []; }); }
  function put(rec) { return tx('outbox', 'readwrite', function (s) { s.put(rec); }); }
  function del(id) { return tx('outbox', 'readwrite', function (s) { s.delete(id); }); }

  // ── 小さな保存場所（下書き・一覧のキャッシュ等） ──
  function kvGet(k) { return run('kv', 'readonly', function (s) { return s.get(k); }).then(function (r) { return r ? r.v : undefined; }); }
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
  var paused = false;      // 取り消しで箱の中身を触っている間は送らない（cancelJob）
  var flushing = null;     // 送信ループ1回分の Promise（cancelJob が終わりを待つ）
  // 管理者のテストモード（2026-11-05 Daisuke「おれだけテスト環境あればいい」）: 箱に入れた記録はサーバーへ送らず、
  // その場で「送信済み（テスト）」にする＝お店・お客さん・売上・請求・経費・打刻のどこにも出ない。切り替えは画面（index.html）
  var testMode = false;
  function setTest(on) { testMode = !!on; }

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
    if (testMode) {   // テストモード: 送らない（写真も持たない）
      var tp = Object.assign({}, payload); delete tp.beforePhotos; delete tp.afterPhotos; delete tp.photoBase64;
      rec.payload = tp; rec.status = 'sent'; rec.sent_at = rec.created_at; rec.test = true; rec.server = { test: true };
    }
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
    if (sending || paused || !cfg.url) return Promise.resolve();
    sending = true;
    return (flushing = getAll().then(function (list) {
      var now = Date.now();
      // 送信済みの古い記録を掃除
      list.filter(function (r) { return r.status === 'sent' && now - Date.parse(r.sent_at || r.created_at) > KEEP_SENT_MS; })
        .forEach(function (r) { del(r.client_id); });
      // 送信中のまま止まった記録（アプリ強制終了など）は送り直し対象へ
      list.forEach(function (r) { if (r.status === 'sending') r.status = 'local'; });

      // 取り消した作業（job_cancel が箱にある）の、まだ送っていない開始・終了は送らずに捨てる（cancelJob が途中で落ちた時の続き）
      var cancelled = {};
      list.forEach(function (r) { if (r.kind === 'job_cancel') cancelled[r.job_client_id] = true; });
      list = list.filter(function (r) {
        if ((r.kind === 'job_start' || r.kind === 'job_end') && cancelled[r.job_client_id] && r.status !== 'sent') { del(r.client_id); return false; }
        return true;
      });

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
      sending = false; flushing = null; notify(); schedule();
    }, function () {
      sending = false; flushing = null; notify(); schedule();
    }));
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

  /**
   * 作業の取り消し（2026-11-05「この作業を取り消す」・管理者だけ）
   *   - 開始がサーバーに届いた（かもしれない）時だけ、取り消しの記録（job_cancel）を入れて送る＝作業記録を「中断」に。
   *     届いたかどうかは端末では決めきれない（応答が来る前に切れた等）ので、一度でも送ろうとした開始は「届いたかも」と扱う
   *     （サーバーは行が無ければ印を残して ok。遅れて届いた開始も配信しない・v7/JobCancel.gs）。
   *     opts.started = 画面が開始を箱に入れた印（送信済みの記録は7日で掃除されるため）
   *   - まだ送れていない開始・終了は箱から消す。取り消しの記録を先に入れてから消す（途中で落ちても flush が捨てる）
   *   - 終了の記録がある仕事は取り消さない → { ended: true }
   *   - 送信の途中で記録を消すと、送り終わった時に書き戻される（put）ので、送信ループ1回分が終わるのを待ってから触る。
   *     待つのは CANCEL_WAIT_MS まで。過ぎたら何もせず { busy: true }（あとでもう一度）
   */
  function startMayHaveReached(st, opts) {
    if (!st) return !!(opts && opts.started);
    return st.status === 'sent' || st.attempts > 0 || st.fails > 0 || !!st.last_error;
  }
  function cancelJob(jobCid, payload, opts) {
    if (paused) return Promise.resolve({ busy: true });
    paused = true;
    var wait = !flushing ? Promise.resolve(true) : Promise.race([
      flushing.then(function () { return true; }, function () { return true; }),
      new Promise(function (r) { setTimeout(function () { r(false); }, CANCEL_WAIT_MS); })
    ]);
    return wait.then(function (ok) {
      if (!ok) return { busy: true };
      return getAll().then(function (list) {
        var mine = list.filter(function (r) { return r.job_client_id === jobCid && (r.kind === 'job_start' || r.kind === 'job_end'); });
        if (mine.some(function (r) { return r.kind === 'job_end'; })) return { ended: true, queued: false };
        var reached = startMayHaveReached(mine.filter(function (r) { return r.kind === 'job_start'; })[0], opts);
        return (reached ? enqueue('job_cancel', payload) : Promise.resolve(null)).then(function (rec) {
          return Promise.all(mine.filter(function (r) { return r.status !== 'sent'; }).map(function (r) { return del(r.client_id); }))
            .then(function () { return { ended: false, queued: !!rec }; });
        });
      });
    }).then(function (r) {
      paused = false; flush(); return r;
    }, function (err) {
      paused = false; flush(); throw err;
    });
  }

  /** 「要確認」も含めて今すぐ送り直す（手動） */
  function retryAll() {
    if (paused) return Promise.resolve();   // 取り消しが箱を触っている間は書き戻さない（cancelJob）
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
    start: start, enqueue: enqueue, flush: flush, retryAll: retryAll, all: getAll, supersede: supersede, cancelJob: cancelJob, setTest: setTest,
    kvGet: kvGet, kvSet: kvSet, kvDel: kvDel, uuid: uuid,
    lastError: function () { return lastError; }
  };
})(window);
