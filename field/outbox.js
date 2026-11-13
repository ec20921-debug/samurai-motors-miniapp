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
 *   - 2026-11-13 写真を先に送る（job_photo）: 作業の写真は撮ったそばから1枚ずつ送り、開始・終了は写真の file_id だけを持つ
 *     軽い記録にする（deps = 先に送り終える写真の一覧）。写真が「要確認」になっても開始・終了は止めない（その写真なしで送る）。
 *     文字だけの知らせ（job_start_notice）はほかの記録より先に送る。小さい送信は keepalive（画面を消しても送り切りやすい）
 */
(function (global) {
  'use strict';

  var DB_NAME = 'sm_field_v1';
  var DB_VER = 1;
  var BACKOFF_MS = [5000, 15000, 60000, 300000];
  var MAX_TRIES = 8;
  var TIMEOUT_MS = 90000;
  var KEEP_SENT_MS = 7 * 24 * 3600 * 1000;
  var KEEPALIVE_MAX = 60000;    // これより小さい本文（バイト数）は keepalive で送る（ブラウザの上限は合計 64KB）
  var DEP_TRIES = 3;            // 写真がこの回数送れなければ、開始・終了はその写真を待たない（[H1] 売上・請求を写真で止めない）
  var JOB_KINDS = { job_start: 1, job_end: 1, job_photo: 1, job_start_notice: 1 };   // 作業にひも付く記録（取り消しで捨てる対象）
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
  // 送信済みと分かった記録（メモリ上）。端末に保存できない時の「その場で直接送る」で、順番を守るために使う（2026-11-09）
  var sentMem = {}, startSentMem = {};
  var dirty = false;      // 送信ループの途中で新しい記録が入った（終わったら待たずにもう一度回す）
  var directBusy = {};    // 送信ループを通さず今すぐ送っている記録（sendNow）。ループ側は触らない
  var fileMem = {};   // 先に送った写真の file_id（メモリ上・端末に保存できない時の直接送信でも開始・終了に付けられるように）
  function markSent(rec) { sentMem[rec.client_id] = true; if (rec.kind === 'job_start') startSentMem[rec.job_client_id] = true; }
  function okEcho(res, rec) {
    // 現場の GAS は status ok/duplicate、勤務Bot側の GAS は ok:true（重複は status duplicate）
    return !!res && String(res.client_id || '') === String(rec.client_id) &&
           (res.status === 'ok' || res.status === 'duplicate' || res.ok === true);
  }
  /** 端末に保存できない時に、箱を通さずその場で送ってよいか（先に送るべき記録が送信済みと分かっている時だけ） */
  function canDirect(rec) {
    if (rec.kind === 'job_cancel') return false;
    if (rec.kind === 'job_end') return !!startSentMem[rec.job_client_id];   // 開始が届いていないのに終了を先に送らない
    if (rec.after) return !!sentMem[rec.after];
    // 先に送る写真の file_id が全部分かっている時だけ（分からないまま送ると、写真なしの開始・終了が「送信済み」になる）
    if ((rec.deps || []).some(function (d) { return !fileMem[d.cid]; })) return false;
    return true;
  }

  /** 記録を箱に入れる（保存できたら resolve ＝「保存しました」を出してよい）
   *  opts.url: 送り先（既定は現場の GAS。勤怠・日報は勤務Bot側の GAS） */
  function enqueue(kind, payload, opts) {
    var rec = {
      url: (opts && opts.url) || '',
      after: (opts && opts.after) || '',   // この記録より先に送り終えるべき記録の client_id（例: 退勤 → 出勤）
      deps: (opts && opts.deps) || [],     // 先に送り終える写真（job_photo）[{slot, cid}]。送る時に file_id を付ける（2026-11-13）
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
      dirty = true;
      notify();
      if (opts && opts.now && !testMode) sendNow(rec); else setTimeout(flush, 50);
      return rec;
    }, function (err) {
      // 2026-11-09 端末に保存できない（つなぎ直してもダメ）時の逃げ道: 電波があれば、その場で直接送る。
      //   届いた（サーバーが client_id を返した）時だけ成功。届かなければ元の保存エラーのまま（記録は画面に残る）
      if (testMode || !canDirect(rec)) throw err;
      return postOnce(rec, (rec.deps || []).length ? { photo_refs: photoRefs(rec, null) } : null).then(function (res) {
        if (!okEcho(res, rec)) throw err;
        if (res.file_id) fileMem[rec.client_id] = res.file_id;
        markSent(rec); rec.status = 'sent'; rec.sent_at = new Date().toISOString(); rec.direct = true;
        try { console.warn('[outbox] saved failed, sent directly', rec.kind, rec.client_id, err); } catch (e) {}
        return rec;
      }, function () { throw err; });
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

  /**
   * 今すぐ送る（opts.now・文字だけの知らせ用）: 送信ループが写真を送っている途中でも、並行して1回だけ送る。
   *   押した直後にスマホをしまっても、出した送信は keepalive で届きやすい。届かなければ箱に残り、ふつうの再送に乗る
   */
  function sendNow(rec) {
    if (paused || (!cfg.url && !rec.url)) { setTimeout(flush, 50); return; }   // 取り消しが箱を触っている間は送らない
    directBusy[rec.client_id] = true;
    rec.status = 'sending'; rec.attempts++;
    put(rec).then(function () { return postOnce(rec); }).then(function (res) {
      if (okEcho(res, rec)) {
        markSent(rec); rec.status = 'sent'; rec.sent_at = new Date().toISOString();
        rec.server = { jobId: res.jobId || '', status: res.status, warning: res.warning || '', file_id: '' };
      } else if (res && res.retryable === false) {
        rec.status = 'review'; rec.last_error = (res.error || res.message || 'rejected');
      } else { rec.status = 'local'; rec.next_at = 0; }
    }, function (err) {
      rec.status = 'local'; rec.next_at = 0; rec.last_error = String((err && err.message) || err || 'error').slice(0, 200);
      if (rec.used_keep && !offline()) rec.no_keep = true;
    }).then(function () { return put(rec); }).catch(function () {}).then(function () {
      delete directBusy[rec.client_id]; notify(); setTimeout(flush, 50);
    });
  }
  /** 先に送った写真の file_id を付ける（届いていない・箱に無い写真は file_id 空＝サーバーが「届いていない写真」と数える） */
  function photoRefs(rec, byId) {
    return (rec.deps || []).map(function (d) {
      var r = byId && byId[d.cid];
      return { slot: d.slot, file_id: (r && r.status === 'sent' && r.server && r.server.file_id) || fileMem[d.cid] || '' };
    });
  }
  function byteLen(s) { try { return new Blob([s]).size; } catch (e) { return s.length * 3; } }   // クメール語・日本語は1文字3バイト
  function offline() { try { return global.navigator && global.navigator.onLine === false; } catch (e) { return false; } }
  function postOnce(rec, extra) {
    var body = Object.assign({}, rec.payload, extra || {}, { action: rec.kind, sent_at: new Date().toISOString(), app_version: cfg.appVersion });
    var text = JSON.stringify(body);
    // 小さい送信（知らせ・写真なしの開始と終了）は keepalive: 画面を消したりアプリを裏に回しても、出した送信は最後まで届きやすい。
    //   keepalive で一度失敗した記録は、次からふつうの送信に戻す（keepalive が使えない環境で止まらないように）
    var keep = !rec.no_keep && rec.kind !== 'job_photo' && byteLen(text) < KEEPALIVE_MAX;
    rec.used_keep = keep;
    var ctrl = global.AbortController ? new AbortController() : null;
    var to = setTimeout(function () { if (ctrl) ctrl.abort(); }, TIMEOUT_MS);
    return fetch(rec.url || cfg.url, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: text,
      keepalive: keep || undefined,
      signal: ctrl ? ctrl.signal : undefined
    }).then(function (r) { return r.json(); })
      .then(function (res) { clearTimeout(to); return res; },
            function (err) { clearTimeout(to); throw err; });
  }

  /** 1回分の送信ループ（同時に1本だけ） */
  function flush() {
    if (sending || paused || !cfg.url) return Promise.resolve();
    sending = true; dirty = false;
    var progress = false;   // この回で1件でも送れた（待っていた記録が送れるようになったかもしれない）
    return (flushing = getAll().then(function (list) {
      var now = Date.now();
      // 送信済みの古い記録を掃除
      list.filter(function (r) { return r.status === 'sent' && now - Date.parse(r.sent_at || r.created_at) > KEEP_SENT_MS; })
        .forEach(function (r) { del(r.client_id); });
      // 送信中のまま止まった記録（アプリ強制終了など）は送り直し対象へ
      list.forEach(function (r) { if (r.status === 'sending' && !directBusy[r.client_id]) r.status = 'local'; });

      // 取り消した作業（job_cancel が箱にある）の、まだ送っていない開始・終了は送らずに捨てる（cancelJob が途中で落ちた時の続き）
      var cancelled = {};
      list.forEach(function (r) { if (r.kind === 'job_cancel') cancelled[r.job_client_id] = true; });
      list = list.filter(function (r) {
        if (JOB_KINDS[r.kind] && cancelled[r.job_client_id] && r.status !== 'sent') { del(r.client_id); return false; }
        return true;
      });

      var sentStart = {};
      list.forEach(function (r) { if (r.status === 'sent') markSent(r); if (r.kind === 'job_start' && r.status === 'sent') sentStart[r.job_client_id] = true; });
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
        // 写真（deps）: まだ送っている途中の写真があれば待つ。「要確認」になった写真は待たない（その写真なしで送る＝売上を止めない）
        for (var di = 0; di < (r.deps || []).length; di++) {
          var dp = byId[r.deps[di].cid];
          if (dp && (dp.status === 'local' || dp.status === 'sending') && !dp.wait_off) return dp;
        }
        return null;
      }
      list.forEach(function (r) {
        var b = r.status === 'local' && blockedBy(r);
        if (b && b.status === 'review') { r.status = 'review'; r.last_error = 'waiting: ' + b.kind; put(r); }
      });

      // [H1] 待つのをやめた写真（wait_off）も、開始・終了を送る直前にもう一度だけ先に試す（再送待ちの時間を待たない）。
      //   電波が戻った直後は写真が先に通るので、写真つきで届く。まだ通らなければ、開始・終了はその写真なしで送る
      var forced = {};
      list.forEach(function (r) {
        if (r.status !== 'local') return;
        (r.deps || []).forEach(function (d) { var dp = byId[d.cid]; if (dp && dp.status === 'local' && dp.wait_off) forced[d.cid] = true; });
      });
      var due = list.filter(function (r) {
        if (r.status !== 'local') return false;
        if (directBusy[r.client_id]) return false;   // [M3] 今すぐ送っている途中の記録は、ループからは送らない
        if (r.next_at && r.next_at > now && !forced[r.client_id]) return false;
        // 終了は同じ仕事の開始の後、退勤は出勤の後（先の記録が箱に無い＝送信済みで掃除された場合は送る）
        if (blockedBy(r)) return false;
        return true;
      }).sort(function (a, b) {
        // 文字だけの知らせは、写真などより先に送る
        var pa = a.kind === 'job_start_notice' ? 0 : 1, pb = b.kind === 'job_start_notice' ? 0 : 1;
        return pa - pb || a.seq_local - b.seq_local;
      });
      var sentEnd = {};
      list.forEach(function (r) { if (r.kind === 'job_end' && r.status === 'sent') sentEnd[r.job_client_id] = true; });

      var chain = Promise.resolve();
      due.forEach(function (rec) {
        chain = chain.then(function () {
          // 同じ回の中で先の記録が失敗していたら、今回は送らない（順番の逆転を防ぐ）
          if (blockedBy(rec) || directBusy[rec.client_id]) return;
          rec.status = 'sending'; rec.attempts++;
          var extra = null;
          if ((rec.deps || []).length) extra = { photo_refs: photoRefs(rec, byId) };
          // 開始・終了を配り終えた後に届く写真（要確認から送り直した等）: サーバーが作業記録に足して管理グループへ知らせる
          else if (rec.kind === 'job_photo' && (rec.payload.phase === 'after' ? sentEnd[rec.job_client_id] : sentStart[rec.job_client_id])) extra = { late: 1 };
          return put(rec).then(function () { notify(); return postOnce(rec, extra); }).then(function (res) {
            if (okEcho(res, rec)) {
              markSent(rec); progress = true;
              rec.status = 'sent';
              rec.sent_at = new Date().toISOString();
              rec.server = { jobId: res.jobId || '', status: res.status, warning: res.warning || '', file_id: res.file_id || '' };
              if (res.file_id) fileMem[rec.client_id] = res.file_id;
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
            if (rec.used_keep && !(err && err.server) && !offline()) rec.no_keep = true;
            // [H1] 写真が DEP_TRIES 回送れなかった: これを待っている開始・終了を先へ進める（次の回でもう一度だけ先に試す）
            //   回数は pfails で数える（attempts は「今すぐ送る」で 0 に戻るため）
            if (rec.kind === 'job_photo') {
              rec.pfails = (rec.pfails || 0) + 1;
              if (rec.pfails >= DEP_TRIES && !rec.wait_off) { rec.wait_off = true; dirty = true; }
            }
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
      // 送れた・新しい記録が入った時は、1秒待たずに続きを送る（写真のあとの開始・終了をすぐ出す）
      if (dirty || progress) setTimeout(flush, 30);
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
        var mine = list.filter(function (r) { return r.job_client_id === jobCid && JOB_KINDS[r.kind]; });
        if (mine.some(function (r) { return r.kind === 'job_end'; })) return { ended: true, queued: false };
        // 開始か、文字だけの知らせ（どちらもサーバーに作業記録の行を作る）が届いたかもしれない時は、取り消しを送る
        var reached = startMayHaveReached(mine.filter(function (r) { return r.kind === 'job_start'; })[0], opts) ||
                      mine.some(function (r) { return r.kind === 'job_start_notice' && startMayHaveReached(r, null); });
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

  /** 再送待ちの時間を消して、今すぐ送る（電波が戻った時・アプリを開き直した時）。失敗の回数は消さない・「要確認」は触らない */
  function kick() {
    if (paused || sending) return flush();
    return getAll().then(function (list) {
      return Promise.all(list.filter(function (r) { return r.status === 'local' && r.next_at; }).map(function (r) { r.next_at = 0; return put(r); }));
    }).catch(function () {}).then(function () { return flush(); });
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
    // 電波が戻った・画面に戻った時は、再送待ちの時間（最大5分）を待たずにすぐ送る（2026-11-13 kick）
    global.addEventListener('online', function () { kick(); });
    document.addEventListener('visibilitychange', function () { if (document.visibilityState === 'visible') kick(); });
    try {
      if (global.Telegram && Telegram.WebApp && Telegram.WebApp.onEvent) Telegram.WebApp.onEvent('activated', function () { kick(); });
    } catch (e) {}
    // 端末の保存を消されにくくする（対応端末のみ）
    try { if (navigator.storage && navigator.storage.persist) navigator.storage.persist(); } catch (e) {}
    notify();
    return flush();
  }

  global.Outbox = {
    start: start, enqueue: enqueue, flush: flush, retryAll: retryAll, all: getAll, supersede: supersede, cancelJob: cancelJob, setTest: setTest,
    kick: kick, drop: del, caps: { deps: 1 },   // caps.deps = 写真を先に送る作り（画面側はこれを見てから使う＝古い outbox.js と組んでも写真を落とさない）
    kvGet: kvGet, kvSet: kvSet, kvDel: kvDel, uuid: uuid,
    lastError: function () { return lastError; }
  };
})(window);
