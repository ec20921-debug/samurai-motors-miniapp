/**
 * tg-auth.js — GAS への通信に Telegram の署名つき本人証明（initData）を自動で添付する
 *
 * なりすまし対策 Phase 1（2026-09-25〜・記録だけ期間）
 *   - 各ページは telegram-web-app.js の直後にこのファイルを読み込むだけ。既存の fetch 呼び出しは無改修。
 *   - POST(JSON 文字列 body) には `_tgInitData`、GET には `_tg` クエリを付ける。
 *   - サーバー側（v7 / v7-operations の TelegramAuth.gs）が署名を検証し、結果を記録する。
 *   - ページ遷移で initData が消える問題（docs/OPS_LESSONS.md #3）に備え、sessionStorage に保持する。
 *     URL には載せない（本人証明なので履歴・共有リンクに残さない）。
 *   - 添付に失敗しても元のリクエストはそのまま送る（業務を止めない）。
 *   - 2026-10-30: 付けるのは当社の2つの GAS だけ（KNOWN_GAS）。Telegram の外ではスタッフ専用リンクの `_fu` / `_fk` を付ける。
 */
(function () {
  var KEY = 'sm_tg_init_data';
  var LINK_KEY = 'sm_field_link';

  // 2026-10-30: 本人証明を付けるのは当社の2つの GAS（v7 = 顧客系 / v7-operations = 社内業務系）だけ。
  //   各ページは開発用に ?gas= を受け付けるので、一覧が無いと細工したリンクで他人の GAS に本人証明を送らされる。
  //   GAS を作り直して URL が変わったら、ここと各ページの GAS_URL を一緒に直す。
  var KNOWN_GAS = [
    'https://script.google.com/macros/s/AKfycbwV8eVN6KJvfMnQLkNmbOqYsjpvQSJTCqk40kfyMFzRjxFItJ8-4VxOV4U9MojLH4U/exec',
    'https://script.google.com/macros/s/AKfycbxHLCD6vhnNZhSywyWmpaUuwt1goC4n9pwLNNlVzo-3gp9eSwAIMU_EXWPVdbt9Vw/exec'
  ];
  function isKnownGas(url) {
    if (typeof url !== 'string') return false;
    for (var i = 0; i < KNOWN_GAS.length; i++) {
      if (url === KNOWN_GAS[i] || url.indexOf(KNOWN_GAS[i] + '?') === 0) return true;
    }
    return false;
  }
  window.SM_GAS_ALLOWED = isKnownGas;

  function getInitData() {
    var d = '';
    try {
      d = (window.Telegram && Telegram.WebApp && Telegram.WebApp.initData) || '';
    } catch (e) { /* noop */ }
    try {
      if (d) sessionStorage.setItem(KEY, d);
      else d = sessionStorage.getItem(KEY) || '';
    } catch (e) { /* noop */ }
    return d;
  }

  // 2026-10-30: ホーム画面アプリ（Telegram の外）のスタッフ専用リンク ?u=<chatId>&k=<鍵>。
  //   Telegram の本人証明が無い時だけ `_fu` / `_fk` として付ける（v7/StaffGate.gs が確かめる）。
  //   同じタブの中で旧アプリ等に移っても使えるよう sessionStorage に持つ（URL には載せ直さない）。
  function getFieldLink() {
    try {
      var q = new URLSearchParams(location.search);
      var u = q.get('u'), k = q.get('k');
      if (u && k) sessionStorage.setItem(LINK_KEY, JSON.stringify({ u: u, k: k }));
      var m = JSON.parse(sessionStorage.getItem(LINK_KEY) || 'null');
      return (m && m.u && m.k) ? m : null;
    } catch (e) {
      return null;
    }
  }

  getInitData(); // 起動ページで保存しておく
  getFieldLink();

  var origFetch = window.fetch;
  if (!origFetch) return;

  window.fetch = function (input, init) {
    try {
      if (typeof input === 'string' && isKnownGas(input)) {
        var d = getInitData();
        var link = d ? null : getFieldLink();
        if (d || link) {
          var method = String((init && init.method) || 'GET').toUpperCase();
          if (method === 'POST' && init && typeof init.body === 'string') {
            var obj = JSON.parse(init.body);
            if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
              if (d) obj._tgInitData = d;
              else if (!obj._fu) { obj._fu = link.u; obj._fk = link.k; }
              init = Object.assign({}, init, { body: JSON.stringify(obj) });
            }
          } else if (method === 'GET') {
            var sep = input.indexOf('?') >= 0 ? '&' : '?';
            if (d) input = input + sep + '_tg=' + encodeURIComponent(d);
            else if (input.indexOf('_fu=') < 0) input = input + sep + '_fu=' + encodeURIComponent(link.u) + '&_fk=' + encodeURIComponent(link.k);
          }
        }
      }
    } catch (e) { /* 添付できなくても元のリクエストを送る */ }
    return origFetch.call(window, input, init);
  };
})();
