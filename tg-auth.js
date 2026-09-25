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
 */
(function () {
  var KEY = 'sm_tg_init_data';

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

  function isGasUrl(url) {
    return /^https:\/\/script\.google(usercontent)?\.com\//.test(url);
  }

  getInitData(); // 起動ページで保存しておく

  var origFetch = window.fetch;
  if (!origFetch) return;

  window.fetch = function (input, init) {
    try {
      if (typeof input === 'string' && isGasUrl(input)) {
        var d = getInitData();
        if (d) {
          var method = String((init && init.method) || 'GET').toUpperCase();
          if (method === 'POST' && init && typeof init.body === 'string') {
            var obj = JSON.parse(init.body);
            if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
              obj._tgInitData = d;
              init = Object.assign({}, init, { body: JSON.stringify(obj) });
            }
          } else if (method === 'GET') {
            input = input + (input.indexOf('?') >= 0 ? '&' : '?') + '_tg=' + encodeURIComponent(d);
          }
        }
      }
    } catch (e) { /* 添付できなくても元のリクエストを送る */ }
    return origFetch.call(window, input, init);
  };
})();
