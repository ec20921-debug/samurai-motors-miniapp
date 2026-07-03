# samurai-motors-miniapp

Samurai Motors ミニアプリ・静的アセットの **GitHub Pages 配信専用リポジトリ**。

- 配信URL: `https://ec20921-debug.github.io/samurai-motors-miniapp/`
- ソース（開発）リポジトリ: `ec20921-debug/samurai-motors-app`（private）

## ⚠️ 運用ルール

1. **このリポジトリは必ず public を維持する**（無料プランでは private リポジトリの GitHub Pages が使えない。private 化するとミニアプリ・チラシが全404になる — 2026-07-03 障害の教訓）
2. **ここに機密・ビジネス資料を置かない**。配信に必要な HTML / 画像のみ
3. **編集は samurai-motors-app 側で行い、このリポジトリへはコピーして反映する**（同期手順: samurai-motors-app の `docs/DEPLOY.md` 参照）

## 配信ファイル

| ファイル | 用途 |
|---|---|
| booking.html | 予約ミニアプリ（顧客用） |
| job-manager.html / index.html | 作業管理ミニアプリ（現場スタッフ用。index はラッパー） |
| home-internal.html / attendance-internal.html / expense-internal.html / report-internal.html / task-internal.html | 社内業務ミニアプリ（日報・勤怠・経費・タスク） |
| exec-dashboard.html | 経営コックピット（`?key=` 共有キー方式） |
| staff-campaign-half-free.html / flyer-3services-2026-06.html | キャンペーン告知ページ |
| flyer-2026-05.jpg / flyer.png | チラシ画像（予約Bot /start で送信） |
| logo.png | ロゴ（各ミニアプリから参照） |
