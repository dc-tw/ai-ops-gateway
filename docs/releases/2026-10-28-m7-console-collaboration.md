# M7：繁中管理台與協作驗證紀錄

日期：2026-10-28

## 本段交付

- React 19／TypeScript／Vite 同源繁中管理台，Docker 多階段建置；涵蓋 Agent／ACL、Skill／匯入、
  automation、Goal／核准、plan／forecast、YAML、唯讀 DAG、搜尋／audit。
- FastAPI OIDC authorization-code + PKCE、nonce、一次性 Redis state、opaque server-side session、
  HttpOnly cookie 與 CSRF。正式環境拒絕 development login；provider token 不持久化。
- planning／forecast Redis Stream SSE 支援 cursor、EventSource 重連／去重、每次傳送前即時 ACL
  重查，只輸出 hash、revision、status 等安全參照。
- 人類 opt-in presence 與 Agent self-only heartbeat 分離，20 秒更新、60 秒過期，明確不是 lock。
  Agent typed credential 無法進行人工核准。
- Goal lifecycle 原子 notification outbox、獨立多副本 consumer、HTTPS hostname allowlist、HMAC、
  event ID、固定延遲重試、dead-letter 查詢與人工 redrive；通知只連回需登入的控制台。

## 驗證證據

- Python lint 與新增 auth／collaboration／notification 單元測試通過。
- 專用 Redis 8.2 容器的 planning、Goal、Workflow、ACL、durable outbox 回歸：
  `196 passed, 2 warnings`。Warnings 來自既有 LangGraph Redis 未來版 deprecation。
- 專用 Redis 8.2＋PostgreSQL 17 的最終全套回歸：`915 passed, 8 warnings in 189.71s`，零 skip。
  八項 warning 均為同一個既有 LangGraph Redis 未來版 deprecation。
- `npm run build` 完成 type-check 與 production bundle；`npm audit`／Docker `npm ci` 回報
  `found 0 vulnerabilities`。Vite 固定為修補已知 traversal／dev-server advisories 的 7.3.7。
- `docker build --build-arg INSTALL_BROWSER=false -t ai-ops-gateway:m7 .` 成功；暫時 image 內
  `/healthz`、`/console/`、`/auth/config` 均回 200，且 development login feature flag 正確。
- Playwright headed Chromium 驗證明確 development login、Agent 建立、Skill／planning hash route、
  YAML／唯讀 DAG／SSE 控制與 CSRF logout。測試曾抓到跨頁 draft state 未重設，修正後複驗通過；
  最終登入頁不產生 401／favicon console error，Skill 頁顯示正確範本。
- 專用 `ai-ops-m7-gateway`／`ai-ops-m7-redis` 已清理；未操作其他專案容器。

## 尚未宣稱

- 本機 OIDC contract 測試不等於已連接某個 production IdP；正式 client／redirect／claim mapping
  仍須由部署者在 staging 驗證。
- Webhook mock 驗證 HMAC 與資料邊界；真實外部收件系統、DNS／憑證輪替與組織網路 egress
  屬 staging 驗收。
- 管理台第一版不是拖拉式 workflow designer，也不把 presence 當成共同編輯同步或鎖。
- M8 ACP、M9 staging／壓測／PITR，以及 M2 audit chain／完整 credential lifecycle 仍待完成。
