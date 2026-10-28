# 繁中管理台、協作與通知

M7 管理台由同一個 FastAPI origin 的 `/console/` 提供。原始碼位於 `ui/`，正式 image 會在
Node build stage 執行 `npm ci`／`npm run build`，runtime image 只包含靜態產物，不包含 Node。

## OIDC 瀏覽器登入

管理台使用 authorization-code + PKCE。`/auth/login` 產生一次性的 `state`、nonce、verifier 與
瀏覽器 binding；資料只以 TTL 保存在 Redis。`/auth/callback` 以 GETDEL 消耗 state，在後端交換
ID token、驗證 issuer／audience／nonce 後，建立 opaque server-side session。Provider access／
refresh／ID token 都不寫入 session、Postgres、cookie 或前端。

必要設定：

```dotenv
APP_ENV=production
OIDC_AUTHORIZATION_URL=https://identity.example/authorize
OIDC_TOKEN_URL=https://identity.example/token
OIDC_CLIENT_ID=ai-ops-gateway
OIDC_CLIENT_SECRET=server-secret
OIDC_REDIRECT_URI=https://gateway.example/auth/callback
OIDC_JWKS_URL=https://identity.example/.well-known/jwks.json
OIDC_ISSUER=https://identity.example/
OIDC_TENANT_CLAIM=tenant_id
OIDC_ROLE_CLAIM=role
GATEWAY_PUBLIC_URL=https://gateway.example
```

Session cookie 為 HttpOnly／SameSite=Lax，正式環境強制 Secure。另一個 SameSite=Strict CSRF cookie
須與 `X-CSRF-Token` 相符，伺服器再比對 session 內的 hash；所有 browser session 的非安全方法
都會檢查。`BROWSER_DEV_LOGIN_ENABLED=true` 只允許 `APP_ENV=development`，正式環境設定時應用
會直接拒絕啟動。API key／Bearer client 不使用 cookie CSRF。

## 管理介面與 planning SSE

頁面涵蓋 Agent／ACL、Skill／套件匯入、自動任務、Goal／逐次核准、Workflow YAML、唯讀 DAG、
forecast、受控歷史搜尋與 audit。第一版沒有拖拉式設計器；YAML 保存仍走既有不可變 revision、
ATR、snapshot 與 CAS。瀏覽器登入只是 UI transport，不繞過原有 RBAC／resource ACL。

`GET /v1/planning-sessions/{id}/events` 接受標準 `Last-Event-ID`，Redis `XREAD` 只讀原子保存的
planning／forecast 安全摘要。每個 payload 與 idle heartbeat 前都重查目前身份與 ACL；撤權後
直接關閉。回應不含計畫正文、persona、Skill instructions 或使用者 draft。EventSource 以 stream
ID 去重並由瀏覽器重連。

## Presence 與 Agent heartbeat

- 人類 presence 預設關閉；使用者在已授權資源上選擇 opt-in 後，管理台每 20 秒更新
  `viewing`／`editing`，60 秒未更新即過期。只分享 subject、資源參照、狀態與時間，不含 draft。
- Agent heartbeat 使用 `actor_type=agent` 的憑證，且只能更新與 subject 同名的 Agent。
  人類 admin 不能替 Agent 偽造 heartbeat。預設同樣 20 秒更新、60 秒過期。
- 兩者都只是可見性訊號，不是 CAS、ownership、execution lease 或鎖；離線不代表取消工作。
- Agent 身份不能呼叫 run／Goal 的核准入口；人工核准仍要求 human principal。

API：

```text
GET|PUT|DELETE /v1/collaboration/presence/{kind}/{resource_id}
GET|POST       /v1/collaboration/agents/{agent_id}/heartbeat
```

## Outbound 通知

Goal 完成、失敗、等待核准及核准逾時與 Goal 狀態在同一 Redis transaction 寫入 secret-free
outbox。獨立 `python -m app.notifier` consumer group 負責 delivery；多副本可安全競爭。設定範例：

```dotenv
NOTIFICATION_ALLOWED_HOSTS=hooks.example
NOTIFICATION_WEBHOOKS_JSON={"acme":[{"id":"ops","url":"https://hooks.example/gateway","secret":"32-or-more-random-characters","events":["goal.failed","approval.waiting"]}]}
NOTIFICATION_RETRY_SECONDS=5
NOTIFICATION_MAX_ATTEMPTS=5
```

目的 URL 必須是 HTTPS 且 hostname 在明確 allowlist，禁止 URL credentials／fragment，不跟隨
redirect。Body 含 event ID、類型、tenant、安全 Goal／approval 參照與需登入的管理台 URL；不含
核准 capability、API key、Webhook secret、prompt 或工具參數。Header：

```text
X-Gateway-Event-ID: <UUID>
X-Gateway-Timestamp: <Unix seconds>
X-Gateway-Signature: sha256=<HMAC-SHA256(secret, timestamp + "." + canonical-body)>
```

接收端應檢查時間窗、HMAC 並以 event ID 去重。暫時失敗保留 pending，超過次數才寫入 tenant
dead-letter；admin 可 `GET /v1/notifications/failures` 檢視安全診斷，並以
`POST /v1/notifications/failures/{stream_id}/redrive` 重新入列。redrive 保留原 event ID，讓接收端
繼續去重；失敗紀錄不含 secret。
