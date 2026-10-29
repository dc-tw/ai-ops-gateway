# ADR-003：Tenant audit hash chain 與簽章 checkpoint

- 狀態：Accepted
- 日期：2026-10-29
- 範圍：M2 身分與執行治理

## 決策

既有 `tenant:{tenant}:audit` 保留為所有相容 producer 的來源；Gateway 將其依 Redis stream cursor
順序投影到 `tenant:{tenant}:audit-chain:v1`。head 位於
`tenant:{tenant}:audit-chain:v1:head`。Credential 與 security-policy transaction 除了專用安全
audit，也在同一 transaction 寫入 tenant audit，避免治理事件落在 chain 之外。
這兩類 tenant audit 仍屬 Redis-only security authority，durable outbox 以固定 action allowlist 排除，
所以 PostgreSQL history maintenance fence 不會阻擋發出、輪替、撤銷或停用；chain verify 仍會涵蓋。

chain entry 的 stream ID 固定為 `<sequence>-0`。同一 tenant 的 writer 以 Redis `WATCH/MULTI`
比較 head 與 tail，再原子寫入 entry＋head；不同 tenant 使用不同 key，不共享順序。verify 與
checkpoint export 會先把來源追平，因此舊 Lua／transaction producer 不必在一次 release 內重寫。
Gateway 背景 projector 預設每 2 秒掃描並追平，設定為
`AUDIT_CHAIN_PROJECTOR_POLL_SECONDS`；多 replica 同時執行仍由相同 `WATCH/MULTI` contract 序列化。

## 固定 hash contract

- scope：單一 tenant。
- genesis：64 個 ASCII `0`。
- canonical JSON：UTF-8、key 依 Unicode code point 排序、無空白 separator、保留 Unicode、拒絕
  NaN／Infinity。
- body 欄位：`schema_version`、`sequence`、`tenant_id`、`source_cursor`、`occurred_at`、`event`。
- hash：`SHA256(ASCII(prev_hash) || UTF8(canonical_json(body)))`。
- `prev_hash` 與 `hash` 不放進 canonical body。
- `occurred_at` 由來源 Redis cursor 的毫秒部分決定，因此競態重試不會改變 hash。

首次部署只會從仍保留的來源 audit 開始建立 chain；`coverage_start_cursor` 明確揭露覆蓋起點，
不宣稱已找回部署前遭截短的紀錄。新 `append_audit` 不再套用 `MAXLEN`。

## 驗證與 checkpoint

`GET /v1/audit-chain/verify` 僅允許 human admin，會檢查 sequence、chain stream ID、tenant、schema、
source cursor 單調性、prev/hash 及 head。回應只含位置、hash 與錯誤代碼，不回傳 audit 正文。

`POST /v1/audit-chain/checkpoints` 先把匯出動作寫入 audit，再追平及驗鏈，最後以
`AUDIT_CHECKPOINT_SIGNING_KEY` 做 HMAC-SHA256。至少 32 bytes；`AUDIT_CHECKPOINT_KEY_ID` 標示
金鑰版本。HMAC 是 shared-secret authenticity，不是公開金鑰簽章。輪替時必須更換 key ID，並在
外部 secret manager 保留舊 key，才能驗證舊 checkpoint；輪替不重寫 chain。

## 保留與威脅邊界

hash chain 可偵測 entry 被改寫、刪除、換序與 head 不一致；不能阻止具有 Redis 寫權限的人同時
重建 entry 與 head，也不能證明事件本身真實。Redis AOF／volume 不是 immutable archive。

只有在已匯出並離線驗證 checkpoint、把 checkpoint 與涵蓋的 audit bundle 寫入 WORM／object-lock
儲存、且保存該物件 URI／version／retention evidence 後，operator 才能依政策刪除舊來源及 chain。
本版刻意不提供 prune API，避免將 hash chain 誤當外部不可變保存。

操作程序見 [audit chain runbook](../audit-chain.md)。
