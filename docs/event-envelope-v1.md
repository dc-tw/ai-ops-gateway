# EventEnvelope v1 與相容轉接

Gateway 的新事件寫入使用 `EventEnvelope`；舊 Redis stream entry 仍可讀，既有 SSE payload 不變。
正式 JSON Schema 位於 [`schemas/event-envelope-v1.schema.json`](../schemas/event-envelope-v1.schema.json)，
legacy type 對照與 namespace 目錄位於
[`schemas/event-types-v1.json`](../schemas/event-types-v1.json)。

## 識別與排序

- `event_id` 是事件內容身分，由 producer 產生，不代表排序。
- Redis stream cursor（例如 `1730000000000-0`）只負責該 stream 的順序與 resume，放在讀取結果的
  tuple／SSE `id:`，不寫入 envelope。
- `idempotency_key` 是 producer／consumer 的去重契約，與 event ID 分欄。舊 entry 的 adapter 使用
  `legacy:{category}:{resource_id}:{cursor}`，因此重讀會穩定得到同一身分。
- ordering scope 是單一 Redis stream；v1 不宣稱跨 tenant 或跨 stream 全域排序。

`schema_version` 使用 JSON integer `1`。新 consumer 遇到 envelope marker 但版本未知時必須拒絕，
不能猜測欄位；純 legacy object 則由 adapter 包裝。`prev_hash`／`hash` 在一般事件為 `null`，只保留
下一階段 audit chain 的型別位置，不能把 null 當成已防竄改。

## 相容行為

- `read_agent_events`、`read_goal_events` 與 planning SSE 會 unwrap `payload`，所以既有 client 不需
  在這個 release 一次改版。
- server-side 新 consumer 可使用 `read_agent_event_envelopes`／`read_goal_event_envelopes` 取得完整
  envelope；每筆仍另外回傳 Redis cursor。
- mixed stream 可同時存在舊 object 與 v1 envelope，adapter 依 entry 判斷，不做破壞性 backfill。
- PostgreSQL history 保存 v1 原文以便稽核與 reconcile，但既有 history events API 回傳 legacy
  `data`；safe full-text index 會先 unwrap，避免計畫／Goal 的既有搜尋內容消失。
- outbound signed webhook 原有 `schema_version: 1` 是外部通知契約，不在本次偷換欄位；它與內部
  EventEnvelope 分開版本化。

## 隱私邊界

Envelope 不會自動讓任意 payload 變安全。producer 仍先經原本的 redact／allowlist：ATR internal
event 只保留 finding ID、rule ID、severity、enforcement action 與 review status，不收 raw input、
matched text、secret 或 hidden chain-of-thought。相容 adapter 不會從舊 payload 推導或新增敏感內容。

目前已套用 agent run、Goal、Workflow Goal、planning 與 trusted internal automation 的新寫入；
Lua 內既有 producer 可暫時寫 legacy object，所有正式 reader 都必須走 adapter。audit stream 會在
tenant SHA-256 chain 階段使用專門的原子 append，而不是僅靠一般 EventEnvelope 的 null hash 欄位。
