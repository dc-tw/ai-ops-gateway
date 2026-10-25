# Buzz-inspired 架構演進 Roadmap

- 狀態：Proposal
- 建立日期：2026-10-25
- 參考資料觀察日期：2026-10-24
- 適用專案：AI Operations Gateway

## 摘要

Buzz 的核心價值不只是「聊天室裡的 Agent」，而是讓人類、Agent、Workflow、Git 與專案記憶
共用身份、事件與稽核模型。本專案不打算搬入完整 Buzz 或 Nostr stack；本文件只採用能強化
Gateway 執行治理、可恢復性與稽核性的設計模式。

建議優先評估五項能力：

1. durable event store；
2. 統一 event envelope；
3. Agent identity 與 policy；
4. tamper-evident audit；
5. per-conversation single-flight。

這五項目前都不是「已完成」宣告。每一項必須經 Discussion、ADR、migration plan、測試與 rollout
gate 才能改為 Implementing 或 Shipped。

## 參考來源與事實邊界

本提案以 Buzz 上游文件為架構參考：

- [Buzz README](https://github.com/block/buzz/blob/main/README.md)：人類與 Agent 共用 workspace、身份、
  event log 與搜尋空間；README 同時標示 workflow approval glue 仍在建置。
- [Buzz Architecture](https://github.com/block/buzz/blob/main/ARCHITECTURE.md)：relay 是 single source of
  truth；Postgres 保存事件並提供 FTS，Redis 處理 pub/sub、presence 與 typing；audit 使用 SHA-256
  hash chain。
- [Agent Vision](https://github.com/block/buzz/blob/main/VISION_AGENT.md)：ACP 作為 client-to-agent
  interface，MCP 作為 agent-to-tool interface，session 具有獨立 MCP server、history 與 context。
- [Projects Vision](https://github.com/block/buzz/blob/main/VISION_PROJECTS.md)：描述 Git、Channel、CI、
  Review 與 signed approval 的整合方向。這是 vision 文件，不應把所有描述視為已交付功能。

已確認的上游限制也必須保留在比較中：Buzz Architecture 明確記錄 workflow
`request_approval` 目前會 suspend，但 token persistence 與 resume wiring 尚未完成；rate limiting
也仍有未實作部分。因此本專案只參考可驗證的模式，不以功能清單作為成熟度證明。

## 與目前 Gateway 的差異

| 面向 | Buzz 參考模式 | AI Operations Gateway 現況 | 缺口 |
| --- | --- | --- | --- |
| 核心用途 | 協作 workspace 與 event substrate | AI execution gateway | 不需要複製完整協作產品 |
| Client interface | WebSocket、CLI、ACP harness | HTTP API、SSE | ACP 僅列入後續評估 |
| Agent workflow | Agent runtime + YAML workflow | LangGraph + Durable Goal | 應避免建立第二套 runtime |
| Tool interface | MCP | MCP、Playwright、remote Browser Worker | 方向相容 |
| Durable data | Postgres event store | Redis 為主 | 長期查詢、關聯與 migration 邊界未定義 |
| 即時資料 | Redis pub/sub、presence、typing | Redis Stream、queue、checkpoint、lock | Redis 職責過多且混合長短期資料 |
| Audit | per-scope SHA-256 hash chain | Redis audit stream | 尚無 tamper evidence 與 chain verification |
| Identity | 人類與 Agent 都有明確身份 | user principal；Agent identity 不完整 | policy 主要綁在 request／部署設定 |
| 同一工作空間的併發 | per-channel single-flight | tenant/run quota，未鎖定 conversation | 同一 conversation 可能發生次序競爭 |
| 安全 | 身份、簽章、membership | RBAC、ATR、quota、approval、Evidence Validation | 可互補，不應互相取代 |

## P0 提案

### P0-1：統一 Event Envelope

先定義語意，再決定儲存技術。SSE、Goal、Audit、ATR finding 與 LangGraph event 應能映射到共同
envelope：

```json
{
  "schema_version": "1",
  "event_id": "uuid-or-stream-id",
  "event_type": "tool.completed",
  "tenant_id": "acme",
  "actor_type": "agent",
  "actor_id": "research-agent",
  "conversation_id": "conv-1",
  "run_id": "run-1",
  "parent_event_id": null,
  "correlation_id": "request-or-goal-id",
  "created_at": "2026-10-25T00:00:00Z",
  "payload": {},
  "prev_hash": null,
  "hash": null
}
```

設計要求：

- `schema_version` 必填，consumer 必須能拒絕或降級處理未知版本。
- `event_type` 使用穩定 namespace，例如 `tool.completed`、`threat.detected`。
- envelope 不直接保存 secret、原始 ATR payload 或 hidden chain-of-thought。
- `event_id`、排序 cursor 與 idempotency key 必須分開定義，不能假設 UUID 同時提供全域順序。
- 先提供 compatibility adapter；既有 SSE contract 不應在一次 release 中破壞。

驗收證據：JSON schema、事件目錄、舊新格式 compatibility tests、SSE replay ordering tests、ATR
privacy regression tests。

### P0-2：Tamper-evident Audit

為 audit event 建立 scope-aware SHA-256 chain：

```text
hash[n] = SHA256(prev_hash || canonical_event[n])
```

`canonical_event` 必須指定 canonical JSON 規則；chain scope、genesis value、key rotation、併發 writer
序列化、刪除／retention 與 verification API 都要在 ADR 中定義。Hash chain 只能偵測竄改，不能自行
防止有權限的寫入者建立惡意但有效的新事件，也不能取代外部 immutable export。

驗收證據：append concurrency test、mutation detection、missing-entry detection、跨 tenant isolation、
verification command，以及匯出至外部不可變儲存的 operational runbook。

### P0-3：Agent Identity 與 Policy

在既有 user principal 之外新增 server-owned Agent principal：

- `agent_id`、`owner_id`、狀態與憑證識別；
- allowed models、tools、domains；
- token budget、concurrent run limit；
- MCP configuration reference；
- ATR policy reference。

Client 不得自行宣告或擴權 Agent principal。Human approval 仍是特定副作用的同意；Agent policy
決定 Agent 可要求什麼；ATR 檢查跨 trust boundary 的內容。三者必須分開。

驗收證據：impersonation rejection、owner/policy authorization matrix、credential rotation、跨 tenant
isolation，以及 audit actor attribution。

### P0-4：Per-conversation Single-flight

同一 `tenant_id + conversation_id` 同時間最多允許一個會改變對話或 browser session 的 run；其餘
排隊、拒絕或合併的策略必須明確。Goal 與即時請求也必須遵守同一 ownership 規則。

驗收證據：多 replica 競爭測試、lease expiry/reclaim、取消後交棒、FIFO 或明確非 FIFO 語意、
duplicate request idempotency，以及 browser session ownership 測試。

### P0-5：Durable Event Store

在 event envelope 與 migration contract 穩定後，再引入 Postgres 保存：

- Conversation 與 message；
- Agent run 與 Goal；
- Tool call、approval 與 execution receipt；
- Threat finding；
- Audit event。

Redis 收斂為 queue、cache、checkpoint、SSE replay buffer 與 distributed lock。遷移不得直接將 Redis
替換為 Postgres；建議採 schema migration、backfill、shadow read／dual write、reconciliation、read
cutover、rollback 六個階段。對每種資料先定義 source of truth，避免雙寫期間產生兩個權威來源。

驗收證據：migration rehearsal、dual-write divergence metrics、reconciliation report、backup/restore、
retention/deletion、tenant isolation、FTS permission recheck，以及 Redis unavailable degradation tests。

## 建議實作順序

```text
事件盤點與 ADR
  → Event Envelope + compatibility adapter
  → Audit chain prototype
  → Agent Identity + single-flight policy
  → Postgres schema + migration rehearsal
  → shadow/dual write
  → read cutover
  → ACP / declarative workflow 等 P1 評估
```

雖然 durable store 是目標架構的核心，但先定義 envelope、identity 與 ordering，能避免先建資料表後
才發現事件語意不穩定。

## P1 候選

- ACP adapter，讓支援 ACP 的 client 使用 Gateway。
- Declarative Workflow YAML，編譯為 LangGraph，而不是建立第二套 workflow executor。
- Postgres FTS 搜尋 Conversation、Goal、Evidence 與 Audit；回傳前重新檢查 tenant／resource permission。
- Agent directory、presence 與 job queue。
- Webhook、schedule 與 event trigger。
- Tool execution receipt 與可搜尋 provenance。

P1 不應在 P0 identity、event semantics 與 durable store 還未穩定前承諾交付日期。

## 明確非目標

目前不規劃：

- 導入完整 Nostr protocol；
- 自建 Slack／Discord 類 UI；
- Git hosting；
- voice huddle；
- GPU community mesh；
- 完整社交身份系統。

若未來要改變非目標，必須另開 Discussion／ADR，說明 Gateway 核心定位是否已改變。

## 決策與狀態

| ID | 決策 | 狀態 | 日期 |
| --- | --- | --- | --- |
| D-001 | 選擇性採用 Buzz 的 event、identity、audit 與 single-flight 模式，不整套移植 | Accepted | 2026-10-25 |
| D-002 | 不把 Buzz vision 文件中的描述自動視為已交付能力 | Accepted | 2026-10-25 |
| D-003 | 先定義 event semantics，再進行 Postgres migration | Proposed | 2026-10-25 |
| D-004 | Discussion 保存探索過程；合併後的 repository 文件保存正式決策 | Accepted | 2026-10-25 |

進度與未決問題記錄於 [discussion-log.md](discussion-log.md)。
