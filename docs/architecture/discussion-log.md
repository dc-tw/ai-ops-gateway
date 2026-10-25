# Architecture Discussion Log

這份文件是 GitHub Discussions 啟用前的暫存索引，也是啟用後的決策回填入口。它不取代 issue、
pull request、ADR 或測試證據。

## 維護規則

1. Discussion 用於提出假設、替代方案、風險與實驗結果。
2. 每則進度應包含日期、scope、evidence、decision／open question 與相關 commit／PR。
3. 可執行工作拆成 issue；程式碼變更走 pull request。
4. 已接受的架構決策回填到
   [`buzz-inspired-roadmap.md`](buzz-inspired-roadmap.md) 或獨立 ADR，不能只存在 Discussion。
5. 未經測試或 rollout 證據的項目不得標記為 Shipped。

建議 Discussion 標題：

> Architecture roadmap: durable events, Agent identity, audit chain and single-flight

建議每次更新格式：

```markdown
## YYYY-MM-DD — 主題

### Scope

### Evidence

### Decision / open question

### Risks and rollback

### Links
```

## 2026-10-25 — 建立 Buzz-inspired roadmap

### Scope

比較 Buzz 與 AI Operations Gateway，將可借鏡方向收斂為 event envelope、tamper-evident audit、
Agent identity、per-conversation single-flight 與 durable event store。

### Evidence

- Buzz README、Architecture、Agent Vision 與 Projects Vision 已分別核對。
- Gateway 目前以 Redis 保存 conversation、run、Goal、SSE replay、checkpoint 與 audit。
- Gateway 已具備 user principal、RBAC、ATR、approval 與 durable Goal，但尚未具備完整 Agent
  principal、統一事件格式、audit hash chain 或 conversation-scoped single-flight。

### Decision / open question

- 已決定：選擇性參考，不導入完整 Buzz／Nostr／Git hosting／協作 UI。
- 已決定：repository 文件是已接受決策的 source of truth。
- 待討論：event envelope 的 ID、ordering、partition 與 schema-version contract。
- 待討論：Postgres migration 期間每類資料的 source of truth 與 rollback point。
- 待討論：single-flight 採排隊、拒絕或可合併工作，以及 Goal 與即時 run 的優先序。

### Risks and rollback

- 過早導入 Postgres 可能在事件語意未定時固化錯誤 schema。
- 雙寫可能造成 Redis／Postgres 分歧；未建立 reconciliation 前不得切換讀取來源。
- Hash chain 若沒有 canonicalization 與 writer serialization，會產生無法驗證或分叉的 chain。

### Links

- [Buzz-inspired roadmap](buzz-inspired-roadmap.md)
- [Buzz README](https://github.com/block/buzz/blob/main/README.md)
- [Buzz Architecture](https://github.com/block/buzz/blob/main/ARCHITECTURE.md)
- [Buzz Agent Vision](https://github.com/block/buzz/blob/main/VISION_AGENT.md)
- [Buzz Projects Vision](https://github.com/block/buzz/blob/main/VISION_PROJECTS.md)

## GitHub Discussions 狀態

截至 2026-10-25，`dc-tw/ai-ops-gateway` 尚未啟用 GitHub Discussions。啟用並建立主題後：

1. 在本節加入 Discussion URL；
2. 將本頁的初始紀錄貼為主題首篇或摘要；
3. 後續進度更新在 Discussion；
4. 每次形成正式決策時，以 PR 回填 roadmap／ADR，並從 Discussion 連回 commit。
