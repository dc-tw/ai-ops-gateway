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

## 2026-10-25～26 — 主動工作、獨立 Agent Persona 與 Chief 派工

### Scope

依已確認的 Q1–Q12 實作定時與受事件觸發的 durable Goal，並加入可獨立管理的 Chief／Worker
角色設定。支援外部 HMAC webhook、ATR finding、Goal failure、cron／interval、重疊／漏跑合併、
逐次寫入核准、API／SSE，以及單層 Chief → Worker 委派與共用團隊 token 上限。

### Evidence

- 真實 Redis 8.2 DB 0 的完整 suite：196 項通過，無 skip；三個第三方 checkpoint deprecation warnings。
- Docker 驗證每日 ATR 摘要、部署失敗 webhook、兩種內部事件、兩個 scheduler／worker 競爭、
  Chief 派工、獨立 persona、共享額度、SSE replay，以及 worker SIGKILL 後恢復。
- 真實 Redis checkpoint 關閉／重開後，兩個寫入操作分別核准，已執行操作不重播。
- 多代理程式複核找出的 schedule cursor crash、事件漏送、slot TTL、重複 pause、取消 cascade
  與 terminal quota refund 問題均已加入回歸測試。
- 中文操作指南與[逐項驗證紀錄](../proactive-agents-verification-2026-10-25.md)保留指令、範圍與限制。

### Decision / open question

- 已決定：沿用 Redis／LangGraph／既有 Goal worker，不另外建立 workflow runtime。
- 已決定：角色 persona 是行為指引，不能擴大服務端工具／網域／額度政策。
- 已決定：Chief 只能委派已核准 Worker；子 Agent 不再遞迴派工；總額度由主任務共享。
- 已決定：外部寫入逐次核准，approval 不略過 ATR；不確定副作用停止並要求人工查核。
- 待後續 Discussion：真模型任務品質、資料歸檔、可觀測性與管理前端；本次不宣稱已部署 production。
- 後續開發心得與實驗結果會持續記錄；GitHub Discussions 啟用後，將本節作為初始進度回填來源。

### Risks and rollback

- Mock inference 只能證明編排與治理，不能證明真模型分工／分析品質，也不是外部寫入 exactly-once 證明。
- Redis 需啟用 AOF／volume；Goal／事件／receipt 尚無自動歸檔，必須監控容量。
- 停止新觸發可停用 automation；停止未完工作需取消 Goal，Chief 取消會清理子 Goal。
- 回退程式前應先停用 automation、清理執行中工作並備份 Redis；本次沒有不可逆資料格式 migration。

### Links

- [主動工作與 Agent 指南](../proactive-agents.md)
- [驗證矩陣與實際流程](../proactive-agents-verification-2026-10-25.md)
- [更新後的 roadmap](buzz-inspired-roadmap.md)

## GitHub Discussions 狀態

截至 2026-10-25，`dc-tw/ai-ops-gateway` 尚未啟用 GitHub Discussions。啟用並建立主題後：

1. 在本節加入 Discussion URL；
2. 將本頁的初始紀錄貼為主題首篇或摘要；
3. 後續進度更新在 Discussion；
4. 每次形成正式決策時，以 PR 回填 roadmap／ADR，並從 Discussion 連回 commit。
