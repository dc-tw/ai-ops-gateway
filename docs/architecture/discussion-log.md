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

## 2026-10-26 — Skill 管理 API 與內建工作流程

### Scope

新增 tenant-scoped Skill registry，讓角色 persona 與可重用的工作流程分開管理。
第一版依使用者確認，交付 API 與中文文件，不包含管理前端。
內建來源摘要、待辦擷取、選項比較、證據檢視、調查計畫、事件初判、ATR 命中檢視、交接報告、
指定網頁查證與 Chief 派工協作十項範本；需由管理者選擇安裝與綁定，不自動賦予權限。

### Evidence

- API、Skill immutable revisions／CAS／rollback、Agent 固定版本、tenant 隔離、撤銷 epoch、
  多入口原子 admission、prompt 注入計費與 audit 隱私均有回歸測試。
- Docker mock inference 會拒絕缺少指定 Skill 指令的請求，驗證 Chief 與兩個 Worker 各用自己的 Skill。
- Docker 發布 revision 2 後仍使用釘住的 revision 1；模型 RPC 期間停用再立即啟用，舊 Goal 仍失敗。
- 既有 SSE、MCP、ATR blocking／不洩漏、checkpoint resume 與 worker SIGKILL 恢復仍納入驗證。
- 詳見[操作指南](../skill-management.md)、[內建目錄](../skills-catalog.md)與
  [本次驗證紀錄](../skills-verification-2026-10-26.md)。

### Decision / open question

- Skill 是指令流程，不是新增執行工具的權限；不執行任意腳本、Shell、Git 或部署動作。
- Agent 明確綁定 `{skill_id, revision}`；發布新內容不自動升級 Agent 或既有 Goal。
- 停用採撤銷語意，epoch 增加後舊快照不因重新啟用而復活；已在途 RPC 不保證能回滾。
- Chief 與 Worker 各自設定 Skill；共用主任務額度、ATR 與逐次寫入核准維持不變。
- 後續 Discussion 可討論按需 Skill 選擇、外部包相容性、管理前端與真模型語意品質評估，
  不把這些未實作能力算入本次交付。

### Risks and rollback

- Mock 只驗證治理與指令傳遞，不證明摘要或調查結論的語意正確性。
- Skill 更新可透過 rollback 建立新 revision，再明確改綁 Agent；不覆寫歷史。
- 沒有實體 DELETE 或自動歸檔；內容及歷史占用 Redis，需容量監控與備份。
- 回退至未支援 Skill 的舊程式前，先停止並清理綁定 Skill 的執行中 Goal，避免舊版本忽略新治理欄位。

## 2026-10-26 — Gateway 規劃模式與 plan-forecast 設計提案

### Scope

使用者確認 plan mode 指 Gateway Agent 的規劃模式；設計只在此模式觸發的 `plan-forecast`，
依有效模型與目前計畫估算 tokens 與機器經過時間。本次僅補規格與 Skill 草案，不修改 runtime。

### Evidence

- 核對目前 API 沒有 mode／Plan artifact，現有 worker 規劃仍屬執行型模型呼叫。
- 核對 Agent model 覆蓋、route alias、Skill hash、團隊 usage 與 active runtime 的現有語義。
- 獨立審查確認需處理模式／版本競爭、缺校正、slot 競爭失敗及 v1 hash 相容，尚無新功能測試證據。

### Decision / open question

- 設計採 tenant session 的服務端 mode gate、不可變計畫與設定快照；不靠 prompt 判斷規劃模式。
- Forecast 只讀、不派工、不呼叫模型；未知模型速度或人工等待時保留 null，不提供虛構 ETA。
- Chief／Worker 用量全隊加總，時間依資源受限依賴計算，與政策額度、已花費用量分開。
- 後續依 P1 模式與快照、P2 估算校正、P3 明確執行銜接逐步實作與驗證。
- 後續校正實驗與開發心得持續記錄於此；GitHub Discussions 啟用後再回填與連結，不宣稱已發布。

### Risks and rollback

- 現有資料不足以對任意模型提供可靠時間；草案不等於可執行能力。
- 新增模式欄位需版本化 canonical hash，不改寫舊 Skill revision／Goal 快照。
- 本次是文件變更，未安裝新 Skill、改動執行流程或推送遠端。

### Links

- [設計規格與驗收](../designs/plan-forecast/design.md)
- [plan-forecast Skill 草案](../designs/plan-forecast/SKILL.md)

## 2026-10-26 — 完成核心 plan mode 與不可變計畫快照

### Scope

依使用者確認實作 Gateway 規劃 session、計畫 revision、有效設定快照與模式切換。
本階段不實作 forecast、planning Skill v2 或從接受計畫啟動 Goal，切 execute 只接受並鎖定計畫。

### Evidence

- 真實 Redis 8.2 的完整 suite 為 404 passed、6 warnings、無 skip；Ruff 與 diff 檢查通過。
- 新增四個測試檔涵蓋 RBAC／tenant、設定 WATCH／CAS 競態、immutable hash、refresh、
  撤銷、模型路由、完整 ATR 掃描與零執行副作用。
- Docker 真 HTTP／Redis 驗證 Chief／Worker 計畫保存、接受、返回編輯與新設定 refresh，
  沒有增加模型用量、Goal queue 或 internal trigger。
- 既有 SSE／MCP／ATR blocking／SIGKILL checkpoint resume 回歸通過，詳見
  [驗證紀錄](../plan-mode-verification-2026-10-26.md)。

### Decision / open question

- session mode 不是 Agent 全域狀態，不會自動停止其他 Goal／automation。
- 固定設定只可透過明確 refresh 更新；Skill 新 head 不偷換釘住的 revision，撤銷 epoch 仍有效。
- 規劃 ATR finding 保存 metadata 但不發布 internal event，避免規劃變成自動派工來源。
- 後續先設計中央模型設定版本，再接 planning Skill gate、估算校正及明確執行銜接；
  既有 Skill v1 hash 不因本階段改變。

### Risks and rollback

- 路由指紋不證明遠端模型／部署版本，尚無跨 process 配置熱更新的一致性保證。
- 規劃資料無自動歸檔，需備份與容量監控；本階段使用獨立新 keys，不遷移舊 Goal／Skill。
- 本節為核心 plan mode 階段的本機紀錄；後續納入下節 milestone，不代表已發布 GitHub
  Discussion／Issue 留言，也沒有推送或部署。

### Links

- [操作與 API](../plan-mode.md)
- [完整設計及未實作部分](../designs/plan-forecast/design.md)

## 2026-10-26 — Skill／plan mode 封版與跨功能驗證

### Scope

依使用者確認，完成現有 Skill 管理與核心規劃快照的回歸、未支援能力防誤用測試及中文缺口
清單，形成一個本機 commit。Forecast、前端、Postgres 等不在此次開發範圍；沒有推送或遠端發文。

### Evidence

- 真 Redis 完整 suite：478 passed、6 個第三方 deprecation warnings、零 skip；Ruff／diff／
  Compose 檢查通過。既有 Docker、proactive／Chief／Skill、MCP remote ATR 三組 E2E 通過。
- 真實 Ollama `qwen2.5:0.5b-instruct` smoke 通過；不是任務品質或 forecast 校正實驗。
- 補上 planning／Skill malformed Unicode 安全 422、MCP 子程序 Browser 設定白名單、未來 API／欄位拒絕、
  plan session 隔離、refresh 政策變更與撤銷回歸。
- 兩個 HTTP process 的三個 SIGKILL 時序未洩漏原始輸出，但確認 checkpoint／SSE append
  非原子，可遺失診斷事件；真 pyatr 亦重現超長內容中段截斷漏掃。
- 語料 seed／benign 無 mismatch；候選 ATR-2026-00213 仍有 3 筆誤報，評估 CLI exit 1，
  維持 detection-only。獨立第二位真人 review 與真實流量驗證仍未完成。

### Decision / open question

- 本版接受上述已揭露限制，不宣稱 production-ready、finding exactly-once 或規則零誤報。
- 未實作項目分為下一階段設計、roadmap 候選與明確非目標，不將整份 roadmap 當此次交付承諾。
- 後續仍在本頁保存開發心得與實驗，Discussion 啟用後再回填；本次沒有新增遠端留言。

### Links

- [完整能力矩陣、重現命令、限制與後續驗收](../releases/2026-10-26-skills-plan-mode.md)

## 2026-10-27 — Postgres 歷史儲存、回填與切讀

### Scope

依使用者指示優先實作 Postgres，再推送 dc-tw/ai-ops-gateway。採獨立 history 平面：
Redis 繼續掌管即時執行／核准／撤銷／queue／checkpoint，PG 保存長期紀錄與查詢。
前端、forecast、統一語意事件與 audit hash chain 不在本次範圍。

### Evidence

- 新增原子 mutation outbox，覆蓋 Store pipeline／WATCH／Lua、automation、Chief 與 receipt。
- 真 Redis／Postgres 測試涵蓋競爭、重送、commit／ACK 中斷點、poison recovery、fence、tenant、
  FTS、回填與刪除防復活；既有控制 API 不改用 PG 延遲快照。
- 完整 suite 668 passed、6 個既有第三方 deprecation warnings、零 skip；新增 190 cases。
- 專用雙 projector Docker stack 已執行 PG／Redis 真停機、補送、零差異對帳、history 切讀、
  RBAC／隔離及真 pg_dump／pg_restore 逐表摘要比對。
- 中文操作、rollback／保留政策／備份缺口與實際驗證均留在 repository，未發布 Bohr Issue 留言。

### Decision / open question

- [ADR-002](adr-002-postgres-history.md)明確定義兩個平面的權威；這不是完整替換 Redis。
- Source ID、outbox 容量、PG 備份/PITR、Redis AOF 與 compact restore 窗口仍需部署方管理。
- Purge 只刪 PG 歷史 payload，fence 防復活但不等於所有副本資料抹除；excluded 必須揭露。
- 後續心得與實驗仍記錄於此；Discussion 啟用後再回填，不把本機文件當成已發布遠端主題。

### Links

- [Postgres 操作指南](../postgres-history.md)
- [本次完整驗證與限制](../postgres-verification-2026-10-27.md)

## GitHub Discussions 狀態

截至 2026-10-25，`dc-tw/ai-ops-gateway` 尚未啟用 GitHub Discussions。啟用並建立主題後：

1. 在本節加入 Discussion URL；
2. 將本頁的初始紀錄貼為主題首篇或摘要；
3. 後續進度更新在 Discussion；
4. 每次形成正式決策時，以 PR 回填 roadmap／ADR，並從 Discussion 連回 commit。
