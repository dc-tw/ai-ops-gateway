# Plan v2、Workflow YAML 與明確執行

已接上規劃、不可變編譯產物、明確 admission 與 Redis 持久化 controller。
保存、編譯及切換 `mode=execute` 都不建立 Goal、不呼叫模型、不保留 token 額度。
只有明確的 `POST .../execute` 才建立執行工作。已有本機完整回歸、實際 Redis checkpoint／
LangGraph 與雙 worker Docker／SIGKILL／Postgres E2E；正式部署、壓測與 PITR 仍屬 M9。

## 啟用與界線

預設 `WORKFLOW_ENABLED=false`。管理者可設定：

```dotenv
WORKFLOW_ENABLED=true
WORKFLOW_EXECUTION_ENABLED=true
WORKFLOW_MAX_NODES=32
WORKFLOW_MAX_EDGES=64
WORKFLOW_MAX_PARALLELISM=4
```

程式硬上限是 128 節點／256 邊／16 平行；部署預設仍為 32／64／4，
平行度也不超過租戶 `TENANT_MAX_CONCURRENT_RUNS`。
每個 Worker 的所有節點預算總和不超過該 Agent policy，所有節點共享 root 預算。
純 readiness 與 Redis 跨 Goal Agent lease 都限制同 Agent 同時一個執行節點。
租戶 execution slot 額滿時持久化等待，不把正常競爭誤判為節點失敗。
兩個 flag 獨立且預設關閉；execution gate 關閉只拒絕新 admission，既有任務仍受管理與結算。
Workflow v1 使用 `wfv1-` ID、`workflow:v1:queue`、獨立的 runtime record／index namespace。
沒有舊資料鍵 fallback；不認得協定的舊 worker／API 不會把它當成普通 Goal 執行或控制。
新 worker 交替處理普通與 Workflow queue；重送、恢復、核准都回到對應 queue。
入場時啟用的 ACL posture 保存在 root，後續副本只能收緊、不能用較鬆本機設定將它降級。
模型呼叫與寫入 receipt CAS 都重查這份治理及當前 ACL，並保留 epoch／scope 的即時檢查。
不可循環、遞迴派工或使用快照未核可的 Agent。Chief 只可當無工具權限的 synthesis 節點。

## 升級與降版界線

- 先升級公開的 mutating API／controller／history projector，保持 execution gate 關閉，
  對帳／驗證完再啟用。不要將任意更舊版本稱為受支援的混版。
- 已驗證不認得 Workflow 的 M3 版本能處理普通工作，卻看不到／取消不了 v1 新工作。
  這不代表舊 executor 會遵守新 Agent lease；涉及同 Agent 的舊任務須先排空，
  或將所有 Agent executor 更新後才承諾跨 Goal 的嚴格併發限制。
- 降版先關 admission，保留 v1-capable worker／scheduler 排空新工作，確認 quota／receipt 結算。
  不可把 v1 工作搬進 legacy queue／record namespace 強迫舊程式處理；普通 Goal 的舊 key／hash 不變。
- `430b618` 的未發布 prototype（普通 UUID、未帶協定）不自動升格；新執行器會拒絕／隔離。
  若本機曾啟用它，先停止舊執行器、完成人工查核／取消，再用新 session 明確建立 v1 工作。
- M5 仍待提供跨副本 route／deployment／profile 的版本 publication；目前安全 route fingerprint
  不代表已查明實際模型 weights／tokenizer 版本，亦不自動升級已在跑的工作。

## API

沿用既有 API key／OIDC、operator 寫入、viewer 讀取、tenant 與 opt-in owner／分享 ACL。

1. `POST /v1/agents/{agent_id}/planning-sessions` 建立快照。
2. `POST /v1/planning-sessions/{session_id}/plans` 可保存既有 Plan 或 `schema_version: 2`。
3. `POST /v1/planning-sessions/{session_id}/workflow-yaml` body 為
   `{"expected_version": 1, "yaml": "完整 YAML 文字"}`。
4. `GET /v1/planning-sessions/{session_id}/compiled?revision=1` 取得編譯產物，
   `execution_started` 與 `execution_authority` 均為 false。
5. 沿用 `POST .../mode` 的 expected_version、`mode: execute` 與當前 plan_revision 接受計畫，
   從回應取得新的 version 與 accepted_hash。
6. `POST /v1/planning-sessions/{session_id}/execute` body：

   ```json
   {"expected_version": 3, "accepted_hash": "回應中的完整64字元hash", "idempotency_key": "run-20261027-1"}
   ```

   hash 必須逐字使用，不是這段示意文字。回傳 root Goal（初始 queued），由 worker／scheduler
   執行 controller；不在 HTTP request 中跑模型。相同 key＋hash 重送回同一 Goal，
   即使第一次回應遺失、expected_version 已舊，也不重複入列或保留額度。
   同 key 換 hash 拒絕；同 accepted Plan 換 key 也不能繞過 active_goal_id 重複執行。
   要另跑一輪，先結束舊 Goal、回 plan mode，再明確接受與提交新一輪。

   root 已標 cancelled 但 controller 尚未完成子任務取消／收據彙整／quota 結算時，
   回 plan mode 仍回 409，須等 `workflow.phase=finished` 與 reservation reconciled。

admission 在同一 Redis CAS 內重查 session／accepted artifact、現行 Agent／Skill／route 快照、
圖限制與 ACL，並原子建立 root、owner／conversation、receipt、團隊 quota、queue 及 session active reference。
入場前重跑 ATR，涵蓋計畫、persona 與 Skill／資源，掃描後的 hash 綁定到 CAS；換版需重試。
預算只保留在 root，子 Goal 不重複扣款；UTC 跨日及 provider 回報較小用量會校正兩日。
每節點及團隊總上限仍受 server policy 約束，不因核准、恢復或重送而增加。

保存使用版本 CAS；非法圖、schema 或超限不新增 revision。
native 編譯產物連同輸入、Agent／Skill 固定版本及快照一起納入 revision hash；讀取重驗完整性。
JSON／YAML 與節點／邊輸入順序不影響同一正規化計畫的 compiled hash。
關閉 flag 後保留 native 歷史讀取，不授予任何執行權。
舊計畫經顯式 adapter 編譯，線性步驟保持順序、Chief assignments 匯入唯讀摘要節點；
不回寫舊 revision，也不改其歷史 hash。超過 adapter 節點容量時拒絕，不截斷。

## YAML 範例

把 Agent ID 換成 session 快照中的已核可 Worker；root 可以是該 Worker 或核可它的 Chief。

```yaml
schema_version: 2
objective: 分析部署事件，列出證據與未知事項
token_budget: 20000
parallelism: 4
nodes:
  - node_id: inspect
    agent_id: worker-a
    query: 依提供的事件資料分類，不做外部寫入
    token_budget: 3000
    output_schema:
      type: object
      required: [needs_review]
      properties:
        needs_review: {type: boolean}
      additionalProperties: false
  - node_id: review
    agent_id: worker-a
    query: 針對需要複核的證據提出建議
    token_budget: 3000
    condition:
      op: eq
      node_id: inspect
      path: /needs_review
      value: true
    inputs:
      classification:
        node_id: inspect
edges:
  - {source: inspect, target: review}
```

條件來源是 `status` 或 `output`，後者必須有上游 JSON Schema 2020-12。
支援 `eq/ne/gt/gte/lt/lte/in/exists/all/any/not`，欄位位置用 JSON Pointer。
僅可引用祖先節點；不接受 Python、JavaScript、模板程式或 remote schema。
`all_success`／`any_success` 都等全部前置節點終態後判斷；條件 false 為 skipped。
缺少 required input 是明確失敗；可選 input 能設 default。
必要節點失敗停止新派工，controller 持久化 cancelling，fence／取消進行中節點，再彙整終態及用量。
waiting_approval 保留依賴阻塞，但釋放租戶與 Agent execution slot；獨立分支仍可執行。
沿用每次工具＋參數的核准及 Redis checkpoint；已取消 root 的舊核准不能再把子 Goal 入列。
root pause 逐步暫停其活躍節點；resume 只恢復由該 root 暫停的節點，不擅自恢復原已個別暫停的節點。
controller／worker 重開後重讀權威狀態；節點 ID 固定，建立節點與 queue intent 同一 transaction。
損毀 artifact 會 fail closed 並隔離為 quarantined，不以壞 graph 派工，也不阻擋其他健康 root；
它需要人工檢查，不可直接當成一般可恢復的 pause 或在原 session 重新接受執行。

寫入 intent receipt 與 Goal／parent authority 以 CAS 綁定；未知 checkpoint 結果不可自動重播。
root 的 `workflow.receipt_reconciliation` 保存收據數量與 `review_required` 參照。
`GET /v1/goals/{goal_id}/receipts?limit=50&after=...` 在當前 Goal ACL 下分頁讀取
action ID、tool、started／completed 與是否需要複核，不返回原始參數。
root 只保存節點狀態及子 Goal 參照，不複製各節點完整輸出；讀取輸出仍須通過子 Goal ACL。
這不代表外部副作用被回滾；需要操作人員查明外部狀態，不能刪 receipt 後假裝安全重試。

## 安全與驗證

- YAML 僅 UTF-8 64 KiB，拒絕重複鍵、alias、自訂 tag、過深巢狀與未知欄位。
- schema 每個 8 KiB，結構化輸出 16 KiB；拒絕重複 JSON 輸出鍵、非法 UTF-8、NaN 等。
  只有 schema 成功驗證的短 JSON 能豁免一般文字答案長度；不略過工具來源／grounding 檢查。
- schema 檢查在可終止子程序，不在 API event loop 跑任意 regex；有 CPU／wall deadline，
  Linux 限記憶體；父程序有 timeout、kill／reap，並限制同時四個驗證程序。
- 僅允許本地 fragment `$ref`，禁止 schema ID 與動態引用；registry 也拒絕外部取用。
  過度複雜或逾時時 fail closed，不把未驗證模型文字用於條件。
- 保存計畫的 ATR／content policy 包含 query、URL、schema、condition 與 default 的文字，
  保留 literal newline，超限不截斷。唯讀預覽不產生自動化觸發事件。
- `scripts/verify_workflow_compiler.py` 可在無網路 Docker／Python 3.12 執行，
  驗證編譯、中文 YAML、schema、惡意 regex 終止與後續復原。
- `sh scripts/test_workflow_runtime_stack.sh` 另驗證真實 HTTP、兩個新 worker、舊版服務並存、
  typed join、SIGKILL、duplicate delivery、ACL posture 與歷史對帳；會清理專用容器／volumes。
  舊版 fixture 由固定 Git commit 重建，不依賴某個可變 tag 恰好還在本機。

本段不是正式部署／模型品質／壓測／PITR 驗收，進度見 [roadmap](implementation-roadmap.md)。
