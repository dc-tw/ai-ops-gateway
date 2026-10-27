# Plan v2 與 Workflow YAML（M4 第一段）

目前交付是**規劃、不可變編譯產物與純決策函式**，不是 DAG 執行引擎。
保存、編譯及切換 `mode=execute` 都不建立 Goal、不呼叫模型、不保留 token 額度。
明確 execute admission、持久化 DAG controller、核准恢復／取消及故障驗證仍在 M4 待辦。

## 啟用與界線

預設 `WORKFLOW_ENABLED=false`。管理者可設定：

```dotenv
WORKFLOW_ENABLED=true
WORKFLOW_MAX_NODES=32
WORKFLOW_MAX_EDGES=64
WORKFLOW_MAX_PARALLELISM=4
```

程式硬上限是 128 節點／256 邊／16 平行；部署預設仍為 32／64／4，
平行度也不超過租戶 `TENANT_MAX_CONCURRENT_RUNS`。
每個 Worker 的所有節點預算總和不超過該 Agent policy，所有節點共享 root 預算。
純 readiness 決策限制同 Agent 同時一個執行節點；**跨 Goal 併發鎖仍待 controller 實作**。
不可循環、遞迴派工或使用快照未核可的 Agent。Chief 只可當無工具權限的 synthesis 節點。

## API

沿用既有 API key／OIDC、operator 寫入、viewer 讀取、tenant 與 opt-in owner／分享 ACL。

1. `POST /v1/agents/{agent_id}/planning-sessions` 建立快照。
2. `POST /v1/planning-sessions/{session_id}/plans` 可保存既有 Plan 或 `schema_version: 2`。
3. `POST /v1/planning-sessions/{session_id}/workflow-yaml` body 為
   `{"expected_version": 1, "yaml": "完整 YAML 文字"}`。
4. `GET /v1/planning-sessions/{session_id}/compiled?revision=1` 取得編譯產物，
   `execution_started` 與 `execution_authority` 均為 false。

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
必要節點失敗的純決策會停止新派工並列出應取消節點。
waiting_approval 保留依賴阻塞但釋放執行平行容量；實際核准與 receipt 安全性待執行層整合驗證。

## 安全與驗證

- YAML 僅 UTF-8 64 KiB，拒絕重複鍵、alias、自訂 tag、過深巢狀與未知欄位。
- schema 每個 8 KiB，結構化輸出 16 KiB；拒絕重複 JSON 輸出鍵、非法 UTF-8、NaN 等。
- schema 檢查在可終止子程序，不在 API event loop 跑任意 regex；有 CPU／wall deadline，
  Linux 限記憶體；父程序有 timeout、kill／reap，並限制同時四個驗證程序。
- 僅允許本地 fragment `$ref`，禁止 schema ID 與動態引用；registry 也拒絕外部取用。
  過度複雜或逾時時 fail closed，不把未驗證模型文字用於條件。
- 保存計畫的 ATR／content policy 包含 query、URL、schema、condition 與 default 的文字，
  保留 literal newline，超限不截斷。唯讀預覽不產生自動化觸發事件。
- `scripts/verify_workflow_compiler.py` 可在無網路 Docker／Python 3.12 執行，
  驗證編譯、中文 YAML、schema、惡意 regex 終止與後續復原；不代表 DAG dispatch E2E 已完成。

本段不是正式部署／模型品質／壓測／PITR 驗收，進度見 [roadmap](implementation-roadmap.md)。
