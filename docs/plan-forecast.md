# plan-forecast 操作與邊界

M5 提供明確啟用的模型發布、逐呼叫遙測、校正 profile 與 plan-only forecast。所有新功能
預設關閉；forecast 不會呼叫模型、工具、建立 Goal、預留額度或自動執行計畫。

## 啟用

Gateway：

```dotenv
MODEL_PUBLICATION_ENABLED=true
PLAN_FORECAST_ENABLED=true
MODEL_TELEMETRY_ENABLED=true
FORECAST_CALIBRATION_MIN_SAMPLES=20
MODEL_TELEMETRY_MAX_RECORDS=10000
```

執行 Goal 的 worker 至少要設定 `MODEL_PUBLICATION_ENABLED=true` 與
`MODEL_TELEMETRY_ENABLED=true`，才能用同一份共享發布身分記錄遙測。跨副本的 local route
設定仍須一致；共享發布會固定其安全 route fingerprint，若某副本的 route 不一致便拒絕快照或
Workflow 執行。

## 1. 發布模型身分

admin 呼叫 `POST /v1/models/publications`：

```json
{
  "alias": "production-chat",
  "expected_revision": 0,
  "actual_model_version": "provider-disclosed-version",
  "deployment_id": "deployment-a",
  "tokenizer_version": "tokenizer-v1",
  "runtime_version": "runtime-v1"
}
```

發布採 Redis CAS；`expected_revision` 不符回 409。每版另保存不可變 revision，current pointer
供所有副本讀取。API 只接受 ModelRouter allowlist 中的 alias，並核對 resolved model 與
route fingerprint。這些版本是管理者提供的 disclosure，不是 Gateway 對權重或供應商的遠端
attestation；未知欄位保持 `null`／`unknown`，alias 本身不被當成實際版本。

viewer 可用 `GET /v1/models/publications?alias=production-chat` 查 current publication。回應不含 endpoint、
API key、費率或 prompt。

## 2. 遙測與明確校正

開啟 telemetry 後，每次 Goal 模型呼叫保存：call／Goal／node ID、安全 route／publication
指紋、開始與結束時間、延遲、保守預留量、provider 實報 tokens、狀態與錯誤類型。有 current
profile 時另保存 profile hash、base input/output/total estimate 與 actual-minus-estimated 差值。紀錄不保存
prompt、response、工具內容或 hidden reasoning。admin 可由
`GET /v1/models/telemetry?after=0-0&limit=100` 分頁查詢；stream 採近似上限保留，並非永久稽核庫。

校正不會自動執行。admin 明確呼叫：

```json
POST /v1/models/calibrations
{
  "alias": "production-chat",
  "publication_revision": 1,
  "expected_revision": 0,
  "minimum_samples": 20
}
```

只採用同一 publication、成功且 provider-reported usage 完整的樣本；不足時回
`insufficient_calibration`，不寫 profile，也不發模型請求。成功結果包含 input-token／UTF-8-byte
比率、output tokens 與完整 call latency 的經驗 q10／median／q90。Profile 同樣使用 CAS current
pointer 與不可變 revision，保存前一版 hash。這些是樣本情境，不是信賴區間、SLA 或品質證明；
失敗呼叫仍保留在 telemetry，但不冒充完成樣本。

## 3. 明確建立 forecast

先建立名為 `plan-forecast`、`schema_version: 2`、`execution_modes: ["plan"]` 的 Skill，並明確
綁到 root Agent 的 `planning_skills`。prompt、tag、一般 execution Skill 或同名文字不會觸發。

PlanningSession 必須仍在 `plan` mode，current revision 必須完整。呼叫：

```json
POST /v1/planning-sessions/{session_id}/forecasts
{
  "expected_version": 2,
  "plan_revision": 1
}
```

Estimator 直接使用該 revision 保存的 canonical compiled Workflow，不重新解析另一份自由文字
計畫。輸入 manifest 逐 node 固定 Agent、persona bytes、Skill／唯讀資源 bytes、計畫內容、
history 上限、工具、重試、tool rounds、model publication 與 calibration hash。Chief synthesis 與
Worker 都是 manifest 中的實際 node，不另做自由拆解。

結果有 low／base／high：

- low：不含條件分支，單次模型呼叫，經驗 q10；
- base：列入全部可能分支、fresh Workflow conversation、不假設重試，經驗 median；
- high：列入全部分支、重試／tool-loop 上限，後續呼叫使用 history ceiling，經驗 q90。

時間使用 DAG dependency、全域 parallelism 與每 Agent 一個 active node做 list scheduling，輸出
`model_only_makespan_seconds`，不是逐 node 秒數直接相加。token 預測不裁切到 team／node budget；
超額另由 `budget_risk`／`node_budget_risk` 呈現。超過 step deadline 的 node 列在
`deadline_risk_nodes`。queue、工具、人工核准等待分開標為 `unknown` 且不納入 makespan，因此
目前不提供 calendar ETA。

任一實際使用 route 缺少相符 profile 時，狀態為 `insufficient_calibration`，token／時間為 null；
不以 mock 延遲、output cap、額度或固定 tokens/sec 填值。相同 plan／snapshot／compiled／profile
來源會回同一 `forecast_sha256` 與 `deduplicated: true`；新計畫或新 profile 產生新 artifact，latest
pointer 合併到最新結果，舊 artifact 不改寫。

發布 artifact 的同一 CAS 會重查 session mode/version、current plan revision、Agent／Skill epoch、
共享 model publication 與 calibration current hash。途中切 execute、撤銷 Skill、換 route 或發布
新模型版本均回 409。viewer 可用
`GET /v1/planning-sessions/{session_id}/forecasts/latest` 讀最新結果；讀取不會重算。

## 驗證與仍不代表的事項

```sh
./scripts/test_forecast_stack.sh
```

此隔離 Docker E2E 使用真實 HTTP、Redis、worker 與 synthetic OpenAI-compatible provider，驗證
provider-reported telemetry、校正、去重、無模型 forecast 及 publication stale rejection。它不代表
真實模型效能、production SLA、實際核准等待或付費 route 驗收。真實模型校正必須另行取得環境、
流量與費用授權；M7 才提供 forecast SSE／管理介面，M9 才做 staging 負載與長期校正證據。
