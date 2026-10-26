# 主動工作、Agent Persona 與 Chief 派工

Gateway 支援 cron／固定間隔、HMAC webhook，以及 `threat.detected`、`goal.failed` 內部事件。
這些入口都會建立 durable Goal，由既有 worker 執行。Agent 設定決定職責與權限，Chief 則將主任務
拆成有限的子任務，交給已核准的 Worker Agent，再彙整結果。

## 啟動

```bash
docker compose --profile ollama up --build -d
```

Compose 同時啟動 Gateway、Goal worker、scheduler、Redis 與 Browser Worker。使用外部 inference
服務時先設定 `LLM_BASE_URL`、`LLM_MODEL`。本機分開啟動可使用：

```bash
uvicorn app.main:app --host 127.0.0.1 --port 8000
python -m app.worker
python -m app.scheduler
```

範例以 `http://localhost:8000` 為 base URL。正式環境應傳入對應 tenant 的 API key 或 OIDC token。
Agent 設定的新增、修改、啟停需要 admin；operator 可以提交已設定 Agent 的工作，viewer 可以查詢。

## 建立獨立 Agent

每個 Agent 保存 `name`、`description`、`persona`、`role`、`model`、`policy` 與 `enabled`。
`model` 使用 `/v1/models` 中的 server-controlled alias；省略時採伺服器預設模型。
亦可用 `skills:[{"skill_id":"...","revision":1}]` 綁定最多八個已安裝 Skill。
Skill 版本、權限相容性與撤銷語意見[Skill 管理指南](skill-management.md)。

先建立資安分析 Agent：

```bash
curl -X POST http://localhost:8000/v1/agents \
  -H 'content-type: application/json' -H 'x-api-key: ADMIN_KEY' \
  -d '{
    "name":"Security Analyst",
    "description":"分析 ATR findings、誤報與信任邊界，提出有證據的資安建議。",
    "persona":"你是資安分析師。使用繁體中文，區分規則命中、已確認攻擊與未知情況；不得自行修改 blocking policy。",
    "role":"worker",
    "policy":{"allowed_tools":[],"token_budget":6000}
  }'
```

再建立運維分析 Agent：

```bash
curl -X POST http://localhost:8000/v1/agents \
  -H 'content-type: application/json' -H 'x-api-key: ADMIN_KEY' \
  -d '{
    "name":"Operations Analyst",
    "description":"分析部署、健康檢查與執行失敗，區分觀察、推測與修復建議。",
    "persona":"你是運維分析師。使用繁體中文，依提供的事件資料分析；資料不足時列出需確認的項目。",
    "role":"worker",
    "policy":{"allowed_tools":["scrape_dynamic_page"],"token_budget":6000}
  }'
```

將回傳的兩個 `agent_id` 填入 Chief 設定：

```bash
curl -X POST http://localhost:8000/v1/agents \
  -H 'content-type: application/json' -H 'x-api-key: ADMIN_KEY' \
  -d '{
    "name":"Chief Agent",
    "description":"接收使用者主要任務，依專長委派、追蹤子任務並彙整回報。",
    "persona":"你是 Chief。依已核准 Agent 的職責派工，避免重複任務；彙整時保留來源、分歧與未解問題。",
    "role":"chief",
    "delegate_agent_ids":["SECURITY_AGENT_ID","OPS_AGENT_ID"],
    "policy":{"allowed_tools":[],"token_budget":20000}
  }'
```

Chief 只規劃與彙整，直接工具權限為空。只能委派同 tenant、已啟用且列入清單的 Worker Agent；
Worker 不能繼續委派。模型必須回傳有效的派工 JSON；未知 Agent、遞迴委派、超額或無效計畫會拒絕。
核准 delegate 清單就是授予 Chief 使用這些 Worker 職責／權限的能力；子任務採各 Worker 自己的
政策，而非繼承 Chief 的空工具清單。管理者應為不同委派範圍建立最小權限的 Worker 設定。

```bash
curl -X POST http://localhost:8000/v1/agents/CHIEF_AGENT_ID/goals \
  -H 'content-type: application/json' -H 'x-api-key: OPERATOR_KEY' \
  -d '{"objective":"分析部署失敗的運維與資安影響，提出下一步建議"}'
```

Chief 會進入 `waiting_children`，子任務各有自己的 Goal、persona、模型與 policy snapshot。
所有子任務成功後，Chief 重新入列並產生彙整；子任務失敗／取消則清楚傳遞到主任務。
取消主任務會取消仍未結束的子任務。等待期間不占用執行 slot。

20,000-token 是整組主任務的預設總額度。系統在派工後保留 Chief 的彙整額度，再切分子任務額度；
每個子任務仍受自身 Agent 上限限制。規劃、重試、恢復與彙整都計入，不會因建立子 Goal 而重設總上限。

## 定時 ATR 摘要

以下 job 每天台北時間 09:00 交給指定 Agent 彙整「前一個當地曆日」保存的 finding metadata：

```bash
curl -X POST http://localhost:8000/v1/automations \
  -H 'content-type: application/json' -H 'x-api-key: OPERATOR_KEY' \
  -d '{
    "name":"每日 ATR 摘要",
    "agent_id":"SECURITY_AGENT_ID",
    "task_type":"atr_summary",
    "goal":{"objective":"整理昨日 ATR findings 與人工審查狀態，使用繁體中文"},
    "trigger":{"kind":"schedule","cron":"0 9 * * *","timezone":"Asia/Taipei"},
    "policy":{"allowed_tools":[],"token_budget":20000,"max_runs_per_day":24}
  }'
```

固定間隔可改為 `"trigger":{"kind":"schedule","interval_seconds":3600}`，最小間隔 60 秒。
Cron 使用五欄格式。DST 時區會跳過不存在的春季時間；秋季重複時間保留兩個不同 UTC 時點，仍受合併策略限制。
結構化統計保存在 `goal.atr_summary`，包括時間範圍、rule、severity 與 review status；它只代表尚未到期的
metadata，不能當成全量攻擊紀錄或 production 偵測率。

## 部署失敗 Webhook

建立 `trigger` 為以下內容的 automation，並可指定 Chief 的 `agent_id`：

```json
{"kind":"webhook","event_types":["deployment.failed"]}
```

在服務端 secret 設定加入該 automation ID 的 HMAC key，至少 32 字元：

```text
AUTOMATION_WEBHOOK_SECRETS_JSON={"AUTOMATION_ID":"a-server-managed-secret-at-least-32-characters"}
```

更新 secret 後重啟 Gateway。將 JSON 原始 bytes 作為簽章內容：

```text
POST /v1/automations/hooks/TENANT_ID/AUTOMATION_ID
x-automation-timestamp: <Unix seconds>
x-automation-signature: sha256=<HMAC_SHA256(secret, timestamp + "." + raw_body)>
```

```json
{
  "event_id":"deployment-20261025-001",
  "type":"deployment.failed",
  "data":{"service":"checkout","reason":"readiness timeout"}
}
```

時間戳允許前後五分鐘，body 上限 64 KiB。同一 automation 的相同 `event_id` 在 30 天內去重。
重新簽章重送也不會建立第二個 Goal。Tenant、Agent、persona、模型與 policy 由服務端設定取得；
event data 只作為不可信分析資料。`source_automation_id` 不接受外部 webhook 宣告。

## Gateway 內部事件

可建立 `kind=internal` 的 trigger，例如：

```json
{"kind":"internal","event_types":["threat.detected"],"rule_ids":["ATR-2026-00213"],"severities":["high","critical"]}
```

或使用 `"event_types":["goal.failed"]`。Rule 與 severity 必須在同一 finding 上符合條件。
內部事件只保存必要 metadata；自動化產生的 Goal／finding 預設不再觸發其他 automation。

## 查詢、核准與停止

| 操作 | API |
| --- | --- |
| 列出 Agent | `GET /v1/agents` |
| 修改 Agent | `PUT /v1/agents/{id}`，完整設定與 `expected_version` |
| 啟停 Agent | `POST /v1/agents/{id}/enabled`，`{"enabled":false}` |
| 列出工作 | `GET /v1/automations` |
| 修改工作 | `PUT /v1/automations/{id}`，完整設定與 `expected_version` |
| 停止新觸發 | `POST /v1/automations/{id}/enabled`，`{"enabled":false}` |
| 手動觸發 | `POST /v1/automations/{id}/run` |
| 查詢各次結果 | `GET /v1/automations/{id}/goals` |
| 工作生命週期 SSE | `GET /v1/automations/{id}/events` |
| Goal 詳情／子任務 | `GET /v1/goals/{goal_id}` |
| Goal 詳細 SSE | `GET /v1/goals/{goal_id}/events` |
| 取消正在執行的 Goal | `POST /v1/goals/{goal_id}/cancel` |

SSE 支援 `Last-Event-ID`。查看 `children_goal_ids` 可繼續查詢各個子 Goal。修改設定不會重寫已經建立的
Goal snapshot；Agent 停用則會在下一次模型／工具邊界阻止繼續執行。

寫入工具會進入 `waiting_approval`。讀取 `pending_approval` 後，傳入當次 `approval_id`：

```bash
curl -X POST http://localhost:8000/v1/goals/GOAL_ID/approval \
  -H 'content-type: application/json' -H 'x-api-key: OPERATOR_KEY' \
  -d '{"approval_id":"PENDING_APPROVAL_ID","approved":true,"reason":"已確認目標與本次操作"}'
```

同意只綁定當次工具與參數 hash，不能放行下一次寫入。`browser_type` 的通知以長度取代原文，因此需搭配
原始任務確認預期輸入。預設 24 小時未核准或明確拒絕會取消該 Goal；期限可設定。

## 執行與恢復語意

- 同一 automation 同時只有一個 active Goal。新觸發合併成一筆 pending，保留最新資料與累積次數。
- 停機漏跑合併為一次；達每日次數上限的工作持久延後，不因 worker 或 scheduler 重啟遺失。
- 預設每日 24 次、每次 20,000 tokens；管理者可調整。Job 與 Agent 的工具、domain 及額度取較小範圍。
- 多個 worker 使用 execution lease、heartbeat 與 owner fencing；重啟不搶走其他健康 worker 的工作。
- Queue 保留待處理項目。Redis AOF、volume 與可用容量是持久性的必要條件；目前使用單一 logical Redis，
  不宣稱支援 Redis Cluster 的跨 hash-slot transaction。
- 外部寫入與 Redis checkpoint 不能形成同一交易。若工具曾嘗試執行但 checkpoint 未確認，系統會停止並要求
  operator 查核外部結果，不盲目重試。
- ATR 在入列及執行時重新檢查，tool response 在進模型前檢查；human approval 不會略過 ATR。
- 指定 per-job domain scope 時，Browser context 也套用該限制；目前 scoped context 拒絕 HTTP redirect 與
  WebSocket。伺服器的 SSRF／全域 allow-list 仍同時生效。
- 未回報 usage 的模型採保守估計；取消／失敗呼叫保留預留量，不能把未知用量記為零。
- Goal、工作事件、tool receipt 與 delegation 記錄目前無自動清除；需依部署容量與保存政策規劃歸檔。

## 驗證

```bash
ruff check .
TEST_REDIS_URL=redis://127.0.0.1:16379/0 pytest -q
sh scripts/test_docker_stack.sh
sh scripts/test_proactive_stack.sh
```

`TEST_REDIS_URL` 必須指向測試專用 Redis 8（含 JSON／Search）的 DB 0，因 Search 索引不支援其他 DB。
沒有設定時，真正 Redis 的測試會明確 skip；
CI 會提供 Redis 執行這些測試。Docker 使用 deterministic mock inference 驗證編排，模型分析品質需另行評估。
