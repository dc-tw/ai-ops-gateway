# Gateway 規劃模式與計畫快照

本階段實作 Gateway 自己的 plan mode，不是 Codex `/plan`。提供規劃 session、不可變計畫
revision、設定快照及明確的模式切換；**不呼叫模型、不執行工具、不建立或入列 Goal**。

`execute` 目前只表示接受某一版完整計畫，並鎖住編輯，**不代表已開始執行**。
時間／token 預測、plan-only Skill、從接受的計畫啟動 Goal，仍屬後續階段。
既有 Chief 的模型拆解、手動 Goal 與 automation 維持原行為，不會自動變成規劃 session；
建立 session 也不會停止同 Agent 的其他工作。
不要在既有 Goal endpoint 放入 `mode: plan` 期待 dry run；該入口仍會建立執行工作，未知
metadata 不會切換規劃模式。需要零執行副作用時，使用本頁的 planning API。

## 狀態、權限與快照

每個 tenant 可以為同一 Agent 建立多個獨立 session，模式不寫入 Agent 全域設定。
建立時為 `mode: plan`、`version: 1`、`plan_revision: 0`。

```text
建立 session → 保存計畫 r1 → 修改成 r2 → 明確切換 execute，接受 r2
                    ↑                         ↓
                    └──── 明確回到 plan ───────┘
```

- operator／admin 可建立、保存、refresh 與切換模式；viewer 可查詢 session 與歷史 revision。
  tenant 取自已驗證的 principal，跨 tenant 與不存在資源均回 404。
- `expected_version` 是 session 的 CAS 版本，不是 plan revision；兩者都需要正整數，不接受
  字串或 boolean。每次保存計畫增加兩種版本；切換模式只增加 session version。
- 已保存的 revision 不覆寫，內容 hash 綁定 schema version、計畫、完成度與完整設定快照。
  GET 會檢查 identity／schema／hash；hash 只做完整性核對，不是數位簽章或防竄改 audit chain。
- 快照包含 root Agent 及 Chief allowlist 中所有 Worker 的 persona、模型、政策、各自釘住的
  Skill revision／指令／hash／撤銷 epoch，以及安全模型路由識別。Worker 不繼承 Chief 的 Skill。
- Agent 版本、模型設定、Skill 綁定或撤銷 epoch 改變時，session 的 `configuration_status`
  變成 `stale`；保存／接受舊設定回 409，必須明確 refresh 或新建 session。
  單純發布 Skill 新 head 不會升級釘住的舊 revision，也不會單憑 head 更新使 session 失效。
- refresh 只允許 plan，重新取得設定；有計畫時複製為新 revision，舊內容與舊快照保留。
  新設定與舊計畫角色不相容時拒絕 refresh，應建立新 session 與相容計畫。
- execute 不允許保存或 refresh；回到 plan 可清除接受參照，不會取消任何 Goal。
  `active_goal_id` 為未來執行銜接保留；若有非終態工作關聯則拒絕回 plan。本階段不建立該關聯。

設定的 WATCH、session CAS、revision 保存、接受參照、metadata event 與 audit 在同一 Redis
交易提交。競爭或設定變動時不會默默覆蓋；持續競爭超過有限重試後回 409，呼叫端需重新查詢。

## API

以下路徑相對於 Gateway base URL；需要 Redis，不可用時回 503。

| 方法與路徑 | 用途 |
|---|---|
| `POST /v1/agents/{agent_id}/planning-sessions` | body `{}` 或省略；建立 plan session 與設定快照。 |
| `GET /v1/planning-sessions/{session_id}` | 查詢版本、模式、設定快照與目前計畫參照。 |
| `POST /v1/planning-sessions/{session_id}/plans` | `{expected_version, plan}`，保存完整計畫的新 revision。 |
| `GET /v1/planning-sessions/{session_id}/plans?after=0&limit=50` | 分頁歷史，回 `{plans, next_after}`；limit 1–100。 |
| `GET /v1/planning-sessions/{session_id}/plans/{revision}` | 讀取不可變計畫、完成度、設定快照與 hash。 |
| `POST /v1/planning-sessions/{session_id}/refresh` | `{expected_version}`，明確重新固定設定。 |
| `POST /v1/planning-sessions/{session_id}/mode` | `{expected_version, mode, plan_revision?}`，切換模式，不入列。 |

`GET session` 的 `configuration_status` 是查詢當下的觀測，不是未來執行授權。
`execution_started: false` 表示此階段未從 session 啟動工作；沒有 `/forecasts` 或 `/execute` endpoint。
metadata 事件已持久化，但本階段尚未提供 planning 專用 SSE／event API。

常見拒絕：401 未驗證、403 權限／內容／ATR、404 tenant 中不存在、409 版本／模式／設定衝突
或 integrity 異常、422 不合法內容、429 tenant request rate limit。變更規劃計入 request rate，
不預留 run slot、不扣模型 tokens，也不消耗 Agent 每日 Goal 次數。
Planning request schema 驗證失敗固定回 422 `{"detail":"Invalid planning request"}`，不回傳
原始 input／欄位位置／錯誤上下文，畸形 Unicode 亦不會因此變成 500；既有 API 格式不變。

## 保存計畫與接受範例

先依[Agent 指南](proactive-agents.md)建立 Worker。以下 ID 與金鑰須換成同一 tenant 的實際資料。

```bash
PLANNING_BASE_URL='http://localhost:8000'
PLANNING_API_KEY='replace-with-operator-key'
PLANNING_AGENT_ID='replace-with-worker-agent-id'

curl --fail-with-body -X POST \
  "$PLANNING_BASE_URL/v1/agents/$PLANNING_AGENT_ID/planning-sessions" \
  -H "x-api-key: $PLANNING_API_KEY" -H 'content-type: application/json' -d '{}'
```

把回應的 `session_id` 填入下方。範例僅適用於尚未修改的 session：

```bash
PLANNING_SESSION_ID='replace-with-created-session-id'

curl --fail-with-body -X POST \
  "$PLANNING_BASE_URL/v1/planning-sessions/$PLANNING_SESSION_ID/plans" \
  -H "x-api-key: $PLANNING_API_KEY" -H 'content-type: application/json' \
  -d '{
    "expected_version":1,
    "plan":{
      "objective":"整理提供的 ATR finding metadata，指出需要人工複核的事項。",
      "context":"目前沒有已確認的攻擊結論。",
      "steps":[{"query":"整理規則、嚴重度與審查狀態，保留未知事項。"}]
    }
  }'

curl --fail-with-body \
  "$PLANNING_BASE_URL/v1/planning-sessions/$PLANNING_SESSION_ID/plans/1" \
  -H "x-api-key: $PLANNING_API_KEY"

curl --fail-with-body -X POST \
  "$PLANNING_BASE_URL/v1/planning-sessions/$PLANNING_SESSION_ID/mode" \
  -H "x-api-key: $PLANNING_API_KEY" -H 'content-type: application/json' \
  -d '{"expected_version":2,"mode":"execute","plan_revision":1}'
```

最後一步回 `mode: execute`、`version: 3`，保存 `accepted_revision`／`accepted_hash`；**沒有開始
執行**。需要繼續修改時，先以 version 3 明確切回 plan，再使用新回傳的 version 保存計畫。
遇到 409 先 GET 查詢，不盲目遞增版本重送。

`plan` 支援 objective、context、steps、assignments、max_retries（預設 2）、max_tool_rounds
（預設 8）與 step_deadline_seconds（預設 300）。不接受自行指定 tenant、model、policy、Skill
快照或已授權工具。context 是明確提供的文字，不會暗中讀取變動中的 conversation history。

Worker 的 assignments 必須空，steps 有內容才完整。Chief 的 root steps 必須空，使用以下
assignments 格式，Agent ID 必須是已允許的 Worker，且每位只能出現一次：

```json
{
  "objective": "分別整理資安與運維證據，再由 Chief 彙整。",
  "assignments": [
    {
      "agent_id": "replace-with-approved-worker-id",
      "objective": "整理資安證據。",
      "steps": [{"query": "核對提供的 finding metadata，不推定攻擊成功。"}]
    }
  ]
}
```

未填 steps／assignments 可保存為不完整草稿，但不能切 execute。僅支援線性 Worker 或單層
Chief fork／join；沒有循環、任意依賴圖、模型自動起草或遞迴派工。

## 安全、容量與目前限制

- 計畫依設定做 PII redaction、內容政策與 ATR 掃描後保存。若文字超過設定的完整 ATR 掃描
  長度則拒絕，不把截斷掃描當成完整檢查。規劃 ATR finding 可保存，但不發布 internal trigger，
  避免規劃間接啟動 automation。refresh 遇到新的 redaction 規則需要改文時拒絕，應新建 session。
- 每份正規化計畫最多 64 KiB UTF-8，設定快照最多 512 KiB；每 tenant 最多 1,000 sessions，
  每 session 最多 1,000 revisions。沒有 DELETE、自動 TTL 或歸檔，須監控 Redis 容量並備份。
- 完整計畫／persona／Skill 指令只在 tenant 受權限保護的紀錄中，viewer 可讀；audit 與事件只含
  ID、版本、hash、mode、subject 與時間。不應把密碼、API key 或機密資料寫進計畫。
- 模型路由只保存 alias、configured model、安全指紋與版本未知聲明，不回傳內部 URL／API key。
  含 URL userinfo、query 或 fragment 的 inference endpoint 無法建立規劃識別，會拒絕；既有
  inference endpoint 行為不因此改變。
- 指紋只能辨識目前服務 process 的 endpoint／model／生成設定，不能證明遠端模型權重、部署或
  tokenizer 版本。尚無跨 process 的中央模型設定 epoch／原子熱更新，P3 執行銜接前仍須補齊。
- 既有 Skill v1 canonical hash 不變；此處保存的是**未來執行設定**，不會在 plan mode 注入 Skill
  或呼叫模型。`planning_skills`、Skill v2 模式 gate 與 `plan-forecast` 未啟用。
- 切 execute 不是人工核准或執行許可，也不略過未來執行時的 ATR、工具政策與逐次寫入核准。

完整後續設計見 [plan-forecast](designs/plan-forecast/design.md)。
本次測試、Docker 觀察與限制見[驗證紀錄](plan-mode-verification-2026-10-26.md)。
最新跨功能回歸、未支援能力與正式環境缺口見[封版彙總](releases/2026-10-26-skills-plan-mode.md)。
