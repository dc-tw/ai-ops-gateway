# Skill 管理操作指南

Skill 是 tenant 管理者保存、可指定內容版本的 instruction pack。Agent 綁定明確的 Skill revision，
建立 Goal 時取得獨立快照；Skill 不會新增工具、執行腳本或替 Agent 授權。

目前提供 API，沒有管理前端、自動選 Skill、外部 Skill package 匯入、執行資源或新增 MCP server
的流程。已發布的 revision 保留歷史，不提供 DELETE；不再使用時應停用。

Gateway plan mode 的 session／計畫快照已提供[獨立 API](plan-mode.md)。`plan-forecast` 與
plan-only Skill 綁定仍是[設計提案](designs/plan-forecast/design.md)，不屬於下列十項內建 Skill；
不能藉由上傳草案或加入 tags 啟用模式限制或估算功能。

## 權限、儲存與容量

Skill 與 Agent 的設定寫入需要 `admin`，查詢與匯出需要至少 `viewer`；使用已設定 Agent 提交
Goal 需要至少 `operator`。資料依驗證憑證的 tenant 隔離，不由 request body 指定 tenant。
API key 與 OIDC 使用既有身分驗證設定。

管理 tenant Skill 需要 Redis；未啟用 Redis 時回傳 503。內建 catalog 的查詢不需要 Redis。
Skill 的 `name` 是 1–64 字元小寫英數與單一連字號組成的 slug，建立後不可改名。
若需改名，請建立另一個 Skill。

每個 Skill 的 `instructions` 最多 8,000 字元，且不得超過 **12,000 UTF-8 bytes**；
每個 Agent 最多綁定 **8 個不同 Skill**，全部綁定 revision 的 instructions 合計最多
**16,000 UTF-8 bytes**。這些是文字容量限制，不是額外 token 額度。Skill 指引會進入模型輸入，
仍計入原本的 token 預算。

## 內建目錄

用途、示範輸入及驗收情境見 [內建 Skill 目錄](skills-catalog.md)。目前共有 10 項：

| Slug | 名稱 | 角色 | 必要工具 |
|---|---|---|---|
| `source-summary` | 來源摘要 | Chief、Worker | 無 |
| `action-extraction` | 待辦事項擷取 | Chief、Worker | 無 |
| `option-comparison` | 選項比較 | Chief、Worker | 無 |
| `evidence-review` | 證據檢視 | Chief、Worker | 無 |
| `investigation-plan` | 調查計畫 | Chief、Worker | 無 |
| `incident-triage` | 事件初判 | Chief、Worker | 無 |
| `atr-finding-review` | ATR 命中檢視 | Chief、Worker | 無 |
| `handoff-report` | 工作交接報告 | Chief、Worker | 無 |
| `web-evidence` | 指定網頁查證 | Worker | `scrape_dynamic_page` |
| `chief-coordination` | Chief 派工與彙整 | Chief | 無 |

## API 一覽

下表路徑均相對於 Gateway base URL。列表回應中的 Skill 摘要省略 `instructions`；
查看單一 Skill、內建項目或指定 revision 才取得完整內容。

| 方法與路徑 | 最低權限 | 用途／request body |
|---|---|---|
| `GET /v1/skills/builtins` | viewer | 列出內建目錄摘要。 |
| `GET /v1/skills/builtins/{name}` | viewer | 查看內建項目的完整內容。 |
| `POST /v1/skills/install` | admin | `{"name":"source-summary"}`，安裝到目前 tenant。 |
| `POST /v1/skills` | admin | 建立自訂 Skill，可附 `enabled`，預設為 true。 |
| `GET /v1/skills` | viewer | 列出目前 tenant 已安裝或自訂的 Skill 摘要。 |
| `GET /v1/skills/{skill_id}` | viewer | 查看目前 Skill head、內容及設定。 |
| `PUT /v1/skills/{skill_id}` | admin | 完整內容欄位加 `expected_version`，發布新 revision。 |
| `POST /v1/skills/{skill_id}/revisions` | admin | 與上述 PUT 相同的發布操作。 |
| `GET /v1/skills/{skill_id}/revisions?after=0&limit=50` | viewer | 歷史摘要；`after` 為已讀 revision，`limit` 為 1–100，回傳 `next_after`。 |
| `GET /v1/skills/{skill_id}/revisions/{revision}` | viewer | 查看指定的不可變內容版本。 |
| `POST /v1/skills/{skill_id}/enabled` | admin | `{"enabled":false,"expected_version":3}`，啟用或停用。 |
| `POST /v1/skills/{skill_id}/rollback` | admin | `{"revision":1,"expected_version":2}`，以舊內容發布新 revision。 |
| `GET /v1/skills/{skill_id}/export?revision=1` | viewer | 匯出指定 revision；省略 query 時使用目前 head 內容。 |
| `POST /v1/agents` | admin | 建立 Agent，以 `skills` 綁定版本。 |
| `PUT /v1/agents/{agent_id}` | admin | 完整 Agent 設定加 Agent 的 `expected_version`。 |
| `POST /v1/agents/{agent_id}/goals` | operator | 使用已保存的 Agent 設定建立手動 Goal。 |

建立或發布的內容欄位為 `name`、`display_name`、`description`、`instructions`、
`required_tools`、`agent_roles` 與 `tags`。後三者可省略，預設依序為 `[]`、
`["chief","worker"]` 與 `[]`。發布為完整內容替換，不是 PATCH；省略選填欄位會採預設值。
不要把 GET 回應中的 ID、版本、時間或其他伺服器欄位原樣加回內容 body。

常見錯誤為：403（權限、內容政策或 ATR 阻擋）、404（目前 tenant 中不存在的 Skill／revision）、
409（名稱或版本衝突）、422（欄位或相容性不合法）。Agent 儲存時的不相容綁定回傳 422；
已保存設定在建立 Goal 時不再符合條件，則可能回傳 409。
Skill request schema 驗證失敗固定回 422 `{"detail":"Invalid Skill request"}`，不回顯
原始 input／欄位位置／錯誤上下文；所有內容文字須為有效 UTF-8，畸形 Unicode 不會存入
revision 或因此變成 500。合法繁中與 emoji 的 v1 canonical hash 維持原規則。

## 查詢與安裝內建 Skill

以下範例以 `http://localhost:8000` 為 base URL。先把金鑰改成同一 tenant 的有效憑證；
若使用同一組 admin key，也可填入各權限變數。範例使用者端的 curl 不會成為 Skill 執行資源。

```bash
SKILLS_BASE_URL='http://localhost:8000'
SKILLS_ADMIN_KEY='replace-with-admin-key'
SKILLS_VIEWER_KEY='replace-with-viewer-key'
SKILLS_OPERATOR_KEY='replace-with-operator-key'

curl --fail-with-body "$SKILLS_BASE_URL/v1/skills/builtins" \
  -H "x-api-key: $SKILLS_VIEWER_KEY"

curl --fail-with-body "$SKILLS_BASE_URL/v1/skills/builtins/source-summary" \
  -H "x-api-key: $SKILLS_VIEWER_KEY"

curl --fail-with-body -X POST "$SKILLS_BASE_URL/v1/skills/install" \
  -H 'content-type: application/json' \
  -H "x-api-key: $SKILLS_ADMIN_KEY" \
  -d '{"name":"source-summary"}'
```

安裝回應包含 tenant 所有的 `skill_id`、`revision:1`、`version:1`、`origin:"builtin"`
及內建模板的 `builtin_version`。同一 tenant 重複安裝相同內建名稱會回傳既有 Skill；
不會覆蓋自訂內容、不會增加 revision，也不會重新啟用已停用的 Skill。若同名自訂 Skill 已存在，
則回傳 409。

內建 catalog、tenant 安裝副本與 Agent 綁定是三件不同的事。catalog 更新或重複安裝都不會自動
升級已存在的副本；要採用新指引，需明確發布內容，再更新 Agent 指定的 revision。

## 建立自訂 Skill 並綁定 Agent

```bash
curl --fail-with-body -X POST "$SKILLS_BASE_URL/v1/skills" \
  -H 'content-type: application/json' \
  -H "x-api-key: $SKILLS_ADMIN_KEY" \
  -d '{
    "name":"daily-handoff",
    "display_name":"每日工作交接",
    "description":"整理每日已完成工作、待確認事項與下一步。",
    "instructions":"僅在需要整理工作交接時套用。區分已完成、待確認與下一步，保留提供的數字及日期，缺漏資料標為未知。依目前階段要求的格式回覆。",
    "required_tools":[],
    "agent_roles":["worker"],
    "tags":["交接","每日"],
    "enabled":true
  }'
```

將回應的 `skill_id` 複製到下一段的 `SKILLS_CUSTOM_ID`。此範例假設是新建的
`revision:1`、`version:1`；若名稱已存在，請先查詢既有資料，不要假設版本仍是 1。

```bash
SKILLS_CUSTOM_ID='replace-with-created-skill-id'

curl --fail-with-body -X POST "$SKILLS_BASE_URL/v1/agents" \
  -H 'content-type: application/json' \
  -H "x-api-key: $SKILLS_ADMIN_KEY" \
  -d "{
    \"name\":\"每日交接助理\",
    \"description\":\"根據提供的工作紀錄整理交接。\",
    \"persona\":\"使用繁體中文，忠實整理提供的資料。\",
    \"role\":\"worker\",
    \"policy\":{\"allowed_tools\":[],\"token_budget\":6000},
    \"skills\":[{\"skill_id\":\"$SKILLS_CUSTOM_ID\",\"revision\":1}]
  }"
```

每個綁定必須是 `{"skill_id":"...","revision":1}`，不可省略 revision、使用 `latest`，
或讓同一 Skill 同時綁定兩個版本。系統檢查同 tenant、Skill 啟用狀態、revision 存在、角色、
必要工具與合計容量。`skills:[]` 表示不綁定 Skill；舊有未設定此欄位的 Agent 行為保持原狀。

將 Agent 回應的 `agent_id` 複製到以下變數，即可提交一個固定當時版本的 Goal：

```bash
SKILLS_AGENT_ID='replace-with-created-agent-id'

curl --fail-with-body -X POST "$SKILLS_BASE_URL/v1/agents/$SKILLS_AGENT_ID/goals" \
  -H 'content-type: application/json' \
  -H "x-api-key: $SKILLS_OPERATOR_KEY" \
  -d '{
    "objective":"整理今日交接：資料整理已完成；驗證尚未開始；外部核准仍在等待。",
    "steps":[{"query":"依提供的紀錄產出交接摘要，缺漏負責人或期限時標為未定。"}]
  }'
```

## 內容版本、設定版本與明確升級

`revision` 是不可變的內容版本，Agent 綁定的是這個數字。`version` 是 Skill 目前設定的
併發控制版本；發布內容及實際啟停狀態變更都會增加它。寫入時的 `expected_version` 必須等於
目前 head 的 `version`，不是 Agent 綁定的 revision。衝突時先 GET 最新設定再決定如何更新，
不要盲目重送舊數字。

`builtin_version` 只記錄內建模板來源版本，不能用於綁定或併發控制。Agent 自己另有 `version`；
更新 Agent 的 `expected_version` 應取自 Agent 回應，不能使用 Skill 的 version。

下例延續新建自訂 Skill，發布 revision 2：

```bash
curl --fail-with-body -X POST "$SKILLS_BASE_URL/v1/skills/$SKILLS_CUSTOM_ID/revisions" \
  -H 'content-type: application/json' \
  -H "x-api-key: $SKILLS_ADMIN_KEY" \
  -d '{
    "name":"daily-handoff",
    "display_name":"每日工作交接",
    "description":"整理每日已完成工作、待確認事項與下一步。",
    "instructions":"僅在需要整理工作交接時套用。區分已完成、待確認與下一步，保留提供的數字、日期與驗證證據。缺漏資料標為未知，不把尚未驗證的工作寫成成功。依目前階段要求的格式回覆。",
    "required_tools":[],
    "agent_roles":["worker"],
    "tags":["交接","每日"],
    "expected_version":1
  }'
```

回應為 `revision:2`、`version:2`。Agent 仍綁定 revision 1；已建立的 Goal 也保留原快照。
若要讓後續 Goal 採用 revision 2，須更新 Agent 的完整設定。下例假設前述 Agent 尚未被修改，
其 `version` 仍是 1：

```bash
curl --fail-with-body -X PUT "$SKILLS_BASE_URL/v1/agents/$SKILLS_AGENT_ID" \
  -H 'content-type: application/json' \
  -H "x-api-key: $SKILLS_ADMIN_KEY" \
  -d "{
    \"name\":\"每日交接助理\",
    \"description\":\"根據提供的工作紀錄整理交接。\",
    \"persona\":\"使用繁體中文，忠實整理提供的資料。\",
    \"role\":\"worker\",
    \"policy\":{\"allowed_tools\":[],\"token_budget\":6000},
    \"skills\":[{\"skill_id\":\"$SKILLS_CUSTOM_ID\",\"revision\":2}],
    \"expected_version\":1
  }"
```

實務上先 GET Agent，保留完整 `policy`、`model`、`enabled`、`delegate_agent_ids` 及其餘綁定，
再提交完整可寫設定；這個範例的省略欄位會採預設值，不應直接套用到不同的既有 Agent。

### 回復舊內容仍建立新 revision

若要回復 revision 1 的內容，以下操作會建立 **revision 3、version 3**，不改寫 revision 1 或 2：

```bash
curl --fail-with-body -X POST "$SKILLS_BASE_URL/v1/skills/$SKILLS_CUSTOM_ID/rollback" \
  -H 'content-type: application/json' \
  -H "x-api-key: $SKILLS_ADMIN_KEY" \
  -d '{"revision":1,"expected_version":2}'

curl --fail-with-body "$SKILLS_BASE_URL/v1/skills/$SKILLS_CUSTOM_ID/revisions?after=0&limit=50" \
  -H "x-api-key: $SKILLS_VIEWER_KEY"

curl --fail-with-body "$SKILLS_BASE_URL/v1/skills/$SKILLS_CUSTOM_ID/revisions/3" \
  -H "x-api-key: $SKILLS_VIEWER_KEY"
```

新 revision 的歷史紀錄帶有 `restored_from_revision:1`。Agent 不會自動改成 revision 3；
若要採用回復內容，仍需明確更新其綁定。停用／啟用不建立內容 revision，也不會隨 rollback 回復。

## 停用、撤銷 epoch 與重新啟用

Skill 停用除了將 `enabled` 設為 false，也會增加 `revocation_epoch`。快照固定保存取得時的
epoch；系統在入列及後續模型／工具執行邊界重新檢查啟用狀態與 epoch。停用後，依賴這個 Skill
的舊 Goal 快照永久失效，包括排隊、待核准、重試或恢復中的工作。

下例延續上面的 rollback 回應，其 Skill `version` 為 3：

```bash
curl --fail-with-body -X POST "$SKILLS_BASE_URL/v1/skills/$SKILLS_CUSTOM_ID/enabled" \
  -H 'content-type: application/json' \
  -H "x-api-key: $SKILLS_ADMIN_KEY" \
  -d '{"enabled":false,"expected_version":3}'

curl --fail-with-body -X POST "$SKILLS_BASE_URL/v1/skills/$SKILLS_CUSTOM_ID/enabled" \
  -H 'content-type: application/json' \
  -H "x-api-key: $SKILLS_ADMIN_KEY" \
  -d '{"enabled":true,"expected_version":4}'
```

若原本 epoch 為 0，停用後為 `version:4`、`revocation_epoch:1`；重新啟用後為
`version:5`、`revocation_epoch:1`。
revision 均維持 3。重新啟用只允許之後新建的 Goal 取得新 epoch，**不會讓停用前的 Goal 復活**。
Agent 綁定仍可指向既有 revision，但必須重新建立 Goal。相同 enabled 狀態的重送仍要求正確的
`expected_version`，不會再增加 version 或 epoch。

停用 API 不會批次改寫所有 Goal 的狀態：例如等待核准的 Goal 可能仍顯示 `waiting_approval`，
但即使之後核准，也無法通過舊 epoch 的執行檢查。若需立即結束等待並釋放預留額度，請取消該 Goal。

撤銷不是外部交易回滾：已在途的模型或工具 RPC 可能完成，已經發生的外部副作用不能復原；
系統會在後續受檢查的邊界阻止繼續。需要中止工作時，可依既有 Goal 控制流程取消並核對結果。
停用保留 Skill、歷史 revision 與稽核資料，不提供刪除歷史的 API。

## 不同入口的快照與權限

| 入口 | 快照行為 |
|---|---|
| 手動 Agent Goal | 建立時解析保存的 Agent 綁定，固定 revision、內容雜湊與當時撤銷 epoch。修改 Agent 或發布 Skill 不會重寫此 Goal。 |
| 綁定 Agent 的 automation | 每次 materialize 新 Goal 時，依當時保存的 Agent 綁定與有效政策獨立解析；已入列或合併等待中的 Goal 保留自己的快照。 |
| Chief 主任務 | 使用 Chief 自己綁定的 Skill 快照；規劃仍回傳系統指定的 JSON，彙整使用主任務的既有快照。 |
| Chief 的 Worker 子任務 | 建立子 Goal 時解析該 Worker 自己的綁定，使用獨立快照；不繼承 Chief 的 Skill 或把 Chief 的空工具清單當作 Worker 政策。 |

因此更新 Agent 綁定會影響後來建立的手動 Goal、automation occurrence 或 Worker 子 Goal，
不是替既有 Goal 升級。若 automation 的額外政策縮小工具範圍，以縮小後的有效政策檢查必要工具；
不能藉 Skill 綁定把工具加回來。已 materialize 的 Goal 即使稍後才重新入列，也須通過撤銷檢查。

`required_tools` 只是相容性需求。例如 `web-evidence` 要求 `scrape_dynamic_page`，
必須先由管理者在 Worker 的 `policy.allowed_tools` 中授權；Skill 本身不授權工具、網域、模型
或新增 MCP。Chief 不能直接使用工具，因此不能綁定這個 Worker 專用 Skill。

Skill 定義仍經內容政策與 ATR 掃描，執行時的指引及其他輸入也維持既有檢查。Skill 不會略過
ATR、逐次人工核准、網域限制、角色與單層派工限制，也不會提高 token／每日額度。
需要寫入核准的 browser click／type 仍走既有核准流程；Skill 沒有額外的腳本執行路徑。

## 匯出範圍與基本格式

```bash
curl --fail-with-body "$SKILLS_BASE_URL/v1/skills/$SKILLS_CUSTOM_ID/export?revision=1" \
  -H "x-api-key: $SKILLS_VIEWER_KEY" \
  --output daily-handoff-SKILL.md
```

匯出是單一 Markdown 檔，包含 YAML frontmatter 的 `name`、`description`，以及 Gateway 的
`metadata`（Skill ID、revision、內容雜湊、必要工具名稱），本文為 instructions。
下載名稱為 `{name}-SKILL.md`；指定 revision 可匯出歷史內容，省略時匯出目前 head 內容。
它不是完整設定備份：不會保留 Agent 綁定、啟用狀態、撤銷 epoch 或完整管理歷史。

此輸出參考 [Agent Skills 規格](https://agentskills.io/specification) 的基本
`SKILL.md` metadata 加 Markdown instructions 結構，**不宣稱完整相容**。
目前不支援完整 package 匯入、目錄管理、`scripts/`、`references/`、`assets/` 等資源的載入與執行，
也不會把匯出 metadata 的工具名稱解讀成授權。若要將檔案放入其他產品，仍須依其格式、目錄與
權限規則另行驗證。

Skill、revision 與 audit 目前沒有自動歸檔或刪除排程；部署時需規劃 Redis AOF／備份、容量與保存政策。
不要把密碼、API key 或使用者隱私直接寫入可由 tenant viewer 查閱的 Skill 指令。
