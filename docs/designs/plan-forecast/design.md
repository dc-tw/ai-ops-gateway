# Gateway 規劃模式與 plan-forecast 設計

日期：2026-10-26；2026-10-27 更新。狀態：**M5 核心已實作並驗證**。

使用者已確認：這裡的 plan mode 指 Gateway Agent 的規劃模式，不是 Codex 的 `/plan`。
已實作的 session／計畫版本／refresh／mode API 與限制見[操作指南](../../plan-mode.md)。
實際 API、環境變數與已驗證邊界以[操作指南](../../plan-forecast.md)為準。本文保留原始設計推導；
其中「擬議」「待實作」段落是歷史文字，不可用來否定後續 M3–M5 已交付能力，也不可把仍屬
M7／M9 的 SSE、UI、真實模型校正當成已完成。

## 1. 目標與非目標

在規劃模式中，對「目前保存的這一版計畫」預估執行所需的 input／output／total tokens，
以及機器執行時間；使用實際生效的 Agent／模型設定，支援 Chief 與不同模型的 Worker。
結果呈現低用量、基準、高用量三種情境、假設與資料缺口，不承諾單一精確完成時間。

第一版沿用 API 與中文文件，不新增管理前端、自動跑 benchmark、模型探測或任意腳本工具。
Forecast 不執行計畫、不派工、不預留執行 slot／Goal 額度、不自動開始工作，也不提高工具權限。
估算服務自身須有 tenant 存取、請求頻率、計畫大小及計算量限制。

## 2. 設計起點與後續能力

以下為提案起點的限制；本次已另以 planning API 補上 session 與計畫 artifact，不是在原本的
Agent／Goal request 夾帶 mode。其餘缺口仍依後續階段處理。

| 現有程式 | 設計上的限制 |
|---|---|
| `app/api/schemas.py` 的 Agent／Goal request | 沒有規劃模式或版本化 Plan artifact。 |
| `app/worker.py` 的 `_plan_steps`、Chief delegation | 是執行中的模型呼叫，可能建立子 Goal；不能拿來當無副作用的 forecast。 |
| `app/agents.py` 的 `apply_agent` | 已保存的 Agent model 會覆蓋 Goal request 的 model；估算不能只看 request。 |
| `app/llm/client.py` 的 ModelRouter | 有 alias 與實際 model／endpoint 對應，尚無版本化速度校正資料。 |
| `app/core/execution_policy.py` 的 BudgetedModel | UTF-8 bytes 與 output cap 用於保守預留額度，不是預期 token 用量。 |
| Skill registry | 目前只有執行指令快照，沒有 `execution_modes`、`planning_skills` 或模式過濾。 |
| usage、TTFT、`runtime_seconds` 與 latency scripts | 不足以提供任意模型的逐呼叫 token／時間聯合分布。 |

既有 mock latency 只反映測試編排；2026-10-24 的 Ollama 0.5b CPU 小樣本不能套用到預設
7b 或其他模型。SSE token event／chunk 數也不是 provider tokens。

## 3. 規劃模式與計畫生命週期

模式屬於 tenant 下的 **PlanningSession**，不改變 Agent 的全域狀態；同一 Agent 可有多個
互不影響的 session。服務端保存 `mode: plan | execute` 與 CAS `version`，只接受已驗證身分的
明確操作。prompt、tags、模型輸出或 body 中自行聲稱的 mode 都不能切換模式。

```text
明確建立 plan session → 保存計畫 revision → 明確要求估算 → 修改計畫 → 重新估算
                                      ↓
                            明確切換 execute（不啟動）
                                      ↓
                         明確提交執行 → 既有 Goal 治理
```

- 第一版接受使用者／呼叫端提交的結構化計畫，不因進入 plan mode 自動呼叫模型。
  後續若加入模型起草，須獨立授權、計費，限制為無工具的規劃呼叫，不得建立執行型 Goal。
- 保存有效 PlanRevision 後，僅當此 Agent 明確綁定啟用的 `plan-forecast` 規劃 Skill，且 operator
  明確呼叫 forecast API 時估算。同快照去重；latest pointer 合併至最新版本，不自動估算過時版本。
- `execute` 模式、缺少模式、未知模式、舊 Agent／Goal API 均不觸發此 Skill。
  使用者在執行指令裡提到「plan-forecast」不構成觸發。
- 切換 execute 只停止規劃，不自動入列；回到 plan 也不自動暫停／取消既有 Goal。
  若 session 尚有非終態 Goal，拒絕回到 plan，需先透過既有控制流程明確結束工作。
- 啟用 forecast 的 session 在切換 execute 時，原子保存接受的 plan／forecast 參照。
  內容指紋未變的已接受結果可供明確啟動時核對，不等於在 execute mode 重新觸發 Skill。
- Forecast 運算前與發布前皆核對 mode／version；途中退出 plan 或計畫變更，回報
  `409 stale_snapshot`，不得發布為目前有效結果。歷史查閱不重觸發估算。

### API（M5 實際路徑以操作指南為準）

tenant 取自 principal，沿用 RBAC；修改／估算／啟動至少 operator，查詢至少 viewer。
管理 Agent、安裝 Skill、調整校正設定需要 admin。

| 方法與路徑 | 行為 |
|---|---|
| `POST /v1/agents/{agent_id}/planning-sessions` | 明確建立 plan session，保存 Agent 設定版本。 |
| `GET /v1/planning-sessions/{session_id}` | 讀取模式、版本、計畫與最新結果參照，不重新估算。 |
| `POST /v1/planning-sessions/{session_id}/plans` | body 含 `expected_version` 與完整計畫；只允許 plan，建立不可變 revision。 |
| `POST /v1/planning-sessions/{session_id}/refresh` | 只允許 plan，body 含 `expected_version`；明確重新解析最新 Agent／模型／Skill／政策，建立新計畫 revision，不改歷史。 |
| `POST /v1/planning-sessions/{session_id}/mode` | body 為 `mode`、`expected_version`；切到 execute 指定接受的 plan revision，不啟動／取消 Goal。 |
| `POST /v1/planning-sessions/{session_id}/forecasts` | body 為 `expected_version`、`plan_revision`；只允許 plan，重用相同指紋結果。 |
| `GET /v1/planning-sessions/{session_id}/forecasts/latest` | 讀取 latest immutable artifact，不重算；歷史列表尚未公開。 |
| `POST /v1/planning-sessions/{session_id}/execute` | 只允許 execute；明確指定 session version／plan revision／已接受的 snapshot fingerprint 及 idempotency key。 |

模式不符為 `409 mode_conflict`；版本或有效設定改變為 `409 stale_snapshot`；不合法計畫為 422；
跨 tenant 目標為 404；權限／ATR 依現行政策處理。輸入無校正資料不是 API 錯誤，回傳有效的
`insufficient_calibration` 結果。Redis 不可用時 fail closed，不以 request 自稱模式代替狀態。

session 的 `version` 僅在模式、目前計畫或固定設定變更時增加；寫入 forecast cache 不增加它，
避免結果一保存就使自己失效。SSE 擬新增 `plan.revised`、`forecast.ready`、`forecast.stale`、
`planning.mode_changed`；事件只包含安全參照與狀態，不含原始指令或憑證。

未綁定 `plan-forecast` 的 session 仍可接受並執行計畫，不必為了執行而安裝 Skill；forecast
不是執行 admission 的同意或前置核准。兩者都固定並驗證計畫／Agent／模型／政策內容。

## 4. 不可變計畫與「目前模型」

PlanRevision 保存 objective、明確工作項目、依賴、預定 Agent、驗證／重試假設與內容雜湊。
目前使用 M4 Plan v2 canonical DAG：最多依設定 32 nodes／64 edges、條件與跨 Worker dependency，
拒絕循環與遞迴委派。舊線性／Chief 計畫透過明確 adapter 使用同一 compiler。

估算輸入由服務端解析並固定，不接受呼叫端偽造 Agent／Skill 快照或模型速度：

- `tenant_id`、`session_id/version`、`plan_id/revision/content_sha256`。
- Chief／每個 Worker 的 Agent ID、設定 version、persona hash、執行與規劃 Skill 的
  revision／hash／revocation epoch、有效工具／網域／token 政策。
- 每個實際 model alias、resolved model／已知版本、非敏感 route fingerprint、部署／硬體設定版本、
  tokenizer ID／版本與已支援的推論設定。未支援的 reasoning 設定不虛構存在。
- 固定的輸入 context／history hash、執行器 graph／prompt template version、校正資料版本、
  estimator version、估算所用的資源／併發假設。

路由指紋必須隨 endpoint、實際模型或部署設定改變；只輸出不透明 ID，不暴露 API key、完整
內部 URL 或其餘秘密。同 alias 換後端不得沿用舊 profile；不知道模型實際版本時揭露限制，
不得把 alias 相同當作模型完全相同。無法解析模型時回報缺口，不猜預設模型。

快照須一致讀取／CAS；結果提交前檢查 Agent version、Skill epoch、路由與校正版本。
任何計畫、context、模型、persona、Skill、政策或相關部署變更都使舊 forecast 不再適用。
相同指紋的結果可快取；快取仍須通過目前 mode、權限與撤銷檢查，不能跨 tenant 共用。

區分兩種檢查：`request_guard` 為 session ID／mode／version，防止運算中途切換；
`snapshot_fingerprint` 固定上述計畫、設定與 estimator／profile 版本，但不含暫態 mode／session
version。切換 execute 會增加 session version、停止目前結果派送，卻不改變已接受計畫的內容
指紋。啟動檢查目前 execute session version 與已接受的內容指紋，不要求它等於產生 forecast
時的 plan session version。Profile 版本代表已發布的不可變校正集合，不隨每筆 telemetry 增加。
設定更新導致 stale 時，明確呼叫 refresh 或建立新 session；不能每次估算悄悄換設定。

### 原子發布與入列

「發布前再檢查」不能只是先 GET 再無條件 SET。保存有效結果、更新最新參照與 durable 事件，
需與 session mode／version、Agent version、Skill enabled／epoch 的 CAS 在同一 Redis 原子
邊界提交；衝突即丟棄或保存為非有效歷史。路由、部署與 profile 亦使用服務端發布的可信設定
epoch／不可變版本指標參與檢查，不能依賴不同 process 各自未同步的記憶體設定。

接受 plan／forecast 的模式切換同樣原子驗證。啟動時，session accepted refs、全部治理版本、
idempotency key → Goal mapping、session 活動 Goal 關聯、Goal 保存與執行入列需同一 admission
transaction；不能先建 Goal 再另寫冪等 key，或先通過檢查再讓撤銷插入空隙。
worker 恢復及子任務 admission 仍保留
既有 fencing／撤銷檢查；交易後發生的停用由後續執行邊界阻止，不宣稱撤回已在途副作用。

### 從計畫開始執行

新增入口必須把接受的 PlanRevision 編譯為固定步驟與明確 Worker 指派，再交給既有 Goal／
delegation 的預算、ATR、核准及恢復機制；不得悄悄回到現行自由拆解並仍宣稱執行原計畫。
分工未完成的計畫不能由此入口啟動。分工完整但 forecast 缺校正或帶風險時，允許操作者明確
確認缺口後提交；這個確認不會豁免任何硬性額度、工具政策、ATR 或逐次寫入核准。
固定分工已完成時，不重複計入／呼叫 Chief 拆解。第一版不保留執行時自由拆解；分工不確定時
先留在 plan 補齊，不能先啟動 Goal 再自動採用未被接受的新分工。
M4 已實作固定 compiler／admission bridge；forecast 與 execution 讀取同一 compiled artifact，
但 forecast 本身不等於 execute admission。

啟動前重新檢查所有版本、Agent／Skill 啟停與政策；不一致則 409，回 plan 更新與重新估算。
依賴的 Worker 在建立子 Goal 前仍需撤銷檢查，不能使用更新後的設定偷換已接受快照。
固定 compiler 必須重用 Chief delegate allowlist、Worker 角色／啟用檢查、工具與網域政策交集、
單層委派與團隊／個別 Agent 額度 admission；釘住 Agent ID 不是繞過這些檢查的授權。
估算不代表核准；外部寫入仍每次等待人工核准，核准後仍經 ATR。啟動也不保證執行成功。

## 5. Token 估算

估算單位是 **實際模型呼叫**，不是一個工作項目等於一次呼叫。先從版本化的執行 graph
建立情境 call manifest：包含尚需的規劃、tool decision、結果生成、驗證／修正、重試及 Chief
彙整；只有實際 graph 存在的階段才計入。已在規劃階段花掉的 tokens 另列，不加進未來執行預測。

```text
input_s  = Σ 每次呼叫完整序列化模型輸入的 tokens
output_s = Σ 每次呼叫的預測輸出 tokens
total_s  = input_s + output_s
team_s   = Chief 所有呼叫 + 每個不同 Worker Goal 的所有呼叫
```

其中 `s` 為 low／base／high 情境；不同模型各自計算再加總，不能把 parent usage 當全隊 usage。
完整輸入包含 system、persona、生效 Skill、task／query、history、工具 schema／回傳、子任務
結果及 chat framing；不能只計算使用者可見的計畫文字。
收集實績時沿 child IDs 去重，一個呼叫只計一次。已完成規劃花費、預測未來花費、實際執行花費
及政策預留／上限使用不同欄位，不混成單一數字。

- 已知 prompt 使用相符 tokenizer／chat template 計數；格式不完全匹配則標 `approximate`。
  沒有 tokenizer 時可用**具來源的近似**並標誤差／限制；UTF-8 byte 預留只能列為政策參考，
  不能標為精確 tokens 或預期值。
- 未知的工具回傳、子任務答案、輸出長度、重試次數依相同工作類型的觀測建立情境。
  只有 token 估算而沒有時間校正時，允許回 tokens、時間為 null；反之亦然。
- Provider 的 output 若已包含 reasoning tokens，不重複加總；額外 usage 欄位依 adapter 的
  明確語義處理，語義未知就標缺口，不索取或保存 hidden chain-of-thought。
- output cap、20,000-token 預設團隊額度、每日上限都是限制，不是預期消耗。
  預測高於限制時回報 `budget_risk`，不裁切預測後宣稱能完成；context window 與 deadline
  也分開檢查。模型 context 上限未配置時標示未知。

## 6. 時間估算與不確定性

每個呼叫以相符 profile 評估 request start 到 end 的完整 latency，依 input／output 長度、
併發、cache 狀態與環境分組。不得同時把「完整 latency」再加一次 TTFT；只有 profile 明確拆成
prefill／TTFT 與 decode 時，才依其分解式計算。工具、驗證、排程成本也不能重複加總。

計畫時間依依賴與**可用資源條件**做受限 critical-path 模擬，不是所有 Worker 時間相加，
也不是無條件假設全部平行。Chief 的先行工作、Worker 分支、彙整，以及共享模型容量都要納入。
目前 run-slot 競爭可能讓 Goal 失敗，不是可靠排隊保證；未補上排隊語義前，只能提供指定 slot
假設下的情境時間，並列競爭失敗風險，不能保證等一等就會執行。

結果分開顯示：

| 欄位 | 定義 |
|---|---|
| `machine_elapsed_seconds` | 排除人工／暫停，依已聲明資源條件計算的整體機器經過時間，已含該情境的資源等待；可為 null。 |
| `resource_wait_seconds` | 上述機器時間中資源／排程等待的診斷分量，不另加一次；未知為 null，不能默認為 0。 |
| `human_wait_seconds` | 核准、人工補件、手動暫停等外部等待；沒有可信資料時為 null。 |
| `calendar_elapsed_seconds` | 包含所有等待的開始到完成時間；任一必要等待未知時為 null。 |

核准期限不是預期核准耗時。`runtime_seconds` 不含完整 waiting_approval／waiting_children
間隔，不能當日曆時間；Child 等待的計算也不能重複計入其工作與父任務等待。
若未來可建模人工等待，日曆時間仍需在同一工作圖插入阻塞後重排，不是把每個分支的等待秒數
直接相加。已含在模型完整 latency 的後端排隊也不能再加到機器時間。
低／基準／高是具假設的情境，不是保證上下界。沒有經驗證的聯合分布校正時，不能命名成
p50／p90 或 90% 信賴區間，更不能把各階段 p95 相加稱為整體 p95。

## 7. 校正與缺資料行為

新增逐呼叫 telemetry：call ID、parent／child Goal ID、stage、model／route／部署版本、
tokenizer、input／output／total usage 與 provider-reported 來源、enqueue／start／first-token／
end 時間、工具耗時、等待區段、重試、取消／失敗／截斷、併發與 cache 狀態。
不收集原始 prompt、工具機密輸出或推理內容；校正資料依 tenant 隔離，跨租戶共享需另行同意。

Provider usage 缺漏／不合法的樣本不得當精確 tokens 實績；保守扣款另記來源。
失敗／截斷資料保留為成本與失敗情境，不能當完整成功樣本，也不能只丟棄失敗而宣稱沒有風險。
Profile 保存樣本數、期間、最後校正時間、來源、環境與模型相容範圍；閾值及有效期由管理者配置。
首版即使樣本足夠也先輸出情境；統計分位數需另有 held-out 校正證據，不能僅憑筆數開啟。

使用順序：同模型／route／環境的實績 → 明確相容且揭露差異的管理者 profile → 無資料。
套用近似 profile 必須揭露，不可默默把其他模型速度當目前模型。Forecast 不自行發送付費模型
請求或執行 benchmark 蒐集樣本；這些實驗要另行授權。

狀態為 `estimated`、`partial`、`insufficient_calibration`；各 dimension 另列來源、可信度與缺口。
必要數值缺資料時為 null，不填固定 tokens/sec、0 或看似合理的精確分鐘數。
計畫本身尚未明確分工／呼叫路徑時也要標示，不能只補模型速度就宣稱完整可估。

### 無校正資料的回應範例

以下是契約範例，ID 為示意；不是對本專案執行時間的實測或預測。

```json
{
  "schema_version": 1,
  "forecast_id": "example-forecast",
  "session_id": "example-session",
  "plan_revision": 1,
  "status": "insufficient_calibration",
  "applicability": "current",
  "models": [
    {"agent_id": "example-worker", "alias": "configured-alias", "resolved_model": "configured-model", "route_fingerprint": "opaque-route-v1"}
  ],
  "execution_forecast": {
    "tokens": {"input": null, "output": null, "total": null},
    "machine_elapsed_seconds": null,
    "resource_wait_seconds": null,
    "human_wait_seconds": null,
    "calendar_elapsed_seconds": null
  },
  "limits": {"team_token_budget": 20000},
  "calibration": {"profile_id": null, "sample_count": 0, "last_calibrated_at": null},
  "confidence": "insufficient",
  "gaps": ["沒有相符模型的逐呼叫用量與 latency profile", "人工核准等待時間未知"],
  "warnings": ["token 額度不是預測用量；目前無法可靠估算完成時間"]
}
```

可估算時，每個數值維度使用 `{ "low": ..., "base": ..., "high": ... }`，附每情境的 call 數、
各模型用量、假設及 provenance；必須可追溯到 plan／snapshot fingerprint／estimator version。
`limits`、已花費的 `planning_actual_usage`、執行後的 `execution_actual_usage` 與上述預測分開。

## 8. plan-forecast Skill 與相容性

新增 `planning_skills` 綁定，與現行 Agent 的 `skills` 執行綁定分開；兩者都釘住 revision，
各自檢查 tenant、啟用狀態、epoch、角色與容量。執行 prompt 不載入 plan-only 指令。
`plan-forecast` 只允許 `execution_modes: ["plan"]`，Chief／Worker 均可使用，無必要工具。
服務端受信任的 catalog／dispatcher 綁定 forecast handler，不以任意 Skill 名稱或本文觸發動作。
自訂同名 Skill 不能取得內建 handler；Skill 文字與 frontmatter 也不能擴權。

Forecast handler 讀取固定快照、呼叫 deterministic estimator、以中文模板呈現結構化結果。
第一版**不為估算／改寫說明再呼叫 LLM**，避免為估算額外消耗未知 tokens 或遞迴預測自己。
Skill 草案描述工作規則，真正的模式、資料與數值檢查都由服務端執行，不靠模型自律。

現行 immutable revision 的 hash 固定使用七個 `CONTENT_FIELDS`，沒有模式欄位。
新增時必須導入明確的 `content_schema_version`／`hash_version`：

1. 缺少版本的歷史記錄按 v1 原欄位驗證，保持 hash 不變，不改寫歷史 revision／Goal 快照。
2. 舊 Skill 的相容語義為僅 execute，既有 Agent 行為不變；規劃使用需明確發布新版與綁定。
3. v2 將版本 discriminator 與 `execution_modes` 納入 canonical hash；竄改模式必須驗證失敗。
   未知 schema／hash version 拒絕，不降級；v1 不接受夾帶 v2 模式欄位，不把它忽略後當新版。
   版本欄位須成對且一致；缺一或混搭也拒絕。只有可信儲存中的真正歷史資料適用無版本 v1
   規則，新請求不得透過移除版本欄位選擇舊驗證流程。
4. `skills` 不接受 plan-only 綁定；`planning_skills` 不接受 execute-only 綁定。
   API admission、注入、automation、Chief、checkpoint resume 均不得繞過 mode 檢查。
   規劃快照只保存為 session 的 `planning_skill_snapshots`，不複製進 Goal 的
   `agent_config.skill_snapshots`。mode 取自服務端入口的可信呼叫 context，不取自模型或
   呼叫端夾帶欄位；既有 Goal／automation／resume 入口一律是 execute context。
5. 停用撤銷 epoch 同樣約束規劃 Skill；結果發布前重新檢查，舊歷史可查但不可當有效 forecast。

不能只把新欄位加到現有 `CONTENT_FIELDS`，否則會破壞舊 hash 或發生缺欄位錯誤。
此目錄的 `SKILL.md` **不能直接匯入目前 Gateway**，也沒有放入 Codex 個人 Skill 目錄；
目前不把它計入已交付的十項內建 Skill。

## 9. 實作階段與驗收

以下是原始實作順序。P1、P3 已分別由 M3／M4 核心完成；P2 的共享發布、telemetry、明確校正、
call manifest、情境計算與不可變 forecast 已由 M5 完成。Forecast SSE 延至 M7，真實模型
held-out 校正與 staging 證據延至 M9：

1. **P1：模式與快照。** PlanningSession／PlanRevision API、RBAC／CAS／大小限制、Skill v2 相容
   與 planning 綁定、端到端 plan-only gate；未校正結果先誠實回 null。
2. **P2：估算與校正。** graph call manifest、tokenizer adapter、逐呼叫 telemetry、校正 profile、
   情境計算／快取與 SSE。無相符 profile 仍不回虛構時間。
3. **P3：執行銜接。** 固定計畫 compiler、Chief／Worker 快照 admission、明確 execute、結果實績
   比對；既有核准、ATR、額度與 crash resume 全部回歸驗證。

必要驗收：

- execute／缺 mode／偽造 prompt 模式／舊 API／automation／重播不觸發 forecast。
- plan 保存後只估最新 revision；同指紋去重；無模型呼叫、無工具、無 Goal／子 Goal 入列。
- 跨 tenant、Skill 停用再啟用、運算途中模式／計畫／模型／route 改變皆不能發布有效舊結果。
- 用 Redis 競爭測試證明發布／模式切換／入列的 CAS 無檢查與提交空隙，重送 execute 不重複建立 Goal。
- v1 hash／舊 Goal 恢復不變；v2 模式欄位被竄改會失敗；plan-only 不進 execute prompt。
- 缺模型校正回 `insufficient_calibration`；有 tokenizer 無速度資料回部分結果；未知等待不填 0。
- 合成算術 fixture：Chief 前置 2 秒、Worker A 5 秒、B 7 秒、彙整 3 秒，容量足夠且無競爭時
  為 12 秒；Worker 僅可一個接一個時為 17 秒。這是算法測試，不是模型效能預測。
- 合成 tokens fixture：Chief 前後合計 300、A 400、B 500，團隊為 1,200，不漏算或重算子任務；
  重試情境增加對應呼叫，不把 input context 只算一次。
- 全隊／單 Agent 預算不足、context／deadline 風險不靠裁切預測掩飾；未知人工等待不產出有界 ETA。
- execute 必須明確提交且冪等；變更分工／設定須回 plan；寫入仍逐次核准且 ATR 不略過。
- refresh 只建立新 revision；固定計畫也不能派給不在 Chief allowlist 的 Agent 或繞過政策交集。

## 10. 進度與後續討論

設計初稿後已完成 plan mode、Skill v2、Workflow admission 與 M5 forecast。操作及實際交付邊界見
[plan-mode.md](../../plan-mode.md)、[plan-forecast.md](../../plan-forecast.md)與
[M5 驗證紀錄](../../releases/2026-10-27-m5-forecast.md)。
後續在 [Discussion log](../../architecture/discussion-log.md) 持續記錄校正資料、實作證據與設計變更；
GitHub Discussions 啟用並取得主題網址後再連回，不把本機紀錄當成已發布的 Discussion。
