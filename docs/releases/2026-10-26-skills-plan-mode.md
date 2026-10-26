# 2026-10-26 封版：Skill 管理與計畫快照

## 範圍與結論

本次以 `9fad6b9`（主動工作與 Chief 派工）為已提交基準，將後續 Skill 管理、核心 plan mode／
不可變快照、回歸測試與中文文件整理成一個本機 commit。依使用者確認，這次**不一併開發**
forecast、前端或 Postgres；未支援能力以拒絕／隔離測試及未來驗收條件記錄，不能算成已實作。
沒有推送遠端、部署正式環境或發布 Bohr Issue／GitHub Discussion 留言。

結果：真實 Redis 完整測試 **478 passed、6 warnings、0 skipped**；既有 Docker E2E、
主動工作 E2E、新增 MCP → Browser Worker ATR E2E，以及真實 Ollama smoke 均通過。
這是目前範圍的工程驗證，不是 production-ready、規則零誤報或模型語意正確的保證。

## 已實作與本次回歸

| 能力 | 交付邊界 | 驗證入口 |
| --- | --- | --- |
| ATR 三個信任邊界 | request 執行前、tool result 進 model context 前、model output 對外釋出前；核准不取代 ATR | `tests/test_threat_detection.py`、`tests/test_sse.py`、`scripts/test_docker_stack.sh` |
| Durable Goal／人工核准 | Redis checkpoint、重試、逐次寫入核准、receipt 與中斷恢復；不保證外部副作用 exactly-once | `tests/test_goal_checkpoint.py`、`tests/test_goals.py`、既有 Docker E2E |
| 主動 Agent 與角色 | cron／interval、簽章 webhook、ATR finding／Goal failure；合併待執行工作、預設不連鎖；Chief 單層派工、獨立 persona／政策、共用額度 | `scripts/test_proactive_stack.sh`、[前階段紀錄](../proactive-agents-verification-2026-10-25.md) |
| Skill 管理 API（新增） | tenant registry、十項可選內建範本、immutable revision、CAS、rollback、固定 Agent revision、撤銷 epoch、prompt 注入計費 | `tests/test_skills.py`、`tests/test_builtin_skills.py`、`tests/test_skill_runtime.py`、`tests/test_skill_admission.py` |
| 核心 plan mode（新增） | 獨立 session、計畫與設定快照、hash、CAS、refresh、stale 檢查、接受計畫；**不呼叫模型／工具、不建立 Goal** | 四個 `tests/test_planning_*.py`、`scripts/verify_planning.py`（已接入 Docker E2E） |
| 未支援能力的邊界（新增） | OpenAPI 無 forecast／import／script／session execute 入口；未來 Skill／planning 欄位拒絕；session 互不影響 | `tests/test_release_boundaries.py` |
| 不合法計畫與政策變更（新增） | surrogate／NaN／中文與 emoji UTF-8 容量、固定安全 422、拒絕後無副作用；refresh 政策變更與同模式接受仍檢查撤銷 | `tests/test_release_plan_validation.py` |
| 不合法 Skill 文字（修正） | 七個內容欄位與清單文字拒絕非法 UTF-8；create／PUT／publish／管理錯誤固定安全 422、歷史與稽核不變；合法繁中／emoji 的 v1 hash 相容 | `tests/test_skills.py` 新增 16 cases |
| MCP remote 設定（修正） | MCP SDK 子程序明確收到 Browser Worker／Playwright 白名單設定；不傳遞模型及其他無關憑證 | `tests/test_release_mcp_environment.py`、新增 MCP E2E |

操作入口：[Skill 管理](../skill-management.md)、[十項內建目錄](../skills-catalog.md)、
[規劃 API](../plan-mode.md)。Skill 是指令，不會增加工具權限。`execute` 只是接受計畫，
不是已排程執行，也不是人工核准。既有 Goal endpoint 仍會執行工作：在其 body 放入
`mode: plan`、forecast 等未知 metadata **不會變成 dry run**，必須使用 planning API。

## 新增的實際安全證據與限制

### MCP stdio → authenticated Browser Worker

`scripts/test_atr_tool_stack.sh` 使用獨立 Compose project、image tag、loopback port 與 Redis volume。
流程經公開 SSE、真正 MCP stdio／`app.mcp_server`、Browser Worker HTTP 與既有 token 驗證，
再以真實 pyatr 檢查合成惡意回傳。測試斷言：

- 無效 Browser token 回 401，MCP session 確實 initialize。
- `tool_response` 被阻擋，後續 mock model context 沒有攻擊 canary。
- live SSE、replay、Redis event／run／finding／audit 不洩漏原始攻擊內容。

測試專用 wrapper 僅在 overlay 載入；在 production `browser_close` handler 完成後插入合成回傳，
不用 Chromium，也不放寬 SSRF／auth。它驗證一條代表性 remote transport 路徑，**不代表所有
browser 方法、序列化格式與攻擊種類皆已覆蓋**。驗證中發現 MCP SDK 預設會過濾父程序設定，
因此補上最小環境白名單；未將整份環境或 LLM secrets 轉交 MCP 子程序。

### 兩個 HTTP process／三個 SIGKILL 時序

`tests/test_release_atr_crash.py` 使用真 Redis／checkpoint、真 pyatr 與 deterministic model，
分別在掃描中、阻擋判定後但事件寫入前、事件寫入後但 live publish 前殺掉第一個 process，
由第二個 process 恢復並 replay。三個案例均未把兩段攻擊文字或未掃描 draft token 送到
live／replay／Redis event／audit；恢復結果保持 `evidence_validated: false`。

已確認的限制：graph checkpoint 與 public SSE event append **不是同一筆交易**。在事件寫入前
中斷，可能遺失診斷事件，雖然 blocked candidate 仍為空、沒有輸出原始攻擊。因此本版不能宣稱
finding 事件 exactly-once／完整投遞；可靠 outbox／reconciliation 仍待後續設計。這三個 gate
也不是任意 distributed failure、所有 partial write 或外部副作用時序的完整證明。

### 一般 ATR 掃描的中段盲點

同檔案的真 pyatr negative control 證明：直接攻擊會被阻擋，但放在超長內容中段、恰好被
head／tail 截斷排除時，可得到 `truncated: true` 且無 finding。這是**已重現的限制，不是安全
通過**。核心 planning API 超出完整掃描容量會拒絕；一般 request／tool／output 路徑仍須依
`truncated` 審慎處理，未命中不能當成安全證明。

## 真實規則語料重跑

本機 Python 3.14.6、pyatr 0.3.1，載入 833 rules；`ATR-2026-00290` condition 3 的 regex compile
warning 仍存在，不能把 rule count 解讀成全部條件均有效。Docker runtime 為 Python 3.12；
遠端 GitHub Actions 尚未執行，不能用本機結果宣稱遠端 CI 已通過。

| 合成語料 | 結果 | 判讀 |
| --- | --- | --- |
| seed 6 筆 | TP 3、TN 3、FP 0、FN 0；mismatch 0 | integration smoke，不是廣泛準確率證據 |
| benign 60 筆 | TN 60、FP 0；mismatch 0 | 只代表這批人工編寫良性樣本 |
| `ATR-2026-00213` 30 筆 | TP 20、TN 7、FP 3、FN 0；mismatch 3 | 評估 CLI **預期 exit 1**，不能報告所有語料通過 |

最後一組的良性誤報率是 `3 / 10 = 30%`；已命中樣本的誤報占比是 `3 / 23 ≈ 13.04%`。
現有 promotion API 的 `false_positive_rate` 使用後者，兩個分母不得混用。維持 detection-only，
不依此將該候選規則升級為正式 blocking allow-list。語料、標記與第二位 reviewer 流程見
[測試語料](../atr-evaluation/test-corpus.md)及[人工審查](../atr-evaluation/review-and-promotion.md)。

## 尚未納入：狀態與未來驗收條件

以下是缺口盤點，不是本次已完成項目，也不承諾全部列入下一版。

| 項目 | 目前狀態 | 後續至少需要的驗收 |
| --- | --- | --- |
| `plan-forecast` 時間／token 估算 | [設計與 Skill 草案](../designs/plan-forecast/design.md)，無 runtime／API | 服務端 mode gate、缺資料回 unknown、具來源 tokenizer／模型 profile、low/base/high 情境、全隊加總與 critical path、逐呼叫校正及偏差報告 |
| plan-only Skill／Skill v2／`planning_skills` | 未支援；v1 hash 不變，未來欄位拒絕 | 版本 discriminator、canonical hash 相容、模式過濾、防降級與撤銷競態回歸 |
| 中央模型設定版本 | 只有 process-local 安全路由指紋，實際部署版本未知 | 跨 replica epoch、原子熱更新、stale／accept／execute 競態測試 |
| 接受計畫 → Goal compiler／執行銜接 | 沒有 session `/execute`；`mode: execute` 不入列 | 逐步 plan／Goal mapping、accepted hash 與全部政策版本原子 admission、重送冪等、逐次寫入核准、ATR／額度／crash 回歸 |
| Skill 自動選擇、外部 package import、resources／scripts | 沒有 API 或任意腳本執行；export 不等於 import | 另定信任、供應鏈、沙箱與權限邊界，再驗證不擴權；不是預設要開放 |
| 管理前端、planning SSE／forecast SSE、外部通知 | 管理用查詢 API；既有 Goal SSE 可用，planning metadata 事件未提供專用公開串流 | RBAC／tenant、reconnect、重複投遞、私密內容與通知權限測試 |
| 完整 Agent principal | 有服務端角色／政策，但無獨立 credential lifecycle、MCP／ATR policy reference | impersonation、rotation、owner／policy matrix 與 audit actor attribution |
| per-conversation single-flight | tenant／run quota 已有；沒有跨入口的 conversation 單一 ownership | 多 replica lease／交棒／取消、明確排序與 browser ownership |
| 統一 event envelope、tamper-evident audit、可靠 outbox | 仍用既有 streams；snapshot hash 不等於 audit chain | schema compatibility、併發／缺件／竄改偵測、durable delivery 與對帳；外部 immutable export |
| Postgres、FTS、全域 provenance 搜尋、retention／archive | roadmap，Redis 仍為主要持久化；規劃／Skill 無自動歸檔 | migration rehearsal、source-of-truth／dual-write 對帳、權限重查、backup／restore／retention 與 rollback |
| ACP、Workflow YAML、presence | roadmap 候選，未實作 | 獨立 ADR／contract；YAML 如採用應編譯為既有 LangGraph，不另建 executor |

完整 Nostr／Slack clone／Git hosting／voice huddle／GPU mesh／社交身份系統是
[明確非目標](../architecture/buzz-inspired-roadmap.md#明確非目標)，不是待補齊的承諾功能。

## 仍需真人或實際環境的驗證

- **第二位獨立真人 reviewer** 尚未複核語料。模型 code review 不能替代盲審標記、disagreement
  記錄與共識版本；不得把原作者的預標記稱為雙人驗證。
- 去識別且經授權的真實流量、代表性攻擊與良性分布仍缺；不能由合成語料推導 production FPR／recall。
- Ollama `qwen2.5:0.5b-instruct` 只通過連線與 SSE smoke；不證明 Chief 分工、摘要或資安判讀品質。
  大樣本隨機交錯／固定輸出的 latency benchmark 與模型 profile 校正尚未完成，也不能用 mock 延遲作 forecast。
- Kubernetes 僅解析 13 份 YAML 文件，沒有實際 cluster rollout、HA／壓測、故障演練或 operational
  backup／restore／retention 演練；正式 secrets、TLS 與不可變 audit export 仍由部署環境補齊。
- regex condition warning、一般 ATR 中段截斷盲點與 checkpoint／event 投遞缺口均未在本版修復。

## 重現與封版檢查

在專用 Redis 8.2（含 JSON／Search）上執行，不要指向正式 Redis：

```bash
TEST_REDIS_URL=redis://127.0.0.1:16379/0 .venv/bin/pytest -q
.venv/bin/ruff check .
git diff --check
docker compose config --quiet
sh scripts/test_docker_stack.sh
sh scripts/test_proactive_stack.sh
sh scripts/test_atr_tool_stack.sh
sh scripts/test_ollama_stack.sh
```

本次完整 suite 為 `478 passed, 6 warnings in 87.75s`；六個 warnings 是既有第三方 RedisSaver
connection helper deprecation，不是 skip。Ruff／diff／Compose 檢查通過。CI 加入新的 ATR MCP
stack；Ollama 保持 opt-in，不讓每次 CI 下載模型。

補充：全 repository `ruff format --check app tests scripts` 仍指出 9 個未修改的既有檔案格式
不一致；本版不混入無關排版變更。本次變更的 37 個 Python 檔 format check 與 CI 的
`ruff check .` 通過，不能把 lint 與全庫 formatter 檢查混稱為全部通過。

語料重現命令見[語料文件](../atr-evaluation/test-corpus.md)；候選規則 mismatch 的非零 exit
是待校準證據，不應為了全綠而改動標記或隱藏失敗。各階段的 281／404 測試紀錄保留為當時
結果，最新彙總以本頁為準。

Docker 腳本清除自身測試容器與 network，新增 ATR／proactive stack 也清除自身 Redis volume；
Ollama 原有模型 cache 與 volume 保留。專用本機測試 Redis 在驗證後移除，不保留暫存測試資料，
不操作其他專案或正式資料。回退程式前先停用相關 automation、清理執行中工作並備份 Redis；
舊程式可能忽略新 Skill 治理欄位，不應直接帶著綁定 Skill 的工作降版續跑。
