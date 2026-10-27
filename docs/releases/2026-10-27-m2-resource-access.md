# M2 第一段：資源 owner／明確分享 ACL

2026-10-27，基準 `ad30bbf`。本段交付 ACL，不代表整個 M2 或 M1–M9 已完成。
操作與遷移步驟見[資源權限指南](../resource-access.md)。

## 實作

- 新增可選 `RESOURCE_ACL_ENABLED`，預設 false；沿用現有 tenant／RBAC，再檢查 owner／
  typed subject grants，不讓同租戶自動互讀。
- 新資源在同一 Redis 交易登記 owner／actor 反向索引；手動 Goal、automation Lua dispatch、
  Chief fan-out 與 immediate run 都覆蓋。子任務使用當前父 ACL 的 owner，不能用候選 Goal／
  webhook／舊 created_by 冒用；不自動繼承父 grants。
- 授權掛入現有 CRUD、list、Skill export、planning revision、Goal／run／automation SSE，
  automation 建立時也驗證 body 引用的 Agent。保留原有管理角色限制。
- `/v1/access` 提供 owner／admin 分享管理、CAS version；admin 單筆 legacy preview 及明確認領。
  權威與索引損壞 fail closed，寫前檢查型別，避免新增 ACL 命令遇 WRONGTYPE 的部分提交。
- immediate conversation 首次認領採 WATCH；其他使用者無法搶同一 ID 讀歷史。
  DELETE 訊息與 ACL revoked 同交易，但本段**不是** conversation 執行鎖／FIFO／fencing。
- SSE 每個 payload／heartbeat 前重新驗證身分及 ACL；停止送出尚未釋出的 buffered payload。
- 歷史 list／search／events 在參數化 SQL 中、LIMIT 前過濾根資源，再於回應前重查 Redis。
  skill／planning revision、receipt 映射到根權限；保留 conversation ID 中的冒號；不授予全域 audit。
  直接歷史讀取查詢前後皆授權；Redis 故障不回退租戶全讀。
- ACL／actor index 不加入 Postgres 歷史權威；既有 outbox／backfill／projector 邊界不變。

## 驗證

專用 Redis 8.2、PostgreSQL 17；全部合成資料，不使用付費模型。以此命令重跑：

```bash
TEST_REDIS_URL=redis://127.0.0.1:16379/0 \
TEST_POSTGRES_URL=postgresql://gateway_test:gateway_test@127.0.0.1:15432/gateway_test \
  .venv/bin/pytest -q
.venv/bin/ruff check .
git diff --check
sh -n scripts/test_acl_stack.sh
sh scripts/test_acl_stack.sh
sh scripts/test_postgres_stack.sh
sh scripts/test_proactive_stack.sh
```

- 最終完整回歸：**731 passed，零 skip**（原有 693 加 38），118.55 秒；6 個既有 RedisSaver
  deprecation warnings，不隱藏或聲稱為零警告。Lint、shell syntax 與 whitespace 檢查通過。
- 新驗證包括八種資源／跨租戶／human-Agent 同名隔離、read／operate 與 RBAC、撤權、
  grants 競態、錯誤 index 型別無副作用、legacy dry-run／明確認領／不可變 owner、
  無輸入回顯、first-writer race、刪除 tombstone，以及三條 SSE generator 撤權後不送 buffered 事件。
- 真實 SQL 驗證 LIMIT 前過濾、search／events、revision／receipt aliases、帶冒號的 conversation；
  HTTP 查詢期間撤權與 Redis 故障另有回歸。
- 新的嚴格 ACL Docker E2E：多 API key 的真實 HTTP／worker／兩個 projector、Agent 引用拒絕、
  Skill export／planning session／Goal 隔離、share、PG history／search 分頁、revoke、Redis outage
  全部通過；backfill／reconcile 為 64 筆 retained snapshots、0 mismatches。
- 原 Postgres Docker E2E 通過：雙 projector、停機繼續寫／恢復投影、backfill／reconcile、
  history cutover、Redis outage 與實際 pg_dump／pg_restore 摘要比對。
- 主動任務 Docker E2E 首次失敗：Chief webhook Goal 的 `goal_objective` ATR 掃描超過既有
  5 秒 deadline，被 fail-closed 拒絕；單獨重跑也在同一位置失敗，因此不歸因於並行測試。
  檢查載入路徑後，將 Skill runtime import 與新增 ACL 管理堆疊分離，僅 create 路徑載入；
  加入乾淨 interpreter 的 import-boundary 與真實 Redis owner 登記回歸。
  其後相同 5 秒期限、相同規則與雙 worker／scheduler 設定通過全部主動任務情境，
  包含 Chief、Skill 固定 revision／停用立刻啟用、內部事件、webhook 去重及 SIGKILL 恢復。
  未停用 ATR、未放寬 deadline，也沒有將前兩次失敗寫成通過；單次重跑成功不證明唯一根因，
  掃描 startup／負載穩定性仍需 M9 長時間測試。

## Review 與限制

- 修正驗證發現的 first-writer 時序：另一位使用者已寫入 ACL 時，落敗者一致回 404，
  不因 owner guard 競態回 500。補齊既有測試替身的 Redis type／SET NX 介面；未跳過回歸。
- 人工 review 著重權限掛點、body 引用、原子 admission、Lua 信任來源、即時撤權與 SQL 分頁，
  並確認 remote 仍只有 `dc-tw/ai-ops-gateway`。沒有修改 Bohr repository 或自動發表 Issue。
- 預設旗標不變。只剩 PG 的未知 owner 舊資料仍 admin-only；不提供自動猜測或批次公開遷移。
- 分享是讀／操作已有快照的能力，不自動刪去嵌入的 Agent／Skill 設定；分享前需人工確認內容。
  移除分享不收回已送出內容、不取消背景 Goal、不撤回已發出的工具呼叫。
- 對話刪除未自動取消進行中的 run；應先取消再刪除。跨入口執行併發與 fencing 仍待下一段。
- Agent 憑證輪替／撤銷、MCP／ATR policy references、EventEnvelope、SHA-256 audit chain／
  簽署 checkpoint 未實作；現有 audit stream 不能被當成可驗鏈或外部不可變保存。
- M3–M9（Skill v2／匯入／自動選擇、DAG／YAML、forecast、搜尋／retention、管理台／presence、
  ACP、完整 staging）仍按[交付追蹤](../implementation-roadmap.md)繼續，沒有在本段偷改為已完成。
- 專用 Docker stacks 與合成測試資料於驗證後清理；不影響其他服務，映像快取保留供重建。
