# M4 執行段：明確 admission 與持久化 DAG controller

基準 `921e272`，接續 [編譯器段落](2026-10-27-m4-workflow-compiler.md)。
這是 M4 的執行整合進度，Docker 多副本／SIGKILL E2E 及最終 review 仍未完成。
M5–M9 仍維持完整原定範圍，不以本段取代整體目標。

## Authority 與實作

- `WORKFLOW_EXECUTION_ENABLED` 獨立預設關閉；只有明確 execute API 可 admission。
  mode execute 只接受計畫，不建立工作、不保留預算或開始推論。
- root identity 由 tenant／session／idempotency key 固定；receipt 綁 accepted hash。
  Redis CAS 同時驗證 session／revision／現行 configuration／圖限制／ACL／quota，
  建立 root／owner／conversation／queue／quota／receipt／active_goal_id；重送不新增工作。
- 執行前重掃計畫及 persona／Skill／resource；掃描 hash 綁入 CAS。
  有效 ACL／epoch 或額度改變時不得使用舊快照 admission。
- controller 不呼叫模型／工具，讀 root＋所有子 Goal 的 WATCH set 作有限 readiness 決策。
  子 Goal ID 固定，建立子 Goal／ACL／queue intent 同一 transaction，多副本不重複派工。
  actual parallelism 由 worker 副本提供；不是在 HTTP request 內產生無上限背景 task。
- 子 Goal 使用既有 LangGraph／checkpointer／工具白名單／逐次 exact-argument approval。
  readonly Chief synthesis 不再走自由拆解；固定節點也不重新產生步驟。
- Agent execution lease 與 Goal claim／renew／finish／cancel 原子更新，適用跨 Goal，
  核准／暫停釋放 execution slot。Agent 或 tenant 容量衝突持久化等待，由 recovery 再入列。
- root 保留整個團隊的 quota，子 Goal 不重複保留；逐呼叫用量原子更新到 child＋root，
  考慮跨 UTC 日、pre-call conservative charge 降低與 terminal 一次性結算。
- 必要節點失敗停止新派工；parent cancelling gate 阻止新模型／工具呼叫，取消進行中節點，
  再彙整用量。暫停／恢復由 root 持久化追蹤，不恢復原本由使用者個別暫停的節點。
- 取消後的舊 approval 不得重新 enqueue；對應 approval audit／event 也改記 parent_stopped，
  不留下「已核准且排隊」的誤導紀錄。
- 寫入 receipt intent 與 Goal／parent／Agent／Skill authority 做 CAS；每 Goal receipt index 有界。
  未知副作用標為 review_required，保留參照與查詢入口，不刪除 receipt、不自動重播。
- root 只保存 DAG 狀態／子 Goal 參照，輸出留在受子 Goal ACL 保護的記錄，
  防止較大配置造成重複正文超過 Postgres record 上限，也不把 root 狀態分享等同 child 輸出分享。
- root 狀態提供相容 goal lifecycle event；root failure 的隱私安全內部事件同交易發出。

## Review 發現

- admission 競爭時，另一 replica 可能在 receipt 讀取後建立 root。不能把它直接當成 ID collision；
  先驗證 WATCH，讓真正競爭重試後取得既有 receipt。
- 同一 transaction 遇到必要 node admission 失敗，不能仍把其他 ready nodes 入列，
  不可等下一次 tick 才取消。
- root resume 的 child control 在 CAS 外可能遇到程序中止；保留 paused-child 清單，
  直到它們都恢復，下一 controller 仍可繼續，避免 reset flag 後永久卡住。
- schema 有效的短 JSON 不應被一般文字最少 20 字規則誤擋；只有 schema 驗證成功可豁免，
  不跳過既有工具來源／grounding／ATR。schema 驗證不在 event loop 同步跑 regex。
- cancellation 與 receipt SET 不能分開依賴一次舊 owner check；改為 authority WATCH＋intent CAS。
  外部呼叫本身不可能與 Redis 回滾，仍必須保留不確定結果並禁止自動重播。
- 最終全量重跑曾在 automation 平行 tick 發現：刻意損毀的 Workflow 會讓全域 scheduler
  reconcile 中斷。修正為該 root 的 quarantined／failed、保留壞 artifact、取消實際 child 記錄，
  不根據損毀 graph 派工。後續 tick 繼續取消與結算，也不擋其他健康 root。
  專項「corrupt root＋healthy root＋parallel scheduler」通過；此失敗不是以 skip 或放寬 hash 檢查消除。

## 已取得證據

使用本輪專用、僅綁 localhost 的 Redis 8.2／Postgres 17-alpine，不動既有其他服務。

```bash
TEST_REDIS_URL=redis://127.0.0.1:16379/0 \
TEST_POSTGRES_URL=postgresql://gateway:gateway-m4-test@127.0.0.1:15432/gateway \
  .venv/bin/pytest -q -x
.venv/bin/ruff check .
git diff --check
```

- 全量 **861 passed、零 skip**，200.75 秒；8 個 RedisSaver deprecation warnings，
  其中兩個來自新加入的獨立 checkpoint 連線驗證，沒有用 mock checkpoint 取代。
- DAG 專項 13 項通過（26.54 秒）：四個 concurrent admission 只建立一個 root／queue／reservation，
  stale configuration／ATR／quota／WRONGTYPE 無半成品、controller replicas 去重、
  typed input／實際 worker／短 JSON、root pause／resume／cancel、共享帳務、跨日校正、
  necessary failure 取消與 stale-owner fencing、當前 ACL、舊 approval 不能恢復。
- actual LangGraph＋真實 Redis checkpoint＋真實 ATR：write branch 等核准時，另一 branch 完成；
  重開 saver／model 後，exact selector 只寫一次；完成的 receipt 可追蹤到 root。
  模型／工具為測試替身，沒有宣稱實際模型品質或實際 browser 外部系統變更。
- 最後 root bounded-reference／收據查詢變更再做相關回歸。
- root／Goal／checkpoint／工具／planning 的最後相關回歸 **105 passed**，50.02 秒，8 個同類警告。
- 加入 route drift fail-closed 後再做相同範圍回歸 **106 passed**，53.40 秒；
  最後 admission replay／replica／root-reference 專項 **3 passed**，重驗 compiled integrity，
  不向錯誤回顯損毀內容，且不重複 enqueue。Ruff／format／compileall／diff whitespace 通過。
- mode 回 plan 必須等 workflow phase finished 與 quota reconciled，不能在取消仍傳播時就開新一輪；
  此項及 planning／receipt replay 最後針對性回歸 **12 passed**，4.59 秒。
- 無網路、唯讀 Docker／Python **3.12.15** 成功匯入 runtime／worker／planning 模組；
  此項只證明平台與依賴相容，不冒稱 HTTP／worker 故障 E2E 已完成。
- 故障注入 fixture 僅清理自己的唯一 synthetic tenant，不 flush server；
  完整驗證另重建本輪專用容器，保留故障案例與健康 root 能並存的明確斷言。
- 修復 quarantine 並重建隔離資料庫後，最終全量 **864 passed、零 skip**，155.71 秒；
  8 個 RedisSaver deprecation warnings 保留。沒有以失敗前的 861 項結果冒充修正後完整驗證。
- worker 在取得模型 client 前及後續呼叫邊界重驗本機 router 的安全 fingerprint，
  不因 alias 相同就改用不同 endpoint／設定。這不等同 M5 的跨副本版本 publication，
  實際模型 weights／tokenizer／deployment 版本仍明確為 unknown。

## 未完成，必須接續

- Docker 真實 HTTP／worker／scheduler，多副本以及程序中斷、重新啟動、queue redelivery E2E。
- 滾動升級／降版隔離尚未驗證：目前 workflow 仍沿用 Goal queue／record namespace。
  **不得在新舊 worker／scheduler／API 混跑時開啟 admission**。接續需要協定／佇列與資料命名空間
  的 fail-closed 隔離及降版測試，不能只靠新增服務認得 metadata 就宣稱舊服務也安全。
- Gateway／worker／scheduler 的安全旗標需要一致。接續要將 admission 的 ACL posture
  持久化並防止副本設定漂移，不能用較寬鬆 worker 設定取代較嚴格的入場治理。
- 補齊 runtime 分支與型別失敗／quota wait／pause-resume 中斷／ACL race 的故障矩陣、
  大型 bounded root 的 Postgres 投影驗證及最終 review；M4 尚不標完成。
- M5 forecast／版本 telemetry／校正、M6 搜尋與 lifecycle、M7 管理台與協作／通知、
  M8 ACP、M9 故障／PITR／壓測及封版證據全部保留。
- 只在 dc-tw/ai-ops-gateway 本機提交；不 push，不修改 Bohr repository 或 Issue。
- 本輪拋棄式 Redis／Postgres 驗證容器完成後清理；不清除既有使用者資料或其他服務。
