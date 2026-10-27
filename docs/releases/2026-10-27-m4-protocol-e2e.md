# M4 收尾：版本化協定、不可降級治理與 Docker 故障驗證

基準 `430b618`。本段完成 M4 的核心契約與驗證；M5–M9 及 M2 剩餘治理沒有被視為已完成。
操作見 [Plan v2 指南](../workflow-plan-v2.md)，仍須遵守下列升級與降版前提。

## Review 與修正

- Workflow root／node 帶 `workflow_protocol: 1`，ID 為 `wfv1-UUID`；root／node kind、parent／root
  identity 及 ACL posture 嚴格驗證。普通 Goal 保留原 namespace，不重寫歷史 hash。
- queue、runtime key／index／tenant index 與 legacy 分離；新的 claim／ACK、approval resume、
  recovery、quota wait 都按協定選 queue，沒有「找不到就到舊 key／queue 試試」的 fallback。
- 新 worker 交替輪詢兩個 queue，不讓其中一種飢餓；未知／無版本 Workflow fail closed，
  不因欄位無法理解就進入 free planner。舊 prototype 須人工查核，不能靜默轉成新任務。
- 根據新 namespace 更新 live resource lookup、Agent admission、寫入 receipt authority、
  planning active-Goal 檢查，以及 Python／Lua archive allowlist；仍投影成既有 Goal record family。
- root 保存 admission 時的 ACL posture；本機 worker／scheduler 設定為 false 不能降低已啟用的治理。
  每次模型邊界／quota 交易／tool check／receipt CAS 都查 root、session、child、Agent、Skill 的現行 ACL。
- 類型損毀的 workflow states 也隔離；不讓 AttributeError 中斷其他 root 的排程。
- 大型 Skill fixture 最初超過既有 8,000 字元上限，測試資料改用合法的繁中文字數／UTF-8 bytes，
  沒有為測試放寬 Skill、compiler 或 Postgres 的容量限制。

## 證據

```bash
TEST_REDIS_URL=redis://127.0.0.1:16379/0 \
TEST_POSTGRES_URL=postgresql://gateway:gateway-m4-test@127.0.0.1:15432/gateway \
  .venv/bin/pytest -q -x
.venv/bin/ruff check .
sh -n scripts/build_workflow_legacy_image.sh
sh -n scripts/test_workflow_runtime_stack.sh
sh scripts/test_workflow_runtime_stack.sh
git diff --check
```

- 完整回歸 **886 passed、零 skip**，196.18 秒；8 個既有類型的 RedisSaver deprecation warnings。
  先前一輪 881 passed（163.59 秒）；新增完整測試後以 886 項結果為準。
- 實際 Redis：namespaced claim／ACK、legacy reader 無新記錄、缺 marker／無版本 prototype
  拒絕、strict posture 在較鬆副本仍生效、模型初始化前撤權、receipt authority／MULTI 間的
  撤權競態無半成品；tenant capacity 等待不發起模型、不誤判失敗、回到正確 queue。
- pause／resume 在 root CAS 之後中斷，再用新 controller instance 繼續，沒有遺失待處理 intent。
- 真實 Postgres：32 節點、完整固定版本繁中 Skill，root 超過 350,000 UTF-8 bytes，
  成功投影且逐字相等，仍低於 2 MiB record 限制；此項是 persistence，不是假稱模型推論或 ATR。
- Docker 使用 production-mode HTTP auth、Redis 8.2、Postgres 17、真正 worker／scheduler／projector，
  模型為受控 OpenAI-compatible stub，不使用付費模型、也不宣稱實際模型品質。
- 舊 worker 實際完成普通 Goal，證明不是假啟動／未連線；新 worker 尚未啟動時，v1 root／nodes
  保持 queued、stub 零呼叫，舊 API 的 Goal GET／cancel 皆 404。
- 啟動兩個新 worker 後觀察模型端同 case 至少兩個同時 active request，typed nodes＋Chief summary 完成；
  三次 generation 的 provider-reported usage 共 54 tokens，root 一次性結算。
- 從真實 queue pending consumer 找到正在持有模型請求的 worker container，SIGKILL 後重新啟動；
  node fence／model_calls 增加，失聯呼叫保留 conservative charge，仍在團隊上限內。
  終態 node 再投遞，必須實際 delivered＋ACK 後確認 stub 呼叫數不增加。
- Gateway 啟用 ACL，而 worker／scheduler 刻意設定 false。模型請求期間撤掉 Agent grant，
  root 失敗、後續節點不入列、沒有第二次呼叫，quota 保留保守計費並結算。
- 先前 cached 舊映像 `sha256:0b0f6530c0fb378812c944a3fb830ccc7698a3390def327ab0bf954fccdac7ed`
  的完整 Docker 流程通過；backfill／reconcile 檢查 1,003 個 retained resources、零 mismatch，
  從 HTTP history 讀出 v1 Goal，execution_authority 為 false。
- 可重建舊版來源固定為 `7c6ebe351ab02726dc0853fd6f237bdabb636463`，用 `git archive` 到新暫存目錄，
  不 checkout／reset 工作樹、不複製 ignored .env，build label 記錄 commit；preflight 驗證它沒有新協定模組。
  重建映像 `sha256:e894c42b34087bfe4609fbb1cae1d2e6a766db1178134f71e5ebd0f951820e61` 的
  完整 Docker 重跑亦通過；995 個 retained resources 對帳、零 mismatch。
  資源數會隨事件／heartbeat 數量改變，不當成固定筆數的虛假斷言。

## 不擴張證據範圍

- 普通舊 Agent 工作不會神奇獲得新 Agent lease；跨版本 rollout 須先更新 mutating API、排空舊 Agent 工作，
  不宣稱任意老版本公開 API 都能安全繼續提供新功能。支援測試的舊基準是上述 M3 commit。
- 降版須先關新 admission，保留相容 worker／controller 排空 v1 工作與核准／收據，不搬 key 或自動重播。
- 目前 pin 是配置 fingerprint，實際 weights／tokenizer／runtime 仍 unknown；跨副本版本 registry 屬 M5。
- 廣泛 Redis／PG 中斷、正式部署、更多資源／租戶壓力與真正 PITR／還原仍屬 M9；
  本段測試不是 production 驗收、real-model quality／forecast calibration，也沒有做外部系統寫入。
- 只有 dc-tw/ai-ops-gateway 本機 commit；不 push，不修改 Bohr repository／Issue。
