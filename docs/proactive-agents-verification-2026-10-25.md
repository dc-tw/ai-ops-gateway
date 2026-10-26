# 2026-10-25～26 主動工作與 Chief 派工驗證

## 範圍與環境

本次驗證的是 Gateway 內的可執行功能，不是 Codex 桌面 app 的排程功能，也沒有部署至正式環境。
程式碼只提交至 `dc-tw/ai-ops-gateway`；Bohr 的學習共筆專案不接收此程式碼。

- 本機完整測試：Python 3.14、測試專用 Redis 8.2（JSON／Search，DB 0）。
- Docker：Python 3.12、Redis、Gateway、Browser Worker、兩個 Goal worker、兩個 scheduler。
- Inference：deterministic OpenAI-compatible mock，驗證控制流程，不證明真實模型分析品質。
- CI：Python 3.12 + Redis 8.2，並執行既有與新增兩組 Docker 腳本。

本機最終完整測試結果：`196 passed, 3 warnings in 22.17s`；沒有 Redis 測試被 skip。
`ruff check .` 與 Kubernetes YAML 結構檢查通過。新版主動工作 Docker 流程也已通過。

## 需求與證據對照

| 已確認需求 | 驗證入口 |
| --- | --- |
| cron／interval、台北時區、昨日 ATR 摘要 | `tests/test_automations.py`、`scripts/verify_automations.py` |
| HMAC webhook、重送去重、部署失敗分析 | 同上，含錯誤簽章與 header／body 邊界測試 |
| ATR finding／Goal failure、預設禁止連鎖 | `tests/test_automations.py`、`tests/test_goal_execution.py`、Docker |
| 重疊／漏跑合併、每日上限、多 replica | `tests/test_automation_store.py`、scheduler crash 回歸、Docker |
| 逐次人工核准、拒絕／逾時、重啟恢復 | `tests/test_goal_execution.py`、`tests/test_goal_checkpoint.py` |
| 持久化結果、查詢 API、SSE replay | API 測試及 Docker 生命週期事件驗證 |
| 獨立 persona／模型／工具／網域／額度／啟停 | `tests/test_agents.py`、`tests/test_execution_policy.py`、`tests/test_browser.py` |
| 單層 Chief 派工、子 Agent 回報與彙整 | `tests/test_chief.py`、`tests/test_delegation.py`、Docker |
| Chief 與子任務共用總 token cap | 分配／越權／恢復測試及 Docker allocation assertion |
| tenant 額度、owner fencing、健康 worker 不被搶走 | 真 Redis concurrency／lease／quota 測試 |

## Docker 觀察結果

`sh scripts/test_proactive_stack.sh` 已通過下列實際跨程序流程：

1. 建立資安、運維與 Chief 三個獨立角色設定。
2. 從前一個台北曆日 retained metadata 產生定時 ATR 摘要。測試將 cursor 設至最近一次 09:00，
   由獨立 scheduler 程序實際派送，不由驗證腳本直接呼叫 tick／dispatch。
3. 簽章部署失敗 webhook 建立 Chief Goal；相同 event ID 重送只建立一次。
4. Chief 派出兩個不同 persona 的 Worker Goal，等待完成後重新入列並彙整；總分配不超過 20,000。
5. ATR finding 與手動 Goal 失敗各自啟動對應內部工作。
6. 兩個 scheduler／兩個 worker 下，每個來源 occurrence 只建立一個主任務，結果與 SSE 可重讀。
7. 模型呼叫期間 SIGKILL 兩個 worker，重啟後由新 execution fence 恢復並完成背景 Goal。

`sh scripts/test_docker_stack.sh` 另驗證既有 HTTP／SSE、usage、conversation memory、Goal queue ACK、
MCP stdio → Browser Worker、前景 SIGKILL checkpoint resume，以及 ATR blocking 在 live SSE 和
Redis replay 中不洩漏被阻擋輸出。

## 複核中修正的邊界

- 排程入列與 schedule cursor 改為同一 Redis 交易，避免 dispatch 後／cursor 更新前 crash 吞掉新漏跑。
- ATR metadata／Goal failure 與安全內部事件同交易保存，避免來源成功保存但事件漏送。
- 長任務 heartbeat 同時更新 concurrency slot 的 score 與集合 TTL；被撤銷的 slot 不會復活。
- 重複 pause 保持 `pausing`，直到 owner 停止或 lease 到期，不能提前 resume 搶走執行權。
- Chief 取消後的子任務清理由 reconciler 補償，包含等待核准中的子任務。
- Terminal Goal 在對帳前 crash 時，由 scheduler 補償返還未用額度；重跑不會二次退款。
- 模型 usage 必須完整、非負且一致；未知或異常數據使用保守預留，不得降低累計額度。

這些是多代理獨立程式複核與可重現回歸測試，不代表已完成真人安全稽核。

## 重跑

```bash
ruff check .
TEST_REDIS_URL=redis://127.0.0.1:16379/0 pytest -q
sh scripts/test_docker_stack.sh
sh scripts/test_proactive_stack.sh
```

測試只清除自己建立的 Docker project／volume；不要將 `TEST_REDIS_URL` 指向正式 Redis。

## 限制

- 目前只有 API／SSE，沒有新增管理前端或外部通知管道。
- Chief 的 child 結果與來源 ID 有保存；模型生成的摘要仍須人工確認，不是子任務 citation 的形式化證明。
- Redis AOF／volume 是 durability 的前提；沒有實作 Postgres migration、audit hash chain 或資料自動歸檔。
- Scoped Browser context 拒絕 HTTP redirect／WebSocket；外部副作用遇到不確定 receipt 時停止等待人工查核。
- ATR bundled rule `ATR-2026-00290` condition 3 的既有 regex warning 仍可重現，見
  [ATR 限制](atr-evaluation/limitations.md)；本次未宣稱修復上游規則。
- 真 Redis checkpoint 測試出現第三方 deprecated connection helper 警告，不影響本次測試結果。
- Kubernetes baseline 只做 YAML 結構驗證，未執行真實 cluster rollout。

操作範例見 [主動工作指南](proactive-agents.md)。後續探索與驗證會繼續記錄於
[Discussion 暫存紀錄](architecture/discussion-log.md)，待啟用 GitHub Discussions 後建立連結。
