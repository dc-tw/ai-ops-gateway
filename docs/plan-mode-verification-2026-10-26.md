# 2026-10-26 規劃模式與計畫快照驗證

## 交付範圍

Gateway 原生 PlanningSession、不可變 PlanRevision、有效設定快照、CAS 與模式切換。
不包含 forecast、plan-only Skill v2、模型自動起草或固定計畫啟動 Goal。
本頁保留核心 plan mode 階段的驗證結果；當時沒有 commit、push、更新 Bohr Issue 或正式環境部署。
後續連同 Skill 管理完成本機 milestone commit，最新回歸與缺口見
[封版彙總](releases/2026-10-26-skills-plan-mode.md)；沒有推送或遠端發文。

## 測試結果

```text
TEST_REDIS_URL=redis://127.0.0.1:16379/0 .venv/bin/pytest -q
404 passed, 6 warnings in 28.63s
```

無 skip。Python 3.14，專用 Redis 8.2 DB 0，含 JSON／Search modules。
六個 warnings 仍為既有 RedisSaver connection helper 的 deprecation warning。
Ruff lint、變更檔案格式與 `git diff --check` 通過。

新增四個測試檔共 123 cases，前一階段 Skill 工作樹基準為 281 cases：

| 測試檔 | 驗證重點 |
|---|---|
| `tests/test_planning_model_routes.py` | 與有效 allowlist／alias／default 一致，無 client 初始化或 RPC，路由指紋不含 API key／費率。 |
| `tests/test_planning_snapshot.py` | Chief／Worker 獨立設定與 Skill、角色／allowlist、來源 WATCH、版本與 schema／hash、通用錯誤不洩漏指令。 |
| `tests/test_planning_store.py` | immutable revision、20 writer 單 winner、設定／mode 競爭、no-op CAS、接受 hash、refresh、active Goal、容量／重試上限。 |
| `tests/test_planning_api.py` | 真 Redis API、RBAC／tenant／404、strict versions、偽造設定拒絕、PII／ATR／完整掃描、模式限制、歷史、零執行副作用。 |

API 測試額外驗證：

- 改變服務端 ModelRouter 的 endpoint／resolved model 後 current → stale；明確 refresh 後
  取得新指紋，舊 revision 保持原值，全程禁止 `router.get()`／模型 RPC。
- ATR finding 偵測及阻擋兩種情境均保存安全 metadata，但沒有發布 automation internal event。
- 文字超過設定的完整掃描長度時回 413，不把截斷掃描當作通過；保留 literal newlines 與 URL。
- 不建立 Goal、不寫入執行 queue、不預留 daily tokens／run slot／Agent 每日執行次數。

## Docker 跨程序驗證

```bash
sh scripts/test_docker_stack.sh
```

Python 3.12 的 Gateway／Redis／Browser Worker／Goal worker／scheduler／mock inference stack
通過；新增 `scripts/verify_planning.py` 由 Gateway container 經真實 HTTP 呼叫規劃 API：

1. 安裝 Skill，建立有自己 Skill 的 Worker 與獨立 Chief。
2. Chief 保存明確 Worker assignment，revision 固定雙方設定且不混用 Skill。
3. 切 execute 保存接受 hash，但沒有開始執行，execute mode 編輯回 409。
4. 回 plan 後更新 Worker persona，session 標 stale；refresh 建立 r2，r1 不變。
5. HTTP 前後模型用量 metrics、Goal queue、tenant Goal 清單與 internal event stream 都沒有增加。
6. audit 不含 persona 或完整 instructions。

其後既有 HTTP／SSE／usage／memory、Goal ACK、MCP stdio → Browser Worker、SIGKILL checkpoint
resume、ATR detection-only／calibration 與 blocking live／replay 不洩漏回歸全部通過。
這是控制流程與治理證據，不是新模型語意品質或完整 production 安全認證。

## 審查與限制

- 獨立審查未發現目前 API 可達的 P1 問題；已補同模式切換的空交易 WATCH 檢查、嚴格
  accepted revision 型別、refresh missing revision 防禦，與相應競態回歸。
- 尚無中央 route／deployment epoch；路由快照只能辨識 serving process 的 configured identity，
  不證明遠端權重／tokenizer／部署版本，也不宣稱跨 process 熱更新原子一致。
- Planning Skill v2、forecast／校正／預測 SSE、計畫 compiler／execute admission bridge 均未實作。
  現有 v1 Skill hash 與 Goal 執行語義未修改；切 execute 只接受計畫，仍不執行。
- 沒有自動歸檔／DELETE，需依固定容量上限、Redis AOF／volume 與備份政策操作。
- 測試 Docker stack 的容器、network 與 volume 由腳本清除；專用測試 Redis 亦於驗證後移除，
  測試暫存不保留。沒有操作既有其他專案的容器或正式資料。

操作方式與 API 範例見 [plan-mode.md](plan-mode.md)。
