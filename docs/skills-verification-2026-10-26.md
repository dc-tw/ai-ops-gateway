# 2026-10-26 Skill 管理驗證紀錄

本頁保留 Skill 階段結果；整合 plan mode 與後續安全補測後的最新結果見
[封版彙總](releases/2026-10-26-skills-plan-mode.md)。

## 交付範圍

依使用者確認，本次為 Gateway 自身的 Skill 管理 API 與中文文件，不是安裝 Codex Skills，
也不包含管理前端。Skill 是可重用的 instruction pack，不是可任意執行的外掛程式。

- 十種內建繁中工作流程、tenant 安裝副本與自訂內容。
- 不可變 revisions、名稱唯一性、設定版本 CAS、rollback 建立新 revision。
- Agent 明確版本綁定、Goal 快照、各 Chief／Worker 獨立指令與既有共享 token 上限。
- Skill 啟停／撤銷 epoch、模型／工具邊界檢查、多入口原子 admission。
- metadata-only audit、基本 `SKILL.md` 文字匯出。

## 最終結果

```text
TEST_REDIS_URL=redis://127.0.0.1:16379/0 .venv/bin/pytest -q
281 passed, 6 warnings in 24.90s
```

沒有 skip。六個 warnings 是兩組真實 checkpoint 測試中，既有 RedisSaver connection helper
的第三方 deprecation warning。測試環境為 Python 3.14 與專用 Redis 8.2 DB 0。

`ruff check .`、已變更 Python 檔的格式檢查與 `git diff --check` 通過。
兩組 Docker 整合腳本以 Python 3.12 與 deterministic mock inference 通過。

## 驗證矩陣

| 要求 | 主要證據 |
| --- | --- |
| 十種可列出／查看／安裝的內建範本 | `tests/test_builtin_skills.py`、API 與 Docker |
| admin 寫入、viewer 查詢、跨 tenant 404 | `tests/test_skills.py` |
| 禁止 caller 偽造身份、policy、scripts 等欄位 | schema／API 回歸 |
| Immutable revisions／名稱唯一性／CAS | 真 Redis 20 個競爭 create／publish，只各有一個成功 |
| rollback 不覆寫舊 revision；匯出可解析 YAML | API revision／export 測試 |
| Agent binding 固定 revision、限制角色／必要工具／數量／UTF-8 bytes | Skill core 與 runtime 測試 |
| 指令不擴大工具／網域／額度、不跳過 ATR | `tests/test_skill_runtime.py` 與既有 policy 回歸 |
| 每次完整／串流模型呼叫都注入且計費 | BudgetedModel 測試與 Docker mock 拒絕缺 Skill 指令的請求 |
| manual／automation／Chief 子任務原子 admission | `tests/test_skill_admission.py` 的真 Redis WATCH／Lua 競態 |
| 停用與立即重新啟用不能復活舊快照 | epoch 單元／admission 測試與 Docker 在途模型呼叫情境 |
| 版本更新不改寫已保存的 Goal／prepared children | immutable snapshot、delegation、Docker pinning |
| 核准與重啟後仍用原 Skill／額度 | 真 Redis AsyncRedisSaver 關閉／重開三次，兩個 action 分別核准 |
| 稽核不含完整 instructions；錯誤不洩漏驗證 input | Skill／Agent／Goal／delegation／automation audit 與損壞 snapshot 測試 |

## Docker 實際觀察

`sh scripts/test_proactive_stack.sh`：

1. 安裝 ATR 命中檢視、事件初判、Chief 派工協作，分別綁定資安、運維與 Chief。
2. 發布 ATR Skill revision 2；已綁 revision 1 的 Agent 仍保存原內容及 hash。
3. 定時 ATR 摘要、HMAC webhook、內部事件、Chief 委派與彙整皆正常完成。
4. Mock inference 檢查每次帶驗證標記的請求，必須包含指定 revision 1 的 Skill 名稱及實際指令內容。
5. 各 Worker 使用自己的 Skill，而非繼承 Chief 的 Skill。
6. 模型 RPC 已開始後，停用並立即重新啟用運維 Skill；該舊 Goal 仍以 PermissionError 失敗，
   且未開始第二次模型呼叫。
7. 原有兩個 scheduler／兩個 worker、SSE replay 與 worker SIGKILL 恢復情境仍通過。

`sh scripts/test_docker_stack.sh`：既有 HTTP／SSE、usage、conversation memory、queue ACK、
MCP → Browser Worker、前景 checkpoint resume 與 ATR blocking live/replay 不洩漏均通過。

## 複核與限制

- 獨立程式複核指出，直接轉述 Pydantic validation error 可能包含損壞指令片段；已改為隱藏 input
  並回傳固定 Skill 拒絕訊息，加入回歸。
- 停用保證後續 admission／受檢查邊界拒絕，不回滾已在途 RPC 或外部副作用；待核准 Goal 不會被
  停用 API 批次改寫狀態，需要立即結束時仍應明確 cancel。
- Mock 證明指令傳遞、版本與治理，不證明摘要、調查或資安判斷的語意品質；內建目錄另附真人驗收情境。
- 沒有腳本執行、完整外部 Skill package 匯入、自動選 Skill、管理前端或新增 MCP server。
- 匯出僅參考 [Agent Skills 基本格式](https://agentskills.io/specification)，不宣稱完整 package 相容。
- 既有 ATR bundled rule `ATR-2026-00290` condition 3 的 regex warning 不在本次修復範圍。
- Redis AOF／volume 與備份仍是持久性前提，沒有新增歷史資料自動歸檔。

## 重跑

```bash
ruff check .
TEST_REDIS_URL=redis://127.0.0.1:16379/0 pytest -q
sh scripts/test_docker_stack.sh
sh scripts/test_proactive_stack.sh
```

請只對測試專用 Redis 執行。Docker 腳本清理自己建立的 project／volume，不操作正式資料。
操作方式見[管理指南](skill-management.md)，內建用途與驗收見[目錄](skills-catalog.md)。
