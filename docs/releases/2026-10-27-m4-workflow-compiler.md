# M4 第一段：Plan v2／YAML 與不可變編譯產物

基準 `7c6ebe3`。這是 M4 的編譯與規劃部分，不宣稱 M4–M9 完成。
操作與 YAML 範例見 [Plan v2 指南](../workflow-plan-v2.md)。

## 實作

- opt-in Plan v2／安全 YAML、拓樸排序、祖先依賴、Agent／團隊預算與圖容量限制。
- compiled nodes 固定 Agent 版本、model、每節點 Skill 選擇與完整 Skill 快照；
  configuration／normalized input／compiled hashes 綁定不可變 revision。
- 舊線性與 Chief Plan 用顯式 adapter，不回寫舊歷史或改 hash；Chief 匯聚為唯讀 synthesis。
- 有限條件／JSON Pointer／結構化輸入、all_success／any_success、失敗／略過／核准等待純決策。
- JSON Schema 2020-12 在可終止子程序，限制 schema／output bytes、wall／CPU／Linux memory、
  驗證程序併發；registry 不提供 remote retrieval，拒絕不支援的 schema 身分／動態引用。
- operator YAML 保存、viewer compiled artifact、既有 tenant／owner／分享 ACL。
  編譯、接受與切換 mode **沒有**模型呼叫、Goal 入列或 quota 保留。

## Review 修復

- JSON 省略 deadline 與 YAML 補齊預設時，`300`／`300.0` 原本造成不同 compiled hash；
  改為一致浮點預設，驗證順序、JSON／YAML 與 defaults 不影響正規化 hash。
- 新 YAML／compiled API 的缺漏匯入已由 Ruff 抓出並補上。
- schema／condition／default 的 literal 字串與換行也納入 ATR，不只掃描 query／URL。
- 模型 JSON 重複鍵一律拒絕，避免不同 parser 對同一輸出解讀不一致。
- 巢狀比較保留 JSON 型別，不把容器內的 `true` 當成 `1`；legacy Chief adapter
  保留 assignment objective 到節點 query，避免轉換時遺失子任務範圍。
- 輸出解析前先檢查 16 KiB UTF-8；超限、invalid schema、逾時或未驗證輸出不得進入條件。
- native refresh 使用部署的圖限制，不誤用預設上限；compiled 竄改轉為安全 409，
  不把損毀內容回顯給使用者。legacy validator 保持非 dict 的安全 ValueError 行為。

## 驗證環境與命令

獨立 localhost Redis 8.2／Postgres 17-alpine 容器，沒有使用既有專案資料庫。
本機 Python 3.14.6、jsonschema 4.26.0、referencing 0.37.0、PyYAML 6.0.3。

```bash
TEST_REDIS_URL=redis://127.0.0.1:16379/0 \
TEST_POSTGRES_URL=postgresql://gateway:gateway-m4-test@127.0.0.1:15432/gateway \
  .venv/bin/pytest -q -x
.venv/bin/ruff check .
git diff --check
```

- 第一輪全量 841 passed；加入輸出 bytes／惡意 regex 與 compiled 竄改後，第二輪
  **845 passed、零 skip**，148.75 秒；6 個既有 RedisSaver deprecation warnings。
- 再加入 Chief legacy 匯聚／節點 Skill 固定／即時分享撤權，專項
  **62 passed**（API＋compiler），6.40 秒。
- 全量擴充後 **848 passed、零 skip**，173.74 秒，仍為 6 個既有警告。
  最後巢狀比較與 assignment objective 修正後，再跑 compiler／API／snapshot／store
  **129 passed**，15.08 秒；Ruff、format、compileall 與 diff whitespace 通過。
- 真實 Redis 驗證 YAML→CAS revision→compiled→接受 mode、role／跨 tenant、超限不寫入、
  compiled 竄改 fail closed、分享與撤權即時生效；同時斷言沒有 Goal 或額度副作用。
- compiler 測試：重複節點／邊、未知 Agent、self-edge／cycle、預算、遞迴派工、schema／祖先引用、
  有限比較、兩種 join 等全部前置終態、false skip、缺必要輸入失敗、必要失敗取消清單、
  核准釋放執行容量但仍阻塞依賴、同 Agent 純決策串行、JSON／YAML 等價與舊 Plan 不變。
- Docker／Python 3.12：以既有測試 image、`--network none`、repository 唯讀掛載執行
  `scripts/verify_workflow_compiler.py`，通過中文 YAML、schema 與惡意 regex kill／reap／後續復原。
  此項驗證 Linux 隔離行為與執行環境相容性，不冒稱整套 DAG dispatch E2E。

## 待交付（不縮減原計畫）

- M4 明確 execute admission：accepted hash、current configuration／epoch、ACL、ATR、quota、
  冪等鍵、原子建立 root Goal／queue／session active reference。
- 持久化 DAG controller 與既有 worker／逐次寫入核准整合；跨 Goal Agent 併發、
  failure cancellation／receipt reconciliation、共用預算、SIGKILL／副本去重與實際 DAG E2E。
- M5–M9 與 M2 剩餘治理維持 [roadmap](../implementation-roadmap.md) 契約。
  不自動跑付費模型，不將本機／mock 測試冒稱正式部署或 PITR 驗收。
- 本次僅本機 commit；沒有 push，也沒有更新 Bohr repository／Issue。
- 本輪隔離 Redis／Postgres 測試容器於驗證後清理；未刪除既有專案資料或其他服務。
