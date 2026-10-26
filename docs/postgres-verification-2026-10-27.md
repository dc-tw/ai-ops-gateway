# 2026-10-27 PostgreSQL 歷史平面驗證

## 交付範圍

依使用者指示，先完成 Postgres 再推送 `dc-tw/ai-ops-gateway`。本次基準為本機 `6ab218f`，
沒有修改 Bohr repository 或發布 Issue 留言。這版交付 **Postgres 長期歷史／查詢平面**，
不是將 Redis 即時 runtime 全面改成 SQL，也不包含前端或 `plan-forecast`。

- 新增 checksummed schema migration、async pool、typed resource／event 儲存與中繼資料搜尋。
- Gateway／Worker／Scheduler 都可選啟用 Redis 原子 outbox；涵蓋既有 Store 的 pipeline、
  WATCH／MULTI、automation Lua 與工具 receipt，不在 API 成功後另做易遺漏的同步雙寫。
- 多 projector 採至少一次投遞；PG transaction 以來源位置／hash 去重及 fence，commit 後才 ACK。
- 提供 maintenance backfill、reconcile、明確 history cutover、purge 防復活與安全 outbox compact。
- 新增 tenant／RBAC history API、opt-in Compose、CI 真資料庫及 managed-PG Kubernetes 範例。

操作、資料權威、維護及回退見[指南](postgres-history.md)與
[ADR-002](architecture/adr-002-postgres-history.md)。既有即時 API／SSE／approval／Skill epoch／
quota／checkpoint 仍由 Redis 判定，不能使用 PG 舊快照取得執行權。

最終完整 suite：**668 passed、6 warnings、0 skipped**，耗時 95.27 秒；相較前版 478 cases
新增 190 cases。六個 warnings 仍為既有第三方 RedisSaver connection helper deprecation。
Ruff lint、此次變更 Python 格式、diff、Shell syntax、Compose 與文件連結檢查均通過。

## 自動化驗證矩陣

| 測試入口 | 實際驗證 |
| --- | --- |
| `tests/test_durable_outbox.py` | 真 Redis SET／NX／pipeline 回應、WATCH 競態、Lua 內 computed key、Agent／Skill／Plan／Goal／Chief／automation／receipt 寫入、Python／Lua key 白名單一致、tenant／冒號 conversation ID、TTL／trim 保留、NUL／容量與錯誤隱私 |
| `tests/test_postgres_store.py` | 真 PG schema 冪等／併發／checksum／未知版拒絕、重送與亂序、event identity、同 request 不同真實 message、刪除／重建／purge fence、未來 cutoff 拒絕、tenant／FTS 隔離、參數化查詢與安全 pool log |
| `tests/test_postgres_projection.py` | 真 Redis＋PG 兩 consumer 競爭、commit 後 ACK 前中斷注入、PG 錯誤不 ACK、poison 可觀測且不餓死後方 pending、backfill／reconcile 重跑、刪除後不復活、maintenance lease／清理、安全 compact、finding review 與 cache eviction、Redis 故障 history／execution 邊界 |
| `tests/test_history.py` | HTTP RBAC／tenant、shadow／verified gate、cursor／分頁、unknown query／SQL／tenant knobs 拒絕、固定安全 422／503、history-only 標記 |
| `scripts/test_postgres_stack.sh` | 公開 HTTP、兩個真 projector、真 PG／Redis stop/recover、回填對帳、切讀、tenant／RBAC／搜尋、真 dump／restore |

Postgres 測試每個案例使用獨立 schema／source／tenant，結束只清理自己的資料；不使用
`FLUSHDB` 或清空共用資料庫。未提供測試服務環境變數時可 skip，但本次完整驗證明確提供
`TEST_REDIS_URL` 與 `TEST_POSTGRES_URL`，不能用 skip 當成通過。

### 專用 Docker 實測

腳本使用獨立 `ai-ops-postgres-test` project、image、volume 與 loopback API port。流程已驗證：

1. shadow 階段透過認證 API 建立 Skill／Agent、計畫 revision／接受狀態、Goal、run、conversation
   及另一 tenant 的 Agent；history reads 正確回 409。
2. 停止 PostgreSQL 後仍能建立 Redis-authoritative Agent；恢復 PG 後 outbox 補送。
3. maintenance backfill 取得當時仍保留的來源，再 reconcile；代表性執行報告為
   `checked: 62`、`excluded: 0`、`mismatches: 0`、`status: verified`。這個數量是該次合成場景，
   不是 production 資料完整率，也不代表能補回先前已過期／trim 的歷史。
4. Gateway 明確切換 `POSTGRES_HISTORY_MODE=postgres` 後，驗證八種資源、歷史事件、穩定分頁、
   中繼資料搜尋、不索引私人 persona、跨 tenant 404／無搜尋命中及 viewer 無 admin 權限。
5. 再停止 PG，history 回固定 503、不偷偷 fallback；既有 Redis 讀取仍可用。恢復後 history 正常。
6. 真正停止 Redis，PG history 仍可查；即時 Agent／Goal 操作回安全 503，不從 archive 執行。
7. 停止此測試 stack 的 writers／projectors，執行 `pg_dump --format=custom`，還原至新的
   `gateway_restore` database；逐張 `gateway_` 表比較 row count 與整列 JSONB 摘要，一致才通過。

同時重跑三組既有 Docker E2E（一般 Gateway／主動 Agent 與 Chief／MCP remote ATR）及真實
Ollama `qwen2.5:0.5b-instruct` smoke，保持原有 Skill／planning／ATR／checkpoint／核准流程。
Mock 與 smoke 只證明控制流程／連線，不是模型分析品質或預測校正。

## Review 後修正的邊界

- 來源 raw JSON 可達 1 MiB，二次 JSON escaping 的 outbox envelope 可能超過 2 MiB；因此
  分開來源／decoded payload／8 MiB envelope 上限，writer 對 aggregate 預先檢查，真資料庫測試
  覆蓋剛好 1 MiB 的反斜線文字與多 message，避免合法來源成為永久 poison。
- XAUTOCLAIM 保留 continuation cursor，不每輪從 `0-0` 重掃；測試證明前段 poison 不會永久
  擋住後方 crashed consumer 的合法 pending。poison 本身仍不自動 ACK，cutover 會拒絕缺口。
- purge 拒絕超過該 tenant 已投影 watermark 的 cutoff，避免誤填未來最大值後永久抑制新歷史。
- Conversation identity 納入完整 message hash，區分同 request 的不同實際 append，又讓
  相同 message 的 live／backfill 重送去重；delete tombstone 防止晚到舊內容復活。
- Redis pipeline／Lua 錯誤與 psycopg pool logs 不回顯完整指令、persona、payload 或連線資料。
  PG 啟用時不合法 NUL／容量回安全錯誤，既有合法中文／emoji 內容不改寫。

## 重現

先起專用 Redis 8.2 JSON／Search 與 PostgreSQL 17。以下帳密只代表可拋棄測試環境，
不得指向正式 Redis／資料庫：

```sh
TEST_REDIS_URL=redis://127.0.0.1:16379/0 \
TEST_POSTGRES_URL=postgresql://gateway:local-test-only@127.0.0.1:15432/gateway \
  .venv/bin/pytest -q
.venv/bin/ruff check .
git diff --check
sh scripts/test_postgres_stack.sh
sh scripts/test_docker_stack.sh
sh scripts/test_proactive_stack.sh
sh scripts/test_atr_tool_stack.sh
sh scripts/test_ollama_stack.sh
```

另已解析 opt-in Compose 與 15 份 Kubernetes YAML；這不等於已在真實 cluster rollout。
CI 新增 PG service 與獨立 postgres-e2e job；本機執行結果不冒充遠端 GitHub Actions 結果。

## 未聲稱完成的部分

- 歷史採 eventual consistency；尚未投影資料仍依賴 Redis AOF／磁碟與備份，沒有 PG 即時零 RPO。
- Runtime control、model context、queue／checkpoint 不搬到 PG；沒有以 archive 恢復舊核准。
- 搜尋只有白名單中繼資料，沒有 conversation／audit 正文全文、中文語意斷詞、向量或全域
  provenance 搜尋；viewer 是 tenant-wide，沒有新增 resource owner ACL。
- purge 是 archive-only，不移除活躍 Redis／outbox／備份／WAL 全部副本；hash／identity ledger
  仍保留。kind-wide fence 會保守排除該 kind 的 backfill，`excluded` 不是完整對帳成功。
- rollback 應只停 history reads，保留 DSN／capture；停止 capture 期間的 TTL／trim 缺口不能保證補回。
  compact 舊 outbox 後也不能保證從任意舊 PG backup 自動追平。
- 真 dump／restore 測試不是 production HA／PITR／容量／RPO／RTO 演練。新系統仍需部署方的
  secrets、TLS、least-privilege DB roles、監控與備份政策。
- 原有 ATR 截斷／regex condition warning、graph checkpoint／公開 SSE 診斷事件投遞缺口、
  audit hash chain、統一語意 event envelope、前端與 forecast 均未被此工作自動解決。
- 真人語料複核由使用者自行處理，未把此工程測試冒稱成第二位真人標記驗證。

測試腳本移除自身可拋棄 stack 的容器／volume，本機專用 Redis／PG 亦在驗證後清理；
只包含可重現的合成資料，不保留暫存 dump。既有 Ollama 模型 cache 與其他專案未刪除。
