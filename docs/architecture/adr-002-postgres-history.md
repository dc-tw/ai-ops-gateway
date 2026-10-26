# ADR-002：PostgreSQL 歷史平面與 Redis 執行權威分離

- 日期：2026-10-27
- 狀態：採納；本版實作範圍與部署驗收分開記錄
- 範圍：長期 record／event／conversation history、tenant 查詢、受限中繼資料搜尋、遷移與保留政策
- 關聯：[Buzz-inspired roadmap](buzz-inspired-roadmap.md)、[操作指南](../postgres-history.md)

## 背景

現有 Gateway 使用 Redis 保存即時與持久化 Goal、approval、Agent／Skill 設定、planning snapshot、
conversation、ATR finding、audit、SSE replay、queue、lease、quota 與 LangGraph checkpoint。
TTL／stream trim 與熱資料容量管理，不適合作為無期限歷史查詢保證。需要獨立長期儲存與搜尋，
但不能讓一份延遲的歷史快照成為可重放已撤銷權限、過期 approval 或舊額度的第二個執行權威。

直接在每個 handler 同步雙寫 Redis／PostgreSQL，會在程序 crash 或其中一方 unavailable 時留下
難以識別的部分成功；將全部執行儲存一次搬到 PostgreSQL 則會擴大至 checkpoint、queue、quota、
tool reconciliation 等既有安全合約。本次不做這種 runtime 改寫。

## 決策

### 1. PostgreSQL 是歷史查詢權威，不是即時執行權威

PostgreSQL 保存白名單領域 record、事件與 conversation message，提供獨立 `/v1/history` API。
read cutover 只切換這組新 API 的可用性，不改既有 Agent／Goal 控制、approval、revocation epoch、
quota、lease、queue、checkpoint 或 SSE API 的權威來源。它們仍使用 Redis。

record 表保存資源最新投影；不可變 Skill／planning revision 另以 revision 資源保留；event 表
提供事件歷史。這不是所有舊 `SET` 的版本資料庫。tool receipt 只保存執行 reconciliation 所需的
安全中繼資料，不保存工具參數；conversation summary／memory cache 不另行歸檔。

### 2. Redis 原子 outbox，再由 PostgreSQL 冪等投影

非空 DSN 時，所有 Gateway／Worker／Scheduler 使用相同 source ID 的 `DurableRedis`。adapter
攔截白名單 `SET`／`XADD`／conversation `RPUSH`／明確刪除操作，與領域變更一起原子寫入
Redis outbox；支援原有 pipeline、`WATCH/MULTI` 與受信任 application Lua。未知 key 與執行內部
狀態不在歸檔範圍。不支援的寫入語法拒絕，新增 writer 必須有 capture regression。

單筆來源 raw JSON 上限 1 MiB、完整 envelope 上限 8 MiB、decoded record JSON 上限 2 MiB，
拒絕 NUL（U+0000）字元，仍接受 JSON `null`；`SET GET`、`XADD MINID`、`EVALSHA` 等不支援語法 fail fast，不改變既有
Store 正常選項的語意。

這是有 schema version 的儲存 mutation envelope，不宣稱已完成 roadmap 的統一語意 event
envelope；既有 SSE payload 不因歸檔而破壞相容性。

projector 使用 Redis consumer group，PostgreSQL commit 成功後才 ACK；consumer crash、PG
中斷或 ACK 遺失都可能重送。來源位置＋envelope hash ledger 保證投影冪等，event identity＋
payload hash 保證事件重播不複製，record 來源位置 fence 防止亂序舊值覆蓋新值。PG transaction
與 tenant advisory lock 序列化同 scope 的變更／purge；多 projector 可並行處理不同工作。
相同 identity 對應不同內容則拒絕，不能默認成普通 duplicate。

`POSTGRES_SOURCE_ID` 是固定的 Redis 來源身分，不是由 request 提供的租戶參數。不同 Redis
不可共用；來源被清空或還原時必須處理游標／ledger 相容性，不能任意沿用造成位置衝突。

### 3. 明確 schema migration 與受維護 fence 保護的 cutover

schema version 1 的 SQL checksum 與 migration ledger 必須相符，migration 使用 PostgreSQL
transaction advisory lock。只有顯式 `migrate` 可建立 schema；runtime 只驗證，不自行修補未知
版本、缺表或 checksum mismatch。

遷移流程為：

```text
migrate
  → 所有 capture writers rollout，清除舊 raw writer
  → shadow capture + projector
  → maintenance fence 下 backfill
  → maintenance fence 下 reconcile = verified
  → POSTGRES_HISTORY_MODE=postgres
```

maintenance 使用可續租 Redis lease；參與的領域 writer 暫停，projector 繼續，lease 遺失或 unresolved
pending 會使驗證失敗。此 fence 無法攔住舊 binary 或外部 raw Redis writer，所以完全 rollout 是
操作前提，不能用「已設定一個環境變數」代替。

backfill 只涵蓋來源仍保留的 record／stream entry／message。TTL／trim 前已遺失的歷史無法補回。
reconcile 比較保留來源子集與 PG，允許 PG 保存額外長期歷史及明確 suppression；最新 report
不是 `verified` 時 history reads 拒絕。它不是全歷史完整性證明。

### 4. 查詢、搜尋與權限在歷史 API 重新判定

每次 API 認證後以 principal tenant scope 查詢，viewer 可讀、admin 可查 status／purge；不能
指定任意 tenant、SQL、table 或排序。tenant 之外的單筆資源不洩漏存在性。DB 查詢使用參數化
資料值，query／cursor 以型別及 allowlist 驗證。

目前 viewer 授權是 tenant-wide，不新增 owner／resource ACL。資料庫本身及備份仍須限制存取。
history payload 可能有 persona、Skill 內容、計畫與對話，不能因搜尋受限就當成完全無敏感資料。

搜尋只索引白名單中繼資料；採 PostgreSQL `simple` FTS＋GIN 及字面子字串補充中文查找，
不提供中文語意斷詞、向量搜尋，也不索引 private prompt、context、對話正文或原始工具內容。
搜尋結果仍必須走 tenant／RBAC scope。[PostgreSQL FTS 文件](https://www.postgresql.org/docs/17/textsearch.html)
說明 parser、dictionary 與索引的區別。

### 5. 保留政策使用 tombstone 與 kind-wide fence，不能只刪 payload

history purge 是 tenant＋kind＋來源位置範圍的 archive-only 刪除，不刪活躍 Redis、不取消工作、
不更動 approval／Skill policy。它保留 resource tombstone 與 kind-wide purge fence，抑制晚到的
舊事件及 backfill 復活；kind-wide fence 對該 kind 的 backfill 採保守阻擋，正常更晚的 live
寫入仍可投影。明確 conversation DELETE 也產生防復活 tombstone，TTL／容量淘汰則不是邏輯刪除。

purge 的 cutoff 不得超過該 tenant 的最新已投影來源位置，防止未來位置永久遮蔽新資料；操作員
從 status 的 `latest_cursor` 或已確認的更早位置選擇。purge 後中止 capture 的新資料，也不能
依賴該 kind 的普通 backfill 補回；reconcile 的 excluded 不等於完整資料一致。重新導入須經
受控的新 source／遷移審查，不能靜默刪除 fence。

操作／事件去重 hash 與 identity ledger 仍保存；這不是資料全域抹除。outbox、Redis 活躍內容、
replica／WAL、備份與外部匯出依資料治理另行清除；還原舊備份後須重新套用刪除政策。

outbox `compact` 僅依唯一預期 consumer group 的 delivered／最早 pending 邊界清理，不能
任意 trim 未 ACK 資料。invalid／conflicting entry 保持 pending，errors stream 只記 event ID
與安全錯誤分類；cutover 不以自動 ACK poison record 掩蓋缺口。

## 故障語意與運維責任

- PG 在已啟動的服務期間中斷：Redis execution／outbox 可繼續，projector 保留 pending；history
  回 503，不回退到短期 Redis 歷史。須監控 backlog 與 Redis 空間，不能讓停機期間無上限累積。
- 啟用 DSN 的 Gateway 在 PG down、schema 未 migrate／不相容時 startup fail closed；不靜默
  啟動未驗證 archive。Worker／Scheduler 的執行仍以 Redis 為主，不靠 PG 做核准。
- Redis unavailable 時，PG history 是獨立讀取路徑；執行必須拒絕／不可啟動，不從 PG 復活已
  失去時效的工作或權限。
- 寫入與 outbox 的原子性只在 Redis 內成立。PG 是批次 eventual projection；整體 RPO 仍受
  Redis AOF／磁碟／備份及事故影響，沒有即時零 RPO 或跨儲存 transaction 承諾。
- rollback read mode 至 shadow 可關閉 history reads並保留 capture；退回舊 writer／移除 DSN
  則會中止 capture，需維護與備份，不是無損 rollback。
- compact 已刪除的 ACK entry，不能用來追補較舊 PG 備份。PG 備份／PITR、Redis AOF 與 outbox
  保留窗口必須協調；不承諾任意舊 backup 都可自動恢復為最新完整歷史。
- 正式環境使用 secret manager、TLS、managed PG、受限網路與獨立備份治理。本機 Compose 公開
  密碼只供可拋棄環境。Kubernetes migration Job 必須手動先執行，不自動以 HTTP request 建表。

## 替代方案與取捨

| 選項 | 決定 | 理由 |
| --- | --- | --- |
| 每個 API handler 同步雙寫 | 不採 | 無法以單一 transaction 解決跨 Redis／PG 的部分成功；漏掉背景／Lua writer 風險高 |
| PostgreSQL 完整取代 Redis runtime | 不在本版 | 改寫 approval、queue、epoch、quota、checkpoint 的風險超出長期歷史需求 |
| 定期 SCAN 作為唯一歷史匯出 | 不採 | 無法補回 TTL／trim 間的事件；只適合受界線限制的 backfill |
| Redis mutation outbox＋PG projection | 採納 | 重用執行合約、可捕捉背景／CAS／Lua、能測試重送與 cutover；代價為 eventual consistency 與 outbox 運維 |
| 對所有 JSON 欄位全文索引 | 不採 | 增加敏感內容暴露與跨用途查詢風險；首版以白名單中繼資料為界 |

## 驗證要求與非目標

驗證以實際測試紀錄為準，不以此 ADR 的存在當作通過證據。自動化入口包含
[`test_postgres_store.py`](../../tests/test_postgres_store.py)、
[`test_postgres_projection.py`](../../tests/test_postgres_projection.py) 與
[`test_postgres_stack.sh`](../../scripts/test_postgres_stack.sh)。專用 Docker 測試應涵蓋兩個
projector、PG 真實停機／恢復、tenant／RBAC、backfill／reconcile、read cutover、搜尋、purge
防復活與真實 `pg_dump`／`pg_restore` 內容摘要比對。[PostgreSQL dump 指南](https://www.postgresql.org/docs/17/backup-dump.html)
是備份工具語意的參考；實際 production restore、HA、容量與 RPO／RTO 仍須獨立演練。

本 ADR 不宣稱完成 tamper-evident audit hash chain、統一語意 event envelope、所有程式副作用
exactly-once、PostgreSQL 接手即時 execution、plan-forecast、前端、中文語意搜尋、production
Kubernetes rollout、全來源無損恢復或所有持久化層的資料抹除。這些需分別評估與驗收。
