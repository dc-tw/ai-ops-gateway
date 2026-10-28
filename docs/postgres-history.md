# PostgreSQL 歷史儲存與操作指南

更新日期：2026-10-28。架構決策見 [ADR-002](architecture/adr-002-postgres-history.md)。

本版增加可選的 PostgreSQL 長期歷史、查詢與中繼資料搜尋，不把 PostgreSQL 當作第二套 Agent
執行引擎，也不取代 Redis 的即時執行權威。預設 `POSTGRES_DSN` 為空，原有部署不會自動遷移。

## 資料權威與覆蓋範圍

| 資料／用途 | 權威來源 | PostgreSQL 的角色 |
| --- | --- | --- |
| Goal／Agent／Skill／計畫的當下狀態與控制 | Redis | 保存已投影的歷史快照，不能用來核准或恢復執行 |
| approval、Skill revocation epoch、額度、lease、queue、checkpoint | Redis | 不從歷史重建這些執行權限；checkpoint 不納入本版歸檔 |
| 已歸檔的 record、event、conversation message 查詢 | PostgreSQL；切換後由 `/v1/history` 提供 | 長期保存、分頁、依 tenant 查詢 |
| SSE 即時傳遞與既有 replay API | Redis | 歷史 events API 是獨立查詢，不改寫既有 SSE 合約 |
| 歷史搜尋 | PostgreSQL | 白名單中繼資料、明確核可的 Goal／Evidence 正文與 provenance；不是任意提示詞或語意搜尋 |

歸檔白名單包括 Agent、Skill 與其 revision、planning session 與其 revision、Goal、run、ATR
finding、automation、tool receipt、conversation，以及 run／Goal／planning／automation／audit
事件。record 表保留每個資源已投影的最新快照，不代表每次 `SET` 都有可查詢的完整舊版；Skill／
planning revision 有各自不可變的資源識別。conversation 的 message 以事件保存，summary／memory
cache 不另行歸檔。tool receipt 只保存 `status`、`tool`、`action_id`，不保存工具參數。

「受控搜尋」不等於「資料庫沒有敏感內容」：Skill 內容、persona、計畫內容與對話 message 等
原有持久化內容仍可能進入歸檔。啟用 `RESOURCE_ACL_ENABLED` 時，查詢先依 Redis 當前 ACL 限縮，
回傳結果、摘要與每個關聯又重查一次；撤權不等待 projector。ACL authority 不可用時回 503，
不退回 tenant-wide PostgreSQL 結果。未啟用 ACL 時沿用 tenant-wide viewer 行為。正式環境仍須
管理資料輸入、tenant membership、資料庫與備份存取權。

## 寫入、投影與一致性

```text
Gateway／Worker／Scheduler
  → 同一 Redis 原子操作：領域寫入 + outbox
  → Redis consumer group（至少一次投遞）
  → PostgreSQL transaction：去重、排序 fence、record／event／tombstone
  → commit 成功後才 XACK
  → 獨立 history API（eventual consistency）
```

`DurableRedis` 在既有 Store 下方攔截白名單的 `SET`、`XADD`、conversation `RPUSH`／明確 `DEL`。
一般 pipeline、`WATCH/MULTI` 與既有受信任 application Lua 都經過同一 outbox capture；未知 key、
憑證 cache、quota、queue、checkpoint 不因啟用此功能而全部歸檔。TTL 到期、stream trim、finding
容量淘汰不被解讀為刪除長期歷史。新增 Store／寫入方式時，必須補對應 capture 測試，不能繞過
adapter 使用 raw Redis writer。`EVALSHA`、pipeline 內巢狀 Lua 等不支援的路徑會拒絕。

資料容量也有明確界線：單筆來源 raw JSON 至多 1 MiB、整個 outbox envelope 至多 8 MiB、解碼後
record JSON 至多 2 MiB；NUL（U+0000）字元拒絕，JSON `null` 不受此限制。`SET GET`、`XADD MINID` 不支援並 fail fast，既有 Store
使用的一般選項仍受支援。這些界線不是允許一次任意大量匯入的承諾。

每個獨立 Redis 資料來源使用固定 `POSTGRES_SOURCE_ID`；其 outbox 為
`gateway:durable:<source_id>:outbox`。不要讓互不相關的 Redis 共用 source ID，也不要在 Redis
資料來源被清空／重建後任意沿用舊 source ID 與來源游標。還原或重建前須規劃來源身分及 ledger
的相容性。相同來源位置但不同內容會形成衝突，而不是靜默覆蓋。

多 projector 可共用 `postgres-history-v1` consumer group。失去 owner 的 pending entry 可在 lease
逾時後被接手。PostgreSQL 以來源位置與 envelope hash 去重，event 另有 identity／payload hash
ledger；record 以來源位置 fence 防止舊快照蓋過新快照。這是「至少一次投遞＋資料庫冪等投影」，
不是工具外部副作用 exactly-once，也不是 audit hash chain。

## 設定與本機啟用

設定範例見 [`.env.example`](../.env.example)。不要提交真實 DSN、密碼或 API key。

| 設定 | 預設 | 操作重點 |
| --- | --- | --- |
| `POSTGRES_DSN` | 空 | 空值維持 Redis-only；正式環境由 secret manager 注入、使用 TLS |
| `POSTGRES_SOURCE_ID` | `gateway` | 同一 Redis 的所有 writer／projector 必須一致；1–64 字元英數、`.`、`_`、`-` |
| `POSTGRES_HISTORY_MODE` | `shadow` | 先捕捉與驗證；只有 `postgres` 才開放歷史 reads，且另須 verified report |
| `POSTGRES_PROJECTOR_POLL_SECONDS` | `1` | 0.1–30 秒；不是端到端延遲保證 |
| `POSTGRES_OUTBOX_LEASE_MS` | `5000` | 1000–300000 ms；接手 pending 的閒置門檻，不代表 ACK SLA |
| `POSTGRES_PASSWORD` | `local-postgres-only` | 僅本機 Compose 範例；更改時 DSN 也須一致 |

Gateway、Goal Worker、Scheduler 三種 writer 都必須設定非空 DSN 與相同 source ID，才能啟用
outbox。projector 也使用同一組來源設定。僅設定 Gateway 不足以覆蓋背景工作。讀取 mode 是
history API 的開關，不會把 Worker 的執行讀取切換到 PostgreSQL。

本機可使用 [Compose overlay](../docker-compose.postgres.yml)。先依原有指南設定模型、認證與
Browser Worker；下列命令沿用該設定，會建立獨立 PostgreSQL volume，但不會搬動既有 Redis：

```sh
docker compose -f docker-compose.yml -f docker-compose.postgres.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.postgres.yml build gateway browser
docker compose -f docker-compose.yml -f docker-compose.postgres.yml up --detach --wait \
  redis postgres browser gateway worker scheduler postgres-projector
```

本機 overlay 會先執行一次 `pg-migrate`，完成後才啟動相依服務。不要把公開的本機預設密碼帶到
正式環境，也不要把會展開 secret 的完整 `docker compose config` 輸出貼到公開 Issue。

## 既有資料遷移與 read cutover

migration 是明確的操作，不由 HTTP request 自動建立表。`migrate` 使用 transaction advisory lock
序列化 append-only、逐版 checksum migration，檢查版本、checksum 及必要 table／index；未知版本、
非連續版本、checksum 不符或缺少關聯都拒絕啟動，不會自動 drop、修表或倒退 migration。schema v2
加入 event 搜尋、trigram 與 retention ledger。verified report 綁定當前 schema version；升級後舊
report 不可沿用，必須重新 backfill／reconcile 才能 read cutover。

正式環境按以下順序執行：

1. 備份 Redis、PostgreSQL 與部署設定，記錄既有資料保存／TTL 範圍及 rollback 版本。
2. 執行 `python -m app.postgres_worker migrate`，確認 schema ready。
3. 所有 Gateway／Worker／Scheduler replica 更新到含 capture adapter 的版本，設定同一 DSN、
   source ID 與 `shadow` mode。確認舊 replica、一次性管理程式與 raw writer 都已停止；maintenance
   fence 無法攔住未使用 adapter 的舊程式。
4. 啟動 projector，觀察 backlog、pending／errors、Redis 記憶體與磁碟空間。
5. 執行 `backfill`；它先分批重建所有既有 PostgreSQL record／event 的安全搜尋投影，再補入 Redis
   仍保留的來源資料。接著執行 `reconcile`；必須得到目前 schema version 的最新 `status=verified`。
6. 將 Gateway 的 `POSTGRES_HISTORY_MODE` 改為 `postgres` 並 rollout。確認 history、tenant
   隔離及故障行為，再讓讀取端採用新的 history API。

CLI 從相同環境／secret 讀取設定，命令格式如下。在 Compose 內可在命令前加上
`docker compose -f docker-compose.yml -f docker-compose.postgres.yml exec -T gateway`：

```sh
python -m app.postgres_worker migrate
python -m app.postgres_worker project
python -m app.postgres_worker backfill
python -m app.postgres_worker reconcile
python -m app.postgres_worker status
python -m app.postgres_worker compact
```

`project` 是常駐程序；其他命令執行完成後結束。`backfill`／`reconcile` 以可續租的 maintenance
lease 暫停參與 capture 的領域寫入，再排空／比對穩定邊界；HTTP writer 可能暫時收到 503，應排
維護窗口。lease 遺失、未處理 pending 或資料分歧會拒絕驗證／cutover。projector 可在維護時繼續。

backfill 可從既有 PostgreSQL payload 重建安全搜尋文字，但 **source backfill 只補得回 Redis 目前
仍保存的資料**。過去已 TTL 到期、trim、刪除或容量淘汰，且 PostgreSQL／outbox 都沒有的內容
無法復原。reconcile 比對保留中的來源子集，允許 PostgreSQL 已保存但 Redis
已到期的額外歷史；不是證明過去所有事件都完整存在。刻意 purge 的資源會記入 excluded，不能
把這個數字當成自動復原需求。

CLI 的 `status` 是來源範圍資訊；admin HTTP status 則對 records／events 等計數套用 tenant scope。
最新已套用 cursor 是觀測值，**不是所有較早 entry 都已 ACK 的證明**；操作員須一併檢查 consumer
group pending 與錯誤 stream，不能只看最大 cursor 判斷遷移完成。

## 歷史 API

沿用既有 API key／OIDC 與 RBAC。tenant 取自認證 principal，不能在 query 指定另一個 tenant。
未知／重複 query 參數、非法 cursor 與不支援 kind 回 422；跨 tenant 的單筆 record 回 404。

| API | 最低角色 | 用途 |
| --- | --- | --- |
| `GET /v1/history/records` | viewer | record 分頁；可選 `kind`、`after`、`limit` |
| `GET /v1/history/records/{kind}/{resource_id}` | viewer | 單筆歷史快照；revision ID 可包含 `:` |
| `GET /v1/history/events` | viewer | event 分頁；可選 `kind`、`resource_id`、`after`、`limit` |
| `GET /v1/history/search?q=...` | viewer | record／event 受控搜尋；可選 `kind`、`scope`、`after`、`limit`、`include_summary`、`expand=relations` |
| `GET /v1/history/status` | admin | tenant 計數、schema、來源游標、migration 狀態 |
| `POST /v1/history/purge` | admin | 先 dry-run、再帶 preview hash 執行 archive-only 清理，見下節 |
| `GET /v1/history/poison` | admin | 列出 pending poison 的安全診斷，不回傳原 payload |
| `POST /v1/history/poison/{event_id}/repair` | admin | 驗證原 payload hash，以替代 envelope 投影成功後才 ACK |

reads 除 status 外，需 `POSTGRES_HISTORY_MODE=postgres`，且最新 migration report 同時為 `verified`
並符合目前 schema version，否則回 409。DSN 未啟用或資料庫不可用回 503。status／purge／poison
可供 shadow 階段管理，但仍須認證。
回應標示 `history_only=true`、`consistency=eventual`、`execution_authority=false`，不能把歷史
Goal 狀態拿去批准工具操作。新資料可能尚未投影，不應把暫時查不到解讀為執行未發生。

records／search 的 `next_cursor` 是不透明字串；events 的 `next_cursor` 是 PostgreSQL event
cursor 整數；`source_cursor` 則是 Redis outbox 的 `毫秒-序號`。三者不可互換。records／search
依 kind／resource ID 排序，不是按最新時間或搜尋相關度排序。所有 API 的 `limit` 上限為 200。
PostgreSQL event cursor 反映歸檔寫入的分頁順序，不是 Redis SSE cursor，也不是跨來源的業務事件
因果順序；即使 PostgreSQL 已切換為 history authority，新資料仍是 eventual consistency。

以下只做讀取，請先在私人終端設定這個 tenant 的 key；不要使用測試 stack 公開 key 存取正式環境：

```sh
GATEWAY_HISTORY_URL='http://127.0.0.1:8000'
# GATEWAY_HISTORY_KEY 由自己的 secret 管理流程注入；不在文件或 shell history 填真實值。
curl --fail-with-body --silent --show-error \
  -H "x-api-key: ${GATEWAY_HISTORY_KEY}" \
  "${GATEWAY_HISTORY_URL}/v1/history/records?kind=goal&limit=20"
curl --fail-with-body --silent --show-error --get \
  -H "x-api-key: ${GATEWAY_HISTORY_KEY}" \
  --data-urlencode 'q=資安分析' --data-urlencode 'kind=agent' \
  "${GATEWAY_HISTORY_URL}/v1/history/search"
```

搜尋一律使用投影出的安全文字，不對 raw JSON 任意全文索引：白名單中繼資料包括名稱、狀態、
role、rule ID、severity、tool、audit action／subject 與資源 ID；Skill package 只索引 source、
repository、固定 commit 與 package hash。Goal objective／step 與完成證據只在 live safety boundary
明確寫入 `history_search_approved=true` 後索引，舊資料不推定為已核可。persona、Skill instructions、
plan context、conversation 正文、原始 ATR 攻擊輸入、工具參數／輸出與 hidden reasoning 不索引。

實作採用 `to_tsvector('simple', ...)`／`plainto_tsquery` GIN、`pg_trgm` 及不區分大小寫的 literal
substring 補充中文命中。trigram 是相似搜尋，結果不保證 exact match 或相關度排序；`%`／`_` 不
是使用者萬用字元。這不是中文語意斷詞、向量／embedding，也不是任意 audit／conversation 正文
搜尋。PostgreSQL 的 parser／dictionary 與索引概念見[官方 FTS 文件](https://www.postgresql.org/docs/17/textsearch.html)。

## 保留政策、刪除與 compact

預設不自動刪除。第一次呼叫 `POST /v1/history/purge` 只接受
`{"kind":"conversation","before_cursor":"<來源毫秒-序號>"}`，回傳 `dry_run=true`、筆數、資源數
與綁定精確目標集合的 `preview_sha256`，不修改資料。操作員審查後才以同一 tenant／kind／cutoff
加上 `"execute":true,"expected_preview_sha256":"..."` 執行；目標集合已變更時回 409，需重新預覽。
單次最多 100,000 個 resource，超過時拒絕，須選較早 cutoff 分批。執行與 retention ledger 寫入
位於同一 PostgreSQL transaction，記錄 operator 與計數而不複製 payload。

執行後不可由 history API 還原；事前必須確認已驗證來源邊界、法規／保留政策與備份。它只刪除
指定來源位置以前的 PostgreSQL 歸檔 payload，**不刪除仍在運作的 Redis 資料，不取消 Goal，也不
撤銷 approval／Skill**。完整移除活躍資料須使用對應 live 操作並另行治理各持久化層。

截止游標須來自同 tenant 的 `GET /v1/history/status` 中 `status.latest_cursor`，或經確認的更早
來源位置；沒有已投影資料、或 cutoff 超過該 tenant 已投影最大位置時會拒絕，不能填未來時間戳。
這個上限檢查不代表 earlier pending 已排空，仍須先檢查待處理／錯誤項目及刪除範圍。

purge 保存 resource tombstone 與 kind-wide purge fence；即使資源只有 events、沒有 record，
舊 payload 也不能藉由重新投影／backfill 復活。kind-wide fence 會抑制該 kind 的 backfill，即使
某份保留快照出現在 purge 邊界之後；正常新增、來源位置更晚的 live 寫入仍可進入歸檔。這是
保守的防復活策略，reconcile 會把受抑制資料列入 excluded。

因此，purge 後若中止 capture，期間新增資料也不能依賴該 kind 的普通 backfill 補回；`excluded`
不等於完整資料一致。重新導入須經受控的新 source／遷移審查與刪除政策確認，不可靜默刪除 fence
來讓測試或 reconcile 變綠。

去重用的操作 hash、event identity／payload hash，以及 tombstone／fence 中繼資料仍保留；purge
不是抹除所有識別資訊的保證。Redis outbox、備份、dump、replica／WAL 與外部匯出可能還有資料，
必須依組織資料治理政策另外處理。從 purge 前備份還原也可能帶回內容，須重新套用有效刪除政策。

`compact` 只清理 outbox 中已安全越過 consumer group ACK 邊界的項目，不是 PostgreSQL retention
工作，也不是任意 `MAXLEN` trim。它會保留最早 pending 起的資料；無 pending 時保留最後 delivered
邊界。遇到非預期 consumer group 數量／名稱會拒絕。不要自行 `XTRIM`、`DEL` outbox 或盲目 `XACK`，
否則可能永久遺失還沒進入 PostgreSQL 的歷史。

## 故障、復原與 rollback

| 情境 | 預期行為與處理 |
| --- | --- |
| 已運作時 PostgreSQL 中斷 | Redis 執行與 outbox 可繼續；projector 保留 pending 並重試；history 503，不偷退回 Redis 歷史 |
| PostgreSQL 中斷時重啟 Gateway | 啟用 DSN 的 Gateway 無法完成 startup；不降級成未驗證的 history store |
| schema 未 migrate／checksum 不符 | Gateway／projector 拒絕開啟 store；先釐清版本，不手改 checksum |
| Redis 不可用、PostgreSQL 可用 | history 仍是獨立讀取路徑；live 執行不能以 PostgreSQL 復活，回 503 或保持不可啟動 |
| envelope 格式／內容／identity 衝突 | 不 ACK 原 entry；errors stream 僅記安全錯誤分類與 event ID；cutover 拒絕未解 pending |
| maintenance 期間 | 參與的領域寫入暫時 fenced；不可用舊 raw writer 繞過 |

projector 失敗時只記安全診斷，不把 DSN、SQL／私人 payload 放進公開 HTTP 錯誤或操作日誌。
排錯先看連線／schema 狀態、group pending、`<outbox>:errors` 的 event ID 與 migration report。
`GET /v1/history/poison` 只回可由 raw envelope key 安全歸屬於目前 principal tenant 的 event ID、
consumer、delivery／idle 與 payload SHA-256；無法歸屬的 malformed entry 不透過 tenant HTTP API
顯示或修復，須在受控 Redis／CLI 維運環境處理。原 payload 不可貼到公開 Issue。

修復時先備份並確認原因，再提交原 payload 的預期 SHA-256 與完整替代 envelope；原始與替代 key
必須屬於 principal 的同一 tenant，不能用修復搬移租戶。服務先用原
event ID 將替代內容冪等寫入 PostgreSQL，成功後才以原子 Lua 驗證 pending、ACK、移除 marker，
並寫入安全 audit；任一步驟失敗都保留可重試狀態。沒有 skip／drop API，拒絕為了讓 reconcile
變綠而盲目 ACK 或刪除。修復替代的是 archive projection，不會重做原 live 業務副作用。

PostgreSQL 中斷期間 outbox 不會自動丟棄，須監控 Redis 空間、磁碟與 backlog；容量耗盡會影響
執行寫入。本機 Redis 使用 AOF，但本方案不保證 PostgreSQL 即時零 RPO：尚未投影資料仍依賴
Redis persistence、磁碟、備份與故障情況。來源 cursor／重播冪等不等於跨儲存交易。

read rollback：先將 Gateway mode 改回 `shadow` 並 rollout，停用 history 客戶端入口；這使 reads
回 409，不是將長期歷史切回 Redis。保留 DSN 與新 capture writer，讓 outbox 繼續保存。若要完全
停用功能，須先在維護窗口排空／備份並記錄切斷邊界；清空 DSN 或退回舊 binary 會停止捕捉，
之後 backfill 無法補回期間已 TTL／trim 的資料，不能宣稱無損 rollback。

## 備份還原、Kubernetes 與驗證

PostgreSQL 備份之外，Redis 尚未投影的 outbox 及執行狀態也須納入備份／災難復原。正式環境須
另訂 RPO／RTO、加密與 restore rehearsal；單次測試不等於已驗證 managed database failover。
PostgreSQL 的 custom-format dump 使用 `pg_restore` 還原，參考[官方 SQL dump 指南](https://www.postgresql.org/docs/17/backup-dump.html)。

outbox compact 會刪除已 ACK 資料；若 PG 還原到早於 compaction 的備份，已 trim 的舊歷史不能
再從 Redis 重建。必須協調 PG 備份／PITR、Redis AOF 與可接受 restore 窗口的 outbox 保留策略；
不保證任意舊備份都能自動追平至事故前狀態。

專用驗證入口：

```sh
sh scripts/test_postgres_stack.sh
```

[測試腳本](../scripts/test_postgres_stack.sh)使用獨立 Compose project、image、volume 與 loopback
port，跑 authenticated HTTP、兩個 projector、真實 PostgreSQL stop／recover、backfill／reconcile、
schema-bound cutover、record／event 受控搜尋、關聯 ACL 重查、兩階段 retention、poison 修復、
tenant／RBAC 與資料隔離，再停止該測試 stack 的 writers，執行真實
`pg_dump`／`pg_restore` 並比對各 `gateway_` 表的筆數與內容摘要。Redis 留在 stack 中，這不是
「從 PostgreSQL 重建執行權限」測試。預設結束會刪除**這個專用測試 project 的 volume**；
`KEEP_STACK=1` 才保留排錯環境。可用 `POSTGRES_TEST_PROJECT`／`POSTGRES_TEST_PORT` 避開衝突；
不要把既有正式 project 名稱指定給此可拋棄測試。實際執行結果以版本驗證紀錄為準。

Kubernetes 的 [opt-in migration／projector 範例](../deploy/kubernetes/postgres-projector.yaml)使用
外部 managed PostgreSQL，不部署叢集內資料庫，也不包含在預設 kustomization 自動執行。先設定
Secret／TLS／網路政策及實際 image，**手動執行並等待 migration Job 成功**，才 rollout projector
與三種 capture writers，再依前述維護流程 backfill／reconcile／cutover。範例 YAML 不是已完成
真實叢集部署或資料庫 HA 驗證的證據。

本版使用 Psycopg async pool，連線採明確 lifecycle 與 startup readiness 檢查；每個 store process
最多四條連線，水平擴張時要計入總數並管理資料庫 connection budget。[Psycopg 官方 pool 說明](https://www.psycopg.org/psycopg3/docs/advanced/pool.html)
提供 pool lifecycle 與啟動等待的背景。
