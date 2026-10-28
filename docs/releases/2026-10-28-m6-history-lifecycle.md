# M6：受控歷史搜尋與生命週期驗證紀錄

日期：2026-10-28

## 本段交付

- PostgreSQL schema v2 採 append-only checksummed migration，新增 event 搜尋、`pg_trgm` 索引與
  retention operation ledger；runtime 同時驗證 table／index，verified report 綁定 schema version。
  backfill 會分批重建升級前已存在於 PostgreSQL 的 record／event 安全搜尋投影，不只等待 Redis
  重播；record 以來源位置條件更新，避免覆蓋同步 projector 的新值。
- record 與 event 共用安全搜尋介面：白名單 metadata、Skill provenance、明確通過 safety boundary
  的 Goal／Evidence 正文，以及安全 Audit 欄位。未核可舊資料、persona、instructions、對話正文、
  原始攻擊輸入、工具參數／輸出與 hidden reasoning 不進搜尋投影。
- `simple` FTS、中文 literal substring 與 trigram 搜尋；明確不含 vector／embedding。
- 啟用 resource ACL 時，SQL 前置限縮後仍對結果、摘要與每個 relation 使用 Redis 當前權威重查；
  authority unavailable 時 fail closed，撤權不等待 projector。
- retention 預設 dry-run；operator 以精確 preview hash 明確執行。目標集合漂移回 409，單次上限
  100,000 resource，record／event-only 資源均留下 tombstone，操作 ledger 與清理同 transaction。
- poison entry 保持 pending。tenant admin 只看 raw key 可安全歸屬本 tenant 且不含 payload 的診斷；
  repair 必須匹配原 payload SHA-256 並保持同 tenant。替代 envelope 先成功投影，之後才原子 ACK、
  移除 marker 並寫安全 audit。無法歸屬的 malformed entry 留給受控維運；沒有 skip／drop API。

## 驗證證據

在專用本機 Redis 8.2 與 PostgreSQL 17 容器執行核心 store／projection 專項：

```text
TEST_REDIS_URL=redis://127.0.0.1:16379/0 \
TEST_POSTGRES_URL=postgresql://gateway:***@127.0.0.1:15432/gateway \
.venv/bin/pytest -q tests/test_postgres_store.py tests/test_postgres_projection.py

73 passed in 15.23s
```

同一組真實服務設定的最終全套回歸：

```text
903 passed, 8 warnings in 223.11s
```

八項 warning 都來自 `langgraph-checkpoint-redis` 已知的未來版 coroutine deprecation；沒有 skip
或測試失敗。

完整生命週期入口：

```text
bash scripts/test_postgres_stack.sh
```

通過項目包括：

- authenticated HTTP 與 tenant isolation；
- 兩個競爭 projector、冪等投影與 pending recovery；
- PostgreSQL stop／recover 時 Redis-authoritative write 持續；
- backfill／reconcile 與 schema-bound read cutover；
- record／event 受控搜尋、摘要、relation 展開與隱私 canary；
- poison 診斷、hash-bound repair、projection-before-ACK 與 audit；
- retention dry-run、preview hash、執行、event/record tombstone 與 backfill 防復活；
- PostgreSQL read outage 明確 503、Redis outage 時只允許歷史讀取且拒絕 live execution；
- 停止 writers 後真實 `pg_dump`／`pg_restore`，逐一比對 `gateway_` tables 的筆數與內容摘要。

測試結束後專用 Compose project 與 volumes 已移除；未碰觸其他執行中的容器。

## 已知界線

- PostgreSQL 仍只負責歷史平面；Redis 是 execution、approval、quota、lease、queue 與 ACL 權威。
- trigram 是相似搜尋，不代表 exact match 或中文語意／向量搜尋。
- purge 是 archive-only。Redis、outbox、WAL、replica、備份與外部匯出須依各自治理處理，不能
  宣稱立即抹除所有副本。
- backfill 只能涵蓋 Redis 仍保留的資料；verified 是受控來源子集對帳，不是過往全歷史證明。
- Docker 證據是隔離本機環境，不等於 production HA、PITR、RPO／RTO 或容量驗收；這些留在 M9。
