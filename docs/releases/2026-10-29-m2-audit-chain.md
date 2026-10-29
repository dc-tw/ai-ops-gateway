# 2026-10-29 M2：Tenant audit chain 與 signed checkpoint

## 完成內容

- 既有 tenant audit stream 相容投影到 per-tenant SHA-256 chain；Credential／security-policy 治理
  audit 同 transaction 納入 tenant scope。
- Redis `WATCH/MULTI` 序列化併發 projector，entry 與 head 原子提交；來源 cursor、chain sequence
  與 hash 身分分離。
- Gateway 背景 projector 預設每 2 秒追平 legacy Lua／transaction producer；verify／export 仍會
  同步追平，不依賴輪詢時機。
- human-admin verify API 檢查缺件、換序、竄改、tenant、schema、head 與 source ordering。
- checkpoint export 先 audit 自身動作，再以 server-only HMAC-SHA256 key 簽章；提供線上與離線驗證。
- 關閉一般 audit 的近似 MAXLEN trimming；不提供未經外部保存證據的 prune API。

## 驗證

- 純函式 canonicalization、mutation、missing entry、foreign tenant、empty genesis、wrong key／short key。
- 真實 Redis：8 個 concurrent projectors、50 筆排序、刪除偵測、不同 tenant 隔離、未截短來源、
  legacy discovery projector、human-admin API export／tenant-scoped verify 與 Agent admin 拒絕；
  audit-chain 專項 `7 passed`，含 Redis discovery scan 瞬斷後自動恢復。
- Credential／policy、durable outbox 與 audit chain 合併專項 `75 passed`；未配對的 security action
  無法繞過 PostgreSQL maintenance fence，配對事件仍進 tenant chain。
- 無外部服務完整回歸：`530 passed, 415 skipped`。
- 真實 Redis DB 0（含 Redis Search／LangGraph checkpoint）完整回歸：
  `872 passed, 73 skipped`；8 個 warning 均為上游 `langgraph-checkpoint-redis` 已標示的未來版
  async connection deprecation，沒有測試失敗。
- 真實 Redis＋PostgreSQL 17 projection／poison repair／search 專項：`73 passed`。
- Ruff、離線 verifier `--help` 與 `git diff --check` 通過。

## 限制

- hash chain 是 tamper-evident，不是 tamper-proof；有完整 Redis 寫權的人仍可能重建 chain。
- 首次啟用只覆蓋當時仍保留的 legacy audit，起點由 `coverage_start_cursor` 揭露。
- HMAC 是 shared-secret authenticity，不具公開驗證性；舊 key 須在外部 secret manager 按 key ID 保存。
- 外部 WORM／Object Lock storage 由部署方提供；本 repository 只提供可驗證 checkpoint 與 runbook，
  不冒稱已完成某個正式 storage 的不可變保存。
