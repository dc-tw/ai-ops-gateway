# 2026-10-29 M2：EventEnvelope v1 compatibility adapter

## 完成內容

- 發布 additional-properties forbidden 的 v1 JSON Schema 與穩定事件型別目錄。
- 分離 UUID `event_id`、Redis ordering cursor 與 `idempotency_key`；明確限定 ordering scope。
- agent run、Goal／Workflow Goal、planning 與 trusted internal automation 新寫入使用 envelope。
- legacy entry 可確定性包裝；未知 envelope version fail closed；mixed stream 保留 cursor ordering。
- 既有 Agent／Goal／planning SSE 與 PostgreSQL history event response unwrap 原 payload，不破壞舊 client。
- ATR internal event 維持 metadata allowlist，envelope 不含 raw input／matched text。

## 驗證

- JSON Schema＋catalog drift、ID 分離、legacy deterministic adapter、未知版本拒絕及 ATR 隱私測試。
- 真實 Redis 混合 legacy／v1 replay、Goal／automation／planning／SSE／durable outbox 合併：
  `154 passed`。
- 完整無外部服務回歸：`527 passed, 410 skipped`；Ruff 與 `git diff --check` 通過。

## 尚待接續

- tenant-scoped SHA-256 audit chain、併發原子排序、verify、missing／mutation detection 與 signed
  checkpoint export。
