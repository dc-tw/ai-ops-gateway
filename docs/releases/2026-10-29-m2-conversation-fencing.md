# 2026-10-29 M2：Conversation single-flight／FIFO／fencing

## 完成內容

- 新增 tenant-scoped Redis conversation authority，讓即時 Agent run 與背景 Goal 共用同一
  single-flight 邊界。
- 即時工作遇到既有 owner／FIFO 回 409；帶 `conversation_id` 的 Goal 依 FIFO 排隊。
- approval wait 保留 logical owner、釋放 worker／tenant execution capacity；終止與取消才原子交棒。
- 新 owner 取得單調遞增 fence；背景 Goal 沿用既有 execution lease，Agent run resume／approval
  使用有期限 execution lease，跨 replica 競爭只能有一個成功。
- Worker 的 conversation history 改用顯式 `conversation_id`；未指定的既有 Goal 仍使用 `goal_id`。

## 驗證

- 真實 Redis 合併 Goal、SSE、安全與 browser auth 回歸：`44 passed`。
- 真實 Redis 的 conversation／durable outbox／API 安全合併回歸：`99 passed`；無外部服務完整回歸：
  `524 passed, 409 skipped`。
- 新增競爭案例涵蓋背景 FIFO、approval 保留 logical owner、終止交棒、即時／背景互斥、遞增 fence、
  stale release／stale tool attempt 拒絕，以及兩個 replica 同時 resume 只有一個取得 execution lease。
- Ruff 與 `git diff --check` 通過。
- M2 收尾重新執行三種真實雙 replica `SIGKILL` phase：有效 execution lease 期間 resume 固定
  回 409，lease 到期才由另一 replica 取得新 execution fence；ATR blocked output 不會洩漏或重播。

## 後續完成

- EventEnvelope v1 compatibility adapter 見
  [M2 EventEnvelope 紀錄](2026-10-29-m2-event-envelope.md)。
- tenant tamper-evident audit chain 與 checkpoint export／verify 見
  [M2 audit chain 紀錄](2026-10-29-m2-audit-chain.md)。
