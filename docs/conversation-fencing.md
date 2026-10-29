# Conversation single-flight、FIFO 與 fencing

Gateway 以 Redis authority 保證同一 tenant、同一 `conversation_id` 同時只有一個邏輯工作。這個
限制跨 API entry point 與 replica 生效，不依賴單一 process 的記憶體 lock。

## 行為

- `POST /v1/agent/stream` 是即時工作。同一 conversation 已有即時工作或背景 FIFO 時，新的即時
  request 直接回 `409 Conversation already has active work`，不暗中排隊。
- `POST /v1/goals` 可帶 `conversation_id`。帶同一 ID 的 Goal 依 Redis FIFO 執行；較晚的 Goal
  保持 `queued` 並標示 `execution_waiting_conversation=true`。
- 未提供 `conversation_id` 的 Goal 保持既有獨立執行語意；它的歷史仍以自己的 `goal_id` 儲存。
- 工作等待人工核准時會釋放 tenant run slot 與 worker execution lease，但保留 conversation 的邏輯
  所有權，因此後面的工作不會在同一上下文插隊。
- completed、failed、rejected、timeout 或 cancel 才會釋放所有權；背景 FIFO 會在同一 Redis 交易中
  移除目前項目並喚醒下一項。

```json
{
  "conversation_id": "incident-prod-42",
  "objective": "分析部署失敗並提出修復建議",
  "steps": [{"query": "整理已知事件"}]
}
```

## Fence 與故障恢復

每次新邏輯 owner 取得遞增 `conversation_fence`。背景 Goal 同時受既有 Goal owner／lease fence 保護；
舊 worker 即使稍後恢復，也不能覆寫已被新 worker 接管的 Goal。approval resume 沿用同一
conversation fence，但取得新的 Goal execution fence。

即時 run 另外保存有期限的 execution lease。多個 replica 同時呼叫 resume／approval 時，只有一個
能把 logical owner 原子切換成 running；其他 replica 回 409。process 在 running 中消失時，原 run
可在 execution lease 到期後由 resume 接管，或由 operator cancel；新的不同工作不會偷走舊 run 的
conversation owner。

conversation authority 只保存 owner ID、phase、fence 與 FIFO ID，不保存 prompt、answer、tool
arguments 或 approval reason。它是 Redis-only 授權／排序狀態，PostgreSQL history 不參與執行判定。
