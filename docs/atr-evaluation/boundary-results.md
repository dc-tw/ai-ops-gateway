# ATR 執行邊界證據

## Request／Goal input

`tests/test_sse.py::test_atr_can_block_malicious_request_before_agent_execution` 驗證：

- 高風險 request 在 blocking 模式回傳 HTTP 403。
- 回應包含 finding，但不包含規則的 matched regex。
- Agent graph 沒有開始執行。
- 沒有建立 run state 或 conversation message。

這表示阻擋發生在 agent execution 之前，而不只是事後產生 alert。

## Tool／MCP／Playwright result

`tests/test_threat_detection.py::test_langgraph_scans_and_blocks_untrusted_tool_response`
使用合成工具回傳間接 prompt injection，驗證：

- finding 的位置是 `tool_response:scrape_dynamic_page`。
- tool 狀態轉為 failed，錯誤指出 ATR 已阻擋。
- 記錄到的後續 model prompts 不包含被阻擋的攻擊內容。

這證明測試案例中的內容沒有被加入後續 model context。它不代表所有 transport、序列化格式
與截斷情況都已覆蓋；MCP 與 remote Browser Worker 還應在 Docker E2E 中各補一個案例。

## Model output

`tests/test_threat_detection.py::test_blocking_mode_does_not_stream_unscanned_model_output`
使用 deterministic model 產生固定攻擊內容，驗證：

- Blocking 模式沒有送出 `draft_token`。
- SSE／graph event 序列中沒有出現原始攻擊內容。
- finding 的位置是 `model_output`，狀態為 `blocked`。

Docker E2E 另外讓 mock model 產生固定攻擊內容，並以 blocking mode 驗證原文沒有出現在：

- live SSE response；
- `/v1/agent/runs/{request_id}/events` replay；
- 底層 Redis event stream。

這涵蓋正常完成後的 reconnect/replay 路徑；audit stream 只保存 privacy-safe finding metadata。
多 replica 同時失敗、process crash 發生在掃描期間等路徑仍未覆蓋。

## 目前結論

自動化測試支持「三個主要邊界的控制流程符合設計」。它們尚不能支持：

- ATR 對未知攻擊的 recall。
- 實際 production 流量的 false-positive rate。
- 多 replica、失敗恢復或跨事件 correlation 的完整安全性。
