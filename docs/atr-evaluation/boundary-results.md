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
與截斷情況都已覆蓋。

2026-10-26 新增 `scripts/test_atr_tool_stack.sh`：公開 SSE 經真正 MCP stdio／`app.mcp_server`
連到 authenticated remote Browser Worker，使用測試 overlay 在既有 `browser_close` handler
回傳後插入合成攻擊。真實 pyatr 阻擋後，後續 mock model context、live SSE、replay 與 Redis
event／run／finding／audit 均沒有 canary；另驗證錯誤 Browser token 回 401。此案例不需要
Chromium，不代表所有 browser 方法或 attack variants 已覆蓋。測試 wrapper 不在正式服務載入。

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

2026-10-26 的 `tests/test_release_atr_crash.py` 再以兩個真正 HTTP process、真 Redis checkpoint、
真 pyatr 及 deterministic model，在掃描中、阻擋判定後但 event write 前、write 後但 live
publish 前 SIGKILL 第一個 process，由第二個恢復與 replay。三個時序均無原始攻擊或未掃描
draft token 洩漏，恢復後 `evidence_validated` 為 false。

已確認 checkpoint 與 SSE append 不在同一交易：write 前中斷可能遺失診斷事件，不能宣稱
finding exactly-once／完整投遞。這是有限故障矩陣，不代表所有 multi-replica、partial-write
或外部副作用情境已覆蓋。測試亦重現超長內容的中段攻擊可被 head／tail 截斷漏掉。

## 目前結論

自動化測試支持「三個主要邊界的控制流程符合設計」。它們尚不能支持：

- ATR 對未知攻擊的 recall。
- 實際 production 流量的 false-positive rate。
- 多 replica、失敗恢復或跨事件 correlation 的完整安全性。

最新結果、已確認限制與尚未納入功能見[封版驗證彙總](../releases/2026-10-26-skills-plan-mode.md)。
