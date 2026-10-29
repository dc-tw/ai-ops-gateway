# ACP v1 遠端 Adapter

`ai-ops-gateway-acp` 是 Agent Client Protocol v1 的 stdio Agent。它不在編輯器程序內啟動
Gateway，也不接收 client 宣告的本機 filesystem、terminal、additional directories 或 MCP 權限；
所有 prompt、工具治理、ATR、ACL、額度、核准與取消仍由遠端 Gateway 執行。

實作依據：

- [ACP v1 overview](https://agentclientprotocol.com/protocol/v1/overview)
- [ACP v1 initialization](https://agentclientprotocol.com/protocol/v1/initialization)
- [ACP v1 session setup](https://agentclientprotocol.com/protocol/v1/session-setup)
- [ACP v1 prompt turn](https://agentclientprotocol.com/protocol/v1/prompt-turn)
- [官方 Python SDK](https://github.com/agentclientprotocol/python-sdk)

## 啟動

在 ACP client 的 Agent command 設定以下環境變數，再執行：

```bash
GATEWAY_BASE_URL=https://gateway.example \
GATEWAY_API_KEY='由 Gateway 管理的 API key' \
ai-ops-gateway-acp
```

可選的 `GATEWAY_MODEL` 固定這個 adapter 使用的 Gateway model route。正式環境只接受 HTTPS。
本機開發若使用 `http://127.0.0.1` 或 `http://localhost`，還必須明確設定
`GATEWAY_ACP_ALLOW_INSECURE_LOCALHOST=true`。

憑證只從啟動環境讀取，不會寫入 ACP 訊息、session、SSE、audit 或 stdout。stdout 僅供
JSON-RPC stdio framing；啟動失敗只在 stderr 回報錯誤類型。

## Session 與 mode

- `session/new` 在 Gateway 建立租戶範圍、Redis-backed 的 session，ACP session ID 同時是
  conversation ID。原始 cwd 不送往 Gateway，只傳 SHA-256。
- `session/load` 先重查目前 ACL，再從 Gateway 回放已授權的 user／assistant history。
- session metadata 以版本 CAS 更新，預設 mode 是 `plan`。
- `session/set_mode` 只更新 `plan`／`execute` metadata；回應明示沒有開始執行、沒有給予核准。
- `plan` prompt 會加入規劃限制提示，但真正的安全邊界仍是 Gateway 的工具 policy、ATR、ACL
  與逐次核准，而不是 prompt 文字本身。

## Prompt、串流與核准

第一版只接受 text content block。adapter 將 Gateway SSE 映射為 ACP：

- token → `agent_message_chunk`
- tool start／finish → `tool_call`／`tool_call_update`
- completion／error／cancel → ACP prompt stop reason
- `approval_required` → `session/request_permission`，只提供 allow-once 與 reject-once

allow-once 只提交這一次工具與參數的決定，不提供 allow-always。Gateway `/approval` 仍重查目前
身分：Agent 型 credential 不能核准，此時 ACP 顯示回控制台登入的提示並等待人類處理；斷線、
例外或沒有回覆一律不是核准。核准後 adapter 用 `Last-Event-ID` 接續 run events，不重播已顯示內容。

`session/cancel` 呼叫既有 run cancel API。等待核准期間取消會先把 run 寫成 terminal cancelled，
之後延遲抵達的核准不能恢復它。adapter 關閉時只嘗試取消 active runs，不會提交核准。

## 明確不支援

- image、audio 或 embedded resource prompt
- client filesystem／terminal methods
- client 提供的 MCP server 或 additional directory
- 把 `set_mode` 當成 workflow execute admission
- 以 Agent credential 代表人類核准

這些限制會以 capability omission 或 invalid params 明確呈現，不會默默擴張權限。
