# Agent 專屬憑證

Gateway 可為一個已啟用的 Agent 發出可輪替、可撤銷、會到期的專屬憑證。這不是人類 API key：
principal 固定為 `actor_type=agent`、`role=operator`，不能執行人工核准，也不能把 request 中的
Agent ID、persona 或 policy 當成權威。

## 安全邊界

- token 格式是 `agc_<credential-id>_<random-secret>`，只在發出／輪替 response 顯示一次。
- Redis 只保存 credential ID 與 SHA-256 digest；secret、完整 token 不進 Agent snapshot、audit
  或 list response。secret 由 32 bytes CSPRNG 產生，digest 不是低熵密碼儲存方案。
- 每次 request 都重查 credential 的撤銷／到期狀態及 Agent 是否仍啟用；沒有本機 acceptance
  cache。輪替交易會先撤銷現行 credential，再建立新 credential。
- 動態 Agent credential 即使在一般 human ACL rollout flag 關閉時，仍強制使用 typed owner／
  grant ACL。它只能讀自己的 Agent 設定、從自己的 Agent endpoint 建立 Goal，以及列出、讀取、
  串流、暫停、恢復或取消自己有權操作的 Goal。
- 不能列出同租戶全部 Agent、建立未綁 Agent policy 的一般 Goal、管理 credential／ACL／Skill、
  存取其他 Agent／Goal，或呼叫 Goal approval。人工核准仍要求已驗證 human principal。
- 每個 Agent 最多保留 10 筆 credential metadata；反覆輪替時只清掉最舊的 expired／revoked
  credential record，audit 事件仍保留。active credential 不會因 retention 被刪除。

## 管理 API

以下操作都要求同租戶的 human admin。`expires_in_seconds` 可設 60 秒到 365 天；預設 30 天。

```bash
# 首次發出；已有 active credential 時回 409
curl -X POST "$GATEWAY/v1/agents/$AGENT_ID/credentials" \
  -H "x-api-key: $ADMIN_KEY" -H 'content-type: application/json' \
  -d '{"label":"production-worker","expires_in_seconds":2592000}'

# 原子輪替：舊 active credential 立即失效
curl -X POST "$GATEWAY/v1/agents/$AGENT_ID/credentials/rotate" \
  -H "x-api-key: $ADMIN_KEY" -H 'content-type: application/json' \
  -d '{"label":"production-worker-rotated","expires_in_seconds":2592000}'

# 只列 metadata 與 active／expired／revoked 狀態
curl "$GATEWAY/v1/agents/$AGENT_ID/credentials" -H "x-api-key: $ADMIN_KEY"

# 撤銷可安全重送；不存在或跨 tenant 一律 404
curl -X DELETE "$GATEWAY/v1/agents/$AGENT_ID/credentials/$CREDENTIAL_ID" \
  -H "x-api-key: $ADMIN_KEY"
```

執行端把一次顯示的 token 放在 `x-agent-key`，不能同時帶 `x-api-key` 或 `Authorization`：

```bash
curl -X POST "$GATEWAY/v1/agents/$AGENT_ID/goals" \
  -H "x-agent-key: $AGENT_TOKEN" -H 'content-type: application/json' \
  -d '{"objective":"依核可政策整理事件證據","model":"qwen2.5:7b-instruct"}'
```

## 輪替與事件處理

1. 管理者呼叫 rotate，安全保存 response 中的新 token；response 遺失時再次 rotate，不查回 secret。
2. 更新執行環境 secret，確認新 token 可用。舊 token 在 rotate transaction 完成時已失效，沒有
   overlap grace period。
3. 疑似外洩時立即 DELETE 撤銷；若要停掉 Agent 的所有執行權，另將 Agent `enabled=false`。
   重新啟用 Agent 會恢復尚未撤銷且未到期的 credential，因此外洩事件仍必須明確 revoke／rotate。
4. Redis-only `security:credential-audit` 只記 action、Agent／credential ID、操作者、到期時間與
   輪替／清理數量，不記 token 或 hash。它刻意不進 PostgreSQL history outbox，確保 history
   maintenance fence 期間仍能發出／撤銷 credential；後續 audit-chain 階段須納入此 stream。

目前 policy reference 尚在下一個 M2 子段落；本功能只保證 credential 綁定現有 server-owned
Agent record，不能宣稱 MCP／ATR policy registry 已完成。credential audit 目前仍是一般 Redis
stream，tamper-evident SHA-256 chain 與 checkpoint export 也屬後續 M2。
