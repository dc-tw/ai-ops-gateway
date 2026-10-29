# Agent MCP／ATR Policy Reference

Agent 可以綁定管理者發布的不可變 MCP 與 ATR policy revision。這些 reference 是額外的
server-side 限制，不取代 Agent 自己的 `allowed_tools`／`allowed_domains`，也不會把 endpoint、
API token、環境變數或 MCP 啟動參數複製到 Agent／Goal snapshot。

## Policy 類型

### MCP

`transport_ref` 第一版只接受 `configured-mcp`。發布時 Gateway 必須真的以
`AGENT_TOOL_TRANSPORT=mcp` 啟動；policy 只保存核可 tool 名稱與 hostname allowlist。

Agent 的 `allowed_tools` 必須是 policy tool 的子集合。policy 有 domain allowlist 時，Agent 也
必須提供非空且更窄或相等的 domain 集合；空 Agent domains 代表 unrestricted，因此會被拒絕。

### ATR

ATR policy 可要求 detection-only 或 blocking、最低掃描 severity、blocking severity 與必要 rule ID。
發布時會核對 live ThreatDetector：ATR 必須啟用、blocking 要求不能由 detection-only detector 冒充、
threshold 不得較弱、必要規則必須是 active。snapshot 保存 ruleset fingerprint，但不保存掃描內容。

## 建立與綁定

所有 policy 管理 API 都要求 human admin；跨 tenant 或不存在 ID 回 404。Schema 額外欄位一律
拒絕並回固定 422，不回顯可能被誤送的 secret。

```bash
MCP_POLICY=$(curl -sS -X POST "$GATEWAY/v1/security-policies" \
  -H "x-api-key: $ADMIN_KEY" -H 'content-type: application/json' \
  -d '{
    "name":"production MCP boundary",
    "config":{
      "kind":"mcp",
      "transport_ref":"configured-mcp",
      "allowed_tools":["scrape_dynamic_page"],
      "allowed_domains":["example.com"]
    }
  }')

ATR_POLICY=$(curl -sS -X POST "$GATEWAY/v1/security-policies" \
  -H "x-api-key: $ADMIN_KEY" -H 'content-type: application/json' \
  -d '{
    "name":"production ATR boundary",
    "config":{
      "kind":"atr",
      "mode":"blocking",
      "minimum_severity":"medium",
      "block_severity":"high",
      "required_rule_ids":["ATR-2026-00213"]
    }
  }')
```

從 response 取 `policy_id` 與 `version`，在 Agent create／update 指定 exact revision：

```json
{
  "name": "bounded analyst",
  "persona": "只依核可政策分析。",
  "policy": {
    "allowed_tools": ["scrape_dynamic_page"],
    "allowed_domains": ["example.com"]
  },
  "mcp_policy_ref": {"policy_id": "...", "revision": 1},
  "atr_policy_ref": {"policy_id": "...", "revision": 1}
}
```

更新 policy 會建立新的 immutable revision，既有 Agent 不會靜默換版；要採用新版必須明確更新
Agent reference。啟停 API 是 policy ID 全域開關：

```bash
curl -X POST "$GATEWAY/v1/security-policies/$POLICY_ID/enabled" \
  -H "x-api-key: $ADMIN_KEY" -H 'content-type: application/json' \
  -d '{"enabled":false}'
```

## 執行時 fail-closed

- Agent create／update 會原子解析 exact revision，並拒絕 tool／domain scope 擴張。
- Goal 建立時重新解析 live policy，保存安全 snapshot；policy 已停用時不入列。
- Worker 在 admission、模型用量與每個受治理執行邊界重新核對：policy enabled、revision identity、
  server fingerprint、live MCP transport 與 live ATR ruleset／threshold。漂移或撤銷會以
  `PermissionError` 終止，不使用 Postgres history snapshot 授權。
- policy authority 與 `security:policy-audit` 是 Redis-only，history maintenance fence 期間仍能
  停用 policy；它們不投影 endpoint、secret 或 digest 到 PostgreSQL。

這是單一 Gateway 設定下的 MCP／ATR reference，尚不是多個遠端 MCP server 的 credential vault。
第一版不接收 URL、command、environment 或 secret；若日後加入多 server registry，秘密仍須由
Vault／External Secrets 等服務端來源解析，不能進 policy revision。
