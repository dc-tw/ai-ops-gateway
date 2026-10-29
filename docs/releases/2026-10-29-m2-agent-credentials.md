# 2026-10-29 M2：Agent credential lifecycle

## 完成內容

- 新增綁定單一 tenant／Agent 的一次顯示高熵 credential；Redis 只存 digest 與安全 metadata。
- human admin 可首次發出、列出、原子輪替及撤銷；支援明確 expiry 與 bounded metadata retention。
- authentication 每次重查 credential 與 live Agent enabled 狀態，不以 snapshot 或 Postgres history
  作執行授權。
- dynamic Agent principal 固定為 typed `agent` actor，不能人工核准；route allowlist 與 typed ACL
  同時生效，human ACL feature flag 關閉也不會退回 tenant-wide 存取。
- credential authority 與安全 audit stream 保持 Redis-only；PostgreSQL history maintenance fence
  期間仍可發出／撤銷，不把 credential metadata 或 digest 投影到歷史平面。
- Agent 建立的 Goal 保存 `created_by_type=agent`，因此後續 list／read／events／control 仍以目前 ACL
  判斷，不會與同名 human owner 混淆。

## 驗證

- Ruff、Python compile 與 whitespace check 通過。
- 既有 security／browser auth／Agent／ACL 測試與新測試在真實 Redis 8.2（含 durable history
  maintenance fence）：`54 passed`。
- 覆蓋一次顯示與 at-rest 無明文、同時首次發證只有一個 winner、active duplicate 拒絕、12 次
  連續輪替、舊 token 即時失效、撤銷、Agent disable、跨 tenant 404、route deny、人工核准 deny、
  audit 不含 token／digest、expiry、ambiguous auth fail-closed，以及在全域 ACL flag 關閉時只
  列出 agent-owned Goal。
- 無外部 Redis／PostgreSQL 的全套回歸：`524 passed, 402 skipped`；skips 是既有 opt-in
  Redis／PostgreSQL／ACP／Docker 測試，真實 Redis credential 專項另如上列。
- 測試 Redis 使用獨立 `ai-ops-agent-credentials-test` 容器，驗證後已移除。

## 尚未完成

- MCP／ATR server-approved policy reference registry 尚未納入。
- conversation single-flight／FIFO／fencing、EventEnvelope v1 adapter、audit SHA-256 chain／checkpoint
  export 仍是 M2 後續；credential audit 寫入專用 Redis stream，不冒稱已防竄改或已外部保存。
- 靜態 `API_KEYS_JSON` 中既有的 typed Agent key是相容入口，不具本 lifecycle；新部署應改用
  `x-agent-key`，但移除靜態入口需要另訂 migration／deprecation 時程。
