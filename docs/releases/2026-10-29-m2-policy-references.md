# 2026-10-29 M2：MCP／ATR policy reference

## 完成內容

- 新增 human-admin、tenant-scoped、immutable revision 的 MCP／ATR policy registry。
- MCP reference 限定目前 server-configured MCP transport、核可工具與 hostname；Agent policy 只能
  取子集合，不能用空 domains 把有限 allowlist 擴張成 unrestricted。
- ATR reference 在發布與執行時核對 mode、severity threshold、必要 active rule 與 ruleset
  fingerprint；detector 變弱或 ruleset 漂移會 fail closed。
- Agent create／update、Goal snapshot 與 Worker live boundary 都納入 exact policy identity；停用
  policy 後新 Goal 不入列，已入列 Goal 在下一個執行邊界停止。
- policy schema 不接受 endpoint、token、command、environment 等欄位；snapshot／audit 只含安全
  config、ID、revision 與 fingerprint。policy 停用不依賴 PostgreSQL history availability。

## 驗證

- Ruff、compile、request validation privacy 與既有無-reference Agent 相容測試通過。
- 真實 Redis 8.2 覆蓋 publish／bind／resolve、tool／domain escape、跨 tenant、秘密欄位拒絕、
  MCP transport 不符、ATR blocking 不足、ruleset fingerprint 漂移、停用後 fail-closed，以及
  history maintenance fence 期間的 policy revocation；合併 credential／ACL 回歸為 `53 passed`。
- 無外部服務的完整回歸：`524 passed, 407 skipped`；skips 是既有 opt-in Redis／PostgreSQL／
  ACP／Docker 測試，真實 Redis 專項另如上列。

## 尚待接續

- conversation single-flight／FIFO／fencing、EventEnvelope v1、tenant audit chain／checkpoint export
  仍未完成。
- registry 目前代表單一 server-configured MCP transport，不是多 endpoint credential vault，也不會
  從 request 啟動任意 MCP process。
