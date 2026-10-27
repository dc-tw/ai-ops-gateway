# 資源擁有者與明確分享（M2 第一段）

`RESOURCE_ACL_ENABLED=false` 為預設。啟用後，tenant／RBAC 之外還須通過資源 ACL；
同租戶不代表可以讀取彼此資料。這不是完整 M2：Agent 憑證、執行 policy reference、
對話 single-flight／FIFO／fencing、統一事件及 SHA-256 audit chain 仍待實作。

## 權限規則

- tenant 來自現有 API key／OIDC 驗證，不接受 query／body 指定 tenant。
- human admin 可管理同租戶全部資源；其他人僅可存取自己擁有或獲明確分享的資源。
- `read` 允許讀取，`operate` 也允許操作，但不能提升全域角色。例如 viewer 即使獲得
  operate，也不能建立 Goal；Agent／Skill 設定管理仍要求原有 admin 角色。
- 只有 owner（至少 operator）或 admin 可修改分享。分享接受完整 subject 與
  `actor_type: human | agent`，兩種身分不互通。Agent 憑證尚未提供，不能只靠欄位自稱 Agent。
- 缺少 ACL 的舊資料先限 admin；不根據舊 `created_by`、webhook、prompt 自動認領。
- ACL 保護現有 Agent、Skill／文字匯出、計畫 session／revision、Goal、run SSE、automation、
  conversation 與 Postgres 歷史查詢。Findings 仍維持既有 admin 入口；歷史 finding 可由 admin
  明確分享。不存在的前端、presence 或新增匯出功能不在本次交付內。
- Skill／plan revision 與 tool receipt 使用根資源權限。授予 Agent／計畫的讀取權，會讀到該
  資源本來包含的設定／快照；不會隱去嵌入內容，分享前應先檢查。
- 分享 automation／Chief Goal 不自動分享新產生的 Goal。新 Goal／子 Goal 與其 conversation
  繼承當前父資源的 owner，不繼承 grants；要分享結果須另設 Goal 權限。
- 撤銷 Agent 分享不會溯及變更已建立 Goal／planning session 的 owner。若需要停止既有任務，
  使用管理者停用 Agent／取消 Goal，再逐筆調整結果資源分享；不要把撤銷入口權限當成取消工作。

## 建立與更新

新資源的 owner 由已驗證的人類 subject 在服務端決定，與資源寫入／入列同交易保存。
即時串流先登記 conversation owner 才讀歷史，先保存 run 與 owner 才發布事件。
這只決定誰能存取，不是同一 conversation 的執行鎖：同一 owner 仍可能啟動重疊工作。

```http
GET /v1/access/goal/GOAL_ID
X-API-Key: OWNER_KEY
```

以回傳的 ACL `version` 作 CAS（不是 Goal／Agent 本身的版本）。更新是完整取代 grants，
傳空陣列表示撤銷全部分享；不會改變 owner。

```http
PUT /v1/access/goal/GOAL_ID
X-API-Key: OWNER_KEY
Content-Type: application/json

{
  "expected_version": 1,
  "grants": [
    {"actor_type": "human", "subject": "reviewer-subject", "permission": "read"}
  ]
}
```

版本衝突回 409；無權限與不存在統一 404；損壞的權威／索引回 503，不回顯其資料。
分享清單最多 100 筆，禁止同一 typed subject 重複。ACL、索引更新與安全中繼資料 audit
在同交易提交，預檢 Redis 型別；這些 audit 還不是可驗證的 hash chain。

## 舊資料遷移

先在隔離副本演練，正式啟用時安排管理維護窗口：啟用 ACL 後，尚未認領的資料會隱藏於
非 admin。保留原設定與 Redis 備份，不一次將全租戶分享給所有人。

1. admin 逐筆 `GET /v1/access/legacy-preview/{kind}/{id}`，回傳 `exists`、`has_acl`、
   `can_adopt`，`writes=false`；預覽不寫入、不根據內容猜 owner。
2. 人工確認 owner 後，admin `PUT /v1/access/{kind}/{id}`：

   ```json
   {"expected_version":0,"legacy_owner":{"actor_type":"human","subject":"verified-owner"},"grants":[]}
   ```

3. 用 owner 與另一位同租戶使用者測試讀取、操作、列表、歷史搜尋，再開放服務。

僅可認領仍存在於 Redis 的資源；只剩 Postgres 的歷史、缺乏可靠 owner 的資料保持 admin-only。
已建立 ACL 的 owner 不可變；不提供批次猜測／強制覆寫介面。對話刪除會在同一交易標記 ACL
revoked 並增加版本，不能透過 grants 更新重新開啟。刪除不會自動取消已執行的 run，需先取消
相關工作以避免其再次寫入訊息；這也不代表備份、WAL 或其他副本已被抹除。

## 即時撤權與歷史

- SSE 在每個 payload 與 idle heartbeat 前重驗身分與當前 ACL，撤權後關閉，不送出已緩衝的
  後續 payload；已送出的內容無法收回。撤權不等於取消背景工作或撤回已發出的外部工具呼叫。
- Postgres list／search／events 在 SQL LIMIT 前按 Redis 的授權集合篩選，回傳前再查當前 ACL。
  修訂與 receipt 正規化至根資源；conversation ID 中的冒號保留。全域 audit 不隱含分享。
- PostgreSQL 不保存 ACL 作授權權威；Redis 不可用時嚴格模式歷史讀取拒絕，不回退成租戶全讀。
- 關閉旗標會恢復既有 tenant／RBAC 模式，也就不再提供 owner 隔離；不是安全的撤權方式。
- Redis ACL 與 actor 反向索引必須一併備份／恢復。索引只提供候選集合，實際授權仍比較完整
  typed identity；索引缺漏造成少見／拒絕，不作「缺漏即允許」的 fallback。

驗證與階段限制見[本次紀錄](releases/2026-10-27-m2-resource-access.md)。
