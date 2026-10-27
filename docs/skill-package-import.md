# 唯讀 Skill 套件匯入

M3 提供上傳 ZIP 與 public GitHub 固定 commit 的匯入。只讀文字、保留來源，先產生隔離預覽，
再由 admin 明確確認發布。沒有安裝套件、執行 scripts、載入 MCP、執行 shell 或提升工具權限。

## 開關與權限

明確設定 `SKILL_V2_ENABLED=true`、`SKILL_PACKAGE_IMPORT_ENABLED=true`；後者預設 false。
所有 `/v1/skill-imports` API 至少要求同租戶 admin，operator 不可預覽、確認或讀取內容。
沿用租戶請求頻率限制；每租戶最多 32 份未過期預覽。預覽 TTL 一小時，發布收據保留一天。
隔離內容不進公開 Postgres history，HTTP 回應使用 `Cache-Control: no-store`。

## 套件格式

ZIP 的選定目錄包含一份 `SKILL.md`，以及最多八份唯讀文字資源。例如：

```text
SKILL.md
references/checklist.md
```

```markdown
---
name: evidence-review-package
description: 依提供證據整理已知與未知
execution_modes: [execute]
metadata:
  author: example-team
  version: "1"
---
核對 references/checklist.md，不要求額外工具或外部操作。
```

必填 name／description，本文是 instructions。可選 display_name、required_tools、agent_roles、
tags、execution_modes；省略 modes 時為 execute。plan-forecast 仍必須明確 plan-only，
來源 metadata 不能啟用 forecast 或繞過服務端規劃模式。

標準描述欄位 license／compatibility／metadata／allowed-tools 只保存成唯讀
`package-metadata.json`，不解讀成授權；metadata 必須是字串對字串的有限 map。
若 archive 已有同名檔案則拒絕，避免覆蓋。只支援唯讀子集，不宣稱完整 Agent Skills runtime 相容。
格式依據見 [Agent Skills specification](https://agentskills.io/specification)。

資源沿用 v2 相對路徑／副檔名與 UTF-8／容量限制。ZIP 不會解壓到主機；拒絕 symlink、特殊檔、
重複成員、加密 ZIP、未知壓縮法、traversal、scripts、二進位資源與不安全 YAML。
YAML 禁止 alias、自訂 constructor 與重複 key，並限制深度；支援 LF／CRLF 的 UTF-8 manifest。

上傳 ZIP 最多 256 KiB，GitHub archive 最多 2 MiB／2,048 成員；只解碼選定目錄，
每個選定檔案最多 16 KiB、合計 64 KiB，另受 v2 最多 12,000 bytes 的完整 context 限制。
拒絕超過 100:1 的成員壓縮比，不用宣告長度取代實際讀取上限。
未選定目錄中的普通檔案不會讀取／執行，archive 全部成員的路徑／特殊檔 metadata 仍會檢查。
這些是應用層有界解析與隔離預覽，不是執行不受信任程式的 OS sandbox。

## API 流程

1. 上傳：`POST /v1/skill-imports/upload`

   ```json
   {"archive_base64":"<ZIP 的標準 base64>","subdirectory":""}
   ```

   或 GitHub：`POST /v1/skill-imports/github`

   ```json
   {"repository":"example-org/readonly-skills","commit":"<完整 40 位小寫 hex commit>","subdirectory":"skills/evidence-review-package"}
   ```

   GitHub 不接受 branch、tag、短 SHA、使用者指定 URL 或任意 host。只從 GitHub API 讀取，
   先核對 Git commit object 回傳的完整 SHA，拒絕以 40 位 hex 命名的浮動 ref；metadata 上限 64 KiB。
   最多跟隨一次至同 repository／commit 的 codeload HTTPS URL；無 proxy／憑證繼承，總期限 30 秒。
   私有 repository 與 GitHub Enterprise 尚不支援；archive 過大也拒絕，不偷偷改成 shallow clone。
   依 [GitHub archive API](https://docs.github.com/en/rest/repos/contents#download-a-repository-archive-zip)
   的 commit ref 行為固定來源，ZIP bytes 的壓縮輸出仍可能變動，實際下載另存 SHA-256。

2. 查看：`GET /v1/skill-imports/{import_id}`。檢查 `status:previewed`、完整 content、
   execution_modes、唯讀資源與 provenance。這代表解析與 ATR 檢查完成，**不代表真人已覆核**，
   也還沒有 Skill／Agent binding 或執行型 Goal。

3. 人工確認後：`POST /v1/skill-imports/{import_id}/confirm`

   ```json
   {"expected_content_sha256":"<預覽回傳的完整內容 hash>"}
   ```

   發布前再次掃描當前內容政策／ATR；同一 Redis transaction 再核對候選 hash、建立 Skill／
   revision／owner 與發布收據。換包、過期、衝突或儲存型別錯誤不留下半份 Skill。
   並行／重送回同一 Skill ID，不重複發布。確認後預覽狀態是 published。

4. 明確更新 Agent 的固定綁定或管理者選擇白名單，才供後續 Goal 使用。
   匯入／確認本身不執行 Skill、不委派、不授予工具、不自動切換 session mode。

同名 Skill 已存在時回 409，不自動覆蓋或升級；後續手動編輯需清除來源宣告，歷史 revision
保留原 provenance。以 rollback API 還原既有不可變 revision 可以保留其可信來源，仍重新掃描。
一般 Skill create／publish body 不能自行宣告來源以冒充已驗證的 package。

錯誤：403 角色／內容拒絕、404 預覽過期／不在租戶、409 旗標／內容或名稱衝突、
422 套件不安全或來源格式錯誤、429 額度、503 外部下載／儲存故障。錯誤不回顯原始 YAML、
敏感路徑或下載位置。完整條件與測試見[交付紀錄](releases/2026-10-27-m3-package-import.md)。
