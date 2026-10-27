# M3：唯讀外部套件隔離匯入

基準 `bf9959e`，接續 v2／規劃綁定／確定性選擇。使用者的完整 M3–M9 目標維持 active，
此段不代表 M4–M9 或 M2 其餘治理已交付。操作見[套件指南](../skill-package-import.md)。

## 實作與信任邊界

- 新增 admin-only ZIP upload／public GitHub full-commit import、唯讀記憶體解析、隔離預覽、
  明確 hash 確認與原子發布收據；管理旗標預設關閉。預覽本身不產生 Skill／binding／Goal。
- 不解壓到 filesystem、不執行 scripts、不安裝依賴／MCP。ZIP 路徑、NUL 截斷、特殊檔／symlink、
  加密／壓縮種類、重複成員、檔案數、實際 bytes、比例與累計容量皆檢查。
- YAML 使用 SafeLoader 加重複鍵／alias／深度限制；未知 constructor／欄位拒絕。支援 LF／CRLF，
  標準描述 metadata／allowed-tools 只當唯讀資源，不是能力授權。
- GitHub URL 由 repository／完整 SHA 在服務端建構；只允許一次匹配的 codeload HTTPS redirect，
  先核對 Git commit object 的 SHA，避免只檢查字串格式而接受 40-hex 浮動 ref。
  不繼承 proxy／認證，不接受任意 URL／branch／tag／短 SHA。總期限 30 秒、bounded raw streaming，
  不解壓 HTTP content-encoding。來源 archive SHA-256 與 repo／commit／subdirectory 保存至 v2 hash。
- 預覽最多 32／租戶，TTL 一小時；發布收據一天；角色／租戶／頻率限制、no-store、隱私安全錯誤。
  原始候選和索引不投影到 Postgres；確認後的 Skill 正常進入既有 outbox／歷史。
- 確認重新 ATR 掃描，在同一 Redis transaction 重查候選與預期內容 hash，再建立 Skill、revision、
  owner 與 receipt；競爭／重送只建立一個 ID。預檢 index／audit 型別，避免 WRONGTYPE 半提交。
- 防止 preview 的 detection-only finding 啟動內部 automation：保留 findings／audit，但禁止這條
  路徑發送內部觸發。匯入與確認不能間接變成執行 admission。
- 一般內容 API 不可偽造 provenance；rollback 只從既有不可變 revision 還原來源並重新檢查。

## 驗證

```bash
TEST_REDIS_URL=redis://127.0.0.1:16379/0 \
TEST_POSTGRES_URL=postgresql://gateway_test:gateway_test@127.0.0.1:15432/gateway_test \
  .venv/bin/pytest -q
.venv/bin/ruff check .
git diff --check
sh -n scripts/test_skills_v2_stack.sh
sh scripts/test_skills_v2_stack.sh
```

- 套件專項真實 Redis／HTTP 測試包含四個並行確認去重、role／跨 tenant／default-off、
  expiry／換包在 scan 與 commit 之間的競態、錯誤 index／audit 無半成品、重新掃描拒絕、
  provenance rollback、quarantine 不歸檔與 detection-only 不啟動內部工作。
- 最終完整回歸 **809 passed，零 skip**，171.73 秒；6 個既有 RedisSaver deprecation
  warnings 保留。Ruff、shell syntax、git diff whitespace 通過；套件專項為 34 項。
- 惡意 ZIP／YAML：traversal、symlink、NUL filename、duplicate、特殊／執行型檔案、
  壓縮炸彈、比例、錯誤 UTF-8、缺 manifest、重複鍵、alias／Python constructor；安全錯誤不回顯輸入。
- GitHub transport MockTransport 驗證固定 SHA、exact provider redirect、SSRF／user-info／
  跨 repo／query 拒絕；HTTP preview 驗證服務端來源欄位而非 body 的來源宣告。
- 另實際讀取 public `dc-tw/ai-ops-gateway` 的完整 commit
  `6553e9289603dc2a5a7c5cda5c7d39799f7cac6a`：428,817 bytes，下載 SHA-256
  `3ae61c647875e066b88dac04ae8e9e9ad96681e349186261e4fd2e3a3aeb306f`。
  此項是 transport smoke，不是把該 repo 的 design-only Skill 安裝為有效 forecast。
- 擴充 Docker E2E 通過：真實 HTTP ZIP→preview→confirm／重送→provenance→明確更新 Agent→
  worker／模型完整流程；mock server 缺唯讀 canary 即拒絕，不用 metadata 取代 model-request 證據。
  model／ATR 仍使用原有模式與期限，沒有為通過測試停用安全檢查。

## Review 發現與修復

- 匯入 provenance 的正常 rollback 原本被反偽造 guard 擋住，改為只信任服務端 revision。
- 預覽期間 policy／候選更動必須阻擋確認；publication preflight 額外檢查 index／audit。
- import finding 不得讓內部事件觸發 Agent；專項測試以租戶事件篩選，避免其他測試的全域 queue
  被誤認為本次 import 產生的副作用。保留失敗過程，沒有把全域佇列必須不存在當合理契約。
- HTTP raw streaming 的 MockTransport 使用真實 stream fixture，沒有用已消費的 response.content
  去模擬網路 stream，也不為測試改回自動解壓。

## 限制與下一階段

- 只讀文字子集；不支援 private GitHub／Enterprise、binary assets、任意 source URL、Git clone、
  executable packages 或同名 Skill 自動升級。這些不會被 metadata 誤啟用。
- ZIP bytes 可能隨 provider 壓縮設定變動，固定 commit 是內容來源約束，不是壓縮 bytes 穩定保證；
  保存實際 artifact hash 而非宣稱兩次下載必然 byte-identical。
- 操作依 [GitHub archive 文件](https://docs.github.com/en/repositories/working-with-files/using-files/downloading-source-code-archives)、
  [Python ZIP 安全注意](https://docs.python.org/3/library/zipfile.html#decompression-pitfalls)
  與 [Agent Skills 格式](https://agentskills.io/specification)，不宣稱完整 executable Skill 相容。
- M4 接續 canonical DAG／Workflow YAML／明確 execute admission；M5–M9 保留全部原定交付。
  M3 的 model-canary 與 mock 測試不取代實際模型品質、forecast 校正、正式環境／PITR 驗收。
