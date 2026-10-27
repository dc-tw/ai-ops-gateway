# Gateway 完整能力補齊進度

基準：`6553e92`；2026-10-27 使用者核准分階段實作 M1–M9。
本表是交付追蹤，不代表全部能力已實作或已通過驗收。

| 階段 | 交付內容 | 狀態 |
| --- | --- | --- |
| M1 | ATR 完整掃描、超限拒絕、可終止隔離、規則健全性、安全決策 journal | 已實作／本機與 Docker 驗證完成 |
| M2 | Agent 憑證／policy reference、owner／分享 ACL、conversation fencing、統一事件及 audit chain | 進行中：owner／分享 ACL 已實作；其餘待實作 |
| M3 | Skill v2、plan-only 綁定、確定性自動選擇、唯讀外部套件匯入 | 已實作／本機與 Docker 驗證完成 |
| M4 | Plan v2、條件／平行 DAG、Workflow YAML、明確且冪等的計畫執行 admission | 進行中：admission／controller 已接上；Docker／故障整合與最終 review 待完成 |
| M5 | plan-forecast、模型版本／逐呼叫 telemetry／校正及情境估算 | 待實作 |
| M6 | Postgres 正文／provenance 搜尋、ACL 重查、retention 及 poison 重送 | 待實作 |
| M7 | 繁中管理台、OIDC、人類自願 presence、Agent heartbeat、規劃 SSE 及簽章 Webhook | 待實作 |
| M8 | ACP v1 stdio client adapter、session／串流／取消／逐次核准 | 待實作 |
| M9 | 全量回歸、隔離 staging／故障／PITR／壓測及封版證據 | 待實作 |

## 已確認邊界

- Redis 保持 runtime／核准／額度權威，Postgres 是可選歷史平面。
- ATR 完整掃描超限、逾時或必要規則失效時拒絕；觀察模式不豁免完整性檢查。
- Skill 只從管理者白名單固定 revision 依規則選擇；套件只含指令／唯讀資源，
  支援上傳或固定完整 commit 的 GitHub，不執行 scripts、不安裝依賴或 MCP。
- DAG 支援 schema 驗證的結構化輸出、條件和平行；必要節點失敗停止派新工作，
  核准只停相依分支；仍沿用 LangGraph／Goal 和逐次寫入核准。
- 採 owner／明確分享；人類 presence 自願；管理台採 OIDC，開發登入不得在正式環境啟用。
- 通知只先做 HTTPS 簽章 Webhook；ACP 採 Agent Client Protocol v1 stdio。
- 正式部署／付費模型測試需另外確認環境、額度與權限，不冒稱本機證據為 production 驗收。
- 每階段獨立 review／測試／中文紀錄／commit。程式遠端僅限 dc-tw/ai-ops-gateway；
  Bohr Issue 只準備草稿，不自動發文或修改其 repository。

## 驗收紀錄

各階段完成後補上實際命令、結果、版本及剩餘限制。真人語料覆核由使用者處理，
不得以工程測試取代獨立真人標記。

- M1：[實作與驗證紀錄](releases/2026-10-27-m1-safety.md)。693 tests、四組 Docker E2E，
  另修復並驗證 cold Redis 的 checkpointer 索引初始化競態。M2–M9 尚未交付。
- M2 第一段：[owner／分享 ACL 實作與驗證](releases/2026-10-27-m2-resource-access.md)。
  731 tests，新增 strict ACL HTTP／Postgres Docker E2E；M2 的憑證、policy reference、
  conversation fencing、統一事件與 audit chain 仍待實作。
- M3 第一段：[v2／planning_skills／確定性選擇實作與驗證](releases/2026-10-27-m3-skill-v2-selection.md)。
  775 tests、真實 HTTP／worker／model-context Docker E2E；套件隔離匯入仍待接續。
- M3 完整段落：[唯讀 package 匯入與驗證](releases/2026-10-27-m3-package-import.md)。
  809 tests；ZIP／固定 GitHub commit、隔離預覽／原子確認、provenance、故障與 model-context E2E。
  M4–M9 仍按原定契約接續，不以 M3 完成取代整體目標。
- M4 第一段：[Plan v2／YAML 與安全編譯說明](workflow-plan-v2.md)。保存不可變 compiled artifact，
  舊 hash 不變；分支／join／核准等待決策與 schema 隔離已加入測試。
  [驗證紀錄](releases/2026-10-27-m4-workflow-compiler.md)：848 tests，review 後專項 129 tests，
  無網路 Docker／Python 3.12 相容性與隔離終止／復原。
  第一段當時尚無 DAG dispatch／execute admission，後續進度見下；不代表 M4 完成。
- M4 執行段：[明確 admission／持久化 controller 紀錄](releases/2026-10-27-m4-workflow-runtime.md)。
  已加入 root 共享 quota、Agent 跨 Goal lease、子 Goal／核准／schema 整合與收據複核參照。
  最終 864 tests、無 skip；實際 Redis checkpoint／ATR／LangGraph 路徑與 corrupt-root 隔離通過。
  Docker 多 worker／故障 E2E 與最終 review 尚待完成，不將本機專項當成整個 M4 驗收。

## 後續階段的固定實作契約

### M2：身分與執行治理

- Agent 憑證需限 Agent、可輪替／撤銷，不能作人類核准；MCP／ATR policy 只能引用
  管理者核可的服務端設定，秘密不進快照。
- owner／明確分享 ACL 必須覆蓋 CRUD、列表、搜尋、SSE、匯出與 presence，授權仍查
  即時權威；舊資料 dry-run 遷移，無法確定 owner 時先限 admin，不自動公開。
- conversation ownership 跨入口／副本共用；即時衝突回 409，背景 FIFO。核准等待保留
  邏輯 ownership、釋放 execution slot；終態／取消交棒；stale worker 必須被 fencing。
- EventEnvelope v1 用 compatibility adapter 保留舊 SSE；分開 event ID、cursor 與冪等鍵。
  tenant scope audit chain 原子排序，提供驗鏈、缺件／竄改檢查與簽署 checkpoint 匯出；
  hash chain 不等於外部不可變保存。

### M3：Skill v2 與唯讀供應鏈

- 保留 v1 hash／revision；v2 的 planning_skills 和 execution binding 分開。
  plan-forecast 必須明確綁定，不可經 prompt、tag 或自動選擇啟用。
- 自動選擇只比較管理者設定的任務欄位／字詞／標籤，固定綁定優先，不新增模型呼叫。
  priority／Skill ID 穩定排序；超過八項或容量限制時拒絕設定，不默默截斷。
- 入列／保存計畫前固定選擇結果、規則版本、Skill revision 與原因；執行中不重選。
- 匯入流程為隔離檢查、預覽、admin 確認及發布。來源為上傳或固定完整 GitHub commit／
  子目錄，保存 provenance；拒絕 path traversal、symlink、壓縮炸彈、SSRF 及 scripts。
  唯讀資源也納入快照、ATR、容量與 token 計算。

### M4：Plan v2、DAG 與 YAML

- 舊線性／Chief 計畫不改歷史 hash，透過明確 adapter 編譯到與 YAML 共用的 canonical plan。
- 預設最多 32 節點、64 邊、平行度 4，並服從更嚴格的租戶／Agent 限制；不可循環、
  不可遞迴委派。Worker 可執行多節點，但不得繞過自身併發與團隊共用額度。
- 條件只依狀態或 JSON Schema 2020-12 驗證後的輸出，採有限比較／布林運算，
  不執行任意程式或解析 remote schema。YAML 禁止自訂 tag／重複鍵。
- join 支援 all_success／any_success，等前置節點終態後判斷；false branch 為 skipped，
  缺必要輸入為明確失敗。必要節點失敗後不派新工作，取消進行中節點並結算 receipt／用量；
  核准只停相關分支，不確定副作用不可重播。
- mode execute 仍只接受計畫；新增明確 execute admission，核對 accepted hash、epoch、ACL、
  Skill、ATR、quota 及冪等鍵，原子建立主任務／入列；重送回同一 Goal。

### M5：Forecast

- 路由／部署／profile 跨副本以 CAS 發布版本；alias 不等於已知實際模型版本。
- 從同一 compiled plan 建立 call manifest，涵蓋 persona、Skill／資源、歷史、工具、重試、
  Chief／Workers，逐呼叫記錄 provider 實報與估算的差異。
- low／base／high 使用 DAG 分支／critical path／平行與資源限制，不直接加總時間。
  核准／佇列／未知工具等待分開；無資料回 unknown／insufficient_calibration，不捏造 ETA。
- plan-only 估算，保存不可變 artifact；相同快照去重、編輯合併、發布前重查 mode／epoch。
  校正工具需明確啟動，不自動跑 benchmark、探測模型或使用付費 route。

### M6：歷史搜尋與生命週期

- 受控正文、Goal／Evidence／Audit／receipt 與 provenance 使用 PG FTS＋中文 literal／trigram；
  不做向量／embedding。索引不含秘密、hidden reasoning 或未核可的原始攻擊內容。
- 結果、摘要、關聯展開重查當前 ACL，撤權不能等待 projector；新的資料族延用既有
  outbox、backfill／reconcile／cutover 門檻。
- retention 預設不自動刪除，admin 先 dry-run，再執行 archive-only 清理與 tombstone。
  poison 提供診斷與修復後重送，不靜默 skip／ACK 並宣稱對帳成功。
- Redis、outbox、WAL、backup 副本的治理與 archive purge 分開，不能宣稱即時抹除所有副本。

### M7：中文管理台與協作

- React／TypeScript／Vite 同源管理台；FastAPI OIDC code＋PKCE、後端 token／session、
  HttpOnly cookie、CSRF；development login 不得在正式環境啟用。
- 頁面覆蓋 Agent／ACL、Skill／匯入／選擇、automation、Goal／核准、plan／forecast、
  搜尋／audit；表單及 YAML 編輯＋唯讀 DAG，第一版不做拖拉設計器。
- planning／forecast SSE 支援 cursor、重連、去重、即時撤權，只傳安全參照。
- Agent heartbeat 與人類 presence 分開；人類 opt-in，只分享授權資源的查看／編輯狀態，
  不傳草稿。預設 heartbeat 20 秒、60 秒過期，不取代 CAS 或 execution lock。
- 完成／失敗／核准等待／核准逾時通知採 HTTPS allowlist、HMAC、event ID、重試及失敗佇列；
  不附可直接核准的憑證，須回管理台登入。

### M8：ACP

- Agent Client Protocol v1 stdio adapter 連遠端 Gateway；固定 schema／reference client。
- initialize、new／load session、prompt／update、cancel、permission 映射既有治理；
  set_mode 不等於執行或同意寫入。
- 啟動環境提供憑證；stdout 只含協定訊息。不因 client 宣告而開放本機 filesystem／terminal／MCP。
  斷線不是同意；取消後不能讓延遲核准生效。

### M9：Staging 與封版

- 全套單元／整合／四組 Docker E2E、新前端／協定 E2E、隔離叢集多副本／SIGKILL、
  Redis／PG 故障、升級／回滾、實際 restore／PITR。
- 固定工作負載以 1／4／16 併發記錄環境、延遲、錯誤、資源、投影落後與復原時間；
  staging 實測不自動成為 production SLA。
- 真實模型品質、經授權流量、forecast 校正分開出具證據；mock／小模型 smoke 不替代。
- 每階段更新「實作／驗證／需環境／不支援」矩陣；新功能預設關閉，不能因回滾
  UI／forecast 而撤掉安全修補、tombstone 或放寬 ACL。
