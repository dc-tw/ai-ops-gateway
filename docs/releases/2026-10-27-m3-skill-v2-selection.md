# M3：Skill v2／規劃綁定／確定性選擇

基準 `8d04187`。這是 M3 的已驗證實作段落；完整套件匯入仍待下一段，M3 不是整體完成。
使用者要求的 M3–M9 目標維持不變，沒有把後續交付縮減成此段。

## 已實作

- v2 `execution_modes`、唯讀 `resources`、provenance schema；v1 序列化、hash 與 revision 相容。
  升級建立新 revision；v1 rollback 不留下 v2 資源／模式殘值。
- 唯讀資源進入 immutable hash、snapshot、ATR scan、模型 context 與保守用量扣款。
  拒絕 traversal、隱藏／執行型路徑、重複路徑、壞 UTF-8 與容量超限，不讀取本機資源檔案。
- Agent `planning_skills` 與執行 `skills` 分離；模式同時受綁定解析、snapshot validation、
  model rendering 與 automation Lua admission 約束。`plan-forecast` 僅 v2／plan-only／明確綁定。
  仍沒有 forecast estimator，也沒有因名字出現在 prompt 而觸發的機制。
- 管理者固定 revision 白名單；只依有限欄位與字詞做 deterministic selection，無 regex／模型呼叫。
  固定綁定優先，priority／Skill ID 穩定排序；設定時驗證最壞情況八項與總容量。
- 建立 Goal 時固定選擇 binding、快照、epoch、規則 hash、原因；Worker 不重新選擇。
  PlanningSnapshot 固定白名單候選，PlanRevision 保存由已雜湊輸入導出的選擇 artifact，讀取時重算驗證。
- 新的 v2／自動選擇管理開關預設 false，保留 tenant／RBAC／ACL 與現有工具政策。
- Agent validation 與 Skill／planning 一樣不回顯敏感 config／畸形 Unicode，避免新規則欄位造成
  輸入洩露或錯誤回應序列化失敗。

API、容量、旗標與限制見 [操作指南](../skills-v2.md)。

## 驗證

全部使用合成資料、隔離 Redis 8.2／PostgreSQL 17 與 mock model，沒有使用付費模型。

```bash
TEST_REDIS_URL=redis://127.0.0.1:16379/0 \
TEST_POSTGRES_URL=postgresql://gateway_test:gateway_test@127.0.0.1:15432/gateway_test \
  .venv/bin/pytest -q
.venv/bin/ruff check .
git diff --check
sh -n scripts/test_skills_v2_stack.sh
sh scripts/test_skills_v2_stack.sh
```

- 專項驗證：v1 hash byte-for-byte、資源內容修改偵測、readonly path／容量、v2→v1 rollback、
  停用立刻啟用不能重用舊 epoch、plan-only 不進 execution snapshot、資源實際增加用量扣款。
- 最終完整回歸：**775 passed，零 skip**，149.48 秒；保留 6 個既有 RedisSaver deprecation
  warnings。Ruff、shell syntax、Compose config 與 git diff whitespace 檢查通過。
- 選擇驗證：固定 revision 優先、排序／規則 hash 不受規則陣列順序影響、有限欄位比對、
  跨八項或最壞容量直接拒絕、只讀 server metadata、Goal 選擇固定、Plan artifact 竄改拒絕。
- 舊封版測試原本禁止全部 `planning_skills`；現在只放行 Agent 管理 schema 的這個已支援欄位，
  以 HTTP 預設關閉測試及 live binding 驗證補上契約；其他 request、scripts、未知入口仍拒絕。
  v1 不能單靠新增 execution_modes 欄位取得 plan 權限。
- 新 Docker E2E 通過：真實 HTTP／ACL／Redis／worker，規劃零推論／零 Goal queue，計畫選擇 artifact、
  自動選擇執行資源、plan-forecast 執行／自動綁定拒絕、Skill disable 後新 Goal 拒絕。
  mock server 強制檢查 `<readonly_resource>` 與僅存在於資源中的 canary，缺漏即 HTTP 400；
  不以只檢查 Redis metadata 取代實際 model request 證據。
- 人工 review 覆蓋 v1 hash／fingerprint 相容、模式閘門、整份資源掃描、容量、快照與 artifact
  驗證、條件選擇的信任來源；不是對模型品質、ATR 誤報率或 production SLA 的宣告。

## 尚未完成（仍需接續）

- M3：上傳／固定 GitHub commit 的隔離 package 檢查、預覽、admin 確認發布、完整 provenance。
  現階段一般內容 API 拒絕自行宣告 provenance；沒有匯入或執行 scripts 的旁路。
- M4–M9：DAG／YAML／execute admission、forecast／telemetry／校正、正文搜尋／retention／poison、
  繁中前端／OIDC／presence／通知、ACP、完整 staging／故障／restore／PITR／負載／真實模型品質。
- M2 尚未完成的憑證、policy references、conversation fencing、event envelope／audit chain
  仍在 roadmap，不會因本段新增 planning Skill 而被宣稱已完成。
- management flags 關閉不自動撤銷已保存設定／快照；撤銷仍使用 Skill disable、Agent disable／
  明確更新／取消。後續回滾不得移除模式、hash、epoch 或 ACL 安全限制。
