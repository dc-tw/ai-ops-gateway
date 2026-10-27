# Skill v2、規劃綁定與確定性選擇

這是 M3 的模式／資源／選擇部分。上傳 ZIP、固定 GitHub commit 的隔離預覽與確認發布見
[唯讀套件匯入](skill-package-import.md)。`plan-forecast` estimator 屬 M5，這裡只提供安全的規劃綁定。

## 相容性及開關

- 既有 v1 canonical fields、hash、revision 不變；未新增設定的舊計畫 fingerprint 也不變。
  發布 v2 會建立新 revision；以 v1 內容 rollback 會清除新 head 的 v2 欄位，不改舊 revision。
- `SKILL_V2_ENABLED=false`、`SKILL_AUTO_SELECTION_ENABLED=false` 預設關閉新管理入口。
  admin 明確啟用後才可透過 API 發布 v2／規劃綁定或選擇規則。
- 這是管理入口開關，不是撤銷已保存設定／工作。關閉開關不刪除既有綁定，不將 v2 當 v1，
  也不繞過 mode、hash 或 revocation epoch。要停止後續工作，停用 Agent 或明確刪除其規則；
  要撤銷 Skill，使用既有 disable，再按需要取消 Goal。

## 建立 v2

```json
{
  "schema_version": 2,
  "name": "evidence-review-v2",
  "display_name": "證據整理",
  "description": "根據唯讀參考整理證據",
  "instructions": "區分已知與未知，不擴大工具權限。",
  "execution_modes": ["execute"],
  "resources": [
    {"path": "references/checklist.md", "content": "核對來源、日期與未解事項。"}
  ]
}
```

仍使用 `POST /v1/skills` 與既有發布／rollback API。資源直接保存文字，執行時不開檔、不下載，
也不當作 scripts 執行。只接受 `.md/.txt/.json/.yaml/.yml` 相對路徑；拒絕 traversal、反斜線、
隱藏路徑、`scripts/`、`node_modules/` 與其他副檔名。最多八份資源、每份文字 8,000 字元；
instructions 加實際呈現的資源／路徑共最多 12,000 UTF-8 bytes，每個 Agent 綁定總額仍為 16,000。
超過上限直接拒絕，不截斷。JSON／YAML 資源是文字，不會解析或執行。

資源與模式進入 canonical hash、不可變 revision、執行／規劃快照；資源文字一併接受內容政策與
ATR、進入 model context 與既有保守 token 扣款。扣款不是精準 tokenizer 計數或 forecast。
摘要列表不回傳資源本文；單筆／revision／export 仍服從既有 ACL。匯出會標示 v2 與模式，
但目前仍是 `SKILL.md` 文字，不宣稱是可往返的 ZIP package。

`provenance` 預留給匯入流程且納入 v2 hash；一般內容 API 不可自行冒稱 package provenance。

## 規劃與執行分離

Agent 的 `skills` 只能綁定允許 `execute` 的版本；`planning_skills` 只能綁定允許 `plan` 的版本。
同一份 v2 可允許兩種模式，但只有 plan 的 Skill 不能要求 executable tools。

`plan-forecast` 名稱保留為 v2、`execution_modes:["plan"]`；只能明確放在 `planning_skills`。
不能放入執行綁定或自動選擇白名單。一般 Goal 提及這個名稱不構成觸發，也不改變 session mode。
規劃快照保存獨立 `planning_skill_snapshots`；Goal `agent_config` 不攜帶規劃綁定。
目前不呼叫 estimator、不自動生成計畫、不自動建立執行 Goal。

## 自動選擇

管理者在 Agent 設定中加入固定 revision 的白名單規則：

```json
{
  "skills": [{"skill_id":"always-bound-id","revision":1}],
  "skill_selection_rules": [
    {"skill_id":"evidence-skill-id","revision":2,"priority":20,
     "field":"objective","values":["證據","evidence"]}
  ]
}
```

- 可比對 `objective`、`task_type`、`event_type`、`tags`。objective 採 Unicode casefold 後的
  字詞包含；其餘採 exact match，tags 比較陣列中的完整元素。event_type 只讀服務端
  `trigger_metadata.event_type`，不相信同名的根欄位。規則沒有任意 regex、腳本、URL 或模型呼叫。
- 固定綁定優先（同 Skill ID 以固定 revision 為準），自動項目依 priority 由高至低，再依 Skill ID。
  每規則最多 16 個字詞，每個 128 UTF-8 bytes。規則最多八條且 Skill ID 不重複。
- 固定＋自動白名單最壞情況最多八個 Skill，設定時一起驗證容量／角色／工具；即使某條規則
  很少命中，也不能配置超額白名單後在執行時悄悄截斷。
- 建立 Goal 時保存選定 binding／完整內容／epoch，以及 `skill_selection` 的規則 fingerprint、
  schema version、固定／規則來源與 priority；不存入命中的原始任務字句。執行中不重新選擇。
- 計畫設定快照先固定整份白名單。保存 plan revision 時依根任務／Worker assignment 的 objective
  固定選擇 artifact，讀取時從已雜湊的 plan＋snapshot 重算檢查，防止 artifact 被改寫。
  目前的結構化 Plan 沒有 event_type／tags，不能把缺少欄位當作命中。
- 自動選擇只選執行 Skills；規劃 Skills 仍明確綁定。選取不新增工具、網域、model 或 token 額度。

套件匯入與後續 DAG／forecast／前端的完成狀態見[roadmap](implementation-roadmap.md)。
