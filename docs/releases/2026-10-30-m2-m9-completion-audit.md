# 2026-10-30：M2–M9 完成稽核

本文件以 `docs/implementation-roadmap.md` 的固定契約逐項核對目前工作樹，不以「找不到 TODO」代替
驗收。結論是 M3–M9 的本機核心交付已完成；本次同時補完先前獨立保留的 M2 audit chain。正式
IdP、外部 webhook、付費／真實模型校正、multi-AZ 與組織 RPO／RTO 屬部署環境驗收，不是缺少的
程式功能，也沒有被本機結果冒稱完成。

| 階段 | 契約與實作權威 | 驗收證據 | 判定 |
| --- | --- | --- | --- |
| M3 | `skill_store.py`、`skill_selection.py`、`skill_packages.py`；v1 hash 保留、plan-only 明確綁定、確定性選擇、固定 GitHub commit／ZIP 隔離匯入 | M3 selection／package release records；固定 revision、容量、SSRF／ZIP、ATR、model-context E2E | 完成 |
| M4 | `workflow_plan.py`、`workflow_execution.py`；canonical Plan v2、安全 YAML／schema、DAG、join／branch、明確且冪等 admission、quota／receipt／fence | compiler、runtime、mixed-version protocol 三份 release records；雙 worker、SIGKILL、舊新版隔離、PG 投影 | 完成 |
| M5 | `plan_forecast.py`、model registry／telemetry；同 compiled plan call manifest、low／base／high、critical path、unknown wait、CAS publication、不可變 artifact | M5 release record；真實 HTTP／Redis／worker synthetic provider 與 calibration drift tests | 核心完成；真實模型 held-out 校正需環境 |
| M6 | `postgres_store.py`、`postgres_worker.py`、`history.py`；受控 FTS／literal／trigram、讀時 ACL、兩階段 retention、poison repair | M6 record；Redis 8.2＋PG17、雙 projector、outage、backfill／reconcile、dump／restore／PITR | 完成 |
| M7 | `ui/`、browser auth、`collaboration.py`、`outbound_notifications.py`；繁中 console、OIDC PKCE／session／CSRF、planning SSE、opt-in presence、heartbeat、HMAC outbox | M7 record；production frontend build、Playwright development-login、HTTP／Redis／PG 回歸 | 核心完成；正式 IdP／收件端需環境 |
| M8 | `acp_adapter.py`、`acp_sessions.py`；ACP v1 stdio、new／load／prompt／update／cancel／permission，沿用 Gateway governance | M8 record；官方 Python reference client subprocess stdio E2E | 契約範圍完成；未宣稱其他 client／未來 v2 |
| M9 | `test_m9_staging_stack.sh`、`verify_m9_staging.py`、`test_postgres_pitr.sh`；多 replica、固定負載、故障、升降版、PITR | M9 record；2×Gateway／Worker／Scheduler／projector、1／4／16 併發、四組 Docker E2E、921 項 Redis＋PG 封版回歸 | 本機 production-like 範圍完成 |
| M2 收尾 | `audit_chain.py`；持續 legacy projector、per-tenant SHA-256 chain、missing／mutation verify、HMAC checkpoint、外部保存 runbook | M2 audit-chain record；最終 Redis 全套、PG17 專項及離線 verifier | 完成；外部 WORM 仍由部署方提供 |

## 最終回歸基線

- 無外部服務：`530 passed, 415 skipped`。
- 真實 Redis DB 0：`872 passed, 73 skipped`。
- 真實 Redis＋PostgreSQL 17 projection／repair／search：`73 passed`。
- M9 既有真實 Redis＋PostgreSQL 完整封版：`921 passed, 0 skipped`。
- Ruff、`git diff --check`、離線 checkpoint verifier 入口通過。

測試數字對應本次最終工作樹；較早 M9 的 921 項 Redis＋PostgreSQL 數字是其原始封版證據，沒有
被誤稱為本次 commit 的重新執行結果。本次另外重跑真實 PG17 的 73 項相關專項。

## 仍需外部環境，但不阻擋程式交付

- 真實／付費模型的 held-out forecast accuracy、漂移與授權流量品質。
- 正式 OIDC IdP、DNS／TLS／egress 與真實 webhook 收件端。
- managed PostgreSQL HA、multi-AZ、長時間 soak、正式 observability 與組織 RPO／RTO rehearsal。
- 其他 ACP client／未來 TCK；外部 Object Lock／WORM audit bundle 保存。

這些項目需要部署憑證、組織基礎設施或付費流量；repository 已保留明確 runbook／測試入口與
不支援邊界，沒有以 mock／本機資料替代正式驗收。
