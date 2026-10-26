# 限制

- ATR Layer-1 是 pattern-based detection；命中不是已證實攻擊，未命中也不是安全證明。
- Fake engine 測試驗證 Gateway control flow，不衡量實際 ATR rule corpus 的 precision 或 recall。
- 在本地以 `pyatr 0.3.1` 載入 bundled rules 時，engine 對 `ATR-2026-00290` 的一個 regex
  condition 發出 compile warning 並略過該 condition；在修正或升級前，不應把載入的規則數
  直接等同於所有條件皆可執行。
- 60 筆良性語料與 `ATR-2026-00213` 的 30 筆校準語料都是人工編寫的合成樣本，不能代表
  production traffic；尤其候選規則語料刻意提高了安全文件與攻擊字句近似的比例。
- 單一 request／tool event 無法完整偵測需要跨事件 correlation 的攻擊。
- 一般 request／tool／output 內容在掃描前會依 `ATR_MAX_CONTENT_CHARS` 截斷；
  `tests/test_release_atr_crash.py` 已用真 pyatr 重現攻擊只在中段時可能被 head／tail 丟棄，
  得到 `truncated: true` 且未命中。這是已確認的漏掃風險，不是安全證明。planning API 則在
  超出完整掃描容量時拒絕保存，不把截斷當完整檢查。
- Privacy-safe finding 不保存原文，降低二次洩漏風險，但也限制事後調查；完整證據需由受控的
  上游 audit 系統另外保存。
- Blocking output buffering 能避免未掃描 token 先送出，但會增加延遲及記憶體用量。
- Redis replay 的一般 blocking、MCP → authenticated Browser Worker 合成惡意回傳，以及兩個
  HTTP process 的三個 SIGKILL 時序已有不洩漏驗證；仍不是各種 distributed failure 的完整矩陣。
  graph checkpoint 與 SSE append 不在同一交易，write 前中斷可遺失診斷事件，不能宣稱 finding
  exactly-once；詳見[邊界證據](boundary-results.md)與[封版彙總](../releases/2026-10-26-skills-plan-mode.md)。
- Recommendation 只根據已 review 樣本；抽樣偏差與 reviewer disagreement 仍可能造成錯誤決策。
