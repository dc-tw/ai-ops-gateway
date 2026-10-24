# 限制

- ATR Layer-1 是 pattern-based detection；命中不是已證實攻擊，未命中也不是安全證明。
- Fake engine 測試驗證 Gateway control flow，不衡量實際 ATR rule corpus 的 precision 或 recall。
- 在本地以 `pyatr 0.3.1` 載入 bundled rules 時，engine 對 `ATR-2026-00290` 的一個 regex
  condition 發出 compile warning 並略過該 condition；在修正或升級前，不應把載入的規則數
  直接等同於所有條件皆可執行。
- 60 筆良性語料與 `ATR-2026-00213` 的 30 筆校準語料都是人工編寫的合成樣本，不能代表
  production traffic；尤其候選規則語料刻意提高了安全文件與攻擊字句近似的比例。
- 單一 request／tool event 無法完整偵測需要跨事件 correlation 的攻擊。
- 內容在掃描前會依 `ATR_MAX_CONTENT_CHARS` 截斷；需要另測攻擊落在中段時的行為與風險。
- Privacy-safe finding 不保存原文，降低二次洩漏風險，但也限制事後調查；完整證據需由受控的
  上游 audit 系統另外保存。
- Blocking output buffering 能避免未掃描 token 先送出，但會增加延遲及記憶體用量。
- Redis replay 的一般 blocking 路徑已有 E2E；尚未完成 multi-replica、掃描期間 process crash
  與各種 partial-write 時序的 non-leakage E2E。
- Recommendation 只根據已 review 樣本；抽樣偏差與 reviewer disagreement 仍可能造成錯誤決策。
