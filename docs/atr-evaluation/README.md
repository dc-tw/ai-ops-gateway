# ATR Gateway 證據包

這個目錄記錄 AI Operations Gateway 整合 Agent Threat Rules（ATR）的可重現證據。
目標不是證明 ATR 能偵測所有攻擊，而是回答以下較窄的工程問題：

1. Gateway 在哪些邊界執行 ATR？
2. Detection-only 與 blocking 模式的行為有何差異？
3. 被阻擋的內容是否仍會進入模型、工具流程或 SSE 輸出？
4. 規則何時只得到「建議阻擋」，何時真正取得 production enforcement 權限？
5. Blocking 對使用者可見延遲有多少影響？

## 證據層級

目前證據分成三層，引用時不得混用：

- **自動化行為測試**：使用 deterministic fake engine、model、tool 與 Redis，驗證 Gateway
  的控制流程。這能證明 integration contract，但不是 ATR 規則準確率 benchmark。
- **合成測試語料**：版本化的攻擊、良性近似與正常案例，用來保持各次實驗輸入一致。
- **部署量測**：需要啟動 Gateway 後執行，包含真實 ATR rule set、SSE、TTFT 與人工 review。
  尚未完成的量測必須標示為待執行，不能以設計或單元測試代替。

## 固定實驗資訊

每次產生可引用結果前，請記錄：

```bash
git rev-parse HEAD
.venv/bin/python --version
.venv/bin/python -c 'import importlib.metadata as m; print(m.version("pyatr"))'
```

另記錄：

- ATR 規則來源與版本。
- 模型、推論後端及其版本。
- `ATR_ENABLED`、`ATR_BLOCKING`、severity 門檻與 rule allow/exclude lists。
- 測試日期、機器環境、樣本數與 concurrency。
- 使用 mock model、Ollama 或 vLLM。

## 快速驗證

```bash
.venv/bin/python -m pytest -q \
  tests/test_atr_evaluation_corpus.py \
  tests/test_threat_detection.py \
  tests/test_sse.py
```

各項證據與限制請見：

- [測試語料](test-corpus.md)
- [執行邊界](boundary-results.md)
- [人工審查與規則升級](review-and-promotion.md)
- [延遲量測](latency-results.md)
- [限制](limitations.md)
- [2026-10-23 本地驗證紀錄](run-2026-10-23.md)
- [2026-10-24 ATR 驗證紀錄](run-2026-10-24.md)
