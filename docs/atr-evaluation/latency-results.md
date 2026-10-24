# ATR 延遲量測

`scripts/benchmark_sse.py` 會分別記錄：

- 第一個任何 token 的時間 `ttft_ms`。
- 第一個未驗證預覽 token 的時間 `draft_ttft_ms`。
- 第一個正式 token 的時間 `final_ttft_ms`。
- 總延遲。
- draft／final token event 數。

## 實驗方式

對同一個 Gateway commit、模型、輸入與 concurrency，分別重啟服務並執行：

```bash
python scripts/benchmark_sse.py --label atr-off --samples 30 > atr-off.json
python scripts/benchmark_sse.py --label detection-only --samples 30 > detection-only.json
python scripts/benchmark_sse.py --label blocking --samples 30 > blocking.json
```

三次啟動設定：

| Label | `ATR_ENABLED` | `ATR_BLOCKING` |
| --- | --- | --- |
| `atr-off` | `false` | `false` |
| `detection-only` | `true` | `false` |
| `blocking` | `true` | `true` |

至少分別量測短輸出與長輸出，報告 p50、p95、max、樣本數與環境。若使用 mock model，只能把
結果描述成 Gateway overhead；不可宣稱是真實 Ollama／vLLM production latency。

## 2026-10-23 本機 mock-model 結果

環境：Python 3.14.6、pyatr 0.3.1、Gateway baseline
`cf0bf34eb77341f06557dccefb6bff1ff7c0f63c` 加上本次未 commit 變更、本機 HTTP、無 Redis、
內建 deterministic mock LLM。每種模式 30 筆、concurrency 1；mock model 每次串流 4 個 draft
chunks，每個 chunk 間隔約 30 ms。

| 模式 | Draft TTFT p50 / p95 | Final TTFT p50 / p95 | Total p50 / p95 |
| --- | ---: | ---: | ---: |
| ATR off | 22.1 / 40.7 ms | 164.0 / 206.7 ms | 164.8 / 207.2 ms |
| Detection-only | 19.5 / 26.0 ms | 164.2 / 170.7 ms | 165.0 / 171.4 ms |
| Blocking | 166.6 / 176.9 ms | 170.5 / 180.5 ms | 171.2 / 181.4 ms |

在這個短輸出 mock workload 中：

- Detection-only 與 ATR off 的 p50 final／total latency 差異只有約 0.2 ms，低於這次簡單
  實驗可以可靠解讀的範圍。
- Blocking 的 draft TTFT p50 比 ATR off 晚約 144.5 ms，符合「完整輸出掃描後才釋出預覽」
  的控制流程。
- Blocking 的 final TTFT p50 比 ATR off 晚約 6.5 ms，total p50 晚約 6.4 ms。

這些結果只能說明本機短輸出 mock-model 的 Gateway overhead。三種模式是分批順序執行，未做
隨機交錯、跨機器重複或 production model 測試；不可據此宣稱 production latency。

## 2026-10-24 本機 Ollama exploratory 結果

環境：Docker `ollama/ollama:0.12.3`、`qwen2.5:0.5b-instruct`、Apple Silicon 主機但容器內
Ollama 回報 CPU inference。輸入要求約 600 個中文字、六段說明；每種模式 5 筆、concurrency 1。
這是實際模型 smoke benchmark，但模型與樣本數都不足以代表 production。

| 模式 | Draft TTFT p50 / p95 | Final TTFT p50 / p95 | Total p50 / p95 |
| --- | ---: | ---: | ---: |
| ATR off | 85.9 / 101.4 ms | 7176.8 / 7884.1 ms | 7185.8 / 7894.2 ms |
| Detection-only | 97.8 / 115.1 ms | 5158.8 / 8319.9 ms | 5165.6 / 8330.6 ms |
| Blocking | 6063.3 / 6257.6 ms | 6129.0 / 6322.3 ms | 6133.4 / 6327.4 ms |

Blocking 的 draft TTFT 接近完整 generation 時間，符合「先 buffer、掃描，再釋出」的設計。
ATR off 與 detection-only 仍可在約 0.1 秒開始顯示 draft。由於模型未穩定遵守輸出長度、每組
只有 5 筆且依序執行，final/total 的組間差異主要受生成長度、warm-up 與 CPU scheduling 影響；
不能由這批資料估計 ATR 對總推論時間的因果 overhead。
