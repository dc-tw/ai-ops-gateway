# ATR 合成測試語料

初始語料位於 `tests/fixtures/atr_evaluation_cases.json`。它目前只是一組 integration smoke
fixtures，不足以支持一般化的偵測率或 false-positive rate 主張。

另有兩組人工編寫的版本化語料：

- `atr_benign_evaluation_cases.json`：60 筆良性案例，三個 boundary 各 20 筆。
- `atr_00213_review_cases.json`：針對 `ATR-2026-00213` 的 20 筆攻擊與 10 筆良性近似。

`tests/test_atr_evaluation_corpus.py::test_bundled_atr_rules_match_the_seed_corpus_expectations`
會使用實際安裝的 `pyatr` bundled rules 執行這批案例；這和其他使用 fake engine 的控制流程
測試互補。規則版本改變後若結果不同，測試會要求重新 review fixture，而不是靜默沿用舊結論。

產生可保存的機器可讀報告：

```bash
.venv/bin/python scripts/evaluate_atr_corpus.py > atr-corpus-report.json
.venv/bin/python scripts/evaluate_atr_corpus.py \
  --corpus tests/fixtures/atr_benign_evaluation_cases.json
.venv/bin/python scripts/evaluate_atr_corpus.py \
  --corpus tests/fixtures/atr_00213_review_cases.json \
  --rule-id ATR-2026-00213
```

報告包含 `pyatr` 版本、載入規則數、每個案例命中的 rule IDs 及 expectation mismatch 數。
指定 `--rule-id` 時，confusion matrix 只依該規則是否命中計算，其他規則仍列在每筆結果供
交叉檢查。評估器遇到 mismatch 會回傳非零 exit code，讓誤報或漏報不能被 CI 靜默忽略。

每筆案例包含：

- `case_id`：跨實驗保持穩定的識別碼。
- `boundary`：`request_input`、`tool_result` 或 `model_output`。
- `category`：案例用途。
- `content`：合成輸入，不含真實 prompt、憑證或客戶資料。
- `expected_detection`：此實驗預期是否產生 finding。
- `expected_block`：blocking 模式下是否應阻擋。
- `review_verdict`：人工 review 的預期方向。

## 擴充規則

要計算單一規則的 Gateway-specific false-positive rate，至少需要：

1. 20 筆已人工判定的樣本，符合 Gateway 預設 recommendation 門檻。
2. 同時包含攻擊、良性近似及一般正常流量。
3. 保存語料版本與規則版本。
4. 將 `uncertain`／`acceptable_risk` 與 precision 分母分開，不把它們偽裝成 true positive。
5. 對外發布比率時同時發布分子、分母與抽樣方法。

20 筆只適合驗證 promotion workflow；若要報告穩定的 rate，應擴充至至少 50–100 筆良性
樣本，並說明它們是否代表實際 Gateway 流量。
