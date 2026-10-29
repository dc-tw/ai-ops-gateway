# Audit chain 驗證與外部保存

## 設定

在 secret manager 注入至少 32 bytes 的 `AUDIT_CHECKPOINT_SIGNING_KEY`，並設定可辨識輪替版本的
`AUDIT_CHECKPOINT_KEY_ID`。不得把 secret 寫進 repository、checkpoint、audit 或 log。
`AUDIT_CHAIN_PROJECTOR_POLL_SECONDS` 預設 2 秒；verify／checkpoint export 不依賴等待週期，會先同步追平。

API 只允許 human admin：

```bash
curl -fsS -H "x-api-key: $GATEWAY_API_KEY" \
  "$GATEWAY_BASE_URL/v1/audit-chain/verify"

curl -fsS -X POST -H "x-api-key: $GATEWAY_API_KEY" \
  "$GATEWAY_BASE_URL/v1/audit-chain/checkpoints" > checkpoint.json
```

離線驗證匯出文件：

```bash
AUDIT_CHECKPOINT_SIGNING_KEY="$AUDIT_CHECKPOINT_SIGNING_KEY" \
  .venv/bin/python scripts/verify_audit_checkpoint.py checkpoint.json
```

成功輸出 `VALID` 並回傳 0；內容或 signature 不符輸出 `INVALID` 並回傳 1。API 的
`POST /v1/audit-chain/checkpoints/verify` 可做線上交叉檢查，但不能取代離線驗證。

## 外部 immutable export runbook

1. 暫停該 tenant 的會改變治理狀態的維護作業，呼叫 verify，確認 `valid=true`。
2. 匯出 signed checkpoint；記錄 `sequence`、`hash`、`source_cursor`、`coverage_start_cursor`、
   `key_id` 與 UTC 時間。
3. 以受限維運帳號匯出涵蓋範圍內的來源 audit 與 chain raw entries；不要經過會改寫 JSON 的工具。
4. 在隔離主機使用對應 key ID 的舊／現行 secret 執行離線驗證。
5. 把 checkpoint、raw bundle、驗證輸出與部署 commit 一起寫入具有 Object Lock／WORM、版本控制、
   retention policy 與獨立管理權限的外部 storage。
6. 重新讀取外部物件並驗證 checksum／checkpoint，保存 object URI、version ID、retention-until 與
   storage audit evidence。
7. 只有完成上述證據後才可依組織政策另行執行 retention；Gateway 本版沒有自動 prune API。

若 verify 回報 `missing_or_duplicate_sequence`、`hash_mismatch`、`previous_hash_mismatch`、
`head_mismatch` 或 tenant／schema 錯誤，立即停止 retention 與 key rotation，保存 Redis snapshot／AOF
和 application logs，建立 incident。不要嘗試「修正」既有 chain；從最後一個已外部保存且驗證過的
checkpoint 對帳。

完整 canonicalization、genesis、併發與限制見 [ADR-003](architecture/adr-003-audit-chain.md)。
