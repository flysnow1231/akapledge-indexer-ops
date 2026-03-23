# AkaPledge 运营扫链工程

## 关键设计
### 1. 循环任务
入口：

```bash
go run ./cmd/indexer -config ./configs/config.yaml
```
主循环会每隔 `poll_interval_seconds` 执行一轮。

### 2. 延迟 6 个区块入库
核心逻辑：

```text
safeLatest = latestBlock - delayBlocks
```

默认：

```yaml
chain:
  delay_blocks: 6
```
也就是说，链上最新块如果是 `1000`，本轮最多只会扫到 `994`。
这样可以降低短暂重组带来的脏数据风险。

### 3. 面向运营的表
- `sync_task_checkpoint`：断点续扫
- `akapledge_event_log`：原始事件流水
- `akapledge_pool`：池子运营总表
- `akapledge_lender_position`：出借人仓位表
- `akapledge_borrower_position`：借款人仓位表

## 已覆盖事件
基于你给的 AkaPledge 合约事件：

- `PoolCreated`
- `PoolStageUpdated`
- `LendDeposited`
- `BorrowDeposited`
- `Settled`
- `LenderRefunded`
- `LenderClaimed`
- `BorrowerRefunded`
- `BorrowerClaimed`
- `HealthChecked`
- `Finished`
- `Liquidated`
- `LenderWithdrawn`
- `BorrowerWithdrawn`

## 初始化数据库
直接执行：

```sql
source ./schema/ddl.sql;
```

## 启动

```bash
go mod tidy
go run ./cmd/indexer -config ./configs/config.yaml
```

只跑一轮：

```bash
go run ./cmd/indexer -config ./configs/config.example.yaml -once
```

## 说明

1. 金额字段全部按链上最小单位字符串存储，避免精度损失。
2. `akapelge_event_log` 使用 `(tx_hash, log_index)` 做幂等。
3. 这版没有做“回滚已落库数据”的复杂重组补偿，只是通过 **延迟 6 块** 降低风险；如果后面你要上主网生产，我建议再补一版“区块 hash 对账 + 回滚重放”。
