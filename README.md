# Stream SQL

流式 SQL 计算引擎：窗口聚合、乱序水位、Exactly-once 状态与背压控制。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

已实现：由乱序水位推进的事件时间翻滚窗口 SQL 聚合、有界背压控制，
以及 Exactly-once 状态（持久化、重启恢复、记录去重）
（`stream_sql.py`，仅标准库，无外部依赖）。

## 公开接口

```python
from stream_sql import compile_query

query = compile_query(sql)                  # -> StreamQuery，无界
query = compile_query(sql, capacity=100)    # -> StreamQuery，最多保留 100 个未输出聚合键
query = compile_query(sql, state_path="query_state.json")  # 启用 Exactly-once 状态
query.push(record)                  # -> "included" | "late" | "backpressured"（持久模式还可能是 "duplicate"）
query.advance_watermark(timestamp)  # 显式推进水位
rows = query.drain()                # -> list[dict]，已确定结果
```

### SQL 语法

仅支持如下形态（关键字与标识符大小写不敏感，空白与别名变化不影响语义）：

```sql
SELECT user_id,
       TUMBLE_START(event_time, INTERVAL 10 SECOND) AS window_start,
       TUMBLE_END(event_time, INTERVAL 10 SECOND) AS window_end,
       SUM(amount) AS total
FROM orders
GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)
```

- 数据源固定为 `orders`，可用字段限定为 `user_id`、`event_time`、`amount`。
- `user_id` 必选；`TUMBLE_START`、`TUMBLE_END`、`SUM(amount)` 为可选别名列，
  未给别名时输出列名依次为 `window_start`、`window_end`、`sum_amount`。
- 窗口间隔必须是正的秒数；`TUMBLE_START`/`TUMBLE_END` 的间隔须与 `TUMBLE` 一致。
- 窗口左闭右开，按 epoch 对齐。

### 记录与水位

- `push(record)` 接收字段名到值的映射，必须含 `user_id`（str 或 int）、
  `event_time`、`amount`（仅接受整数）。
- `event_time` 与 `advance_watermark` 的时间参数接受 Unix 毫秒整数或带时区的
  ISO 8601 字符串，内部统一到 UTC 毫秒。
- `event_time` 小于当前水位的记录为迟到记录：`push` 返回 `"late"`，
  不修改聚合状态，也不产生输出。
- 水位达到窗口结束时刻（`watermark >= window_end`）后该窗口结果确定，
  由下一次 `drain()` 输出并移除。
- 每次 `drain()` 的结果按 `window_start`、`window_end`、`user_id` 排序；
  每行只含查询声明的列；时间值输出 ISO 8601 UTC 字符串，`SUM(amount)` 输出整数。
- 相同查询对相同记录顺序、水位顺序和 drain 时机给出相同结果。

### 背压控制

- `compile_query(sql, capacity=...)` 的 `capacity` 省略或为 `None` 时查询无界；
  给定正整数时表示查询可保留的最大未输出聚合键数
  （聚合键由窗口起点与 `user_id` 确定）。
- `capacity` 只接受大于零的整数；`bool`、零、负数和其他类型都抛出
  `QueryConfigurationError`，且不创建查询实例。
- 合法记录属于已存在的聚合键时，即使容量已满也返回 `"included"` 并累加；
  属于新聚合键且容量已满时返回 `"backpressured"`，不修改聚合状态、
  不产生结果，也不改变水位。
- 合法但迟到的记录优先返回 `"late"`，不因容量已满变成 `"backpressured"`。
- 推进水位本身不释放容量；`drain()` 移除已确定聚合键后释放对应容量，
  后续新聚合键即可被接收。
- 每次拒绝记录后，同一查询可继续处理后续合法记录。

### Exactly-once 状态

- `compile_query(sql, capacity=..., state_path=...)` 传入非空字符串路径时启用
  持久模式；省略或为 `None` 时保持纯内存语义，内存模式的输入判定、返回值、
  异常与列输出完全不变（记录中多出的 `record_id` 字段被忽略）。
- 持久模式下每条记录必须携带非空字符串 `record_id`，缺失或类型不符抛出
  `InvalidRecordError`。
- 同一查询状态内已处理（被接收计入聚合）的 `record_id` 再次出现时，无论事件
  时间、容量或水位如何，都不再累加、不产生结果、不改变状态，返回
  `"duplicate"`；该判定优先于 `late` 与 `backpressured`。迟到或被背压拒绝的
  记录未被处理，其 `record_id` 不进入去重集合，之后可重试。
- `push`、`advance_watermark`、`drain` 的每次状态变更都原子提交完整状态到
  `state_path`（同目录临时文件 + 原子替换），中途失败不留下部分更新，也不
  额外创建日志或隐式目录；提交失败时内存状态一并回滚。
- 使用同一 SQL、`capacity` 和 `state_path` 重新构造查询即可恢复未输出窗口、
  当前水位与去重信息；已成功 `drain` 返回的窗口在重启后不会重复返回。
- `state_path` 为空字符串、非字符串、不可写路径，或状态文件损坏、版本不
  兼容、与 SQL / `capacity` 不一致时，统一抛出 `StateStorageError`，且失败
  操作不修改当前已提交状态。

### 异常

- `QuerySyntaxError`：SQL 出现限定字段之外的字段、不支持的表达式，
  或窗口间隔不是正的秒数。
- `QueryConfigurationError`：`capacity` 不是大于零的整数或 `None`。
- `InvalidRecordError`：记录缺少字段、类型不符或时间无法解析
  （`advance_watermark` 的时间参数无法解析时同样抛出）。
- `WatermarkRegressionError`：`advance_watermark` 回退水位。
- `StateStorageError`：`state_path` 不可用（空字符串、非字符串、不可写），
  或状态文件损坏、版本不兼容、与 SQL / `capacity` 不一致。

异常抛出后查询仍可接收后续合法输入，已确定结果不改变。

## 测试

```bash
python3 -m unittest discover -s tests
```

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
