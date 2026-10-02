# Stream SQL

流式 SQL 计算引擎：窗口聚合、乱序水位、Exactly-once 状态与背压控制。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

已实现：由乱序水位推进的事件时间翻滚窗口 SQL 聚合（`stream_sql.py`，仅标准库，无外部依赖），
以及有界背压控制（`compile_query(sql, capacity=...)`）。
尚未实现：Exactly-once 状态（持久化、重启恢复、记录去重）；当前实现无任何落盘行为。

## 公开接口

```python
from stream_sql import compile_query

query = compile_query(sql, capacity=None)  # -> StreamQuery
query.push(record)                  # -> "included" | "late" | "backpressured"
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

### 有界背压（capacity）

- `compile_query(sql, capacity=n)` 中，聚合键由窗口起点与 `user_id` 确定，
  `capacity` 是查询可保留的最大未输出聚合键数；省略或传 `None` 时查询无界，
  既有行为完全不变。
- `capacity` 只接受正整数：`bool`、零、负数及其他类型一律抛
  `QueryConfigurationError`，且不创建查询实例。
- 记录属于已存在的聚合键时，即使容量已满也返回 `"included"` 并累加；
  属于尚未存在的新键且容量已满时返回 `"backpressured"`，
  不修改聚合状态、不生成结果，也不改变水位。
- 迟到判断优先于容量判断：容量已满时迟到记录仍返回 `"late"`。
- 推进水位本身不释放容量；`drain()` 输出并移除已确定聚合键时才释放对应容量，
  之后新聚合键可被接收。被拒绝后同一查询仍可继续处理后续合法记录。

### 异常

- `QuerySyntaxError`：SQL 出现限定字段之外的字段、不支持的表达式，
  或窗口间隔不是正的秒数。
- `QueryConfigurationError`：`capacity` 不是正整数或 `None`。
- `InvalidRecordError`：记录缺少字段、类型不符或时间无法解析
  （`advance_watermark` 的时间参数无法解析时同样抛出）。
- `WatermarkRegressionError`：`advance_watermark` 回退水位。

异常抛出后查询仍可接收后续合法输入，已有聚合状态与已确定结果不改变。

## 测试

```bash
python3 -m unittest discover -s tests
```

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
