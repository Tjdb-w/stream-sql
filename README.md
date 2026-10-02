# Stream SQL

流式 SQL 计算引擎：窗口聚合、乱序水位、Exactly-once 状态与背压控制。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

已实现：由乱序水位推进的事件时间翻滚窗口聚合（纯 Python，无第三方依赖）。

尚未实现：Exactly-once 状态（持久化、重启恢复、记录去重）与背压控制。

## 公开 API

```python
from stream_sql import (
    compile_query,
    StreamQuery,
    QuerySyntaxError,
    InvalidRecordError,
    WatermarkRegressionError,
)
```

- `compile_query(sql) -> StreamQuery`：编译一条受限 SQL。
- `StreamQuery.push(record) -> "included" | "late"`：摄入一条字段名到值的映射。
- `StreamQuery.advance_watermark(timestamp) -> None`：显式推进水位。
- `StreamQuery.drain() -> list[dict]`：取走自上次 drain 以来已确定的结果。

## SQL 语法

```sql
SELECT user_id, TUMBLE_START [AS 别名], TUMBLE_END [AS 别名], SUM(amount) [AS 别名]
FROM orders
GROUP BY user_id, TUMBLE(event_time, INTERVAL n SECOND)
```

- 数据源固定为 `orders`，可用字段仅 `user_id`、`event_time`、`amount`。
- SELECT 列只能是 `user_id`、`TUMBLE_START`、`TUMBLE_END`、`SUM(amount)`，
  每列可带别名（`AS name` 或裸 `name`）；`TUMBLE_START`/`TUMBLE_END` 也可写成
  带参数形式 `TUMBLE_START(event_time, INTERVAL n SECOND)`。
- GROUP BY 必须恰好包含 `user_id` 与 `TUMBLE(event_time, INTERVAL n SECOND)`，
  顺序不限；`n` 必须是正整数秒（支持 `INTERVAL 10 SECOND` 与 `INTERVAL '10' SECOND`）。
- 关键字与标识符大小写不敏感，空白与别名变化不影响语义；末尾允许一个分号。
- 不满足以上约束时 `compile_query` 抛 `QuerySyntaxError`。

## 语义

- `event_time` 接受 Unix 毫秒整数或带时区的 ISO 8601 字符串，统一换算到 UTC；
  `amount` 只接受整数；`user_id` 接受字符串或整数。
- 窗口按纪元对齐，左闭右开 `[start, end)`；水位达到窗口结束时刻后窗口确定并进入
  待输出队列。
- `event_time` 严格小于当前水位的记录由 `push` 返回 `"late"`，不修改聚合状态，
  也不产生输出；等于水位的记录仍被接收。
- `drain()` 按 `(window_start, window_end, user_id)` 排序返回，每行只含查询声明的
  列（键为别名或规范名 `user_id` / `TUMBLE_START` / `TUMBLE_END` / `SUM(amount)`）；
  时间值输出 ISO 8601 UTC 字符串（如 `1970-01-01T00:00:10Z`，非整秒时带毫秒），
  `SUM(amount)` 输出整数。drain 后队列清空。
- 相同查询对相同记录顺序、水位顺序和 drain 时机给出相同结果。
- 记录缺少字段、类型不符或时间无法解析时 `push` 抛 `InvalidRecordError`；
  `advance_watermark` 回退水位时抛 `WatermarkRegressionError`。
  异常后查询仍可接收后续合法输入，已确定结果不改变。

## 测试

```bash
python3 -m pytest tests/
```

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
