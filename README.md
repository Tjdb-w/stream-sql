# Stream SQL

流式 SQL 计算引擎：窗口聚合、乱序水位、Exactly-once 状态与背压控制。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

已实现：由乱序水位推进的事件时间翻滚（TUMBLE）/滑动（HOP）窗口 SQL 聚合、
有界背压控制与 Exactly-once 状态（持久化、重启恢复、记录去重）
（`stream_sql.py`，仅标准库，无外部依赖）。

## 公开接口

```python
from stream_sql import compile_query

query = compile_query(sql)                  # -> StreamQuery，无界，纯内存
query = compile_query(sql, capacity=100)    # -> StreamQuery，最多保留 100 个未输出聚合键
query = compile_query(sql, state_path="query.state")  # -> 持久模式，状态落盘可恢复
query.push(record)                  # -> "included" | "late" | "backpressured"
                                    #    （持久模式还可能是 "duplicate"）
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

滑动窗口使用 `HOP`，窗口长度 `size`、滑动步长 `slide`：

```sql
SELECT user_id,
       HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) AS window_start,
       HOP_END(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) AS window_end,
       SUM(amount) AS total
FROM orders
GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)
```

- 数据源固定为 `orders`，可用字段限定为 `user_id`、`event_time`、`amount`。
- `user_id` 必选；`TUMBLE_START`/`TUMBLE_END`、`HOP_START`/`HOP_END`、
  `SUM(amount)` 为可选别名列，未给别名时输出列名依次为
  `window_start`、`window_end`、`sum_amount`。
- 窗口间隔必须是正的秒数；边界函数（`TUMBLE_START`/`TUMBLE_END` 或
  `HOP_START`/`HOP_END`）的参数须与分组窗口一致。
- TUMBLE 窗口左闭右开，按窗口长度（epoch）对齐。
- HOP 的 `size`、`slide` 为正整数秒且 `slide` 不大于 `size`；窗口左闭右开，
  按 `slide`（epoch）对齐。一条记录落入满足 `window_start <= event_time <
  window_start + size` 且 `window_start` 为 `slide` 整数倍的全部窗口，因此会
  同时进入多个聚合键；`slide = size` 时 HOP 与 TUMBLE 等价。
- 同一查询只能使用一种窗口：混用 TUMBLE 与 HOP、边界函数参数与分组不一致、
  `slide > size`，或出现不支持的表达式时，`compile_query` 抛 `QuerySyntaxError`。

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
  （聚合键由窗口起点与 `user_id` 确定）。HOP 下一条记录会进入多个窗口，
  其对应的每个 `(window_start, user_id)` 各占一个名额。
- `capacity` 只接受大于零的整数；`bool`、零、负数和其他类型都抛出
  `QueryConfigurationError`，且不创建查询实例。
- 合法记录的所有目标键均已存在时，即使容量已满也返回 `"included"` 并累加；
  只要会引入一个放不下的新聚合键，整条记录即返回 `"backpressured"`
  （HOP 下必须全部新键都能容纳才接收），不做部分累加，不修改聚合状态、
  不产生结果、不登记 `record_id`，也不改变水位。
- 合法但迟到的记录优先返回 `"late"`，不因容量已满变成 `"backpressured"`。
- 推进水位本身不释放容量；`drain()` 移除已确定聚合键后释放对应容量，
  后续新聚合键即可被接收。
- 每次拒绝记录后，同一查询可继续处理后续合法记录。

### Exactly-once 状态（state_path）

- `compile_query(sql, capacity=..., state_path=...)` 给定 `state_path` 时进入持久模式；
  省略或为 `None` 时保持纯内存语义，返回值、异常与列输出完全不变。
- 持久模式下每次 `push`、`advance_watermark`、`drain` 的成功变更都会把完整状态
  （水位、未输出窗口聚合、已处理 `record_id`）原子提交到 `state_path` 指向的文件；
  中途失败不留下部分更新，已提交状态保持不变。持久化只写该文件，
  不额外创建日志或隐式目录。
- 使用同一 SQL（语义等价即可）、同一 `capacity` 和同一 `state_path` 重新构造查询，
  即可恢复未输出窗口、当前水位与去重信息，继续 `push` / `drain`；
  已成功 `drain` 返回的窗口不会在重启后重复返回。状态指纹区分窗口类型
  （TUMBLE/HOP）、`size` 与 `slide`，HOP 的多个窗口、水位与去重信息在同一
  原子文件中保存。
- 持久模式下每条记录必须带非空字符串 `record_id`，缺失或类型不符抛出
  `InvalidRecordError`。同一查询状态内已处理（`included` 或 `late`）的
  `record_id` 再次出现时，无论事件时间、容量或水位如何都返回 `"duplicate"`，
  不再累加、不产生结果。被背压拒绝的记录未被处理，容量释放后可用同一
  `record_id` 重新摄入。`record_id` 仅限持久模式；内存模式不校验该字段，
  既有输入判定不变。
- `state_path` 必须是可写文件路径；空字符串、非字符串、不可写路径，以及状态
  文件损坏、版本不兼容或与 SQL、`capacity` 不一致，统一抛出 `StateStorageError`，
  失败操作不修改当前已提交状态。

### 异常

- `QuerySyntaxError`：SQL 出现限定字段之外的字段、不支持的表达式，
  窗口间隔不是正的秒数，HOP 的 `slide` 大于 `size`，混用 TUMBLE 与 HOP，
  或窗口边界函数参数与分组窗口不一致。
- `QueryConfigurationError`：`capacity` 不是大于零的整数或 `None`。
- `InvalidRecordError`：记录缺少字段、类型不符或时间无法解析
  （`advance_watermark` 的时间参数无法解析时同样抛出）。
- `WatermarkRegressionError`：`advance_watermark` 回退水位。
- `StateStorageError`：`state_path` 不是非空字符串、路径不可写，或状态文件
  损坏、版本不兼容、与 SQL 或 `capacity` 不一致。

异常抛出后查询仍可接收后续合法输入，已确定结果不改变。

## 测试

```bash
python3 -m unittest discover -s tests
```

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
