# Stream SQL

流式 SQL 计算引擎：窗口聚合、乱序水位、Exactly-once 状态与背压控制。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

已实现：由乱序水位推进的事件时间翻滚/滑动/会话窗口 SQL 聚合、有界背压控制与
Exactly-once 状态（持久化、重启恢复、记录去重）
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
       SUM(amount) AS total,
       COUNT(amount) AS cnt,
       MIN(amount) AS min_amount,
       MAX(amount) AS max_amount
FROM orders
GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)
```

- 数据源固定为 `orders`，可用字段限定为 `user_id`、`event_time`、`amount`。
- `user_id` 必选；`TUMBLE_START`、`TUMBLE_END` 为可选边界列。
- 聚合支持 `SUM(amount)`、`COUNT(amount)`、`MIN(amount)`、`MAX(amount)`，
  可任意排列、省略或加 `AS` 别名；同一聚合可用不同别名重复输出。
  未给别名时输出列名依次为 `sum_amount`、`count_amount`、`min_amount`、
  `max_amount`；输出列名（含别名）不得重复。SUM 累加 amount，COUNT 统计
  amount 条数，MIN/MAX 取最小、最大整数（amount 允许为负整数）。
- 未知聚合、或把 `MIN/MAX/COUNT/SUM` 作用于 `amount` 之外的字段均抛
  `QuerySyntaxError`。
- 窗口间隔必须是正的秒数；`TUMBLE_START`/`TUMBLE_END` 的间隔须与 `TUMBLE` 一致。
- 窗口左闭右开，按 epoch 对齐。

滑动窗口 `HOP(event_time, INTERVAL n SECOND, INTERVAL m SECOND)` 与
`HOP_START`/`HOP_END` 同理：size 与 slide 均为正整数秒且 slide 不大于 size，
一条记录落入所有覆盖其事件时间的窗口，四类聚合在每个覆盖窗口分别更新。

会话窗口形态：

```sql
SELECT user_id,
       SESSION_START(event_time, INTERVAL 30 SECOND),
       SESSION_END(event_time, INTERVAL 30 SECOND),
       SUM(amount)
FROM orders
GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)
```

- gap 必须是正的秒数（整数或对应字符串）；`SESSION_START`/`SESSION_END` 的
  间隔须与 `SESSION` 一致；缺省列名为 `session_start`、`session_end`、
  `sum_amount`、`count_amount`、`min_amount`、`max_amount`（聚合按需选择）。
- 同一 `user_id` 内相邻事件时间差不超过 gap 的记录归入同一会话；一条记录可
  同时连接前后两个会话并将其合并，四类聚合（SUM/COUNT/MIN/MAX）对该记录
  都只计入一次。
- `session_start` 为会话内最小事件时间，`session_end` 为最大事件时间加 gap，
  不做 epoch 对齐。
- 水位严格大于 `session_end` 后会话确定，由下一次 `drain()` 按
  (`session_start`, `session_end`, `user_id`) 排序输出并移除。
- SESSION 与 TUMBLE、HOP 不可混用；窗口参数不符或混用时 `compile_query`
  抛出 `QuerySyntaxError`。

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
  每行只含查询声明的列；时间值输出 ISO 8601 UTC 字符串，四类聚合均输出整数。
- 一条记录的四类聚合在同一更新中原子变更：提交失败时全部回滚，不会出现
  SUM 已更新而 COUNT/MIN/MAX 未更新的中间状态。
- 相同查询对相同记录顺序、水位顺序和 drain 时机给出相同结果。

### 背压控制

- `compile_query(sql, capacity=...)` 的 `capacity` 省略或为 `None` 时查询无界；
  给定正整数时表示查询可保留的最大未输出聚合键数
  （聚合键由窗口起点与 `user_id` 确定；SESSION 窗口的聚合键为合并后的
  （`user_id`, 会话））。
- `capacity` 只接受大于零的整数；`bool`、零、负数和其他类型都抛出
  `QueryConfigurationError`，且不创建查询实例。
- 合法记录属于已存在的聚合键时，即使容量已满也返回 `"included"` 并累加；
  属于新聚合键且容量已满时返回 `"backpressured"`，不修改聚合状态、
  不产生结果，也不改变水位。
- 合法但迟到的记录优先返回 `"late"`，不因容量已满变成 `"backpressured"`。
- 推进水位本身不释放容量；`drain()` 移除已确定聚合键后释放对应容量，
  后续新聚合键即可被接收。
- 每次拒绝记录后，同一查询可继续处理后续合法记录。

### Exactly-once 状态（state_path）

- `compile_query(sql, capacity=..., state_path=...)` 给定 `state_path` 时进入持久模式；
  省略或为 `None` 时保持纯内存语义，返回值、异常与列输出完全不变。
- 持久模式下每次 `push`、`advance_watermark`、`drain` 的成功变更都会把完整状态
  （水位、未输出窗口的四类聚合、已处理 `record_id`）原子提交到 `state_path`
  指向的文件；中途失败不留下部分更新，已提交状态保持不变。持久化只写该文件，
  不额外创建日志或隐式目录。
- 使用同一 SQL（语义等价即可）、同一 `capacity` 和同一 `state_path` 重新构造查询，
  即可恢复未输出窗口、当前水位与去重信息，继续 `push` / `drain`；
  已成功 `drain` 返回的窗口不会在重启后重复返回。
- 旧版本（仅 SUM）状态文件仍可被同构的仅含 `SUM(amount)` 的查询恢复；旧窗口的
  COUNT/MIN/MAX 没有历史数据可重建，这类窗口在后续提交中继续保持仅 SUM 短表。
  含 `COUNT/MIN/MAX` 的新查询读取旧状态（或任何聚合集合不匹配的状态）时
  抛 `StateStorageError`。
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
  或窗口间隔不是正的秒数。
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
