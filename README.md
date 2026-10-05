# Stream SQL

流式 SQL 计算引擎：窗口聚合（翻滚 / 滑动 / 会话）、乱序水位、Exactly-once 状态与背压控制。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

已实现：由乱序水位推进的事件时间翻滚窗口、滑动窗口与会话窗口 SQL 聚合、
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

会话窗口形态（其余为 HOP 滑动窗口形态，见源码文档字符串）：

```sql
SELECT user_id,
       SESSION_START(event_time, INTERVAL 30 SECOND),
       SESSION_END(event_time, INTERVAL 30 SECOND),
       SUM(amount)
FROM orders
GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)
```

- 数据源固定为 `orders`，可用字段限定为 `user_id`、`event_time`、`amount`。
- `user_id` 必选；`TUMBLE_START`、`TUMBLE_END`、`SUM(amount)` 为可选别名列，
  未给别名时输出列名依次为 `window_start`、`window_end`、`sum_amount`。
- 窗口间隔必须是正的秒数；`TUMBLE_START`/`TUMBLE_END` 的间隔须与 `TUMBLE` 一致。
- 窗口左闭右开，按 epoch 对齐。

#### SESSION 会话窗口

- `SELECT` 可选列为 `SESSION_START(event_time, INTERVAL n SECOND)`、
  `SESSION_END(event_time, INTERVAL n SECOND)`、`SUM(amount)`；未给别名时
  输出列名依次为 `session_start`、`session_end`、`sum_amount`。
- 间隔参数（gap）只接受正秒整数或对应字符串（`INTERVAL 30 SECOND` 或
  `INTERVAL '30' SECOND`），且两个边界函数的 gap 必须与
  `GROUP BY ... SESSION(event_time, INTERVAL n SECOND)` 一致。
- TUMBLE、HOP、SESSION 三类窗口不可混用；参数不符或窗口混用时
  `compile_query` 抛 `QuerySyntaxError`。
- 每个 `user_id` 内独立分组：相邻事件时间差不超过 gap 的记录归入同一会话；
  一条记录可以同时连接其左右两侧的已有会话（三者合并），会话内 SUM 随之合并。
  `session_start` 取会话内最小事件时间，`session_end` 取最大事件时间加 gap，
  不做 epoch 对齐；记录的 amount 只累加一次。
- 早于当前水位的记录返回 `"late"` 且不改状态；其余记录返回 `"included"`。
  水位严格大于 `session_end`（`watermark > session_end`）后会话才确定，
  由下一次 `drain()` 按 `session_start`、`session_end`、`user_id` 排序输出
  声明列并从状态移除，时间列为 ISO 8601 UTC 字符串。

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
  （TUMBLE/HOP 的聚合键由窗口起点与 `user_id` 确定；SESSION 为一个
  `user_id` 内一个尚未输出的会话）。
- `capacity` 只接受大于零的整数；`bool`、零、负数和其他类型都抛出
  `QueryConfigurationError`，且不创建查询实例。
- 合法记录属于已存在的聚合键时，即使容量已满也返回 `"included"` 并累加；
  属于新聚合键且容量已满时返回 `"backpressured"`，不修改聚合状态、
  不产生结果，也不改变水位。SESSION 下容量按记录并入（可能连接两侧会话）
  合并后的会话总数判断：合并只会减少会话数，故并入已有会话不会触发背压，
  合并释放的名额可供后续新会话使用。
- 合法但迟到的记录优先返回 `"late"`，不因容量已满变成 `"backpressured"`。
- 推进水位本身不释放容量；`drain()` 移除已确定聚合键后释放对应容量，
  后续新聚合键即可被接收。
- 每次拒绝记录后，同一查询可继续处理后续合法记录。

### Exactly-once 状态（state_path）

- `compile_query(sql, capacity=..., state_path=...)` 给定 `state_path` 时进入持久模式；
  省略或为 `None` 时保持纯内存语义，返回值、异常与列输出完全不变。
- 持久模式下每次 `push`、`advance_watermark`、`drain` 的成功变更都会把完整状态
  （水位、未输出窗口/会话聚合、已处理 `record_id`）原子提交到 `state_path` 指向的文件；
  中途失败不留下部分更新，已提交状态保持不变。持久化只写该文件，
  不额外创建日志或隐式目录。
- 使用同一 SQL（语义等价即可）、同一 `capacity` 和同一 `state_path` 重新构造查询，
  即可恢复未输出窗口/会话、当前水位与去重信息，继续 `push` / `drain`；
  已成功 `drain` 返回的窗口或会话不会在重启后重复返回。
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
  或窗口间隔不是正的秒数、SESSION gap 参数不一致、窗口函数混用。
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
