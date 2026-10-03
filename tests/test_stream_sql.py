import unittest

from stream_sql import (
    compile_query,
    QuerySyntaxError,
    QueryConfigurationError,
    InvalidRecordError,
    WatermarkRegressionError,
)

SQL_FULL = """
    SELECT user_id,
           TUMBLE_START(event_time, INTERVAL 10 SECOND) AS window_start,
           TUMBLE_END(event_time, INTERVAL 10 SECOND) AS window_end,
           SUM(amount) AS total
    FROM orders
    GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)
"""

HOP_SQL = """
    SELECT user_id,
           HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) AS window_start,
           HOP_END(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) AS window_end,
           SUM(amount) AS total
    FROM orders
    GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)
"""

HOP_SQL_SUMS = """
    SELECT user_id, SUM(amount) AS total
    FROM orders
    GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)
"""


def make_query(sql=SQL_FULL):
    return compile_query(sql)


class CompileTest(unittest.TestCase):
    def test_full_query_compiles(self):
        q = make_query()
        self.assertEqual(q.columns, ("user_id", "window_start", "window_end", "total"))
        self.assertEqual(q.window_ms, 10_000)

    def test_whitespace_and_alias_variations_are_equivalent(self):
        sql_a = ("SELECT user_id, SUM(amount) AS s FROM orders "
                 "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        sql_b = ("  SELECT  user_id ,\n SUM ( amount )  AS  s  FROM orders\n"
                 "GROUP BY user_id, TUMBLE( event_time, INTERVAL 10 SECOND ) ;")
        records = [
            {"user_id": "u1", "event_time": 1_000, "amount": 5},
            {"user_id": "u1", "event_time": 2_000, "amount": 7},
        ]
        results = []
        for sql in (sql_a, sql_b):
            q = compile_query(sql)
            for rec in records:
                q.push(rec)
            q.advance_watermark(10_000)
            results.append(q.drain())
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], [{"user_id": "u1", "s": 12}])

    def test_keywords_and_fields_are_case_insensitive(self):
        q = compile_query(
            "select USER_ID, sum(amount) as total from ORDERS "
            "group by USER_ID, tumble(EVENT_TIME, interval 10 second)")
        q.push({"user_id": "u1", "event_time": 0, "amount": 3})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [{"user_id": "u1", "total": 3}])

    def test_optional_columns_may_be_omitted(self):
        q = compile_query(
            "SELECT user_id FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 5 SECOND)")
        q.push({"user_id": "u1", "event_time": 0, "amount": 3})
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 4})
        q.advance_watermark(5_000)
        self.assertEqual(q.drain(), [{"user_id": "u1"}])

    def test_default_column_names(self):
        q = compile_query(
            "SELECT user_id, TUMBLE_START(event_time, INTERVAL 10 SECOND), "
            "TUMBLE_END(event_time, INTERVAL 10 SECOND), SUM(amount) "
            "FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        self.assertEqual(q.columns, ("user_id", "window_start", "window_end", "sum_amount"))

    def test_quoted_interval_and_reversed_group_by(self):
        q = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY TUMBLE(event_time, INTERVAL '10' SECOND), user_id")
        self.assertEqual(q.window_ms, 10_000)

    def test_syntax_errors(self):
        bad_sql = [
            "",
            "SELECT 1",
            "SELECT user_id FROM orders",  # 缺 GROUP BY
            "SELECT user_id, event_time FROM orders "  # 不支持的 select 表达式
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, COUNT(amount) FROM orders "  # 不支持的聚合
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # 限定字段之外的字段
            "GROUP BY user_id, TUMBLE(ts, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # 间隔为零
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 0 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # 间隔为负
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL -5 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # 非秒单位
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 MINUTE)",
            "SELECT user_id, SUM(amount) FROM orders "  # 缺 TUMBLE 分组
            "GROUP BY user_id",
            "SELECT user_id, SUM(amount) FROM orders "  # 缺 user_id 分组
            "GROUP BY TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT SUM(amount) AS total FROM orders "  # select 缺 user_id
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) FROM order_items "  # 表名不符
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) AS t, SUM(amount) AS t FROM orders "  # 别名重复
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, TUMBLE_START(event_time, INTERVAL 5 SECOND) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",  # 间隔不一致
            "SELECT user_id, HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 4 SECOND) "
            "FROM orders "  # HOP_START slide 与分组不一致
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
            "SELECT user_id, HOP_END(event_time, INTERVAL 9 SECOND, INTERVAL 5 SECOND) "
            "FROM orders "  # HOP_END size 与分组不一致
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
            "SELECT user_id, TUMBLE_START(event_time, INTERVAL 10 SECOND) FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",  # 混用
            "SELECT user_id, HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) "
            "FROM orders "  # HOP_START 配 TUMBLE 分组
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # slide 大于 size
            "GROUP BY user_id, HOP(event_time, INTERVAL 5 SECOND, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # HOP size 为零
            "GROUP BY user_id, HOP(event_time, INTERVAL 0 SECOND, INTERVAL 0 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # HOP slide 为零
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 0 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # HOP slide 非正（负号无法词法）
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 'x' SECOND)",
            "SELECT user_id, HOP_START(event_time, INTERVAL 10 SECOND) FROM orders "  # 缺 slide 参数
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
            "SELECT user_id, HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND, "
            "INTERVAL 2 SECOND) FROM orders "  # HOP_START 参数过多
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # 不支持的表达式
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 MINUTE, INTERVAL 5 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "  # 多余子句
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) "
            "HAVING SUM(amount) > 1",
        ]
        for sql in bad_sql:
            with self.assertRaises(QuerySyntaxError, msg=sql):
                compile_query(sql)

    def test_compile_error_does_not_affect_other_queries(self):
        q = make_query()
        with self.assertRaises(QuerySyntaxError):
            compile_query("SELECT nope FROM orders")
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.advance_watermark(10_000)
        self.assertEqual(len(q.drain()), 1)


class PushAndDrainTest(unittest.TestCase):
    def test_window_aggregation_and_output_format(self):
        q = make_query()
        self.assertEqual(q.push({"user_id": "u1", "event_time": 1_000, "amount": 5}), "included")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 9_999, "amount": 7}), "included")
        self.assertEqual(q.push({"user_id": "u2", "event_time": 0, "amount": 3}), "included")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 10_000, "amount": 100}), "included")
        self.assertEqual(q.drain(), [])  # 水位尚未推进
        q.advance_watermark(10_000)
        rows = q.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z", "total": 12},
            {"user_id": "u2", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z", "total": 3},
        ])
        self.assertEqual(q.drain(), [])  # drain 之后结果移除

    def test_window_is_left_closed_right_open(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 10_000, "amount": 1})  # 属于第二个窗口
        q.advance_watermark(9_999)
        self.assertEqual(q.drain(), [])
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [])  # 水位需达到窗口结束 20_000
        q.advance_watermark(20_000)
        rows = q.drain()
        self.assertEqual(rows[0]["window_start"], "1970-01-01T00:00:10Z")
        self.assertEqual(rows[0]["window_end"], "1970-01-01T00:00:20Z")

    def test_result_ordering(self):
        q = make_query()
        q.push({"user_id": "u2", "event_time": 11_000, "amount": 1})
        q.push({"user_id": "u1", "event_time": 11_000, "amount": 2})
        q.push({"user_id": "u9", "event_time": 1_000, "amount": 3})
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 4})
        q.advance_watermark(20_000)
        rows = q.drain()
        self.assertEqual(
            [(r["window_start"], r["user_id"]) for r in rows],
            [("1970-01-01T00:00:00Z", "u1"),
             ("1970-01-01T00:00:00Z", "u9"),
             ("1970-01-01T00:00:10Z", "u1"),
             ("1970-01-01T00:00:10Z", "u2")])

    def test_late_records_do_not_change_state(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        q.advance_watermark(10_000)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 9_999, "amount": 100}), "late")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 10_000, "amount": 2}), "included")
        rows = q.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["total"], 5)

    def test_iso8601_event_time_normalized_to_utc(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": "1970-01-01T08:00:05+08:00", "amount": 4})
        q.push({"user_id": "u1", "event_time": "1970-01-01T00:00:07Z", "amount": 6})
        q.advance_watermark("1970-01-01T00:00:10Z")
        rows = q.drain()
        self.assertEqual(rows[0]["total"], 10)
        self.assertEqual(rows[0]["window_start"], "1970-01-01T00:00:00Z")

    def test_determinism(self):
        records = [
            {"user_id": "u2", "event_time": 3_000, "amount": 1},
            {"user_id": "u1", "event_time": 12_000, "amount": 2},
            {"user_id": "u1", "event_time": 2_000, "amount": 3},
            {"user_id": "u2", "event_time": 1_000, "amount": 4},
        ]
        outputs = []
        for _ in range(2):
            q = make_query()
            for rec in records:
                q.push(rec)
            q.advance_watermark(10_000)
            first = q.drain()
            q.advance_watermark(20_000)
            outputs.append((first, q.drain()))
        self.assertEqual(outputs[0], outputs[1])


class BackpressureTest(unittest.TestCase):
    def test_capacity_validation(self):
        for bad in (True, False, 0, -1, -100, 1.5, "3", [], object()):
            with self.assertRaises(QueryConfigurationError, msg=repr(bad)):
                compile_query(SQL_FULL, capacity=bad)
        # 合法值与省略都能创建查询
        self.assertIsNone(make_query().capacity)
        self.assertIsNone(compile_query(SQL_FULL, capacity=None).capacity)
        self.assertEqual(compile_query(SQL_FULL, capacity=1).capacity, 1)
        self.assertEqual(compile_query(SQL_FULL, capacity=100).capacity, 100)

    def test_new_key_rejected_when_full(self):
        q = compile_query(SQL_FULL, capacity=2)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 1_000, "amount": 1}), "included")
        self.assertEqual(q.push({"user_id": "u2", "event_time": 2_000, "amount": 2}), "included")
        # 第三个新聚合键（窗口或 user_id 不同）被背压拒绝
        self.assertEqual(q.push({"user_id": "u3", "event_time": 3_000, "amount": 3}), "backpressured")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 11_000, "amount": 4}), "backpressured")
        # 已有聚合键即使容量已满仍可累加
        self.assertEqual(q.push({"user_id": "u1", "event_time": 4_000, "amount": 5}), "included")
        self.assertEqual(q.push({"user_id": "u2", "event_time": 5_000, "amount": 6}), "included")
        q.advance_watermark(10_000)
        self.assertEqual(
            q.drain(),
            [
                {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
                 "window_end": "1970-01-01T00:00:10Z", "total": 6},
                {"user_id": "u2", "window_start": "1970-01-01T00:00:00Z",
                 "window_end": "1970-01-01T00:00:10Z", "total": 8},
            ],
        )

    def test_drain_frees_capacity(self):
        q = compile_query(SQL_FULL, capacity=1)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 1_000, "amount": 1}), "included")
        self.assertEqual(q.push({"user_id": "u2", "event_time": 1_000, "amount": 2}), "backpressured")
        q.advance_watermark(10_000)
        self.assertEqual(len(q.drain()), 1)
        # drain 释放容量后，新聚合键可以被接收
        self.assertEqual(q.push({"user_id": "u2", "event_time": 11_000, "amount": 2}), "included")
        q.advance_watermark(20_000)
        self.assertEqual([r["user_id"] for r in q.drain()], ["u2"])

    def test_late_takes_priority_over_backpressure(self):
        q = compile_query(SQL_FULL, capacity=1)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 1})
        q.advance_watermark(10_000)
        # 容量已满且记录迟到：优先返回 late
        self.assertEqual(q.push({"user_id": "u2", "event_time": 2_000, "amount": 2}), "late")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 2_000, "amount": 2}), "late")

    def test_backpressure_does_not_change_state_or_watermark(self):
        q = compile_query(SQL_FULL, capacity=1)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        q.advance_watermark(5_000)
        self.assertEqual(q.push({"user_id": "u2", "event_time": 6_000, "amount": 9}), "backpressured")
        self.assertEqual(q.watermark, 5_000)
        # 被拒绝的记录没有进入聚合状态
        q.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q.drain()], [5])
        self.assertEqual(q.drain(), [])
        # 拒绝之后查询仍可正常处理后续记录
        self.assertEqual(q.push({"user_id": "u2", "event_time": 11_000, "amount": 9}), "included")

    def test_unbounded_query_behavior_unchanged(self):
        records = [
            {"user_id": "u%d" % i, "event_time": (i % 3) * 1_000 + 1, "amount": i}
            for i in range(50)
        ]
        results = []
        for q in (make_query(), compile_query(SQL_FULL, capacity=None)):
            for rec in records:
                self.assertEqual(q.push(rec), "included")
            q.advance_watermark(10_000)
            results.append(q.drain())
        self.assertEqual(results[0], results[1])
        self.assertEqual(len(results[0]), 50)

    def test_deterministic_with_capacity(self):
        records = [
            {"user_id": "u1", "event_time": 1_000, "amount": 1},
            {"user_id": "u2", "event_time": 2_000, "amount": 2},
            {"user_id": "u3", "event_time": 3_000, "amount": 3},
            {"user_id": "u1", "event_time": 4_000, "amount": 4},
        ]
        runs = []
        for _ in range(2):
            q = compile_query(SQL_FULL, capacity=2)
            pushes = [q.push(rec) for rec in records]
            q.advance_watermark(10_000)
            runs.append((pushes, q.drain()))
        self.assertEqual(runs[0], runs[1])
        self.assertEqual(runs[0][0], ["included", "included", "backpressured", "included"])


class ErrorHandlingTest(unittest.TestCase):
    def test_invalid_records(self):
        q = make_query()
        bad_records = [
            {"event_time": 0, "amount": 1},  # 缺 user_id
            {"user_id": "u1", "amount": 1},  # 缺 event_time
            {"user_id": "u1", "event_time": 0},  # 缺 amount
            {"user_id": "u1", "event_time": 0, "amount": 1.5},  # amount 非整数
            {"user_id": "u1", "event_time": 0, "amount": True},  # bool 不是整数
            {"user_id": "u1", "event_time": "not a time", "amount": 1},  # 时间无法解析
            {"user_id": "u1", "event_time": "1970-01-01T00:00:00", "amount": 1},  # 缺时区
            {"user_id": None, "event_time": 0, "amount": 1},  # user_id 类型不符
            {"user_id": "u1", "event_time": 1.5, "amount": 1},  # event_time 类型不符
        ]
        for rec in bad_records:
            with self.assertRaises(InvalidRecordError, msg=repr(rec)):
                q.push(rec)
        with self.assertRaises(InvalidRecordError):
            q.push("not a mapping")

    def test_watermark_regression(self):
        q = make_query()
        q.advance_watermark(10_000)
        q.advance_watermark(10_000)  # 相等不是回退
        with self.assertRaises(WatermarkRegressionError):
            q.advance_watermark(9_999)
        with self.assertRaises(WatermarkRegressionError):
            q.advance_watermark("1969-12-31T23:59:59Z")

    def test_state_usable_after_errors(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u1", "amount": 100})
        q.advance_watermark(10_000)
        with self.assertRaises(WatermarkRegressionError):
            q.advance_watermark(0)
        q.push({"user_id": "u1", "event_time": 11_000, "amount": 7})
        rows = q.drain()
        self.assertEqual([r["total"] for r in rows], [5])
        q.advance_watermark(20_000)
        self.assertEqual([r["total"] for r in q.drain()], [7])


class HopCompileTest(unittest.TestCase):
    def test_hop_query_compiles(self):
        q = compile_query(HOP_SQL)
        self.assertEqual(q.columns, ("user_id", "window_start", "window_end", "total"))
        self.assertEqual(q.window_type, "hop")
        self.assertEqual(q.size_ms, 10_000)
        self.assertEqual(q.slide_ms, 5_000)

    def test_hop_default_column_names(self):
        q = compile_query(
            "SELECT user_id, "
            "HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND), "
            "HOP_END(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND), SUM(amount) "
            "FROM orders GROUP BY user_id, "
            "HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)")
        self.assertEqual(q.columns,
                         ("user_id", "window_start", "window_end", "sum_amount"))

    def test_hop_case_insensitive_and_quoted_interval_and_reversed_group_by(self):
        q = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY hop(EVENT_TIME, interval '10' second, INTERVAL 5 SECOND), user_id")
        self.assertEqual(q.window_type, "hop")
        self.assertEqual((q.size_ms, q.slide_ms), (10_000, 5_000))

    def test_slide_equals_size_is_tumble_equivalent(self):
        hop = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 10 SECOND)")
        tumble = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        for q in (hop, tumble):
            q.push({"user_id": "u1", "event_time": 1_000, "amount": 2})
            q.push({"user_id": "u1", "event_time": 9_000, "amount": 3})
            q.advance_watermark(10_000)
        self.assertEqual(hop.drain(), tumble.drain())


class HopExecutionTest(unittest.TestCase):
    def test_record_fans_into_multiple_windows(self):
        q = compile_query(HOP_SQL_SUMS)
        # t=7000 落入窗口 [0,10) 与 [5,15)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 7_000, "amount": 4}),
                         "included")
        self.assertEqual(sorted(q._state), [(0, "u1"), (5_000, "u1")])
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "total": 4},
        ])
        # [5,15) 尚未确定
        self.assertEqual(q.drain(), [])
        q.advance_watermark(15_000)
        self.assertEqual(q.drain(), [{"user_id": "u1", "total": 4}])

    def test_windows_are_slide_aligned_left_closed_right_open(self):
        q = compile_query(HOP_SQL_SUMS)
        # t=5000 恰为边界：落入 [5,15) 但不落入 [0,10) 之前的 [0,10)? 落入
        # [0,10)（0 <= 5000）与 [5,15)；不再落入 [-5,5)（右开，5000 排除）
        self.assertEqual(q.push({"user_id": "u1", "event_time": 5_000, "amount": 1}),
                         "included")
        self.assertEqual(sorted(q._state), [(0, "u1"), (5_000, "u1")])

    def test_aggregation_across_overlapping_windows(self):
        q = compile_query(
            "SELECT user_id, HOP_START(event_time, INTERVAL 10 SECOND, "
            "INTERVAL 5 SECOND) AS window_start, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)")
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 2})  # [-5,5),[0,10)
        q.push({"user_id": "u1", "event_time": 6_000, "amount": 3})  # [0,10),[5,15)
        q.advance_watermark(15_000)
        rows = q.drain()
        totals = {r["window_start"]: r["total"] for r in rows}
        self.assertEqual(totals, {
            "1969-12-31T23:59:55Z": 2,
            "1970-01-01T00:00:00Z": 5,
            "1970-01-01T00:00:05Z": 3,
        })

    def test_output_ordering_by_start_end_user(self):
        q = compile_query(HOP_SQL)
        q.push({"user_id": "u2", "event_time": 1_000, "amount": 1})
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 2})
        q.advance_watermark(5_000)
        rows = q.drain()
        self.assertEqual(
            [(r["window_start"], r["user_id"]) for r in rows],
            [("1969-12-31T23:59:55Z", "u1"),
             ("1969-12-31T23:59:55Z", "u2")])

    def test_window_determines_when_watermark_reaches_end(self):
        q = compile_query(HOP_SQL_SUMS)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 1})
        q.advance_watermark(4_999)
        self.assertEqual(q.drain(), [])  # [-5,5) 结束于 5000
        q.advance_watermark(5_000)
        self.assertEqual([k[0] for k, _ in q._state.items()], [-5_000, 0])
        self.assertEqual(len(q.drain()), 1)

    def test_late_records_not_added_to_any_window(self):
        q = compile_query(HOP_SQL_SUMS)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})  # -5:5, 0:5
        q.push({"user_id": "u1", "event_time": 6_000, "amount": 7})  # 0:12, 5:7
        q.advance_watermark(10_000)
        # t=9000 本应进入 [0,10) 与 [5,15)，但早于水位 -> late，两窗都不变
        self.assertEqual(q.push({"user_id": "u1", "event_time": 9_000, "amount": 100}),
                         "late")
        rows = q.drain()  # 确定 -5 与 0
        self.assertEqual([r["total"] for r in rows], [5, 12])
        q.advance_watermark(15_000)
        rows = q.drain()  # [5,15) 不包含迟到的 100
        self.assertEqual([r["total"] for r in rows], [7])

    def test_non_divisible_slide(self):
        # slide 不整除 size 也按同一成员条件落窗
        q = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 4 SECOND)")
        # t=7000：4k 对齐起点 …,0,4；[0,10) 与 [4,14) 都包含 7000
        q.push({"user_id": "u1", "event_time": 7_000, "amount": 1})
        self.assertEqual(sorted(k[0] for k in q._state), [0, 4_000])


class HopBackpressureTest(unittest.TestCase):
    def test_all_new_keys_must_fit(self):
        q = compile_query(HOP_SQL_SUMS, capacity=2)
        # 一条记录扇出 2 个新键，恰好占满
        self.assertEqual(q.push({"user_id": "u1", "event_time": 7_000, "amount": 1}),
                         "included")
        # 另一用户同样扇出 2 个新键，无法全部容纳 -> 整条背压
        self.assertEqual(q.push({"user_id": "u2", "event_time": 7_000, "amount": 1}),
                         "backpressured")
        self.assertEqual(sorted(q._state), [(0, "u1"), (5_000, "u1")])

    def test_no_partial_accumulation(self):
        q = compile_query(HOP_SQL_SUMS, capacity=2)
        q.push({"user_id": "u1", "event_time": 7_000, "amount": 1})  # 占 0,5
        blocked = {"user_id": "u1", "event_time": 12_000, "amount": 9}
        # t=12000 扇出 [5,15)（已存在）与 [10,20)（新键），放不下
        self.assertEqual(q.push(blocked), "backpressured")
        # 已存在的 [5,15) 不允许部分累加
        self.assertEqual(q._state[(5_000, "u1")], 1)
        self.assertNotIn((10_000, "u1"), q._state)

    def test_existing_keys_still_accepted_when_full(self):
        q = compile_query(HOP_SQL_SUMS, capacity=2)
        q.push({"user_id": "u1", "event_time": 7_000, "amount": 1})  # 占 0,5
        # t=3000 扇出 [0,10)（存在）与 [-5,5)（新）-> 背压
        self.assertEqual(q.push({"user_id": "u1", "event_time": 3_000, "amount": 1}),
                         "backpressured")
        # t=7000 的两个目标键都已存在 -> 容量满也 included
        self.assertEqual(q.push({"user_id": "u1", "event_time": 7_000, "amount": 4}),
                         "included")
        self.assertEqual(q._state[(0, "u1")], 5)
        self.assertEqual(q._state[(5_000, "u1")], 5)

    def test_drain_frees_capacity_per_window_user_pair(self):
        # HOP size=10 slide=5，capacity=2：t=7000 扇出 [0,10)、[5,15) 两键占满
        q = compile_query(HOP_SQL_SUMS, capacity=2)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 7_000, "amount": 1}),
                         "included")
        # 新用户同位置需 2 个新键 -> 共 4 > 2，背压
        self.assertEqual(q.push({"user_id": "u2", "event_time": 7_000, "amount": 1}),
                         "backpressured")
        q.advance_watermark(10_000)
        self.assertEqual(len(q.drain()), 1)  # 仅 [0,10) 确定，[5,15) 仍占名额
        # 水位之后的事件扇出 [5,15)、[10,20) 两个新键：1 + 2 > 2 仍背压
        self.assertEqual(q.push({"user_id": "u2", "event_time": 12_000, "amount": 1}),
                         "backpressured")
        q.advance_watermark(15_000)
        self.assertEqual(len(q.drain()), 1)  # [5,15) 确定并释放
        # 容量已全部释放，新事件扇出 2 个新键可被接收
        self.assertEqual(q.push({"user_id": "u2", "event_time": 17_000, "amount": 2}),
                         "included")


if __name__ == "__main__":
    unittest.main()
