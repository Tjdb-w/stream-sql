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
            "SELECT user_id, AVG(amount) FROM orders "  # 未知聚合
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, COUNT(user_id) FROM orders "  # 聚合只能作用于 amount
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, MIN(event_time) FROM orders "  # 聚合只能作用于 amount
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, MAX(amount FROM orders "  # 聚合语法残缺
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
            "SELECT user_id, SUM(amount) FROM orders "  # 多余子句
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND) HAVING SUM(amount) > 1",
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


if __name__ == "__main__":
    unittest.main()
