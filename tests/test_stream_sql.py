import unittest

from stream_sql import (
    compile_query,
    StreamQuery,
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


class CapacityConfigurationTest(unittest.TestCase):
    def test_none_and_positive_int_are_accepted(self):
        q_default = compile_query(SQL_FULL)
        self.assertIsNone(q_default.capacity)
        q_explicit = compile_query(SQL_FULL, capacity=None)
        self.assertIsNone(q_explicit.capacity)
        q = compile_query(SQL_FULL, capacity=3)
        self.assertEqual(q.capacity, 3)
        self.assertIsInstance(q, StreamQuery)

    def test_invalid_capacity_raises_and_creates_no_query(self):
        bad_values = [0, -1, True, False, 1.5, "2", (2,), [2], 2.0, object()]
        for value in bad_values:
            with self.assertRaises(QueryConfigurationError, msg=repr(value)):
                compile_query(SQL_FULL, capacity=value)

    def test_invalid_capacity_is_rejected_even_when_sql_is_bad(self):
        # 配置不合法时不创建查询实例；SQL 与 capacity 同时非法时抛配置错误。
        with self.assertRaises(QueryConfigurationError):
            compile_query("SELECT nope FROM orders", capacity=0)


class BackpressureTest(unittest.TestCase):
    def test_new_key_is_backpressured_when_capacity_full(self):
        q = compile_query(SQL_FULL, capacity=2)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 0, "amount": 1}), "included")
        self.assertEqual(q.push({"user_id": "u2", "event_time": 0, "amount": 2}), "included")
        # 第三个聚合键（不同窗口）超出容量。
        self.assertEqual(
            q.push({"user_id": "u3", "event_time": 10_000, "amount": 4}), "backpressured")

    def test_existing_key_is_included_even_at_capacity(self):
        q = compile_query(SQL_FULL, capacity=2)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.push({"user_id": "u2", "event_time": 0, "amount": 2})
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 5_000, "amount": 10}), "included")
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 9_999, "amount": 20}), "included")
        q.advance_watermark(10_000)
        rows = q.drain()
        self.assertEqual([r["total"] for r in rows], [11, 22])

    def test_backpressured_record_changes_nothing(self):
        q = compile_query(SQL_FULL, capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 1_000, "amount": 99}), "backpressured")
        # 同键仍可累加；水位推进后被拒绝的键不出现在结果中。
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 2_000, "amount": 7}), "included")
        q.advance_watermark(10_000)
        rows = q.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["user_id"], "u1")
        self.assertEqual(rows[0]["total"], 12)

    def test_late_record_takes_precedence_over_backpressure(self):
        q = compile_query(SQL_FULL, capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.advance_watermark(10_000)
        # 容量已满且记录迟到：必须返回 late，而不是 backpressured。
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 9_999, "amount": 5}), "late")
        # 非迟到的新键仍然背压。
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 10_000, "amount": 5}), "backpressured")

    def test_advancing_watermark_does_not_release_capacity(self):
        q = compile_query(SQL_FULL, capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.advance_watermark(20_000)
        # 仅推进水位不 drain：容量仍被已确定但未输出的键占用。
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 20_000, "amount": 2}), "backpressured")
        rows = q.drain()
        self.assertEqual(len(rows), 1)
        # drain 释放容量后新键可被接收。
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 20_000, "amount": 2}), "included")

    def test_drain_releases_capacity_for_new_keys(self):
        q = compile_query(SQL_FULL, capacity=2)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.push({"user_id": "u2", "event_time": 0, "amount": 2})
        q.advance_watermark(10_000)
        q.drain()  # 两个键确定并移除，容量全部释放
        self.assertEqual(
            q.push({"user_id": "u3", "event_time": 10_000, "amount": 3}), "included")
        self.assertEqual(
            q.push({"user_id": "u4", "event_time": 10_000, "amount": 4}), "included")
        self.assertEqual(
            q.push({"user_id": "u5", "event_time": 20_000, "amount": 5}), "backpressured")
        q.advance_watermark(20_000)
        # 窗口 [10s,20s) 在水位 20s 确定；未确定的键继续保留容量。
        rows = q.drain()
        self.assertEqual([r["user_id"] for r in rows], ["u3", "u4"])
        self.assertEqual(
            q.push({"user_id": "u5", "event_time": 20_000, "amount": 5}), "included")

    def test_query_remains_usable_after_backpressure(self):
        q = compile_query(SQL_FULL, capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 0, "amount": 2}), "backpressured")
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 1_000, "amount": 9}), "included")
        q.advance_watermark(10_000)
        rows = q.drain()
        self.assertEqual([r["total"] for r in rows], [10])

    def test_errors_do_not_change_state_under_capacity(self):
        q = compile_query(SQL_FULL, capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u2", "event_time": 0, "amount": 1.5})
        with self.assertRaises(InvalidRecordError):
            q.push("not a mapping")
        q.advance_watermark(5_000)
        with self.assertRaises(WatermarkRegressionError):
            q.advance_watermark(0)
        # 异常不释放容量，也不损坏状态；新键被背压，已有键仍可累加。
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 6_000, "amount": 2}), "backpressured")
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 6_000, "amount": 4}), "included")
        q.advance_watermark(10_000)
        rows = q.drain()
        self.assertEqual([r["total"] for r in rows], [5])

    def test_unbounded_query_never_backpressures(self):
        q = compile_query(SQL_FULL)
        for i in range(100):
            self.assertEqual(
                q.push({"user_id": "u%d" % i, "event_time": 0, "amount": 1}), "included")

    def test_determinism_with_capacity(self):
        records = [
            {"user_id": "u1", "event_time": 0, "amount": 1},
            {"user_id": "u2", "event_time": 1_000, "amount": 2},
            {"user_id": "u3", "event_time": 2_000, "amount": 3},
            {"user_id": "u1", "event_time": 5_000, "amount": 10},
        ]
        outputs = []
        for _ in range(2):
            q = compile_query(SQL_FULL, capacity=2)
            replies = [q.push(rec) for rec in records]
            q.advance_watermark(10_000)
            outputs.append((replies, q.drain()))
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(outputs[0][0], ["included", "included", "backpressured", "included"])


if __name__ == "__main__":
    unittest.main()
