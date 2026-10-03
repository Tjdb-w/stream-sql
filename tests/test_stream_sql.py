import json
import os
import tempfile
import unittest

from stream_sql import (
    compile_query,
    QuerySyntaxError,
    QueryConfigurationError,
    InvalidRecordError,
    WatermarkRegressionError,
    StateStorageError,
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


class PersistentStateTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_path = os.path.join(self._tmp.name, "state.json")

    def make_persistent(self, sql=SQL_FULL, capacity=None):
        return compile_query(sql, capacity=capacity, state_path=self.state_path)

    @staticmethod
    def rec(record_id, user_id, event_time, amount):
        return {"record_id": record_id, "user_id": user_id,
                "event_time": event_time, "amount": amount}

    def test_restart_recovers_windows_and_watermark(self):
        q = self.make_persistent()
        self.assertEqual(q.push(self.rec("r1", "u1", 1_000, 5)), "included")
        self.assertEqual(q.push(self.rec("r2", "u1", 9_999, 7)), "included")
        self.assertEqual(q.push(self.rec("r3", "u2", 11_000, 3)), "included")
        q.advance_watermark(10_000)

        reopened = self.make_persistent()
        self.assertEqual(reopened.watermark, 10_000)
        rows = reopened.drain()  # 第一个窗口恢复并可输出
        self.assertEqual(rows, [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z", "total": 12},
        ])
        self.assertEqual(reopened.drain(), [])

        # 再次重启：已 drain 的窗口不会重复返回，未确定的窗口仍在
        reopened2 = self.make_persistent()
        self.assertEqual(reopened2.drain(), [])
        reopened2.advance_watermark(20_000)
        rows = reopened2.drain()
        self.assertEqual([r["user_id"] for r in rows], ["u2"])
        self.assertEqual(rows[0]["total"], 3)
        self.assertEqual(rows[0]["window_start"], "1970-01-01T00:00:10Z")

    def test_drained_results_not_repeated_after_restart(self):
        q = self.make_persistent()
        q.push(self.rec("r1", "u1", 1_000, 5))
        q.advance_watermark(10_000)
        self.assertEqual(len(q.drain()), 1)
        reopened = self.make_persistent()
        self.assertEqual(reopened.drain(), [])

    def test_duplicate_records_are_not_counted(self):
        q = self.make_persistent()
        self.assertEqual(q.push(self.rec("r1", "u1", 1_000, 5)), "included")
        # 相同 record_id：无论事件时间如何都不再累加
        self.assertEqual(q.push(self.rec("r1", "u1", 1_000, 5)), "duplicate")
        self.assertEqual(q.push(self.rec("r1", "u2", 2_000, 99)), "duplicate")
        self.assertEqual(q.push(self.rec("r1", "u1", 11_000, 99)), "duplicate")
        q.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q.drain()], [5])

    def test_duplicate_takes_priority_over_late_and_backpressure(self):
        q = self.make_persistent(capacity=1)
        q.push(self.rec("r1", "u1", 1_000, 1))
        q.advance_watermark(10_000)
        # 容量已满且事件时间迟到，但 record_id 已处理：返回 duplicate
        self.assertEqual(q.push(self.rec("r1", "u2", 2_000, 2)), "duplicate")
        self.assertEqual(q.push(self.rec("r2", "u2", 2_000, 2)), "late")
        self.assertEqual(q.push(self.rec("r3", "u2", 12_000, 2)), "backpressured")

    def test_duplicate_detection_survives_restart(self):
        q = self.make_persistent()
        q.push(self.rec("r1", "u1", 1_000, 5))
        reopened = self.make_persistent()
        self.assertEqual(reopened.push(self.rec("r1", "u1", 1_000, 5)), "duplicate")
        reopened.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in reopened.drain()], [5])

    def test_late_and_backpressured_ids_can_be_retried(self):
        q = self.make_persistent(capacity=1)
        q.push(self.rec("r1", "u1", 1_000, 1))
        q.advance_watermark(10_000)
        # 迟到与背压的记录未被处理，其 record_id 不进入去重集合
        self.assertEqual(q.push(self.rec("r2", "u1", 2_000, 2)), "late")
        self.assertEqual(q.push(self.rec("r2", "u1", 2_000, 2)), "late")
        self.assertEqual(q.push(self.rec("r3", "u2", 12_000, 3)), "backpressured")
        q.drain()  # 释放容量
        self.assertEqual(q.push(self.rec("r3", "u2", 12_000, 3)), "included")
        q.advance_watermark(20_000)
        self.assertEqual([r["total"] for r in q.drain()], [3])

    def test_record_id_validation(self):
        q = self.make_persistent()
        bad_records = [
            {"user_id": "u1", "event_time": 0, "amount": 1},  # 缺 record_id
            self.rec("", "u1", 0, 1),  # 空字符串
            self.rec(None, "u1", 0, 1),
            self.rec(123, "u1", 0, 1),  # 非字符串
            self.rec(True, "u1", 0, 1),
            self.rec(["r"], "u1", 0, 1),
        ]
        for rec in bad_records:
            with self.assertRaises(InvalidRecordError, msg=repr(rec)):
                q.push(rec)
        # 校验失败后查询仍可正常使用
        self.assertEqual(q.push(self.rec("r1", "u1", 0, 1)), "included")

    def test_memory_mode_ignores_record_id(self):
        q = make_query()
        # 不带 record_id 仍按原规则处理
        self.assertEqual(q.push({"user_id": "u1", "event_time": 0, "amount": 1}), "included")
        # 携带 record_id 不改变内存模式的输入判定，也不触发去重
        rec = {"record_id": "r1", "user_id": "u1", "event_time": 1_000, "amount": 2}
        self.assertEqual(q.push(rec), "included")
        self.assertEqual(q.push(dict(rec)), "included")
        q.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q.drain()], [5])

    def test_state_path_validation(self):
        for bad in ("", 0, 1.5, True, [], object()):
            with self.assertRaises(StateStorageError, msg=repr(bad)):
                compile_query(SQL_FULL, state_path=bad)
        # 父目录不存在：不隐式创建目录
        with self.assertRaises(StateStorageError):
            compile_query(SQL_FULL, state_path=os.path.join(self._tmp.name, "nope", "s.json"))
        # state_path 指向目录
        with self.assertRaises(StateStorageError):
            compile_query(SQL_FULL, state_path=self._tmp.name)
        # 省略与 None 都是内存模式
        self.assertIsNotNone(make_query())
        self.assertIsNotNone(compile_query(SQL_FULL, state_path=None))

    def test_corrupted_or_incompatible_state_file(self):
        with open(self.state_path, "wb") as handle:
            handle.write(b"not json at all")
        with self.assertRaises(StateStorageError):
            self.make_persistent()

        with open(self.state_path, "w", encoding="utf-8") as handle:
            json.dump({"version": 999, "signature": {}, "watermark_ms": None,
                       "state": [], "seen_ids": []}, handle)
        with self.assertRaises(StateStorageError):
            self.make_persistent()

        with open(self.state_path, "w", encoding="utf-8") as handle:
            json.dump({"version": 1, "signature": {}, "watermark_ms": "soon",
                       "state": [], "seen_ids": []}, handle)
        with self.assertRaises(StateStorageError):
            self.make_persistent()

    def test_sql_or_capacity_mismatch(self):
        self.make_persistent(capacity=2)
        with self.assertRaises(StateStorageError):
            self.make_persistent(capacity=3)
        with self.assertRaises(StateStorageError):
            self.make_persistent()  # 持久化时 capacity=2，现在为 None
        with self.assertRaises(StateStorageError):
            self.make_persistent(sql=(
                "SELECT user_id, SUM(amount) AS total FROM orders "
                "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)"))
        # 同一 SQL 与 capacity 可以正常恢复
        self.assertIsNotNone(self.make_persistent(capacity=2))

    def test_failed_commit_rolls_back_and_keeps_committed_state(self):
        q = self.make_persistent()
        q.push(self.rec("r1", "u1", 1_000, 5))
        q.advance_watermark(10_000)
        # 破坏 state_path（替换成目录），使后续提交失败
        os.unlink(self.state_path)
        os.mkdir(self.state_path)
        with self.assertRaises(StateStorageError):
            q.push(self.rec("r2", "u1", 12_000, 7))
        with self.assertRaises(StateStorageError):
            q.advance_watermark(20_000)
        # 内存状态已回滚：水位不变，r2 未计入，也未进入去重集合
        self.assertEqual(q.watermark, 10_000)
        # 恢复路径后提交重新可用，且没有部分更新
        os.rmdir(self.state_path)
        self.assertEqual(q.push(self.rec("r2", "u1", 12_000, 7)), "included")
        rows = q.drain()  # 水位 10_000 只能确定第一个窗口
        self.assertEqual([r["total"] for r in rows], [5])
        q.advance_watermark(20_000)
        self.assertEqual([r["total"] for r in q.drain()], [7])
        reopened = self.make_persistent()
        self.assertEqual(reopened.drain(), [])

    def test_only_state_file_is_created(self):
        q = self.make_persistent()
        q.push(self.rec("r1", "u1", 1_000, 5))
        q.advance_watermark(10_000)
        q.drain()
        self.assertEqual(os.listdir(self._tmp.name), ["state.json"])

    def test_persistent_and_memory_modes_agree(self):
        records = [
            {"user_id": "u%d" % i, "event_time": (i % 3) * 1_000 + 1, "amount": i}
            for i in range(20)
        ]
        memory = make_query()
        persistent = self.make_persistent()
        for i, rec in enumerate(records):
            expected = memory.push(rec)
            actual = persistent.push(dict(rec, record_id="r%d" % i))
            self.assertEqual(actual, expected)
        memory.advance_watermark(10_000)
        persistent.advance_watermark(10_000)
        self.assertEqual(persistent.drain(), memory.drain())


if __name__ == "__main__":
    unittest.main()