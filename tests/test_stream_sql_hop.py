import json
import os
import tempfile
import unittest

from stream_sql import (
    compile_query,
    QuerySyntaxError,
    InvalidRecordError,
    StateStorageError,
)

HOP_SQL = """
    SELECT user_id,
           HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) AS window_start,
           HOP_END(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) AS window_end,
           SUM(amount) AS total
    FROM orders
    GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)
"""


def make_query(sql=HOP_SQL, **kwargs):
    return compile_query(sql, **kwargs)


class HopCompileTest(unittest.TestCase):
    def test_hop_query_compiles(self):
        q = make_query()
        self.assertEqual(q.columns, ("user_id", "window_start", "window_end", "total"))
        self.assertEqual(q.window_ms, 10_000)
        self.assertEqual(q.slide_ms, 5_000)

    def test_default_column_names(self):
        q = compile_query(
            "SELECT user_id, HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND), "
            "HOP_END(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND), SUM(amount) "
            "FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)")
        self.assertEqual(q.columns, ("user_id", "window_start", "window_end", "sum_amount"))

    def test_reversed_group_by_and_quoted_intervals(self):
        q = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY HOP(event_time, INTERVAL '10' SECOND, INTERVAL '5' SECOND), user_id")
        self.assertEqual(q.window_ms, 10_000)
        self.assertEqual(q.slide_ms, 5_000)

    def test_slide_equal_to_size_is_allowed(self):
        q = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 10 SECOND)")
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 3})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [{"user_id": "u1", "total": 3}])

    def test_hop_syntax_errors(self):
        bad_sql = [
            # slide 大于 size
            "SELECT user_id FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 20 SECOND)",
            # 缺 slide
            "SELECT user_id FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND)",
            # 间隔为零 / 为负
            "SELECT user_id FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 0 SECOND, INTERVAL 5 SECOND)",
            "SELECT user_id FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL -5 SECOND)",
            # 边界参数与 GROUP BY 不一致
            "SELECT user_id, HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) "
            "FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 2 SECOND)",
            "SELECT user_id, HOP_END(event_time, INTERVAL 20 SECOND, INTERVAL 5 SECOND) "
            "FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
            # 边界参数自身 slide > size
            "SELECT user_id, HOP_START(event_time, INTERVAL 5 SECOND, INTERVAL 10 SECOND) "
            "FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
            # TUMBLE 与 HOP 混用
            "SELECT user_id, TUMBLE_START(event_time, INTERVAL 10 SECOND) FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
            "SELECT user_id, HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) "
            "FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND), "
            "HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
            # 重复 HOP
            "SELECT user_id FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND), "
            "HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
        ]
        for sql in bad_sql:
            with self.assertRaises(QuerySyntaxError, msg=sql):
                compile_query(sql)


class HopPushAndDrainTest(unittest.TestCase):
    def test_record_falls_into_multiple_windows(self):
        q = make_query()
        # t=12000 落入 [5000,15000) 与 [10000,20000)；t=6000 落入 [0,10000) 与 [5000,15000)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 12_000, "amount": 4}),
                         "included")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 6_000, "amount": 3}),
                         "included")
        q.advance_watermark(10_000)
        rows = q.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z", "total": 3},
        ])
        q.advance_watermark(15_000)
        rows = q.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:05Z",
             "window_end": "1970-01-01T00:00:15Z", "total": 7},
        ])
        q.advance_watermark(20_000)
        rows = q.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:10Z",
             "window_end": "1970-01-01T00:00:20Z", "total": 4},
        ])
        self.assertEqual(q.drain(), [])

    def test_window_boundary_membership(self):
        # 窗口左闭右开：t=10000 属于 [10000,20000) 与 [5000,15000)，不属于 [0,10000)
        q = make_query()
        q.push({"user_id": "u1", "event_time": 10_000, "amount": 1})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [])
        q.advance_watermark(15_000)
        rows = q.drain()
        self.assertEqual([r["window_start"] for r in rows], ["1970-01-01T00:00:05Z"])
        q.advance_watermark(20_000)
        rows = q.drain()
        self.assertEqual([r["window_start"] for r in rows], ["1970-01-01T00:00:10Z"])

    def test_result_ordering(self):
        q = make_query()
        q.push({"user_id": "u2", "event_time": 1_000, "amount": 1})
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 2})
        q.advance_watermark(10_000)
        rows = q.drain()
        self.assertEqual(
            [(r["window_start"], r["user_id"]) for r in rows],
            [("1969-12-31T23:59:55Z", "u1"),
             ("1969-12-31T23:59:55Z", "u2"),
             ("1970-01-01T00:00:00Z", "u1"),
             ("1970-01-01T00:00:00Z", "u2")])

    def test_late_records_do_not_change_state(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        q.advance_watermark(10_000)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 9_999, "amount": 100}), "late")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 11_000, "amount": 2}), "included")
        rows = q.drain()
        self.assertEqual([r["total"] for r in rows], [5, 5])
        q.advance_watermark(20_000)
        self.assertEqual([r["total"] for r in q.drain()], [2, 2])


class HopBackpressureTest(unittest.TestCase):
    def test_each_window_user_pair_counts_one_slot(self):
        q = make_query(capacity=2)
        # t=12000 落入两个窗口，占两个名额
        self.assertEqual(q.push({"user_id": "u1", "event_time": 12_000, "amount": 1}),
                         "included")
        # 同一对聚合键可继续累加
        self.assertEqual(q.push({"user_id": "u1", "event_time": 13_000, "amount": 5}),
                         "included")
        q.advance_watermark(20_000)
        self.assertEqual(sorted(r["total"] for r in q.drain()), [6, 6])

    def test_backpressure_is_all_or_nothing(self):
        q = make_query(capacity=2)
        q.push({"user_id": "u1", "event_time": 12_000, "amount": 1})
        # t=6000 需要新键 [0,10000)：容量不足，整体拒绝
        self.assertEqual(q.push({"user_id": "u1", "event_time": 6_000, "amount": 9}),
                         "backpressured")
        q.advance_watermark(20_000)
        # 被拒绝的记录没有任何部分累加
        self.assertEqual(sorted(r["total"] for r in q.drain()), [1, 1])

    def test_drain_frees_capacity(self):
        q = make_query(capacity=2)
        q.push({"user_id": "u1", "event_time": 12_000, "amount": 1})
        self.assertEqual(q.push({"user_id": "u2", "event_time": 22_000, "amount": 2}),
                         "backpressured")
        q.advance_watermark(20_000)
        self.assertEqual(len(q.drain()), 2)
        self.assertEqual(q.push({"user_id": "u2", "event_time": 22_000, "amount": 2}),
                         "included")
        q.advance_watermark(30_000)
        self.assertEqual(sorted(r["total"] for r in q.drain()), [2, 2])


class HopStateTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_path = os.path.join(self._tmp.name, "query.state")

    def make_query(self, sql=HOP_SQL, capacity=None):
        return compile_query(sql, capacity=capacity, state_path=self.state_path)

    def test_restart_recovers_multiple_windows_watermark_and_dedup(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 12_000, "amount": 4, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 6_000, "amount": 3, "record_id": "r2"})
        q.advance_watermark(10_000)

        q2 = self.make_query()
        self.assertEqual(q2.watermark, 10_000)
        self.assertEqual(
            q2.push({"user_id": "u1", "event_time": 12_000, "amount": 4, "record_id": "r1"}),
            "duplicate")
        self.assertEqual([r["total"] for r in q2.drain()], [3])
        q2.advance_watermark(20_000)

        q3 = self.make_query()
        self.assertEqual(sorted(r["total"] for r in q3.drain()), [4, 7])
        # 已 drain 的窗口重启后不重复输出
        self.assertEqual(self.make_query().drain(), [])

    def test_fingerprint_distinguishes_window_kind_and_params(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        mismatched = [
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 2 SECOND)",
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 20 SECOND, INTERVAL 5 SECOND)",
        ]
        for sql in mismatched:
            with self.assertRaises(StateStorageError, msg=sql):
                compile_query(sql, state_path=self.state_path)
        # 同一 SQL 语义仍可恢复
        q2 = self.make_query()
        q2.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q2.drain()], [1, 1])

    def test_tumble_fingerprint_format_unchanged(self):
        path = os.path.join(self._tmp.name, "tumble.state")
        q = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            state_path=path)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        with open(path, "rb") as handle:
            doc = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(doc["fingerprint"]["window_ms"], 10_000)
        self.assertNotIn("hop", doc["fingerprint"])

    def test_backpressured_record_id_can_be_retried(self):
        q = self.make_query(capacity=2)
        q.push({"user_id": "u1", "event_time": 12_000, "amount": 1, "record_id": "r1"})
        blocked = {"user_id": "u2", "event_time": 22_000, "amount": 2, "record_id": "r2"}
        self.assertEqual(q.push(blocked), "backpressured")
        q.advance_watermark(20_000)
        q.drain()
        self.assertEqual(q.push(blocked), "included")
        self.assertEqual(q.push(blocked), "duplicate")

    def test_failed_commit_rolls_back_all_window_accumulations(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 6_000, "amount": 3, "record_id": "r1"})
        os.unlink(self.state_path)
        os.mkdir(self.state_path)  # 使后续提交失败
        with self.assertRaises(StateStorageError):
            q.push({"user_id": "u1", "event_time": 7_000, "amount": 5, "record_id": "r2"})
        os.rmdir(self.state_path)
        q._commit()
        q.advance_watermark(20_000)
        # 失败记录的多个窗口累加全部回滚，record_id 未登记
        self.assertEqual(sorted(r["total"] for r in q.drain()), [3, 3])
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 7_000, "amount": 5, "record_id": "r2"}),
            "late")

    def test_invalid_records_still_rejected(self):
        q = self.make_query()
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u1", "event_time": 0, "amount": 1.5, "record_id": "r1"})
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"}),
            "included")


if __name__ == "__main__":
    unittest.main()
