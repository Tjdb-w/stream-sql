import json
import os
import tempfile
import unittest

from stream_sql import (
    compile_query,
    QuerySyntaxError,
    StateStorageError,
)

TUMBLE_AGG_SQL = """
    SELECT user_id,
           TUMBLE_START(event_time, INTERVAL 10 SECOND) AS window_start,
           TUMBLE_END(event_time, INTERVAL 10 SECOND) AS window_end,
           SUM(amount) AS s,
           COUNT(amount) AS c,
           MIN(amount) AS lo,
           MAX(amount) AS hi
    FROM orders
    GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)
"""

HOP_AGG_SQL = """
    SELECT user_id,
           HOP_START(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) AS window_start,
           HOP_END(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) AS window_end,
           SUM(amount), COUNT(amount), MIN(amount), MAX(amount)
    FROM orders
    GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)
"""

SESSION_AGG_SQL = """
    SELECT user_id,
           SESSION_START(event_time, INTERVAL 30 SECOND) AS win_start,
           SESSION_END(event_time, INTERVAL 30 SECOND) AS win_end,
           SUM(amount), COUNT(amount), MIN(amount), MAX(amount)
    FROM orders
    GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)
"""


class AggregateCompileTest(unittest.TestCase):
    def test_all_four_aggregates_compile_with_declared_order(self):
        q = compile_query(
            "SELECT user_id, MAX(amount) hi, MIN(amount) lo, COUNT(amount) cnt, "
            "SUM(amount) s FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        self.assertEqual(q.columns, ("user_id", "hi", "lo", "cnt", "s"))

    def test_default_column_names(self):
        q = compile_query(
            "SELECT user_id, COUNT(amount), MIN(amount), MAX(amount), SUM(amount) "
            "FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        self.assertEqual(
            q.columns, ("user_id", "count_amount", "min_amount", "max_amount", "sum_amount"))

    def test_aggregates_may_be_omitted(self):
        q = compile_query(
            "SELECT user_id, MIN(amount) AS lo, MAX(amount) AS hi FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 2})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [{"user_id": "u1", "lo": 2, "hi": 5}])

    def test_same_aggregate_may_be_repeated_with_distinct_aliases(self):
        q = compile_query(
            "SELECT user_id, SUM(amount) AS s1, SUM(amount) AS s2, "
            "COUNT(amount) c1, COUNT(amount) c2 FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 7})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "s1": 12, "s2": 12, "c1": 2, "c2": 2}])

    def test_duplicate_default_name_rejected(self):
        with self.assertRaises(QuerySyntaxError):
            compile_query(
                "SELECT user_id, SUM(amount), SUM(amount) FROM orders "
                "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")

    def test_aggregate_syntax_errors(self):
        bad_sql = [
            # 未知聚合
            "SELECT user_id, AVG(amount) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            # MIN/MAX/COUNT 作用于 amount 之外的字段
            "SELECT user_id, COUNT(user_id) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, MIN(event_time) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, MAX(ts) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(event_time) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            # 缺 user_id
            "SELECT COUNT(amount) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            # HOP 窗口混用 SESSION 边界
            "SELECT user_id, COUNT(amount), SESSION_START(event_time, INTERVAL 10 SECOND) "
            "FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND)",
            # SESSION 与 TUMBLE 混用
            "SELECT user_id, MIN(amount) FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND), "
            "TUMBLE(event_time, INTERVAL 10 SECOND)",
            # 非法窗口
            "SELECT user_id, COUNT(amount) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 0 SECOND)",
        ]
        for sql in bad_sql:
            with self.assertRaises(QuerySyntaxError, msg=sql):
                compile_query(sql)


class TumbleAggregateTest(unittest.TestCase):
    def test_four_aggregates_update_together(self):
        q = compile_query(TUMBLE_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        q.push({"user_id": "u1", "event_time": 2_000, "amount": -3})
        q.push({"user_id": "u1", "event_time": 3_000, "amount": 8})
        q.push({"user_id": "u2", "event_time": 4_000, "amount": 7})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1",
             "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z",
             "s": 10, "c": 3, "lo": -3, "hi": 8},
            {"user_id": "u2",
             "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z",
             "s": 7, "c": 1, "lo": 7, "hi": 7},
        ])

    def test_single_record_window(self):
        q = compile_query(TUMBLE_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 0, "amount": 42})
        q.advance_watermark(10_000)
        (row,) = q.drain()
        self.assertEqual((row["s"], row["c"], row["lo"], row["hi"]), (42, 1, 42, 42))

    def test_empty_window_is_not_emitted(self):
        q = compile_query(TUMBLE_AGG_SQL)
        q.advance_watermark(30_000)
        self.assertEqual(q.drain(), [])

    def test_aggregates_are_scoped_per_window(self):
        q = compile_query(TUMBLE_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 10})
        q.push({"user_id": "u1", "event_time": 11_000, "amount": 2})
        q.advance_watermark(20_000)
        rows = q.drain()
        self.assertEqual(
            [(r["window_start"], r["s"], r["c"], r["lo"], r["hi"]) for r in rows],
            [("1970-01-01T00:00:00Z", 10, 1, 10, 10),
             ("1970-01-01T00:00:10Z", 2, 1, 2, 2)])


class HopAggregateTest(unittest.TestCase):
    def test_record_contributes_to_each_covering_window(self):
        q = compile_query(HOP_AGG_SQL)
        # t=6000 落入 [0,10000) 与 [5000,15000)；t=12000 落入 [5000,15000) 与 [10000,20000)
        q.push({"user_id": "u1", "event_time": 6_000, "amount": 3})
        q.push({"user_id": "u1", "event_time": 12_000, "amount": 5})
        q.advance_watermark(20_000)
        rows = q.drain()
        self.assertEqual(
            [(r["window_start"], r["sum_amount"], r["count_amount"],
              r["min_amount"], r["max_amount"]) for r in rows],
            [("1970-01-01T00:00:00Z", 3, 1, 3, 3),
             ("1970-01-01T00:00:05Z", 8, 2, 3, 5),
             ("1970-01-01T00:00:10Z", 5, 1, 5, 5)])


class SessionAggregateTest(unittest.TestCase):
    def test_merged_session_counts_each_record_once(self):
        q = compile_query(SESSION_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 10_000, "amount": 1})
        q.push({"user_id": "u1", "event_time": 60_000, "amount": 9})
        # 桥接记录：合并前后两个会话，其 amount 与计数都只贡献一次
        q.push({"user_id": "u1", "event_time": 40_000, "amount": 4})
        q.advance_watermark(90_001)
        (row,) = q.drain()
        self.assertEqual(
            (row["sum_amount"], row["count_amount"],
             row["min_amount"], row["max_amount"]),
            (14, 3, 1, 9))

    def test_separate_sessions_have_independent_aggregates(self):
        q = compile_query(SESSION_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})
        q.push({"user_id": "u1", "event_time": 100_000, "amount": 2})
        q.advance_watermark(130_001)
        rows = q.drain()
        self.assertEqual(
            [(r["sum_amount"], r["count_amount"], r["min_amount"], r["max_amount"])
             for r in rows],
            [(5, 1, 5, 5), (2, 1, 2, 2)])


class AggregateStateTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_path = os.path.join(self._tmp.name, "query.state")

    def test_restart_recovers_four_aggregates(self):
        q = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 2_000, "amount": 9, "record_id": "r2"})

        q2 = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        self.assertIsNone(q2.watermark)
        q2.push({"user_id": "u1", "event_time": 3_000, "amount": 2, "record_id": "r3"})
        q2.advance_watermark(10_000)
        (row,) = q2.drain()
        self.assertEqual((row["s"], row["c"], row["lo"], row["hi"]), (16, 3, 2, 9))
        # 已 drain 的窗口重启后不重复
        self.assertEqual(compile_query(
            TUMBLE_AGG_SQL, state_path=self.state_path).drain(), [])

    def test_state_file_layout_carries_four_values(self):
        q = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5, "record_id": "r1"})
        with open(self.state_path, "rb") as handle:
            doc = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(doc["version"], 2)
        self.assertEqual(doc["windows"], [[0, "u1", 5, 1, 5, 5]])

    def test_mismatched_aggregate_set_is_rejected(self):
        q = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        # 新增/缺省聚合导致指纹不一致
        sum_only = (
            "SELECT user_id, SUM(amount) AS s FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        with self.assertRaises(StateStorageError):
            compile_query(sum_only, state_path=self.state_path)
        no_count = (
            "SELECT user_id, SUM(amount) AS s, MIN(amount) AS lo, MAX(amount) AS hi "
            "FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        with self.assertRaises(StateStorageError):
            compile_query(no_count, state_path=self.state_path)

    def test_legacy_sum_only_state_recovers(self):
        # 手工构造旧版（version=1）仅 SUM 状态
        legacy_doc = {
            "version": 1,
            "fingerprint": {
                "columns": [["user_id", "user_id"], ["sum_amount", "total"]],
                "capacity": None,
                "window_ms": 10_000,
            },
            "watermark_ms": None,
            "seen_ids": ["r1"],
            "windows": [[0, "u1", 5]],
        }
        with open(self.state_path, "w") as handle:
            json.dump(legacy_doc, handle)
        sum_sql = (
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        q = compile_query(sum_sql, state_path=self.state_path)
        # 旧记录已去重
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 1_000, "amount": 5, "record_id": "r1"}),
            "duplicate")
        # 新记录正常并入；旧窗口仍是仅 SUM 形态，输出 total
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 2_000, "amount": 7, "record_id": "r2"}),
            "included")
        # 提交后的状态保持短表（COUNT/MIN/MAX 历史不可重建）
        with open(self.state_path, "rb") as handle:
            doc = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(doc["version"], 2)
        self.assertEqual(doc["windows"], [[0, "u1", 12]])
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [{"user_id": "u1", "total": 12}])

    def test_legacy_state_rejected_for_non_sum_query(self):
        legacy_doc = {
            "version": 1,
            "fingerprint": {
                "columns": [["user_id", "user_id"], ["sum_amount", "total"]],
                "capacity": None,
                "window_ms": 10_000,
            },
            "watermark_ms": None,
            "seen_ids": [],
            "windows": [],
        }
        with open(self.state_path, "w") as handle:
            json.dump(legacy_doc, handle)
        with self.assertRaises(StateStorageError):
            compile_query(
                "SELECT user_id, SUM(amount) AS total, COUNT(amount) AS c FROM orders "
                "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
                state_path=self.state_path)

    def test_corrupted_aggregate_payload_rejected(self):
        q = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        with open(self.state_path, "rb") as handle:
            doc = json.loads(handle.read().decode("utf-8"))
        bad_docs = [
            dict(doc, windows=[[0, "u1", 1, 0, 1, 1]]),       # count 为 0
            dict(doc, windows=[[0, "u1", 1, 1, 2, 1]]),       # min > max
            dict(doc, windows=[[0, "u1", 1, -1, 1, 1]]),      # count 为负
            dict(doc, windows=[[0, "u1", 1, 1.0, 1, 1]]),     # count 非整数
            dict(doc, windows=[[0, "u1", 1, 1]]),             # 长度非法
            dict(doc, windows=[[0, "u1", 1, 1, 1, 1, 0]]),    # 长度非法
            dict(doc, windows=[[0, "u1", 1]]),  # v2 短表与四聚合查询不匹配
        ]
        for bad in bad_docs:
            with open(self.state_path, "w") as handle:
                json.dump(bad, handle)
            with self.assertRaises(StateStorageError, msg=repr(bad)):
                compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)

    def test_failed_commit_rolls_back_all_four_aggregates(self):
        q = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 3, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 2_000, "amount": 9, "record_id": "r2"})
        os.unlink(self.state_path)
        os.mkdir(self.state_path)
        with self.assertRaises(StateStorageError):
            q.push({"user_id": "u1", "event_time": 3_000, "amount": 1, "record_id": "r3"})
        os.rmdir(self.state_path)
        q.advance_watermark(10_000)
        (row,) = q.drain()
        # 回滚后四类聚合全部恢复到提交前
        self.assertEqual((row["s"], row["c"], row["lo"], row["hi"]), (12, 2, 3, 9))
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 3_000, "amount": 1, "record_id": "r3"}),
            "late")


if __name__ == "__main__":
    unittest.main()
