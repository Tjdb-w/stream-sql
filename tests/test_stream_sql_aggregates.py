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

TUMBLE_AGG_SQL = """
    SELECT user_id,
           TUMBLE_START(event_time, INTERVAL 10 SECOND) AS window_start,
           TUMBLE_END(event_time, INTERVAL 10 SECOND) AS window_end,
           SUM(amount), COUNT(amount), MIN(amount), MAX(amount)
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
           SESSION_START(event_time, INTERVAL 30 SECOND),
           SESSION_END(event_time, INTERVAL 30 SECOND),
           SUM(amount), COUNT(amount), MIN(amount), MAX(amount)
    FROM orders
    GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)
"""


class AggregateCompileTest(unittest.TestCase):
    def test_default_column_names_for_all_window_kinds(self):
        self.assertEqual(
            compile_query(TUMBLE_AGG_SQL).columns,
            ("user_id", "window_start", "window_end",
             "sum_amount", "count_amount", "min_amount", "max_amount"))
        self.assertEqual(
            compile_query(HOP_AGG_SQL).columns,
            ("user_id", "window_start", "window_end",
             "sum_amount", "count_amount", "min_amount", "max_amount"))
        self.assertEqual(
            compile_query(SESSION_AGG_SQL).columns,
            ("user_id", "session_start", "session_end",
             "sum_amount", "count_amount", "min_amount", "max_amount"))

    def test_aggregates_can_be_reordered(self):
        q = compile_query(
            "SELECT user_id, MAX(amount) AS hi, MIN(amount) AS lo, "
            "COUNT(amount) AS n, SUM(amount) AS s FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        self.assertEqual(q.columns, ("user_id", "hi", "lo", "n", "s"))

    def test_aggregates_can_be_omitted(self):
        q = compile_query(
            "SELECT user_id, COUNT(amount) AS n FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 7})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [{"user_id": "u1", "n": 2}])

    def test_same_aggregate_repeated_with_distinct_aliases(self):
        q = compile_query(
            "SELECT user_id, SUM(amount) AS s1, MIN(amount) AS m1, "
            "SUM(amount) AS s2, MIN(amount) AS m2 FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 7})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "s1": 12, "m1": 5, "s2": 12, "m2": 5}])

    def test_repeated_aggregate_without_alias_duplicates_default_name(self):
        with self.assertRaises(QuerySyntaxError):
            compile_query(
                "SELECT user_id, SUM(amount), SUM(amount) FROM orders "
                "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")

    def test_unknown_aggregate_is_syntax_error(self):
        for fn in ("AVG", "MEAN", "COUNT_DISTINCT"):
            with self.assertRaises(QuerySyntaxError, msg=fn):
                compile_query(
                    "SELECT user_id, %s(amount) FROM orders "
                    "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)" % fn)

    def test_aggregates_only_apply_to_amount(self):
        bad_sql = [
            "SELECT user_id, COUNT(user_id) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, MIN(event_time) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, MAX(user_id) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(ts) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, COUNT(*) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, MIN(amount FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
        ]
        for sql in bad_sql:
            with self.assertRaises(QuerySyntaxError, msg=sql):
                compile_query(sql)

    def test_user_id_still_required(self):
        with self.assertRaises(QuerySyntaxError):
            compile_query(
                "SELECT SUM(amount), COUNT(amount), MIN(amount), MAX(amount) FROM orders "
                "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")

    def test_aggregate_case_insensitive_with_whitespace(self):
        q = compile_query(
            "select USER_ID , sum ( amount ) s, count( amount ) c, "
            "min( amount ) lo, max( amount ) hi from orders "
            "group by user_id, tumble(event_time, interval 10 second) ;")
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "s": 5, "c": 1, "lo": 5, "hi": 5}])


class TumbleAggregateTest(unittest.TestCase):
    def test_four_aggregates_basic(self):
        q = compile_query(TUMBLE_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        q.push({"user_id": "u1", "event_time": 4_000, "amount": 7})
        q.push({"user_id": "u1", "event_time": 9_000, "amount": 2})
        q.push({"user_id": "u2", "event_time": 2_000, "amount": -4})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z",
             "sum_amount": 14, "count_amount": 3, "min_amount": 2, "max_amount": 7},
            {"user_id": "u2", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z",
             "sum_amount": -4, "count_amount": 1, "min_amount": -4, "max_amount": -4},
        ])

    def test_min_max_with_negative_amounts(self):
        q = compile_query(
            "SELECT user_id, MIN(amount) AS lo, MAX(amount) AS hi, "
            "SUM(amount) AS s, COUNT(amount) AS n FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        for amount in (-5, 2, -10, 0, 3):
            q.push({"user_id": "u1", "event_time": 0, "amount": amount})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "lo": -10, "hi": 3, "s": -10, "n": 5}])

    def test_late_records_do_not_change_any_aggregate(self):
        q = compile_query(TUMBLE_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        q.advance_watermark(10_000)
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 9_999, "amount": 100}), "late")
        rows = q.drain()
        self.assertEqual(
            [(r["sum_amount"], r["count_amount"], r["min_amount"], r["max_amount"])
             for r in rows],
            [(5, 1, 5, 5)])

    def test_backpressure_is_atomic_across_four_aggregates(self):
        q = compile_query(TUMBLE_AGG_SQL, capacity=1)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        # 已有键继续累加，四类聚合共同更新
        q.push({"user_id": "u1", "event_time": 2_000, "amount": 2})
        # 新键被整体背压：不产生任何部分更新
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 3_000, "amount": 9}),
            "backpressured")
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z",
             "sum_amount": 7, "count_amount": 2, "min_amount": 2, "max_amount": 5}])

    def test_query_usable_after_record_error(self):
        q = compile_query(TUMBLE_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u1", "event_time": 1_000, "amount": 1.5})
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u1", "event_time": "bad", "amount": 3})
        q.push({"user_id": "u1", "event_time": 2_000, "amount": 3})
        q.advance_watermark(10_000)
        rows = q.drain()
        self.assertEqual(
            (rows[0]["sum_amount"], rows[0]["count_amount"],
             rows[0]["min_amount"], rows[0]["max_amount"]),
            (8, 2, 3, 5))


class HopAggregateTest(unittest.TestCase):
    def test_record_contributes_four_aggregates_to_each_covering_window(self):
        q = compile_query(HOP_AGG_SQL)
        # t=12000 落入 [5000,15000) 与 [10000,20000)；t=6000 落入 [0,10000) 与 [5000,15000)
        q.push({"user_id": "u1", "event_time": 12_000, "amount": 4})
        q.push({"user_id": "u1", "event_time": 6_000, "amount": 3})
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z",
             "sum_amount": 3, "count_amount": 1, "min_amount": 3, "max_amount": 3},
        ])
        q.advance_watermark(15_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:05Z",
             "window_end": "1970-01-01T00:00:15Z",
             "sum_amount": 7, "count_amount": 2, "min_amount": 3, "max_amount": 4},
        ])
        q.advance_watermark(20_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:10Z",
             "window_end": "1970-01-01T00:00:20Z",
             "sum_amount": 4, "count_amount": 1, "min_amount": 4, "max_amount": 4},
        ])


class SessionAggregateTest(unittest.TestCase):
    def test_merge_combines_four_aggregates_and_counts_each_record_once(self):
        q = compile_query(SESSION_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 10_000, "amount": 1})
        q.push({"user_id": "u1", "event_time": 60_000, "amount": 2})
        # 连接前后两个会话：记录只贡献一次，count=3 而非 4
        q.push({"user_id": "u1", "event_time": 40_000, "amount": 4})
        q.advance_watermark(90_001)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "session_start": "1970-01-01T00:00:10Z",
             "session_end": "1970-01-01T00:01:30Z",
             "sum_amount": 7, "count_amount": 3, "min_amount": 1, "max_amount": 4},
        ])

    def test_out_of_order_merge_min_max_and_count(self):
        q = compile_query(SESSION_AGG_SQL)
        for ts, amount in ((50_000, 3), (25_000, 9), (26_000, -2)):
            q.push({"user_id": "u1", "event_time": ts, "amount": amount})
        q.advance_watermark(80_001)
        rows = q.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["session_start"], "1970-01-01T00:00:25Z")
        self.assertEqual(
            (rows[0]["sum_amount"], rows[0]["count_amount"],
             rows[0]["min_amount"], rows[0]["max_amount"]),
            (10, 3, -2, 9))

    def test_distinct_sessions_aggregate_independently(self):
        q = compile_query(SESSION_AGG_SQL)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        q.push({"user_id": "u1", "event_time": 31_000, "amount": 7})   # 同一会话
        q.push({"user_id": "u1", "event_time": 62_000, "amount": 9})   # 新会话
        q.advance_watermark(61_001)
        rows = q.drain()
        self.assertEqual(
            [(r["sum_amount"], r["count_amount"], r["min_amount"], r["max_amount"])
             for r in rows],
            [(12, 2, 5, 7)])


class AggregateStateTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_path = os.path.join(self._tmp.name, "query.state")

    def write_state(self, doc):
        with open(self.state_path, "w", encoding="utf-8") as handle:
            json.dump(doc, handle)

    def read_state(self):
        with open(self.state_path, "rb") as handle:
            return json.loads(handle.read().decode("utf-8"))

    def test_restart_recovers_four_aggregates_for_windows(self):
        q = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 2_000, "amount": 2, "record_id": "r2"})
        q.advance_watermark(5_000)

        q2 = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        self.assertEqual(q2.watermark, 5_000)
        q2.advance_watermark(10_000)
        self.assertEqual(q2.drain(), [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z",
             "sum_amount": 7, "count_amount": 2, "min_amount": 2, "max_amount": 5}])
        self.assertEqual(compile_query(
            TUMBLE_AGG_SQL, state_path=self.state_path).drain(), [])

    def test_restart_recovers_four_aggregates_for_sessions(self):
        q = compile_query(SESSION_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 10_000, "amount": 1, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 40_000, "amount": 4, "record_id": "r2"})
        q2 = compile_query(SESSION_AGG_SQL, state_path=self.state_path)
        # 恢复出的会话仍可与新记录合并，四类聚合连续
        q2.push({"user_id": "u1", "event_time": 60_000, "amount": 2, "record_id": "r3"})
        q2.advance_watermark(90_001)
        rows = q2.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            (rows[0]["sum_amount"], rows[0]["count_amount"],
             rows[0]["min_amount"], rows[0]["max_amount"]),
            (7, 3, 1, 4))

    def test_legacy_sum_only_window_state_still_recovers(self):
        self.write_state({
            "version": 1,
            "fingerprint": {
                "columns": [["user_id", "user_id"], ["sum", "sum_amount"]],
                "capacity": None,
                "window_ms": 10_000,
            },
            "watermark_ms": None,
            "seen_ids": [],
            "windows": [[0, "u1", 5]],
        })
        sql = ("SELECT user_id, SUM(amount) FROM orders "
               "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        q = compile_query(sql, state_path=self.state_path)
        # 旧仅 SUM 状态：count 视为 1，min/max 取该窗口唯一可用值 sum。
        self.assertEqual(q._state[(0, "u1")], [5, 1, 5, 5])
        # 恢复后继续摄入，四类聚合正确演进
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 3, "record_id": "r1"})
        self.assertEqual(q._state[(0, "u1")], [8, 2, 3, 5])
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [{"user_id": "u1", "sum_amount": 8}])

    def test_legacy_sum_only_session_state_still_recovers(self):
        self.write_state({
            "version": 1,
            "fingerprint": {
                "columns": [["user_id", "user_id"], ["sum", "sum_amount"]],
                "capacity": None,
                "session": {"gap_ms": 30_000},
            },
            "watermark_ms": None,
            "seen_ids": [],
            "sessions": [["u1", 1_000, 1_000, 5]],
        })
        sql = ("SELECT user_id, SUM(amount) FROM orders "
               "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)")
        q = compile_query(sql, state_path=self.state_path)
        self.assertEqual(q._sessions["u1"], [[1_000, 1_000, 5, 1, 5, 5]])
        q.push({"user_id": "u1", "event_time": 2_000, "amount": 3, "record_id": "r1"})
        self.assertEqual(q._sessions["u1"], [[1_000, 2_000, 8, 2, 3, 5]])

    def test_mismatched_query_columns_raise_state_storage_error(self):
        q = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        # 状态含四类聚合列，新查询只选 SUM：指纹不匹配
        sum_only_sql = ("SELECT user_id, SUM(amount) AS total FROM orders "
                        "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        with self.assertRaises(StateStorageError):
            compile_query(sum_only_sql, state_path=self.state_path)
        # 旧仅 SUM 状态也不能被四聚合查询读取
        self.write_state({
            "version": 1,
            "fingerprint": {
                "columns": [["user_id", "user_id"], ["sum", "sum_amount"]],
                "capacity": None,
                "window_ms": 10_000,
            },
            "watermark_ms": None,
            "seen_ids": [],
            "windows": [[0, "u1", 5]],
        })
        with self.assertRaises(StateStorageError):
            compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)

    def test_corrupted_window_aggregates_raise_state_storage_error(self):
        q = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        doc = self.read_state()
        bad_windows = [
            [[0, "u1", 1, 0, 1, 1]],          # count 为 0
            [[0, "u1", 1, -2, 1, 1]],         # count 为负
            [[0, "u1", 1, 1, 5, 3]],          # min > max
            [[0, "u1", 1.0, 1, 1, 1]],        # sum 为浮点
            [[0, "u1", 1, 1, 1, True]],       # max 为 bool
            [[0, "u1", 1, 1, 1]],             # 长度 5（新旧布局都不是）
            [[0, "u1", 1, 1, 1, 1, 1]],       # 长度 7
            [[0, "u1", "1", 1, 1, 1]],        # sum 为字符串
        ]
        for windows in bad_windows:
            self.write_state(dict(doc, windows=windows))
            with self.assertRaises(StateStorageError, msg=repr(windows)):
                compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)

    def test_corrupted_session_aggregates_raise_state_storage_error(self):
        q = compile_query(SESSION_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        doc = self.read_state()
        bad_sessions = [
            [["u1", 0, 0, 1, 0, 1, 1]],         # count 为 0
            [["u1", 0, 0, 1, 1, 9, 3]],         # min > max
            [["u1", 0, 0, 1.5, 1, 1, 1]],       # sum 为浮点
            [["u1", 0, 0, 1, 1, 1]],            # 长度 5
            [["u1", 0, 0, 1, 1, 1, 1, 1]],      # 长度 8
        ]
        for sessions in bad_sessions:
            self.write_state(dict(doc, sessions=sessions))
            with self.assertRaises(StateStorageError, msg=repr(sessions)):
                compile_query(SESSION_AGG_SQL, state_path=self.state_path)

    def test_failed_commit_rolls_back_all_four_aggregates(self):
        q = compile_query(TUMBLE_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 3, "record_id": "r1"})
        os.unlink(self.state_path)
        os.mkdir(self.state_path)  # 使后续提交失败
        with self.assertRaises(StateStorageError):
            q.push({"user_id": "u1", "event_time": 2_000, "amount": 5,
                    "record_id": "r2"})
        os.rmdir(self.state_path)
        # 四类聚合共同回滚：仍是 r1 一条记录的状态，record_id 未登记
        self.assertEqual(q._state[(0, "u1")], [3, 1, 3, 3])
        q._commit()
        q.advance_watermark(10_000)
        self.assertEqual(q.drain(), [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z",
             "sum_amount": 3, "count_amount": 1, "min_amount": 3, "max_amount": 3}])
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 2_000, "amount": 5,
                    "record_id": "r2"}),
            "late")

    def test_failed_drain_commit_restores_aggregates(self):
        q = compile_query(SESSION_AGG_SQL, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 4, "record_id": "r2"})
        q.advance_watermark(31_001)
        os.unlink(self.state_path)
        os.mkdir(self.state_path)
        with self.assertRaises(StateStorageError):
            q.drain()
        os.rmdir(self.state_path)
        # 会话未被移除，四类聚合仍在
        self.assertEqual(q._sessions["u1"], [[0, 1_000, 5, 2, 1, 4]])
        rows = q.drain()
        self.assertEqual(
            [(r["sum_amount"], r["count_amount"], r["min_amount"], r["max_amount"])
             for r in rows],
            [(5, 2, 1, 4)])

    def test_backpressured_record_id_can_be_retried_with_aggregates(self):
        q = compile_query(TUMBLE_AGG_SQL, capacity=1, state_path=self.state_path)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 1, "record_id": "r1"})
        blocked = {"user_id": "u2", "event_time": 11_000, "amount": 2, "record_id": "r2"}
        self.assertEqual(q.push(blocked), "backpressured")
        q.advance_watermark(10_000)
        q.drain()
        self.assertEqual(q.push(blocked), "included")
        self.assertEqual(q.push(blocked), "duplicate")


if __name__ == "__main__":
    unittest.main()
