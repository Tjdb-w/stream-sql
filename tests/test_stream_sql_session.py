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

SESSION_SQL = """
    SELECT user_id,
           SESSION_START(event_time, INTERVAL 30 SECOND),
           SESSION_END(event_time, INTERVAL 30 SECOND),
           SUM(amount) AS total
    FROM orders
    GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)
"""


def make_query(sql=SESSION_SQL, **kwargs):
    return compile_query(sql, **kwargs)


class SessionCompileTest(unittest.TestCase):
    def test_session_query_compiles(self):
        q = make_query()
        self.assertEqual(
            q.columns, ("user_id", "session_start", "session_end", "total"))
        self.assertIsNone(q.window_ms)
        self.assertIsNone(q.slide_ms)
        self.assertEqual(q.gap_ms, 30_000)

    def test_default_column_names(self):
        q = compile_query(
            "SELECT user_id, SESSION_START(event_time, INTERVAL 10 SECOND), "
            "SESSION_END(event_time, INTERVAL 10 SECOND), SUM(amount) "
            "FROM orders GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND)")
        self.assertEqual(
            q.columns, ("user_id", "session_start", "session_end", "sum_amount"))

    def test_optional_columns_may_be_omitted(self):
        q = compile_query(
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 5 SECOND)")
        q.push({"user_id": "u1", "event_time": 0, "amount": 3})
        q.advance_watermark(5_001)
        self.assertEqual(q.drain(), [{"user_id": "u1"}])

    def test_reversed_group_by_and_quoted_interval(self):
        q = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY SESSION(event_time, INTERVAL '30' SECOND), user_id")
        self.assertEqual(q.gap_ms, 30_000)
        q.push({"user_id": "u1", "event_time": 0, "amount": 3})
        q.advance_watermark(30_001)
        self.assertEqual(q.drain(), [{"user_id": "u1", "total": 3}])

    def test_whitespace_alias_and_case_equivalence(self):
        sql_a = ("SELECT user_id, SUM(amount) AS s FROM orders "
                 "GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND)")
        sql_b = ("  select  USER_ID ,\n sum ( amount ) as s from ORDERS\n"
                 "group by session( EVENT_TIME, interval '10' second ) , user_id ;")
        records = [
            {"user_id": "u1", "event_time": 0, "amount": 5},
            {"user_id": "u1", "event_time": 2_000, "amount": 7},
        ]
        results = []
        for sql in (sql_a, sql_b):
            q = compile_query(sql)
            for rec in records:
                q.push(rec)
            q.advance_watermark(12_001)
            results.append(q.drain())
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], [{"user_id": "u1", "s": 12}])

    def test_syntax_errors(self):
        bad_sql = [
            # gap 为零 / 为负 / 非整数
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 0 SECOND)",
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL -5 SECOND)",
            # 非秒单位
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 MINUTE)",
            # 边界函数 gap 与 GROUP BY 不一致
            "SELECT user_id, SESSION_START(event_time, INTERVAL 10 SECOND) FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)",
            "SELECT user_id, SESSION_END(event_time, INTERVAL 20 SECOND) FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)",
            # SESSION 与 TUMBLE/HOP 混用
            "SELECT user_id, SESSION_START(event_time, INTERVAL 10 SECOND) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, TUMBLE_START(event_time, INTERVAL 10 SECOND) FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, HOP_END(event_time, INTERVAL 10 SECOND, INTERVAL 5 SECOND) "
            "FROM orders GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND), "
            "TUMBLE(event_time, INTERVAL 10 SECOND)",
            # 重复 SESSION
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND), "
            "SESSION(event_time, INTERVAL 10 SECOND)",
            # 缺 user_id
            "SELECT SUM(amount) AS total FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) FROM orders "
            "GROUP BY SESSION(event_time, INTERVAL 10 SECOND)",
            # 限定字段之外
            "SELECT user_id, SUM(amount) FROM orders "
            "GROUP BY user_id, SESSION(ts, INTERVAL 10 SECOND)",
            "SELECT user_id, COUNT(amount) FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND)",
        ]
        for sql in bad_sql:
            with self.assertRaises(QuerySyntaxError, msg=sql):
                compile_query(sql)


class SessionPushAndDrainTest(unittest.TestCase):
    def test_gap_merges_and_split_output(self):
        q = make_query()
        self.assertEqual(q.push({"user_id": "u1", "event_time": 0, "amount": 5}),
                         "included")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 29_000, "amount": 7}),
                         "included")  # 差 29s <= 30s，同会话
        self.assertEqual(q.push({"user_id": "u1", "event_time": 100_000, "amount": 9}),
                         "included")  # 差 71s，新会话
        self.assertEqual(q.drain(), [])  # 水位尚未推进
        q.advance_watermark(130_001)
        rows = q.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "session_start": "1970-01-01T00:00:00Z",
             "session_end": "1970-01-01T00:00:59Z", "total": 12},
            {"user_id": "u1", "session_start": "1970-01-01T00:01:40Z",
             "session_end": "1970-01-01T00:02:10Z", "total": 9},
        ])
        self.assertEqual(q.drain(), [])

    def test_adjacent_events_exactly_gap_apart_stay_in_session(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.push({"user_id": "u1", "event_time": 30_000, "amount": 2})
        q.push({"user_id": "u1", "event_time": 60_000, "amount": 4})
        q.advance_watermark(90_001)
        rows = q.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["session_start"], "1970-01-01T00:00:00Z")
        self.assertEqual(rows[0]["session_end"], "1970-01-01T00:01:30Z")
        self.assertEqual(rows[0]["total"], 7)

    def test_end_is_max_event_plus_gap_without_epoch_alignment(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 1})
        q.push({"user_id": "u1", "event_time": 2_500, "amount": 2})
        q.advance_watermark(32_501)
        rows = q.drain()
        self.assertEqual(rows[0]["session_start"], "1970-01-01T00:00:01Z")
        self.assertEqual(rows[0]["session_end"], "1970-01-01T00:00:32.500Z")

    def test_out_of_order_events_extend_existing_session(self):
        q = make_query()
        for ts, amount in ((100_000, 9), (0, 5), (20_000, 1), (29_009, 2)):
            q.push({"user_id": "u1", "event_time": ts, "amount": amount})
        q.advance_watermark(130_001)
        rows = q.drain()
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            (rows[0]["session_start"], rows[0]["session_end"], rows[0]["total"]),
            ("1970-01-01T00:00:00Z", "1970-01-01T00:00:59.009Z", 8))
        self.assertEqual(rows[1]["total"], 9)

    def test_record_bridges_sessions_on_both_sides(self):
        # [0,30000] 与 [60000,90000] 间隔 60s；t=30000 与两侧都恰差 30s，
        # 一条记录把两个会话连同自身合并为一个，amount 只计一次。
        for order in (
            (0, 60_000, 30_000),   # 先两侧后桥接
            (0, 30_000, 60_000),   # 顺序到达
            (60_000, 0, 30_000),   # 乱序
        ):
            q = make_query()
            q.push({"user_id": "u1", "event_time": order[0], "amount": 1})
            q.push({"user_id": "u1", "event_time": order[1], "amount": 2})
            q.push({"user_id": "u1", "event_time": order[2], "amount": 3})
            q.advance_watermark(90_001)
            rows = q.drain()
            self.assertEqual(len(rows), 1, msg=order)
            self.assertEqual(rows[0]["session_start"], "1970-01-01T00:00:00Z")
            self.assertEqual(rows[0]["session_end"], "1970-01-01T00:01:30Z")
            self.assertEqual(rows[0]["total"], 6, msg=order)

    def test_same_timestamp_follows_same_rules(self):
        q = make_query()
        for _ in range(3):
            q.push({"user_id": "u1", "event_time": 5_000, "amount": 2})
        q.advance_watermark(35_001)
        rows = q.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["total"], 6)
        self.assertEqual(rows[0]["session_start"], "1970-01-01T00:00:05Z")
        self.assertEqual(rows[0]["session_end"], "1970-01-01T00:00:35Z")

    def test_watermark_must_be_strictly_greater_than_end(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})  # end = 30000
        q.advance_watermark(30_000)
        self.assertEqual(q.drain(), [])  # 相等不确定
        q.advance_watermark(30_001)
        rows = q.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["total"], 5)

    def test_late_records_do_not_change_state(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})
        q.advance_watermark(30_001)
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 30_000, "amount": 100}),
            "late")
        rows = q.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["total"], 5)
        # 水位之后的非迟到记录开启新会话
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 90_000, "amount": 2}),
            "included")

    def test_result_ordering(self):
        q = make_query()
        q.push({"user_id": "u2", "event_time": 0, "amount": 1})        # [0,30000]
        q.push({"user_id": "u1", "event_time": 0, "amount": 2})        # [0,30000]
        q.push({"user_id": "u3", "event_time": 1_000, "amount": 4})    # [1000,31000]
        q.push({"user_id": "u1", "event_time": 40_000, "amount": 3})   # [40000,70000]
        q.advance_watermark(100_000)
        self.assertEqual(
            [(r["session_start"], r["user_id"]) for r in q.drain()],
            [("1970-01-01T00:00:00Z", "u1"),
             ("1970-01-01T00:00:00Z", "u2"),
             ("1970-01-01T00:00:01Z", "u3"),
             ("1970-01-01T00:00:40Z", "u1")])

    def test_iso8601_event_time_normalized_to_utc(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": "1970-01-01T08:00:05+08:00",
                "amount": 4})
        q.push({"user_id": "u1", "event_time": "1970-01-01T00:00:07Z",
                "amount": 6})
        q.advance_watermark("1970-01-01T00:00:37.001Z")
        rows = q.drain()
        self.assertEqual(rows[0]["total"], 10)
        self.assertEqual(rows[0]["session_start"], "1970-01-01T00:00:05Z")

    def test_determinism(self):
        records = [
            {"user_id": "u2", "event_time": 3_000, "amount": 1},
            {"user_id": "u1", "event_time": 120_000, "amount": 2},
            {"user_id": "u1", "event_time": 2_000, "amount": 3},
            {"user_id": "u2", "event_time": 90_000, "amount": 4},
        ]
        outputs = []
        for _ in range(2):
            q = make_query()
            for rec in records:
                q.push(rec)
            q.advance_watermark(60_001)
            first = q.drain()
            q.advance_watermark(150_000)
            outputs.append((first, q.drain()))
        self.assertEqual(outputs[0], outputs[1])


class SessionBackpressureTest(unittest.TestCase):
    def test_new_session_rejected_when_full(self):
        q = make_query(capacity=2)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 0, "amount": 1}),
                         "included")
        self.assertEqual(q.push({"user_id": "u2", "event_time": 0, "amount": 2}),
                         "included")
        # 容量按合并后的会话总数计算；新会话被拒绝
        self.assertEqual(q.push({"user_id": "u3", "event_time": 100_000, "amount": 3}),
                         "backpressured")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 200_000, "amount": 4}),
                         "backpressured")
        # 已有会话即使容量满仍可累加
        self.assertEqual(q.push({"user_id": "u1", "event_time": 10_000, "amount": 5}),
                         "included")
        q.advance_watermark(60_000)
        # 起始相同按 end 排序：u2 [0,30000) 先于 u1 [0,40000)
        self.assertEqual(
            [(r["user_id"], r["total"]) for r in q.drain()],
            [("u2", 2), ("u1", 6)])

    def test_merge_accepted_when_full_and_frees_capacity(self):
        q = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 60 SECOND)",
            capacity=2)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.push({"user_id": "u1", "event_time": 100_000, "amount": 2})
        self.assertEqual(q.push({"user_id": "u2", "event_time": 0, "amount": 3}),
                         "backpressured")
        # 桥接记录连接两个已有会话：合并后只剩 1 个会话，不占新容量
        self.assertEqual(q.push({"user_id": "u1", "event_time": 50_000, "amount": 4}),
                         "included")
        # 合并释放的名额可供新会话使用
        self.assertEqual(q.push({"user_id": "u2", "event_time": 0, "amount": 3}),
                         "included")

    def test_drain_frees_capacity(self):
        q = make_query(capacity=1)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 0, "amount": 1}),
                         "included")
        self.assertEqual(q.push({"user_id": "u2", "event_time": 100_000, "amount": 2}),
                         "backpressured")
        q.advance_watermark(30_001)
        self.assertEqual(len(q.drain()), 1)
        self.assertEqual(q.push({"user_id": "u2", "event_time": 100_000, "amount": 2}),
                         "included")

    def test_late_takes_priority_over_backpressure(self):
        q = make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.advance_watermark(30_001)
        self.assertEqual(q.push({"user_id": "u2", "event_time": 0, "amount": 2}),
                         "late")

    def test_backpressure_does_not_change_state(self):
        q = make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 5})
        q.advance_watermark(10_000)
        self.assertEqual(q.push({"user_id": "u2", "event_time": 100_000, "amount": 9}),
                         "backpressured")
        self.assertEqual(q.watermark, 10_000)
        q.advance_watermark(30_001)
        self.assertEqual([r["total"] for r in q.drain()], [5])
        # 拒绝之后查询仍可处理后续记录
        self.assertEqual(q.push({"user_id": "u2", "event_time": 100_000, "amount": 9}),
                         "included")

    def test_deterministic_with_capacity(self):
        records = [
            {"user_id": "u1", "event_time": 0, "amount": 1},
            {"user_id": "u2", "event_time": 100_000, "amount": 2},
            {"user_id": "u3", "event_time": 200_000, "amount": 3},
            {"user_id": "u1", "event_time": 10_000, "amount": 4},
        ]
        runs = []
        for _ in range(2):
            q = make_query(capacity=2)
            pushes = [q.push(rec) for rec in records]
            q.advance_watermark(60_000)
            runs.append((pushes, q.drain()))
        self.assertEqual(runs[0], runs[1])
        self.assertEqual(
            runs[0][0], ["included", "included", "backpressured", "included"])


class SessionStateTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_path = os.path.join(self._tmp.name, "query.state")

    def make_query(self, sql=SESSION_SQL, capacity=None):
        return compile_query(sql, capacity=capacity, state_path=self.state_path)

    def read_state_file(self):
        with open(self.state_path, "rb") as handle:
            return json.loads(handle.read().decode("utf-8"))

    def test_restart_recovers_sessions_watermark_and_dedup(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 5, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 20_000, "amount": 7, "record_id": "r2"})
        q.push({"user_id": "u2", "event_time": 1_000, "amount": 3, "record_id": "r3"})
        q.advance_watermark(50_001)

        q2 = self.make_query()
        self.assertEqual(q2.watermark, 50_001)
        self.assertEqual(
            q2.push({"user_id": "u1", "event_time": 0, "amount": 5, "record_id": "r1"}),
            "duplicate")
        rows = q2.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "session_start": "1970-01-01T00:00:00Z",
             "session_end": "1970-01-01T00:00:50Z", "total": 12},
            {"user_id": "u2", "session_start": "1970-01-01T00:00:01Z",
             "session_end": "1970-01-01T00:00:31Z", "total": 3},
        ])
        # 已 drain 的会话重启后不重复输出
        self.assertEqual(self.make_query().drain(), [])

    def test_state_file_records_sessions(self):
        q = self.make_query()
        doc = self.read_state_file()
        self.assertIsNone(doc["watermark_ms"])
        self.assertEqual(doc["sessions"], [])
        q.push({"user_id": "u1", "event_time": 0, "amount": 3, "record_id": "r1"})
        doc = self.read_state_file()
        self.assertEqual(doc["sessions"], [["u1", 0, 30_000, 3]])
        self.assertEqual(doc["seen_ids"], ["r1"])
        self.assertEqual(doc["fingerprint"]["session"], {"gap_ms": 30_000})
        self.assertNotIn("windows", doc)

    def test_fingerprint_distinguishes_gap_and_window_kind(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        mismatched = [
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 30 SECOND)",
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 30 SECOND, INTERVAL 10 SECOND)",
        ]
        for sql in mismatched:
            with self.assertRaises(StateStorageError, msg=sql):
                compile_query(sql, state_path=self.state_path)
        # 语义等价 SQL（列与别名一致、空白与引号不同）仍可恢复
        q2 = self.make_query(
            "  select USER_ID, "
            "session_start(EVENT_TIME, interval '30' second),"
            " session_end(event_time, interval 30 second),"
            " sum(amount) as total from orders "
            "GROUP BY SESSION(event_time, INTERVAL 30 SECOND), user_id ;")
        q2.advance_watermark(30_001)
        self.assertEqual([r["total"] for r in q2.drain()], [1])

    def test_tumble_state_file_format_unchanged(self):
        path = os.path.join(self._tmp.name, "tumble.state")
        q = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            state_path=path)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        with open(path, "rb") as handle:
            doc = json.loads(handle.read().decode("utf-8"))
        self.assertEqual(doc["windows"], [[0, "u1", 1]])
        self.assertNotIn("sessions", doc)

    def test_backpressured_record_id_can_be_retried(self):
        q = self.make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        blocked = {"user_id": "u2", "event_time": 100_000, "amount": 2,
                   "record_id": "r2"}
        self.assertEqual(q.push(blocked), "backpressured")
        q.advance_watermark(30_001)
        q.drain()
        self.assertEqual(q.push(blocked), "included")
        self.assertEqual(q.push(blocked), "duplicate")

    def test_late_record_id_is_deduplicated(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        q.advance_watermark(30_001)
        late = {"user_id": "u1", "event_time": 1_000, "amount": 2,
                "record_id": "r2"}
        self.assertEqual(q.push(late), "late")
        self.assertEqual(q.push(late), "duplicate")

    def test_failed_commit_rolls_back_session_merge(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 3, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 100_000, "amount": 9, "record_id": "r2"})
        os.unlink(self.state_path)
        os.mkdir(self.state_path)  # 使后续提交失败
        with self.assertRaises(StateStorageError):
            # t=0 并入已有会话 [0,30000]：提交失败须回滚合并并撤销 record_id
            q.push({"user_id": "u1", "event_time": 0, "amount": 5,
                    "record_id": "r3"})
        os.rmdir(self.state_path)
        q._commit()
        q.advance_watermark(130_001)
        # 失败记录未累加、record_id 未登记
        self.assertEqual([r["total"] for r in q.drain()], [3, 9])
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 0, "amount": 5,
                    "record_id": "r3"}),
            "late")

    def test_failed_commit_on_drain_restores_sessions(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 3, "record_id": "r1"})
        q.advance_watermark(30_001)
        os.unlink(self.state_path)
        os.mkdir(self.state_path)  # 使 drain 的提交失败
        with self.assertRaises(StateStorageError):
            q.drain()
        os.rmdir(self.state_path)
        # 提交失败不改结果：会话仍在，恢复可写后可重新 drain
        q._commit()
        rows = q.drain()
        self.assertEqual([r["total"] for r in rows], [3])
        self.assertEqual(self.make_query().drain(), [])

    def test_corrupted_sessions_rejected(self):
        self.make_query().push(
            {"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        base = self.read_state_file()
        bad_docs = []
        doc = json.loads(json.dumps(base))
        doc["sessions"] = [["u1", 0, 30_000]]  # 条目缺字段
        bad_docs.append(doc)
        doc = json.loads(json.dumps(base))
        doc["sessions"] = [["u1", 30_000, 0, 1]]  # end 早于 start
        bad_docs.append(doc)
        doc = json.loads(json.dumps(base))
        doc["sessions"] = [["u1", 0, 30_000, 1], ["u1", 10_000, 40_000, 1]]
        bad_docs.append(doc)  # 会话重叠
        doc = json.loads(json.dumps(base))
        doc["sessions"] = [["u1", 0, "30000", 1]]  # 类型不符
        bad_docs.append(doc)
        for bad in bad_docs:
            with open(self.state_path, "w") as handle:
                json.dump(bad, handle)
            with self.assertRaises(StateStorageError, msg=str(bad)):
                self.make_query()

    def test_invalid_records_still_rejected(self):
        q = self.make_query()
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u1", "event_time": 0, "amount": 1.5,
                    "record_id": "r1"})
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 0, "amount": 1,
                    "record_id": "r1"}),
            "included")


if __name__ == "__main__":
    unittest.main()
