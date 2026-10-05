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

SESSION_SQL = """
    SELECT user_id,
           SESSION_START(event_time, INTERVAL 30 SECOND),
           SESSION_END(event_time, INTERVAL 30 SECOND),
           SUM(amount) AS total
    FROM orders
    GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)
"""

GAP = 30_000


def make_query(sql=SESSION_SQL, **kwargs):
    return compile_query(sql, **kwargs)


class SessionCompileTest(unittest.TestCase):
    def test_session_query_compiles(self):
        q = make_query()
        self.assertEqual(q.columns, ("user_id", "session_start", "session_end", "total"))
        self.assertEqual(q.gap_ms, GAP)

    def test_gap_ms_is_none_for_other_windows(self):
        q = compile_query(
            "SELECT user_id FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)")
        self.assertIsNone(q.gap_ms)

    def test_default_column_names(self):
        q = compile_query(
            "SELECT user_id, SESSION_START(event_time, INTERVAL 30 SECOND), "
            "SESSION_END(event_time, INTERVAL 30 SECOND), SUM(amount) "
            "FROM orders GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)")
        self.assertEqual(q.columns, ("user_id", "session_start", "session_end", "sum_amount"))

    def test_optional_columns_may_be_omitted(self):
        q = compile_query(
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)")
        q.push({"user_id": "u1", "event_time": 0, "amount": 3})
        q.advance_watermark(31_000)
        self.assertEqual(q.drain(), [{"user_id": "u1"}])

    def test_case_whitespace_alias_and_group_order_variations(self):
        sql_a = ("SELECT user_id, SUM(amount) AS s FROM orders "
                 "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)")
        sql_b = ("  select USER_ID ,\n sum ( amount )  as  s  FROM orders\n"
                 "group by SESSION( event_time, interval '30' second ), USER_ID ;")
        records = [
            {"user_id": "u1", "event_time": 1_000, "amount": 5},
            {"user_id": "u1", "event_time": 2_000, "amount": 7},
        ]
        results = []
        for sql in (sql_a, sql_b):
            q = compile_query(sql)
            for rec in records:
                q.push(rec)
            q.advance_watermark(40_000)
            results.append(q.drain())
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], [{"user_id": "u1", "s": 12}])

    def test_session_syntax_errors(self):
        bad_sql = [
            # 间隔为零 / 为负 / 非整数 / 非秒单位
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 0 SECOND)",
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL -5 SECOND)",
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL '1.5' SECOND)",
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 MINUTE)",
            # 起止参数与分组不一致
            "SELECT user_id, SESSION_START(event_time, INTERVAL 10 SECOND) FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)",
            "SELECT user_id, SESSION_END(event_time, INTERVAL 10 SECOND) FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)",
            # 窗口混用
            "SELECT user_id, SESSION_START(event_time, INTERVAL 30 SECOND) FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 30 SECOND)",
            "SELECT user_id, TUMBLE_START(event_time, INTERVAL 30 SECOND) FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)",
            "SELECT user_id, SESSION_END(event_time, INTERVAL 30 SECOND) FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 30 SECOND, INTERVAL 10 SECOND)",
            "SELECT user_id, HOP_START(event_time, INTERVAL 30 SECOND, INTERVAL 10 SECOND) "
            "FROM orders GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)",
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND), "
            "TUMBLE(event_time, INTERVAL 30 SECOND)",
            # 重复 SESSION / 多余参数
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND), "
            "SESSION(event_time, INTERVAL 30 SECOND)",
            "SELECT user_id FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND, INTERVAL 10 SECOND)",
            # 缺 user_id
            "SELECT SESSION_START(event_time, INTERVAL 30 SECOND) FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 30 SECOND)",
        ]
        for sql in bad_sql:
            with self.assertRaises(QuerySyntaxError, msg=sql):
                compile_query(sql)


class SessionPushAndDrainTest(unittest.TestCase):
    def test_gap_forms_sessions(self):
        q = make_query()
        # 相邻差 <= gap 的记录归入同一会话；差 > gap 另起会话
        self.assertEqual(q.push({"user_id": "u1", "event_time": 1_000, "amount": 5}),
                         "included")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 31_000, "amount": 7}),
                         "included")  # 差 30s，恰好等于 gap，同一会话
        self.assertEqual(q.push({"user_id": "u1", "event_time": 62_000, "amount": 9}),
                         "included")  # 差 31s > gap，新会话
        q.advance_watermark(61_000)  # 仅确定第一个会话（end = 31s + 30s = 61s，需严格大于）
        self.assertEqual(q.drain(), [])
        q.advance_watermark(61_001)
        rows = q.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "session_start": "1970-01-01T00:00:01Z",
             "session_end": "1970-01-01T00:01:01Z", "total": 12},
        ])
        q.advance_watermark(92_001)
        rows = q.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "session_start": "1970-01-01T00:01:02Z",
             "session_end": "1970-01-01T00:01:32Z", "total": 9},
        ])
        self.assertEqual(q.drain(), [])

    def test_record_bridges_two_sessions_and_counts_amount_once(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 10_000, "amount": 1})
        q.push({"user_id": "u1", "event_time": 60_000, "amount": 2})
        # 与两侧会话的时间差都不超过 gap：合并为一个会话，amount 只累加一次
        q.push({"user_id": "u1", "event_time": 40_000, "amount": 4})
        q.advance_watermark(90_001)
        rows = q.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "session_start": "1970-01-01T00:00:10Z",
             "session_end": "1970-01-01T00:01:30Z", "total": 7},
        ])

    def test_out_of_order_record_extends_session_start(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 50_000, "amount": 3})
        q.push({"user_id": "u1", "event_time": 25_000, "amount": 5})  # 向前扩展
        q.advance_watermark(80_001)
        rows = q.drain()
        self.assertEqual(rows[0]["session_start"], "1970-01-01T00:00:25Z")
        self.assertEqual(rows[0]["session_end"], "1970-01-01T00:01:20Z")
        self.assertEqual(rows[0]["total"], 8)

    def test_sessions_are_per_user(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.push({"user_id": "u2", "event_time": 1_000, "amount": 2})  # 不并入 u1 的会话
        q.advance_watermark(31_001)
        rows = q.drain()
        self.assertEqual(
            [(r["user_id"], r["total"]) for r in rows],
            [("u1", 1), ("u2", 2)])

    def test_result_ordering(self):
        q = make_query()
        q.push({"user_id": "u2", "event_time": 5_000, "amount": 1})
        q.push({"user_id": "u1", "event_time": 5_000, "amount": 2})
        q.push({"user_id": "u1", "event_time": 200_000, "amount": 3})
        q.push({"user_id": "u9", "event_time": 5_000, "amount": 4})
        q.advance_watermark(230_001)
        rows = q.drain()
        self.assertEqual(
            [(r["session_start"], r["session_end"], r["user_id"]) for r in rows],
            [("1970-01-01T00:00:05Z", "1970-01-01T00:00:35Z", "u1"),
             ("1970-01-01T00:00:05Z", "1970-01-01T00:00:35Z", "u2"),
             ("1970-01-01T00:00:05Z", "1970-01-01T00:00:35Z", "u9"),
             ("1970-01-01T00:03:20Z", "1970-01-01T00:03:50Z", "u1")])

    def test_late_records_do_not_change_state(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        q.advance_watermark(40_000)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 39_999, "amount": 100}),
                         "late")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 40_000, "amount": 2}),
                         "included")
        rows = q.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["total"], 5)
        q.advance_watermark(70_001)
        self.assertEqual([r["total"] for r in q.drain()], [2])

    def test_iso8601_event_time_and_millisecond_output(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": "1970-01-01T08:00:00.500+08:00", "amount": 4})
        q.advance_watermark("1970-01-01T00:00:31Z")
        rows = q.drain()
        self.assertEqual(rows[0]["session_start"], "1970-01-01T00:00:00.500Z")
        self.assertEqual(rows[0]["session_end"], "1970-01-01T00:00:30.500Z")

    def test_determinism(self):
        records = [
            {"user_id": "u2", "event_time": 3_000, "amount": 1},
            {"user_id": "u1", "event_time": 40_000, "amount": 2},
            {"user_id": "u1", "event_time": 2_000, "amount": 3},
            {"user_id": "u1", "event_time": 20_000, "amount": 4},
            {"user_id": "u2", "event_time": 1_000, "amount": 5},
        ]
        outputs = []
        for _ in range(2):
            q = make_query()
            for rec in records:
                q.push(rec)
            q.advance_watermark(70_001)
            outputs.append(q.drain())
        self.assertEqual(outputs[0], outputs[1])
        # u1 的三条记录连成同一会话
        u1_rows = [r for r in outputs[0] if r["user_id"] == "u1"]
        self.assertEqual(len(u1_rows), 1)
        self.assertEqual(u1_rows[0]["total"], 9)


class SessionBackpressureTest(unittest.TestCase):
    def test_new_session_rejected_when_full(self):
        q = make_query(capacity=2)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 0, "amount": 1}), "included")
        self.assertEqual(q.push({"user_id": "u2", "event_time": 0, "amount": 2}), "included")
        # 第三个新会话被背压拒绝
        self.assertEqual(q.push({"user_id": "u3", "event_time": 0, "amount": 3}),
                         "backpressured")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 100_000, "amount": 4}),
                         "backpressured")
        # 并入已有会话即使容量已满也可接收
        self.assertEqual(q.push({"user_id": "u1", "event_time": 10_000, "amount": 5}),
                         "included")
        q.advance_watermark(40_001)
        self.assertEqual(sorted(r["total"] for r in q.drain()), [2, 6])

    def test_bridging_record_reduces_key_count(self):
        q = make_query(capacity=2)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        q.push({"user_id": "u1", "event_time": 60_000, "amount": 2})
        # 容量已满，但该记录连接两个会话，合并后总数降为 1
        self.assertEqual(q.push({"user_id": "u1", "event_time": 30_000, "amount": 4}),
                         "included")
        q.advance_watermark(90_001)
        self.assertEqual([r["total"] for r in q.drain()], [7])

    def test_backpressure_does_not_change_state_or_watermark(self):
        q = make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        q.advance_watermark(5_000)
        self.assertEqual(q.push({"user_id": "u2", "event_time": 6_000, "amount": 9}),
                         "backpressured")
        self.assertEqual(q.watermark, 5_000)
        q.advance_watermark(31_001)
        self.assertEqual([r["total"] for r in q.drain()], [5])
        self.assertEqual(q.drain(), [])
        # drain 释放容量后新会话可被接收
        self.assertEqual(q.push({"user_id": "u2", "event_time": 40_000, "amount": 9}),
                         "included")

    def test_late_takes_priority_over_backpressure(self):
        q = make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 1})
        q.advance_watermark(40_000)
        self.assertEqual(q.push({"user_id": "u2", "event_time": 2_000, "amount": 2}), "late")

    def test_drain_frees_capacity(self):
        q = make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 1})
        self.assertEqual(q.push({"user_id": "u2", "event_time": 2_000, "amount": 2}),
                         "backpressured")
        q.advance_watermark(31_001)
        self.assertEqual(len(q.drain()), 1)
        self.assertEqual(q.push({"user_id": "u2", "event_time": 40_000, "amount": 2}),
                         "included")
        q.advance_watermark(70_001)
        self.assertEqual([r["user_id"] for r in q.drain()], ["u2"])


class SessionErrorHandlingTest(unittest.TestCase):
    def test_invalid_records(self):
        q = make_query()
        bad_records = [
            {"event_time": 0, "amount": 1},
            {"user_id": "u1", "amount": 1},
            {"user_id": "u1", "event_time": 0},
            {"user_id": "u1", "event_time": 0, "amount": 1.5},
            {"user_id": "u1", "event_time": 0, "amount": True},
            {"user_id": "u1", "event_time": "not a time", "amount": 1},
            {"user_id": None, "event_time": 0, "amount": 1},
        ]
        for rec in bad_records:
            with self.assertRaises(InvalidRecordError, msg=repr(rec)):
                q.push(rec)

    def test_watermark_regression(self):
        q = make_query()
        q.advance_watermark(10_000)
        with self.assertRaises(WatermarkRegressionError):
            q.advance_watermark(9_999)

    def test_capacity_validation(self):
        for bad in (True, 0, -1, 1.5, "3"):
            with self.assertRaises(QueryConfigurationError, msg=repr(bad)):
                compile_query(SESSION_SQL, capacity=bad)

    def test_state_usable_after_errors(self):
        q = make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u1", "amount": 100})
        q.advance_watermark(31_001)
        with self.assertRaises(WatermarkRegressionError):
            q.advance_watermark(0)
        self.assertEqual([r["total"] for r in q.drain()], [5])


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
        q.push({"user_id": "u1", "event_time": 10_000, "amount": 1, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 60_000, "amount": 2, "record_id": "r2"})
        q.advance_watermark(40_000)

        q2 = self.make_query()
        self.assertEqual(q2.watermark, 40_000)
        self.assertEqual(
            q2.push({"user_id": "u1", "event_time": 10_000, "amount": 1, "record_id": "r1"}),
            "duplicate")
        # 恢复后的会话仍可被新记录连接合并
        self.assertEqual(
            q2.push({"user_id": "u1", "event_time": 40_000, "amount": 4, "record_id": "r3"}),
            "included")
        q2.advance_watermark(90_001)
        rows = q2.drain()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["session_start"], "1970-01-01T00:00:10Z")
        self.assertEqual(rows[0]["session_end"], "1970-01-01T00:01:30Z")
        self.assertEqual(rows[0]["total"], 7)
        # 已 drain 的会话重启后不重复输出
        self.assertEqual(self.make_query().drain(), [])

    def test_drained_sessions_are_not_returned_after_restart(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 5, "record_id": "r1"})
        q.advance_watermark(30_001)
        self.assertEqual(len(q.drain()), 1)
        self.assertEqual(self.make_query().drain(), [])

    def test_state_file_layout(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 3, "record_id": "r1"})
        doc = self.read_state_file()
        doc = self.read_state_file()
        self.assertEqual(doc["version"], 2)
        self.assertEqual(doc["fingerprint"]["session"], {"gap_ms": GAP})
        self.assertNotIn("windows", doc)
        self.assertEqual(doc["sessions"], [["u1", 1_000, 1_000, 3, 1, 3, 3]])
        self.assertEqual(doc["seen_ids"], ["r1"])

    def test_fingerprint_distinguishes_window_kind_and_gap(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        mismatched = [
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 30 SECOND)",
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, HOP(event_time, INTERVAL 30 SECOND, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, SESSION(event_time, INTERVAL 10 SECOND)",
        ]
        for sql in mismatched:
            with self.assertRaises(StateStorageError, msg=sql):
                compile_query(sql, state_path=self.state_path)
        # 同一 SQL 语义仍可恢复
        q2 = self.make_query()
        q2.advance_watermark(30_001)
        self.assertEqual([r["total"] for r in q2.drain()], [1])

    def test_corrupted_session_state(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        doc = self.read_state_file()
        bad_docs = [
            b"not json",
            json.dumps({k: v for k, v in doc.items() if k != "sessions"}).encode(),
            json.dumps(dict(doc, sessions={})).encode(),
            json.dumps(dict(doc, sessions=[["u1", 0]])).encode(),
            json.dumps(dict(doc, sessions=[["u1", 5_000, 0, 1]])).encode(),  # start > max
            json.dumps(dict(doc, sessions=[["u1", 0, 0, 1.5]])).encode(),
            json.dumps(dict(doc, sessions=[["u1", 0, 0, 1],
                                           ["u1", 10_000, 10_000, 2]])).encode(),  # 应已合并
            json.dumps(dict(doc, sessions=[["u1", 0, 0, 1],
                                           ["u1", 0, 0, 2]])).encode(),  # 重复会话
        ]
        for content in bad_docs:
            with open(self.state_path, "wb") as handle:
                handle.write(content)
            with self.assertRaises(StateStorageError, msg=repr(content)):
                self.make_query()

    def test_backpressured_record_id_can_be_retried(self):
        q = self.make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        blocked = {"user_id": "u2", "event_time": 40_000, "amount": 2, "record_id": "r2"}
        self.assertEqual(q.push(blocked), "backpressured")
        q.advance_watermark(30_001)
        q.drain()
        self.assertEqual(q.push(blocked), "included")
        self.assertEqual(q.push(blocked), "duplicate")

    def test_late_record_id_is_deduplicated(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 1, "record_id": "r1"})
        q.advance_watermark(40_000)
        late = {"user_id": "u1", "event_time": 2_000, "amount": 2, "record_id": "r2"}
        self.assertEqual(q.push(late), "late")
        self.assertEqual(q.push(late), "duplicate")

    def test_failed_commit_rolls_back_session_merge(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        q.push({"user_id": "u1", "event_time": 60_000, "amount": 2, "record_id": "r2"})
        os.unlink(self.state_path)
        os.mkdir(self.state_path)  # 使后续提交失败
        with self.assertRaises(StateStorageError):
            q.push({"user_id": "u1", "event_time": 30_000, "amount": 4, "record_id": "r3"})
        os.rmdir(self.state_path)
        q._commit()
        # 合并被回滚：两个会话保持独立，record_id 未登记
        q.advance_watermark(90_001)
        self.assertEqual(sorted(r["total"] for r in q.drain()), [1, 2])
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 30_000, "amount": 4, "record_id": "r3"}),
            "late")

    def test_failed_drain_commit_restores_sessions(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        q.advance_watermark(30_001)
        os.unlink(self.state_path)
        os.mkdir(self.state_path)
        with self.assertRaises(StateStorageError):
            q.drain()
        os.rmdir(self.state_path)
        # 会话未被移除，恢复可写后仍能输出
        self.assertEqual([r["total"] for r in q.drain()], [1])

    def test_record_id_required_in_persistent_mode(self):
        q = self.make_query()
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u1", "event_time": 0, "amount": 1})
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": ""})
        self.assertEqual(q.push({"user_id": "u1", "event_time": 0, "amount": 1,
                                 "record_id": "r1"}), "included")

    def test_capacity_survives_restart(self):
        q = self.make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        q2 = self.make_query(capacity=1)
        self.assertEqual(q2.push({"user_id": "u2", "event_time": 40_000, "amount": 2,
                                  "record_id": "r2"}), "backpressured")
        q2.advance_watermark(30_001)
        self.assertEqual(len(q2.drain()), 1)
        self.assertEqual(q2.push({"user_id": "u2", "event_time": 40_000, "amount": 2,
                                  "record_id": "r2"}), "included")


if __name__ == "__main__":
    unittest.main()
