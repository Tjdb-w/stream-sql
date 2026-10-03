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


class StateTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_path = os.path.join(self._tmp.name, "query.state")

    def make_query(self, sql=SQL_FULL, capacity=None):
        return compile_query(sql, capacity=capacity, state_path=self.state_path)

    def read_state_file(self):
        with open(self.state_path, "rb") as handle:
            return json.loads(handle.read().decode("utf-8"))


class PersistenceRecoveryTest(StateTestCase):
    def test_restart_recovers_watermark_and_pending_windows(self):
        q = self.make_query()
        self.assertEqual(q.push({"user_id": "u1", "event_time": 1_000, "amount": 5,
                                 "record_id": "r1"}), "included")
        self.assertEqual(q.push({"user_id": "u2", "event_time": 12_000, "amount": 7,
                                 "record_id": "r2"}), "included")
        q.advance_watermark(10_000)

        # 同一 SQL、capacity、state_path 重新构造：恢复水位与未输出窗口
        q2 = self.make_query()
        self.assertEqual(q2.watermark, 10_000)
        rows = q2.drain()
        self.assertEqual(rows, [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z", "total": 5},
        ])
        self.assertEqual(q2.drain(), [])
        # 第二个窗口仍未确定，重启后依然保留
        q3 = self.make_query()
        q3.advance_watermark(20_000)
        rows = q3.drain()
        self.assertEqual([r["total"] for r in rows], [7])
        self.assertEqual(rows[0]["window_start"], "1970-01-01T00:00:10Z")

    def test_drained_windows_are_not_returned_again_after_restart(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5, "record_id": "r1"})
        q.advance_watermark(10_000)
        self.assertEqual(len(q.drain()), 1)
        q2 = self.make_query()
        self.assertEqual(q2.drain(), [])

    def test_state_file_is_written_on_each_mutation(self):
        q = self.make_query()
        doc = self.read_state_file()
        self.assertIsNone(doc["watermark_ms"])
        self.assertEqual(doc["windows"], [])
        q.push({"user_id": "u1", "event_time": 0, "amount": 3, "record_id": "r1"})
        doc = self.read_state_file()
        self.assertEqual(doc["windows"], [[0, "u1", 3]])
        self.assertEqual(doc["seen_ids"], ["r1"])
        q.advance_watermark(10_000)
        self.assertEqual(self.read_state_file()["watermark_ms"], 10_000)
        q.drain()
        self.assertEqual(self.read_state_file()["windows"], [])

    def test_only_state_path_file_is_created(self):
        self.make_query().push(
            {"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        self.assertEqual(os.listdir(self._tmp.name), ["query.state"])

    def test_capacity_and_backpressure_survive_restart(self):
        q = self.make_query(capacity=1)
        self.assertEqual(q.push({"user_id": "u1", "event_time": 1_000, "amount": 1,
                                 "record_id": "r1"}), "included")
        q2 = self.make_query(capacity=1)
        # 容量被恢复的窗口占用，新聚合键仍被背压
        self.assertEqual(q2.push({"user_id": "u2", "event_time": 11_000, "amount": 2,
                                  "record_id": "r2"}), "backpressured")
        q2.advance_watermark(10_000)
        self.assertEqual(len(q2.drain()), 1)
        self.assertEqual(q2.push({"user_id": "u2", "event_time": 11_000, "amount": 2,
                                  "record_id": "r2"}), "included")

    def test_sql_semantics_equivalent_text_recovers(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 3, "record_id": "r1"})
        q2 = compile_query(
            "  SELECT user_id, TUMBLE_START(event_time, INTERVAL 10 SECOND) AS window_start,"
            " TUMBLE_END(event_time, INTERVAL 10 SECOND) AS window_end,"
            " SUM(amount) AS total FROM orders"
            " GROUP BY user_id, TUMBLE( event_time, INTERVAL '10' SECOND ) ;",
            state_path=self.state_path)
        q2.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q2.drain()], [3])


class DeduplicationTest(StateTestCase):
    def test_duplicate_record_id_returns_duplicate(self):
        q = self.make_query()
        record = {"user_id": "u1", "event_time": 1_000, "amount": 5, "record_id": "r1"}
        self.assertEqual(q.push(record), "included")
        self.assertEqual(q.push(record), "duplicate")
        q.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q.drain()], [5])

    def test_duplicate_survives_restart(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5, "record_id": "r1"})
        q2 = self.make_query()
        self.assertEqual(
            q2.push({"user_id": "u1", "event_time": 1_000, "amount": 5, "record_id": "r1"}),
            "duplicate")
        q2.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q2.drain()], [5])

    def test_duplicate_regardless_of_event_time_capacity_and_watermark(self):
        q = self.make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 5, "record_id": "r1"})
        q.advance_watermark(10_000)
        # 不同事件时间（会迟到）、不同 user_id、容量已满：都优先判 duplicate
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 500, "amount": 9, "record_id": "r1"}),
            "duplicate")
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 15_000, "amount": 9, "record_id": "r1"}),
            "duplicate")
        self.assertEqual(q.drain()[0]["total"], 5)

    def test_late_record_id_is_deduplicated(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 1, "record_id": "r1"})
        q.advance_watermark(10_000)
        late = {"user_id": "u1", "event_time": 2_000, "amount": 2, "record_id": "r2"}
        self.assertEqual(q.push(late), "late")
        self.assertEqual(q.push(late), "duplicate")

    def test_backpressured_record_id_can_be_retried(self):
        q = self.make_query(capacity=1)
        q.push({"user_id": "u1", "event_time": 1_000, "amount": 1, "record_id": "r1"})
        blocked = {"user_id": "u2", "event_time": 11_000, "amount": 2, "record_id": "r2"}
        self.assertEqual(q.push(blocked), "backpressured")
        q.advance_watermark(10_000)
        q.drain()
        # 被背压拒绝的记录未被处理，容量释放后同一 record_id 可以重新摄入
        self.assertEqual(q.push(blocked), "included")
        q.advance_watermark(20_000)
        self.assertEqual([r["total"] for r in q.drain()], [2])

    def test_record_id_required_in_persistent_mode(self):
        q = self.make_query()
        bad_records = [
            {"user_id": "u1", "event_time": 0, "amount": 1},  # 缺 record_id
            {"user_id": "u1", "event_time": 0, "amount": 1, "record_id": ""},  # 空字符串
            {"user_id": "u1", "event_time": 0, "amount": 1, "record_id": None},
            {"user_id": "u1", "event_time": 0, "amount": 1, "record_id": 123},
            {"user_id": "u1", "event_time": 0, "amount": 1, "record_id": True},
            {"user_id": "u1", "event_time": 0, "amount": 1, "record_id": ["r1"]},
        ]
        for rec in bad_records:
            with self.assertRaises(InvalidRecordError, msg=repr(rec)):
                q.push(rec)
        # 既有字段校验不受影响
        with self.assertRaises(InvalidRecordError):
            q.push({"user_id": "u1", "event_time": 0, "amount": 1.5, "record_id": "r1"})
        # 错误之后查询仍可正常使用
        self.assertEqual(q.push({"user_id": "u1", "event_time": 0, "amount": 1,
                                 "record_id": "r1"}), "included")

    def test_memory_mode_ignores_record_id(self):
        q = compile_query(SQL_FULL)
        record = {"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"}
        self.assertEqual(q.push(record), "included")
        # 内存模式不做去重，也不强制 record_id
        self.assertEqual(q.push(record), "included")
        self.assertEqual(q.push({"user_id": "u1", "event_time": 1, "amount": 2}), "included")
        q.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q.drain()], [4])


class StateStorageErrorTest(StateTestCase):
    def test_invalid_state_path(self):
        for bad in ("", 0, 1.5, [], {}, object()):
            with self.assertRaises(StateStorageError, msg=repr(bad)):
                compile_query(SQL_FULL, state_path=bad)

    def test_unwritable_state_path(self):
        missing_dir = os.path.join(self._tmp.name, "no-such-dir", "query.state")
        with self.assertRaises(StateStorageError):
            compile_query(SQL_FULL, state_path=missing_dir)
        with self.assertRaises(StateStorageError):
            compile_query(SQL_FULL, state_path=self._tmp.name)  # 路径是目录

    def test_corrupted_state_file(self):
        bad_contents = [
            b"",
            b"not json",
            b"[1, 2, 3]",
            json.dumps({"version": 1}).encode(),
            json.dumps({"version": 1, "fingerprint": {}, "watermark_ms": None,
                        "windows": {}, "seen_ids": []}).encode(),
            json.dumps({"version": 1, "fingerprint": {}, "watermark_ms": "now",
                        "windows": [], "seen_ids": []}).encode(),
        ]
        for content in bad_contents:
            with open(self.state_path, "wb") as handle:
                handle.write(content)
            with self.assertRaises(StateStorageError, msg=repr(content)):
                self.make_query()

    def test_incompatible_version(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        doc = self.read_state_file()
        doc["version"] = 999
        with open(self.state_path, "w") as handle:
            json.dump(doc, handle)
        with self.assertRaises(StateStorageError):
            self.make_query()

    def test_sql_or_capacity_mismatch(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        with self.assertRaises(StateStorageError):
            self.make_query(capacity=5)
        with self.assertRaises(StateStorageError):
            compile_query(
                "SELECT user_id, SUM(amount) AS total FROM orders "
                "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
                state_path=self.state_path)
        with self.assertRaises(StateStorageError):
            compile_query(
                "SELECT user_id, SUM(amount) AS total FROM orders "
                "GROUP BY user_id, TUMBLE(event_time, INTERVAL 5 SECOND)",
                state_path=self.state_path)
        # 失败不修改已提交状态：同一 SQL 仍能恢复
        q2 = self.make_query()
        q2.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q2.drain()], [1])

    def test_failed_commit_leaves_committed_state_untouched(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        q.advance_watermark(10_000)
        committed = self.read_state_file()
        # 用目录顶替状态文件，使后续提交失败
        os.unlink(self.state_path)
        os.mkdir(self.state_path)
        with self.assertRaises(StateStorageError):
            q.push({"user_id": "u2", "event_time": 11_000, "amount": 2, "record_id": "r2"})
        with self.assertRaises(StateStorageError):
            q.advance_watermark(20_000)
        with self.assertRaises(StateStorageError):
            q.drain()
        # 内存状态回滚：水位未变、聚合未变、record_id 未登记
        self.assertEqual(q.watermark, 10_000)
        os.rmdir(self.state_path)
        q._commit()  # 恢复可写后，当前（未变的）状态可重新提交
        self.assertEqual(self.read_state_file(), committed)
        self.assertEqual(q.push({"user_id": "u2", "event_time": 11_000, "amount": 2,
                                 "record_id": "r2"}), "included")

    def test_existing_exceptions_unchanged_in_persistent_mode(self):
        q = self.make_query()
        with self.assertRaises(QuerySyntaxError):
            compile_query("SELECT nope FROM orders", state_path=self.state_path + ".x")
        with self.assertRaises(QueryConfigurationError):
            compile_query(SQL_FULL, capacity=0, state_path=self.state_path + ".y")
        q.advance_watermark(10_000)
        with self.assertRaises(WatermarkRegressionError):
            q.advance_watermark(9_999)
        with self.assertRaises(InvalidRecordError):
            q.advance_watermark("not a time")
        q.push({"user_id": "u1", "event_time": 11_000, "amount": 3, "record_id": "r1"})
        q.advance_watermark(20_000)
        self.assertEqual([r["total"] for r in q.drain()], [3])


class HopPersistenceTest(StateTestCase):
    def test_multi_window_state_is_persisted_and_recovered(self):
        q = self.make_query(sql=HOP_SQL)
        # t=7000 扇出 [0,10)、[5,15)
        self.assertEqual(
            q.push({"user_id": "u1", "event_time": 7_000, "amount": 4,
                    "record_id": "r1"}),
            "included")
        doc = self.read_state_file()
        self.assertEqual(
            doc["fingerprint"],
            {"columns": [["user_id", "user_id"],
                         ["window_start", "window_start"],
                         ["window_end", "window_end"],
                         ["sum_amount", "total"]],
             "window_type": "hop", "size_ms": 10_000, "slide_ms": 5_000,
             "capacity": None})
        self.assertEqual(sorted(w[0] for w in doc["windows"]), [0, 5_000])

        q2 = self.make_query(sql=HOP_SQL)
        q2.advance_watermark(10_000)
        self.assertEqual(q2.drain(), [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:00Z",
             "window_end": "1970-01-01T00:00:10Z", "total": 4},
        ])
        # 重启后未确定的 [5,15) 仍保留，已 drain 的窗口不重复
        q3 = self.make_query(sql=HOP_SQL)
        self.assertEqual(q3.drain(), [])
        q3.advance_watermark(15_000)
        self.assertEqual(q3.drain(), [
            {"user_id": "u1", "window_start": "1970-01-01T00:00:05Z",
             "window_end": "1970-01-01T00:00:15Z", "total": 4},
        ])

    def test_semantically_equivalent_hop_text_recovers(self):
        q = self.make_query(sql=HOP_SQL_SUMS)
        q.push({"user_id": "u1", "event_time": 7_000, "amount": 3, "record_id": "r1"})
        q2 = compile_query(
            "SELECT user_id, SUM(amount) AS total FROM orders "
            "GROUP BY user_id, hop( event_time , INTERVAL '10' SECOND , "
            "INTERVAL 5 SECOND ) ;",
            state_path=self.state_path)
        q2.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q2.drain()], [3])

    def test_backpressured_multi_window_records_leave_no_trace(self):
        q = self.make_query(sql=HOP_SQL_SUMS, capacity=2)
        q.push({"user_id": "u1", "event_time": 7_000, "amount": 1, "record_id": "r1"})
        blocked = {"user_id": "u2", "event_time": 7_000, "amount": 2,
                   "record_id": "r2"}
        self.assertEqual(q.push(blocked), "backpressured")
        # 背压不登记 record_id、不落任何窗口（u1 仍占其扇出的两个窗口）
        doc = self.read_state_file()
        self.assertEqual(doc["seen_ids"], ["r1"])
        self.assertEqual(sorted((w[0], w[1]) for w in doc["windows"]),
                         [(0, "u1"), (5_000, "u1")])
        q.advance_watermark(15_000)
        q.drain()
        # 同一 record_id 可在容量释放后重新摄入（取水位之后的事件时间）
        blocked["event_time"] = 17_000
        self.assertEqual(q.push(blocked), "included")
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 17_000, "amount": 2,
                    "record_id": "r2"}),
            "duplicate")

    def test_failed_multi_window_commit_rolls_back_all_keys(self):
        q = self.make_query(sql=HOP_SQL_SUMS)
        q.push({"user_id": "u1", "event_time": 7_000, "amount": 1, "record_id": "r1"})
        q.advance_watermark(10_000)
        committed = self.read_state_file()
        os.unlink(self.state_path)
        os.mkdir(self.state_path)
        # 该记录扇出 [5,15)（新键）与 [10,20)（新键），提交须整体失败
        with self.assertRaises(StateStorageError):
            q.push({"user_id": "u2", "event_time": 12_000, "amount": 2,
                    "record_id": "r2"})
        self.assertNotIn((5_000, "u2"), q._state)
        self.assertNotIn((10_000, "u2"), q._state)
        self.assertNotIn("r2", q._seen_ids)
        os.rmdir(self.state_path)
        q._commit()
        self.assertEqual(self.read_state_file(), committed)
        self.assertEqual(
            q.push({"user_id": "u2", "event_time": 12_000, "amount": 2,
                    "record_id": "r2"}),
            "included")

    def test_fingerprint_distinguishes_tumble_size_and_slide(self):
        q = self.make_query(sql=HOP_SQL_SUMS)
        q.push({"user_id": "u1", "event_time": 7_000, "amount": 1, "record_id": "r1"})
        # HOP 状态不能用 TUMBLE 打开（即使 size 相同）
        with self.assertRaises(StateStorageError):
            compile_query(
                "SELECT user_id, SUM(amount) AS total FROM orders "
                "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
                state_path=self.state_path)
        # slide 不同
        with self.assertRaises(StateStorageError):
            self.make_query(sql=(
                "SELECT user_id, SUM(amount) AS total FROM orders "
                "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, "
                "INTERVAL 2 SECOND)"))
        # size 不同
        with self.assertRaises(StateStorageError):
            self.make_query(sql=(
                "SELECT user_id, SUM(amount) AS total FROM orders "
                "GROUP BY user_id, HOP(event_time, INTERVAL 8 SECOND, "
                "INTERVAL 5 SECOND)"))
        # 同一 HOP 仍可恢复
        q2 = self.make_query(sql=HOP_SQL_SUMS)
        q2.advance_watermark(10_000)
        self.assertEqual([r["total"] for r in q2.drain()], [1])

    def test_tumble_state_cannot_be_opened_as_hop(self):
        q = self.make_query()
        q.push({"user_id": "u1", "event_time": 0, "amount": 1, "record_id": "r1"})
        with self.assertRaises(StateStorageError):
            self.make_query(sql=(
                "SELECT user_id, SUM(amount) AS total FROM orders "
                "GROUP BY user_id, HOP(event_time, INTERVAL 10 SECOND, "
                "INTERVAL 10 SECOND)"))


if __name__ == "__main__":
    unittest.main()
