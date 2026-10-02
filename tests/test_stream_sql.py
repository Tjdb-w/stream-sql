import pytest

from stream_sql import (
    InvalidRecordError,
    QuerySyntaxError,
    WatermarkRegressionError,
    compile_query,
)

SQL = (
    "SELECT user_id, TUMBLE_START AS ws, TUMBLE_END AS we, SUM(amount) AS total "
    "FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)"
)


def make_query(sql=SQL):
    return compile_query(sql)


class TestCompile:
    def test_whitespace_and_alias_do_not_change_semantics(self):
        sql_a = "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)"
        sql_b = "  SELECT  user_id , SUM( amount ) AS s FROM orders\nGROUP BY user_id, TUMBLE( event_time, INTERVAL 10 SECOND ) ;"
        records = [
            {"user_id": "u1", "event_time": 1_000, "amount": 5},
            {"user_id": "u1", "event_time": 2_000, "amount": 7},
        ]
        results = []
        for sql in (sql_a, sql_b):
            query = compile_query(sql)
            for record in records:
                query.push(record)
            query.advance_watermark(10_000)
            results.append([tuple(row.values()) for row in query.drain()])
        assert results[0] == results[1]

    def test_group_by_order_and_case_insensitive(self):
        query = compile_query(
            "select SUM(amount), user_id from ORDERS "
            "group by TUMBLE(event_time, interval 10 second), user_id"
        )
        query.push({"user_id": "u1", "event_time": 0, "amount": 3})
        query.advance_watermark(10_000)
        assert query.drain() == [{"SUM(amount)": 3, "user_id": "u1"}]

    def test_tumble_start_end_with_args(self):
        query = compile_query(
            "SELECT TUMBLE_START(event_time, INTERVAL 10 SECOND), "
            "TUMBLE_END(event_time, INTERVAL 10 SECOND), user_id "
            "FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)"
        )
        query.push({"user_id": "u1", "event_time": 0, "amount": 1})
        query.advance_watermark(10_000)
        assert query.drain() == [
            {
                "TUMBLE_START": "1970-01-01T00:00:00Z",
                "TUMBLE_END": "1970-01-01T00:00:10Z",
                "user_id": "u1",
            }
        ]

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT event_time FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT price FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT COUNT(*) FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 0 SECOND)",
            "SELECT user_id FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL -5 SECOND)",
            "SELECT user_id FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 1.5 SECOND)",
            "SELECT user_id FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 1 MINUTE)",
            "SELECT user_id FROM orders GROUP BY user_id",
            "SELECT user_id FROM orders GROUP BY TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id FROM orders GROUP BY user_id, TUMBLE(amount, INTERVAL 10 SECOND)",
            "SELECT user_id FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND), amount",
            "SELECT user_id FROM users GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT user_id, SUM(user_id) FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "SELECT * FROM orders GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)",
            "not sql at all",
            "",
        ],
    )
    def test_syntax_errors(self, sql):
        with pytest.raises(QuerySyntaxError):
            compile_query(sql)


class TestPush:
    def test_included_and_sum(self):
        query = make_query()
        assert query.push({"user_id": "u1", "event_time": 1_000, "amount": 5}) == "included"
        assert query.push({"user_id": "u1", "event_time": 9_999, "amount": 7}) == "included"
        assert query.push({"user_id": "u2", "event_time": 5_000, "amount": 1}) == "included"
        query.advance_watermark(10_000)
        assert query.drain() == [
            {
                "user_id": "u1",
                "ws": "1970-01-01T00:00:00Z",
                "we": "1970-01-01T00:00:10Z",
                "total": 12,
            },
            {
                "user_id": "u2",
                "ws": "1970-01-01T00:00:00Z",
                "we": "1970-01-01T00:00:10Z",
                "total": 1,
            },
        ]

    def test_iso8601_event_time_normalized_to_utc(self):
        query = make_query()
        assert (
            query.push(
                {"user_id": "u1", "event_time": "1970-01-01T08:00:05+08:00", "amount": 2}
            )
            == "included"
        )
        query.advance_watermark("1970-01-01T00:00:10Z")
        assert query.drain()[0]["total"] == 2

    def test_window_left_closed_right_open(self):
        query = make_query()
        query.push({"user_id": "u1", "event_time": 10_000, "amount": 1})
        query.advance_watermark(10_000)
        assert query.drain() == []  # 10_000 属于下一个窗口
        query.advance_watermark(20_000)
        rows = query.drain()
        assert rows[0]["ws"] == "1970-01-01T00:00:10Z"

    def test_late_record_not_applied(self):
        query = make_query()
        query.push({"user_id": "u1", "event_time": 1_000, "amount": 5})
        query.advance_watermark(10_000)
        assert query.push({"user_id": "u1", "event_time": 2_000, "amount": 100}) == "late"
        assert query.push({"user_id": "u1", "event_time": 10_000, "amount": 3}) == "included"
        rows = query.drain()
        assert [row["total"] for row in rows] == [5]
        query.advance_watermark(20_000)
        assert [row["total"] for row in query.drain()] == [3]

    def test_event_time_equal_to_watermark_is_included(self):
        query = make_query()
        query.advance_watermark(10_000)
        assert query.push({"user_id": "u1", "event_time": 10_000, "amount": 1}) == "included"

    @pytest.mark.parametrize(
        "record",
        [
            {"event_time": 0, "amount": 1},
            {"user_id": "u1", "amount": 1},
            {"user_id": "u1", "event_time": 0},
            {"user_id": "u1", "event_time": 0, "amount": 1.5},
            {"user_id": "u1", "event_time": 0, "amount": "1"},
            {"user_id": "u1", "event_time": 0, "amount": True},
            {"user_id": "u1", "event_time": "not-a-time", "amount": 1},
            {"user_id": "u1", "event_time": "1970-01-01T00:00:00", "amount": 1},
            {"user_id": "u1", "event_time": 0.5, "amount": 1},
            {"user_id": None, "event_time": 0, "amount": 1},
        ],
    )
    def test_invalid_records(self, record):
        query = make_query()
        with pytest.raises(InvalidRecordError):
            query.push(record)

    def test_query_usable_after_invalid_record(self):
        query = make_query()
        with pytest.raises(InvalidRecordError):
            query.push({"user_id": "u1", "event_time": 0})
        assert query.push({"user_id": "u1", "event_time": 0, "amount": 4}) == "included"
        query.advance_watermark(10_000)
        assert query.drain()[0]["total"] == 4


class TestWatermark:
    def test_window_emitted_only_when_watermark_reaches_end(self):
        query = make_query()
        query.push({"user_id": "u1", "event_time": 0, "amount": 1})
        query.advance_watermark(9_999)
        assert query.drain() == []
        query.advance_watermark(10_000)
        assert len(query.drain()) == 1

    def test_regression_raises_and_state_unchanged(self):
        query = make_query()
        query.advance_watermark(10_000)
        with pytest.raises(WatermarkRegressionError):
            query.advance_watermark(9_999)
        query.advance_watermark(10_000)  # 相等允许
        query.push({"user_id": "u1", "event_time": 11_000, "amount": 2})
        query.advance_watermark(20_000)
        assert query.drain()[0]["total"] == 2

    def test_query_usable_after_regression_error(self):
        query = make_query()
        query.advance_watermark(10_000)
        with pytest.raises(WatermarkRegressionError):
            query.advance_watermark(5_000)
        query.push({"user_id": "u1", "event_time": 12_000, "amount": 6})
        query.advance_watermark(20_000)
        assert query.drain()[0]["total"] == 6


class TestDrain:
    def test_sorted_and_cleared(self):
        query = make_query()
        query.push({"user_id": "u2", "event_time": 11_000, "amount": 1})
        query.push({"user_id": "u1", "event_time": 21_000, "amount": 2})
        query.push({"user_id": "u1", "event_time": 1_000, "amount": 3})
        query.push({"user_id": "u2", "event_time": 2_000, "amount": 4})
        query.advance_watermark(30_000)
        rows = query.drain()
        assert [(r["ws"], r["user_id"]) for r in rows] == [
            ("1970-01-01T00:00:00Z", "u1"),
            ("1970-01-01T00:00:00Z", "u2"),
            ("1970-01-01T00:00:10Z", "u2"),
            ("1970-01-01T00:00:20Z", "u1"),
        ]
        assert query.drain() == []

    def test_determinism(self):
        def run():
            query = make_query()
            for record in [
                {"user_id": "u1", "event_time": 3_000, "amount": 1},
                {"user_id": "u2", "event_time": 1_000, "amount": 2},
                {"user_id": "u1", "event_time": 12_000, "amount": 3},
            ]:
                query.push(record)
            query.advance_watermark(15_000)
            first = query.drain()
            query.advance_watermark(25_000)
            return first, query.drain()

        assert run() == run()

    def test_only_declared_columns(self):
        query = compile_query(
            "SELECT SUM(amount) AS total FROM orders "
            "GROUP BY user_id, TUMBLE(event_time, INTERVAL 10 SECOND)"
        )
        query.push({"user_id": "u1", "event_time": 0, "amount": 9})
        query.advance_watermark(10_000)
        assert query.drain() == [{"total": 9}]
