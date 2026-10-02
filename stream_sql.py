"""流式 SQL 计算引擎：事件时间翻滚窗口聚合（乱序水位推进、有界背压）。

公开入口：
    compile_query(sql, capacity=None) -> StreamQuery

StreamQuery:
    push(record)            -> "included" | "late" | "backpressured"
    advance_watermark(ts)   -> None
    drain()                 -> list[dict]

异常：
    QuerySyntaxError          SQL 不合法
    QueryConfigurationError   capacity 等查询配置不合法
    InvalidRecordError        记录缺字段 / 类型不符 / 时间无法解析
    WatermarkRegressionError  水位回退
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from collections.abc import Mapping

__all__ = [
    "compile_query",
    "StreamQuery",
    "QuerySyntaxError",
    "QueryConfigurationError",
    "InvalidRecordError",
    "WatermarkRegressionError",
]


class QuerySyntaxError(Exception):
    """SQL 超出受支持的语法或语义范围。"""


class QueryConfigurationError(Exception):
    """查询配置（如 capacity）不合法。"""


class InvalidRecordError(Exception):
    """记录缺少字段、字段类型不符或事件时间无法解析。"""


class WatermarkRegressionError(Exception):
    """advance_watermark 试图回退水位。"""


# ---------------------------------------------------------------------------
# 时间处理
# ---------------------------------------------------------------------------

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _parse_iso8601(text):
    """解析带时区的 ISO 8601 字符串，返回 UTC 的 Unix 毫秒。无法解析返回 None。"""
    if not isinstance(text, str):
        return None
    candidate = text.strip()
    if candidate.endswith(("Z", "z")):
        candidate = candidate[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    delta = dt.astimezone(timezone.utc) - _EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def _parse_timestamp(value, exc_factory):
    """把 Unix 毫秒整数或带时区 ISO 8601 字符串统一为 UTC 毫秒整数。"""
    if isinstance(value, bool):
        raise exc_factory("timestamp must be int milliseconds or ISO 8601 string, got bool")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        parsed = _parse_iso8601(value)
        if parsed is None:
            raise exc_factory("unparseable timestamp (need timezone-aware ISO 8601): %r" % (value,))
        return parsed
    raise exc_factory("unsupported timestamp type: %s" % type(value).__name__)


def _format_iso8601(ms):
    """把 UTC 毫秒整数格式化为 ISO 8601 UTC 字符串。"""
    dt = _EPOCH + timedelta(milliseconds=ms)
    base = "%04d-%02d-%02dT%02d:%02d:%02d" % (
        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,
    )
    remainder = ms % 1_000
    if remainder:
        return "%s.%03dZ" % (base, remainder)
    return base + "Z"


# ---------------------------------------------------------------------------
# SQL 词法分析
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(
    r"""
    \s*(?:
        (?P<number>\d+)
      | (?P<string>'(?:[^']|'')*')
      | (?P<ident>[A-Za-z_][A-Za-z0-9_]*)
      | (?P<punct>[(),;*])
      | (?P<bad>.)
    )
    """,
    re.VERBOSE | re.DOTALL,
)

_KEYWORDS = frozenset({
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "AS",
    "AND", "OR", "NOT", "TUMBLE", "TUMBLE_START", "TUMBLE_END", "SUM",
    "INTERVAL", "SECOND", "ORDERS",
})

_SOURCE_FIELDS = ("user_id", "event_time", "amount")


def _tokenize(sql):
    tokens = []
    pos = 0
    while pos < len(sql):
        if sql[pos].isspace():
            pos += 1
            continue
        match = _TOKEN_RE.match(sql, pos)
        if not match:
            raise QuerySyntaxError("cannot tokenize SQL near: %r" % sql[pos:pos + 16])
        pos = match.end()
        kind = match.lastgroup
        text = match.group(kind)
        if kind == "bad":
            raise QuerySyntaxError("unexpected character in SQL: %r" % text)
        tokens.append((kind, text))
    return tokens


# ---------------------------------------------------------------------------
# SQL 语法分析
#
# query       := SELECT select_list FROM orders GROUP BY group_list [ ';' ]
# select_list := select_item ( ',' select_item )*
# select_item := ( user_id
#                | TUMBLE_START '(' event_time ',' interval ')'
#                | TUMBLE_END   '(' event_time ',' interval ')'
#                | SUM '(' amount ')' ) [ [AS] alias ]
# group_list  := user_id ',' TUMBLE '(' event_time ',' interval ')'
#                （两项顺序可互换）
# interval    := INTERVAL ( number | 'number' ) SECOND
# ---------------------------------------------------------------------------

class _Parser:
    def __init__(self, tokens):
        self._tokens = tokens
        self._pos = 0

    def _peek(self):
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return (None, None)

    def _next(self):
        kind, text = self._peek()
        if kind is None:
            raise QuerySyntaxError("unexpected end of SQL")
        self._pos += 1
        return kind, text

    def _expect_keyword(self, keyword):
        kind, text = self._next()
        if kind != "ident" or text.upper() != keyword:
            raise QuerySyntaxError("expected %s, got %r" % (keyword, text))

    def _expect_punct(self, punct):
        kind, text = self._next()
        if kind != "punct" or text != punct:
            raise QuerySyntaxError("expected %r, got %r" % (punct, text))

    def _expect_field(self, name):
        kind, text = self._next()
        if kind != "ident" or text.lower() != name:
            raise QuerySyntaxError("expected field %s, got %r" % (name, text))

    def parse(self):
        self._expect_keyword("SELECT")
        select_items = self._parse_select_list()
        self._expect_keyword("FROM")
        self._expect_field("orders")
        self._expect_keyword("GROUP")
        self._expect_keyword("BY")
        window_ms = self._parse_group_list()
        kind, text = self._peek()
        if kind == "punct" and text == ";":
            self._next()
            kind, text = self._peek()
        if kind is not None:
            raise QuerySyntaxError("unexpected trailing SQL: %r" % text)

        for item in select_items:
            if item[1] in ("window_start", "window_end") and item[2] != window_ms:
                raise QuerySyntaxError(
                    "%s interval must match the TUMBLE window interval" % item[1].upper())

        seen_names = set()
        columns = []
        for _, col_kind, _, alias in select_items:
            name = alias if alias is not None else col_kind
            if name in seen_names:
                raise QuerySyntaxError("duplicate output column name: %r" % name)
            seen_names.add(name)
            columns.append((col_kind, name))
        if not any(kind == "user_id" for kind, _ in columns):
            raise QuerySyntaxError("SELECT list must include user_id")
        return columns, window_ms

    def _parse_select_list(self):
        items = [self._parse_select_item()]
        while self._peek() == ("punct", ","):
            self._next()
            items.append(self._parse_select_item())
        return items

    def _parse_select_item(self):
        kind, text = self._next()
        if kind != "ident":
            raise QuerySyntaxError("expected a select expression, got %r" % text)
        upper = text.upper()
        if upper == "USER_ID":
            item = ("user_id", "user_id", None)
        elif upper == "SUM":
            self._expect_punct("(")
            self._expect_field("amount")
            self._expect_punct(")")
            item = ("sum", "sum_amount", None)
        elif upper in ("TUMBLE_START", "TUMBLE_END"):
            self._expect_punct("(")
            self._expect_field("event_time")
            self._expect_punct(",")
            interval_ms = self._parse_interval()
            self._expect_punct(")")
            col_kind = "window_start" if upper == "TUMBLE_START" else "window_end"
            item = ("tumble_bound", col_kind, interval_ms)
        else:
            raise QuerySyntaxError("unsupported select expression: %r" % text)

        alias = None
        kind, text = self._peek()
        if kind == "ident" and text.upper() == "AS":
            self._next()
            alias = self._parse_alias()
        elif kind == "ident" and text.upper() not in _KEYWORDS:
            alias = self._parse_alias()
        return item + (alias,)

    def _parse_alias(self):
        kind, text = self._next()
        if kind != "ident" or text.upper() in _KEYWORDS:
            raise QuerySyntaxError("invalid column alias: %r" % text)
        return text

    def _parse_interval(self):
        self._expect_keyword("INTERVAL")
        kind, text = self._next()
        if kind == "number":
            value_text = text
        elif kind == "string":
            value_text = text[1:-1].replace("''", "'")
        else:
            raise QuerySyntaxError("expected interval length, got %r" % text)
        if not value_text.isdigit():
            raise QuerySyntaxError("window interval must be a positive number of seconds")
        seconds = int(value_text)
        self._expect_keyword("SECOND")
        if seconds <= 0:
            raise QuerySyntaxError("window interval must be a positive number of seconds")
        return seconds * 1_000

    def _parse_group_list(self):
        saw_user_id = False
        window_ms = None
        while True:
            kind, text = self._next()
            if kind != "ident":
                raise QuerySyntaxError("expected a GROUP BY item, got %r" % text)
            upper = text.upper()
            if upper == "USER_ID":
                if saw_user_id:
                    raise QuerySyntaxError("duplicate GROUP BY user_id")
                saw_user_id = True
            elif upper == "TUMBLE":
                if window_ms is not None:
                    raise QuerySyntaxError("duplicate TUMBLE in GROUP BY")
                self._expect_punct("(")
                self._expect_field("event_time")
                self._expect_punct(",")
                window_ms = self._parse_interval()
                self._expect_punct(")")
            else:
                raise QuerySyntaxError("unsupported GROUP BY expression: %r" % text)
            if self._peek() == ("punct", ","):
                self._next()
                continue
            break
        if not saw_user_id:
            raise QuerySyntaxError("GROUP BY must include user_id")
        if window_ms is None:
            raise QuerySyntaxError("GROUP BY must include TUMBLE(event_time, INTERVAL n SECOND)")
        return window_ms


# ---------------------------------------------------------------------------
# 查询执行
# ---------------------------------------------------------------------------

def _validate_user_id(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise InvalidRecordError("user_id must be a str or int, got %s" % type(value).__name__)
    return value


def _validate_amount(value):
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidRecordError("amount must be an int, got %s" % type(value).__name__)
    return value


def _user_id_sort_key(user_id):
    # 混合 int / str 的 user_id 也能得到确定性的排序。
    if isinstance(user_id, int):
        return (0, user_id, "")
    return (1, 0, user_id)


class StreamQuery:
    """由 compile_query 编译得到的可执行流式查询。"""

    def __init__(self, columns, window_ms, capacity=None):
        self._columns = tuple(columns)  # ((kind, output_name), ...)
        self._window_ms = window_ms
        self._capacity = capacity  # None 表示无界
        self._watermark_ms = None
        self._state = {}  # (window_start_ms, user_id) -> sum(amount)

    @property
    def columns(self):
        return tuple(name for _, name in self._columns)

    @property
    def window_ms(self):
        return self._window_ms

    @property
    def capacity(self):
        """最大未输出聚合键数；None 表示无界。"""
        return self._capacity

    @property
    def watermark(self):
        """当前水位（UTC 毫秒），尚未推进过时为 None。"""
        return self._watermark_ms

    def push(self, record):
        """摄入一条记录，返回 "included"、"late" 或 "backpressured"。

        迟到记录不改变聚合状态；因容量已满被背压拒绝的记录同样不改变
        聚合状态、不产生结果，也不影响水位。
        """
        if not isinstance(record, Mapping):
            raise InvalidRecordError("record must be a mapping of field name to value")
        for field in _SOURCE_FIELDS:
            if field not in record:
                raise InvalidRecordError("record is missing field %r" % field)
        user_id = _validate_user_id(record["user_id"])
        event_ms = _parse_timestamp(record["event_time"], InvalidRecordError)
        amount = _validate_amount(record["amount"])

        if self._watermark_ms is not None and event_ms < self._watermark_ms:
            return "late"
        window_start = event_ms - (event_ms % self._window_ms)
        key = (window_start, user_id)
        if key not in self._state:
            if self._capacity is not None and len(self._state) >= self._capacity:
                return "backpressured"
            self._state[key] = 0
        self._state[key] += amount
        return "included"

    def advance_watermark(self, timestamp):
        """显式推进水位；回退水位抛 WatermarkRegressionError。"""
        ms = _parse_timestamp(timestamp, InvalidRecordError)
        if self._watermark_ms is not None and ms < self._watermark_ms:
            raise WatermarkRegressionError(
                "watermark cannot regress: %r is before current watermark" % (timestamp,))
        self._watermark_ms = ms

    def drain(self):
        """返回所有已确定（窗口结束时刻不超过当前水位）的结果，并从状态中移除。"""
        if self._watermark_ms is None:
            return []
        ready = [
            (window_start, user_id)
            for (window_start, user_id) in self._state
            if window_start + self._window_ms <= self._watermark_ms
        ]
        ready.sort(key=lambda item: (
            item[0], item[0] + self._window_ms, _user_id_sort_key(item[1])))
        rows = []
        for window_start, user_id in ready:
            total = self._state.pop((window_start, user_id))
            rows.append(self._build_row(window_start, user_id, total))
        return rows

    def _build_row(self, window_start, user_id, total):
        row = {}
        for kind, name in self._columns:
            if kind == "user_id":
                row[name] = user_id
            elif kind == "window_start":
                row[name] = _format_iso8601(window_start)
            elif kind == "window_end":
                row[name] = _format_iso8601(window_start + self._window_ms)
            else:  # sum_amount
                row[name] = total
        return row


def _validate_capacity(capacity):
    """capacity 省略或为 None 表示无界；否则必须是大于零的整数。"""
    if capacity is None:
        return None
    if isinstance(capacity, bool) or not isinstance(capacity, int):
        raise QueryConfigurationError(
            "capacity must be a positive int or None, got %s" % type(capacity).__name__)
    if capacity <= 0:
        raise QueryConfigurationError(
            "capacity must be a positive int, got %d" % capacity)
    return capacity


def compile_query(sql, capacity=None):
    """编译限定语法的窗口聚合 SQL，返回 StreamQuery。

    支持的形态（关键字与标识符大小写不敏感，空白不影响语义）：

        SELECT user_id,
               [TUMBLE_START(event_time, INTERVAL n SECOND) [AS alias],]
               [TUMBLE_END(event_time, INTERVAL n SECOND) [AS alias],]
               [SUM(amount) [AS alias]]
        FROM orders
        GROUP BY user_id, TUMBLE(event_time, INTERVAL n SECOND)

    capacity 省略或为 None 时查询无界；给定正整数时表示查询可保留的
    最大未输出聚合键数（聚合键由窗口起点与 user_id 确定）。容量已满时，
    属于新聚合键的记录被背压拒绝（push 返回 "backpressured"）；
    drain() 移除已确定聚合键后释放对应容量。
    """
    capacity = _validate_capacity(capacity)
    if not isinstance(sql, str):
        raise QuerySyntaxError("sql must be a string, got %s" % type(sql).__name__)
    if not sql.strip():
        raise QuerySyntaxError("sql must not be empty")
    columns, window_ms = _Parser(_tokenize(sql)).parse()
    return StreamQuery(columns, window_ms, capacity)
