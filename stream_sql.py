"""流式 SQL 计算引擎：事件时间翻滚/滑动/会话窗口聚合（乱序水位推进、有界背压、Exactly-once 状态）。

公开入口：
    compile_query(sql, capacity=None, state_path=None) -> StreamQuery

StreamQuery:
    push(record)            -> "included" | "late" | "backpressured"（持久模式还可能是 "duplicate"）
    advance_watermark(ts)   -> None
    drain()                 -> list[dict]

异常：
    QuerySyntaxError          SQL 不合法
    QueryConfigurationError   capacity 等查询配置不合法
    InvalidRecordError        记录缺字段 / 类型不符 / 时间无法解析
    WatermarkRegressionError  水位回退
    StateStorageError         状态文件不可写 / 损坏 / 版本不兼容 / 与 SQL、capacity 不一致
"""

from __future__ import annotations

import json
import os
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
    "StateStorageError",
]


class QuerySyntaxError(Exception):
    """SQL 超出受支持的语法或语义范围。"""


class QueryConfigurationError(Exception):
    """查询配置（如 capacity）不合法。"""


class InvalidRecordError(Exception):
    """记录缺少字段、字段类型不符或事件时间无法解析。"""


class WatermarkRegressionError(Exception):
    """advance_watermark 试图回退水位。"""


class StateStorageError(Exception):
    """状态文件不可写、损坏、版本不兼容或与 SQL、capacity 不一致。"""


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
    "AND", "OR", "NOT", "TUMBLE", "TUMBLE_START", "TUMBLE_END",
    "HOP", "HOP_START", "HOP_END",
    "SUM", "COUNT", "MIN", "MAX",
    "SESSION", "SESSION_START", "SESSION_END",
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
#                | HOP_START '(' event_time ',' interval ',' interval ')'
#                | HOP_END   '(' event_time ',' interval ',' interval ')'
#                | SESSION_START '(' event_time ',' interval ')'
#                | SESSION_END   '(' event_time ',' interval ')'
#                | agg '(' amount ')' ) [ [AS] alias ]
# agg         := SUM | COUNT | MIN | MAX
# group_list  := user_id ',' window_fn （两项顺序可互换）
# window_fn   := TUMBLE '(' event_time ',' interval ')'
#              | HOP '(' event_time ',' interval ',' interval ')'
#              | SESSION '(' event_time ',' interval ')'
# interval    := INTERVAL ( number | 'number' ) SECOND
#
# 聚合列可任意排列、省略或使用 AS 别名；同一聚合可按不同别名重复输出。
# 缺省列名依次为 sum_amount、count_amount、min_amount、max_amount。
# 四类聚合（SUM/COUNT/MIN/MAX）对每条记录共同原子更新，与是否出现在
# SELECT 列表无关；SELECT 只决定输出哪些列。
# ---------------------------------------------------------------------------

def _validate_hop_params(size_ms, slide_ms):
    """HOP 的 size、slide 均为正整数秒（已换算为毫秒），且 slide 不大于 size。"""
    if slide_ms > size_ms:
        raise QuerySyntaxError("HOP slide must not exceed size")


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
        window = self._parse_group_list()
        kind, text = self._peek()
        if kind == "punct" and text == ";":
            self._next()
            kind, text = self._peek()
        if kind is not None:
            raise QuerySyntaxError("unexpected trailing SQL: %r" % text)

        window_kind, size_ms, slide_ms = window
        for item in select_items:
            if item[0] == "tumble_bound" and (
                    window_kind != "tumble" or item[2] != size_ms):
                raise QuerySyntaxError(
                    "%s interval must match the TUMBLE window interval" % item[1].upper())
            if item[0] == "hop_bound" and (
                    window_kind != "hop" or item[2] != (size_ms, slide_ms)):
                raise QuerySyntaxError(
                    "%s parameters must match the HOP window parameters" % item[1].upper())
            if item[0] == "session_bound" and (
                    window_kind != "session" or item[2] != size_ms):
                raise QuerySyntaxError(
                    "%s interval must match the SESSION gap interval" % item[1].upper())

        seen_names = set()
        columns = []
        for tag, col_kind, param, alias in select_items:
            if tag == "agg":
                kind = param  # sum / count / min / max
                default_name = col_kind  # sum_amount / count_amount / ...
            else:
                kind = default_name = col_kind
            name = alias if alias is not None else default_name
            if name in seen_names:
                raise QuerySyntaxError("duplicate output column name: %r" % name)
            seen_names.add(name)
            columns.append((kind, name))
        if not any(kind == "user_id" for kind, _ in columns):
            raise QuerySyntaxError("SELECT list must include user_id")
        return columns, window

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
        elif upper in ("SUM", "COUNT", "MIN", "MAX"):
            self._expect_punct("(")
            kind2, field = self._next()
            if kind2 != "ident" or field.lower() != "amount":
                raise QuerySyntaxError(
                    "%s is only supported on field 'amount', got %r" % (upper, field))
            self._expect_punct(")")
            item = ("agg", upper.lower() + "_amount", upper.lower())
        elif upper in ("TUMBLE_START", "TUMBLE_END"):
            self._expect_punct("(")
            self._expect_field("event_time")
            self._expect_punct(",")
            interval_ms = self._parse_interval()
            self._expect_punct(")")
            col_kind = "window_start" if upper == "TUMBLE_START" else "window_end"
            item = ("tumble_bound", col_kind, interval_ms)
        elif upper in ("HOP_START", "HOP_END"):
            self._expect_punct("(")
            self._expect_field("event_time")
            self._expect_punct(",")
            size_ms = self._parse_interval()
            self._expect_punct(",")
            slide_ms = self._parse_interval()
            self._expect_punct(")")
            _validate_hop_params(size_ms, slide_ms)
            col_kind = "window_start" if upper == "HOP_START" else "window_end"
            item = ("hop_bound", col_kind, (size_ms, slide_ms))
        elif upper in ("SESSION_START", "SESSION_END"):
            self._expect_punct("(")
            self._expect_field("event_time")
            self._expect_punct(",")
            gap_ms = self._parse_interval()
            self._expect_punct(")")
            col_kind = "session_start" if upper == "SESSION_START" else "session_end"
            item = ("session_bound", col_kind, gap_ms)
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
        window = None
        while True:
            kind, text = self._next()
            if kind != "ident":
                raise QuerySyntaxError("expected a GROUP BY item, got %r" % text)
            upper = text.upper()
            if upper == "USER_ID":
                if saw_user_id:
                    raise QuerySyntaxError("duplicate GROUP BY user_id")
                saw_user_id = True
            elif upper in ("TUMBLE", "HOP", "SESSION"):
                if window is not None:
                    raise QuerySyntaxError("duplicate window function in GROUP BY")
                self._expect_punct("(")
                self._expect_field("event_time")
                self._expect_punct(",")
                size_ms = self._parse_interval()
                if upper == "TUMBLE":
                    self._expect_punct(")")
                    window = ("tumble", size_ms, size_ms)
                elif upper == "HOP":
                    self._expect_punct(",")
                    slide_ms = self._parse_interval()
                    self._expect_punct(")")
                    _validate_hop_params(size_ms, slide_ms)
                    window = ("hop", size_ms, slide_ms)
                else:
                    self._expect_punct(")")
                    window = ("session", size_ms, size_ms)
            else:
                raise QuerySyntaxError("unsupported GROUP BY expression: %r" % text)
            if self._peek() == ("punct", ","):
                self._next()
                continue
            break
        if not saw_user_id:
            raise QuerySyntaxError("GROUP BY must include user_id")
        if window is None:
            raise QuerySyntaxError(
                "GROUP BY must include TUMBLE(event_time, INTERVAL n SECOND), "
                "HOP(event_time, INTERVAL n SECOND, INTERVAL m SECOND) or "
                "SESSION(event_time, INTERVAL n SECOND)")
        return window


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


def _new_accumulator(amount):
    """新聚合键的四类聚合初值 [sum, count, min, max]。"""
    return [amount, 1, amount, amount]


# SELECT 聚合列类型到四元组下标的映射。
_AGG_INDEX = {"sum": 0, "count": 1, "min": 2, "max": 3}


def _update_accumulator(acc, amount):
    """把一条记录的 amount 共同原子地并入四类聚合。"""
    acc[0] += amount
    acc[1] += 1
    if amount < acc[2]:
        acc[2] = amount
    if amount > acc[3]:
        acc[3] = amount


# ---------------------------------------------------------------------------
# Exactly-once 状态持久化
#
# 状态文件为单个 JSON 文档，包含格式版本、查询指纹（输出列、窗口函数
# 及其参数、capacity）、当前水位、未输出窗口的四类聚合（SUM/COUNT/MIN/MAX）
# 与已处理 record_id 集合。每次成功变更通过 临时文件 + os.replace 原子提交
# 完整状态；提交失败时内存状态回滚到变更前，已提交状态保持不变。
#
# windows 条目布局为 [window_start, user_id, sum, count, min, max]；
# sessions 条目布局为 [user_id, start, max_event, sum, count, min, max]。
# 旧版仅 SUM 的 3/4 元素条目仍可读取（count 视为 1，min/max 取 sum 值）。
# ---------------------------------------------------------------------------

_STATE_VERSION = 1


def _validate_state_user_id(value, path):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise StateStorageError("state file %r is corrupted: bad user_id" % path)
    return value


def _parse_state_acc(values, path):
    """校验状态中的四类聚合 [sum, count, min, max]。"""
    if len(values) != 4:
        raise StateStorageError("state file %r is corrupted: bad aggregate" % path)
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int):
            raise StateStorageError(
                "state file %r is corrupted: bad aggregate" % path)
    total, count, minimum, maximum = values
    if count <= 0 or minimum > maximum:
        raise StateStorageError(
            "state file %r is corrupted: bad aggregate" % path)
    return [total, count, minimum, maximum]


class StreamQuery:
    """由 compile_query 编译得到的可执行流式查询。"""

    def __init__(self, columns, window, capacity=None, state_path=None):
        self._columns = tuple(columns)  # ((kind, output_name), ...)
        self._window_kind, self._size_ms, self._slide_ms = window
        self._capacity = capacity  # None 表示无界
        self._watermark_ms = None
        # (window_start_ms, user_id) -> [sum, count, min, max](amount)
        self._state = {}
        # SESSION 窗口：user_id -> [[session_start_ms, max_event_ms,
        #                          sum, count, min, max], ...]
        # 每个用户的会话按 session_start 升序，且两两间隔大于 gap（否则已合并）。
        self._sessions = {}
        self._state_path = state_path  # None 表示纯内存模式
        self._seen_ids = set() if state_path is not None else None

    @property
    def columns(self):
        return tuple(name for _, name in self._columns)

    @property
    def window_ms(self):
        """窗口大小（UTC 毫秒）。"""
        return self._size_ms

    @property
    def slide_ms(self):
        """滑动步长（UTC 毫秒）；TUMBLE 窗口等于窗口大小。"""
        return self._slide_ms

    @property
    def gap_ms(self):
        """SESSION 窗口的会话间隔（UTC 毫秒）；非 SESSION 查询为 None。"""
        if self._window_kind == "session":
            return self._size_ms
        return None

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

        记录落入所有覆盖其事件时间的窗口（HOP 下可能有多个），分别累加
        到各 (window_start, user_id) 聚合键。迟到记录不改变聚合状态；
        因容量已满被背压拒绝的记录同样不改变聚合状态、不产生结果，
        也不影响水位。

        持久模式（构造时给定 state_path）下每条记录还须带非空字符串
        record_id；已处理过的 record_id 再次出现时返回 "duplicate"，
        无论事件时间、容量或水位如何都不再累加、不产生结果。
        """
        if not isinstance(record, Mapping):
            raise InvalidRecordError("record must be a mapping of field name to value")
        for field in _SOURCE_FIELDS:
            if field not in record:
                raise InvalidRecordError("record is missing field %r" % field)
        persistent = self._state_path is not None
        record_id = None
        if persistent:
            if "record_id" not in record:
                raise InvalidRecordError("record is missing field 'record_id'")
            record_id = record["record_id"]
            if not isinstance(record_id, str) or not record_id:
                raise InvalidRecordError(
                    "record_id must be a non-empty str, got %r" % (record_id,))
        user_id = _validate_user_id(record["user_id"])
        event_ms = _parse_timestamp(record["event_time"], InvalidRecordError)
        amount = _validate_amount(record["amount"])

        if persistent and record_id in self._seen_ids:
            return "duplicate"
        if self._watermark_ms is not None and event_ms < self._watermark_ms:
            if persistent:
                self._seen_ids.add(record_id)
                try:
                    self._commit()
                except StateStorageError:
                    self._seen_ids.discard(record_id)
                    raise
            return "late"
        if self._window_kind == "session":
            return self._push_session(user_id, event_ms, amount, persistent, record_id)
        keys = [(start, user_id) for start in self._window_starts(event_ms)]
        created = [key for key in keys if key not in self._state]
        # 全部新聚合键都能被容量容纳才接收；任一超容量即整体背压，
        # 不部分累加、不登记 record_id。
        if self._capacity is not None and len(self._state) + len(created) > self._capacity:
            return "backpressured"
        # MIN/MAX 的并入不可逆，提交失败时用并入前快照整体回滚。
        previous = {key: list(self._state[key]) for key in keys if key not in created}
        created_set = set(created)
        for key in created:
            self._state[key] = _new_accumulator(amount)
        for key in keys:
            if key not in created_set:
                _update_accumulator(self._state[key], amount)
        if persistent:
            self._seen_ids.add(record_id)
            try:
                self._commit()
            except StateStorageError:
                self._seen_ids.discard(record_id)
                for key in created:
                    del self._state[key]
                for key, acc in previous.items():
                    self._state[key] = acc
                raise
        return "included"

    def _window_starts(self, event_ms):
        """返回覆盖 event_ms 的全部窗口起点：slide 的整数倍 start，
        满足 start <= event_ms < start + size。TUMBLE 视为 slide == size。"""
        start = event_ms - (event_ms % self._slide_ms)
        starts = []
        while start + self._size_ms > event_ms:
            starts.append(start)
            start -= self._slide_ms
        return starts

    def _push_session(self, user_id, event_ms, amount, persistent, record_id):
        """SESSION 窗口摄入：把事件并入时间差不超过 gap 的相邻会话。

        一条记录可能同时连接前后两个会话，此时合并为一个会话；该记录的
        四类聚合只贡献一次（SUM 加一份、COUNT 加一、MIN/MAX 并入一次）。
        容量按合并后的会话总数判断：合并后不超容量才接收，
        否则整体背压，不修改任何会话状态。
        """
        gap = self._size_ms
        had_user = user_id in self._sessions
        sessions = self._sessions.get(user_id, [])
        previous = [list(entry) for entry in sessions]  # 提交失败时回滚用
        lo = hi = None
        for idx, (start, max_event, _sum, _count, _min, _max) in enumerate(sessions):
            if start - gap <= event_ms <= max_event + gap:
                if lo is None:
                    lo = idx
                hi = idx
        merged = 0 if lo is None else hi - lo + 1
        total_keys = sum(len(entries) for entries in self._sessions.values())
        # 合并后聚合键数 = 现有会话数 - 被合并会话数 + 1；超容量即整体拒绝。
        if self._capacity is not None and total_keys - merged + 1 > self._capacity:
            return "backpressured"
        if lo is None:
            pos = 0
            while pos < len(sessions) and sessions[pos][0] < event_ms:
                pos += 1
            sessions.insert(pos, [event_ms, event_ms, amount, 1, amount, amount])
        else:
            # 记录只贡献一次：SUM/COUNT 加一份，MIN/MAX 并入一次。
            merged_entries = sessions[lo:hi + 1]
            new_entry = [
                min(event_ms, sessions[lo][0]),
                max(event_ms, sessions[hi][1]),
                amount + sum(entry[2] for entry in merged_entries),
                1 + sum(entry[3] for entry in merged_entries),
                min([amount] + [entry[4] for entry in merged_entries]),
                max([amount] + [entry[5] for entry in merged_entries]),
            ]
            sessions[lo:hi + 1] = [new_entry]
        if not had_user:
            self._sessions[user_id] = sessions
        if persistent:
            self._seen_ids.add(record_id)
            try:
                self._commit()
            except StateStorageError:
                self._seen_ids.discard(record_id)
                if had_user:
                    self._sessions[user_id] = previous
                else:
                    del self._sessions[user_id]
                raise
        return "included"

    def advance_watermark(self, timestamp):
        """显式推进水位；回退水位抛 WatermarkRegressionError。"""
        ms = _parse_timestamp(timestamp, InvalidRecordError)
        if self._watermark_ms is not None and ms < self._watermark_ms:
            raise WatermarkRegressionError(
                "watermark cannot regress: %r is before current watermark" % (timestamp,))
        if self._state_path is not None:
            previous = self._watermark_ms
            self._watermark_ms = ms
            try:
                self._commit()
            except StateStorageError:
                self._watermark_ms = previous
                raise
        else:
            self._watermark_ms = ms

    def drain(self):
        """返回所有已确定（窗口结束时刻不超过当前水位）的结果，并从状态中移除。"""
        if self._watermark_ms is None:
            return []
        if self._window_kind == "session":
            return self._drain_session()
        ready = [
            (window_start, user_id)
            for (window_start, user_id) in self._state
            if window_start + self._size_ms <= self._watermark_ms
        ]
        ready.sort(key=lambda item: (
            item[0], item[0] + self._size_ms, _user_id_sort_key(item[1])))
        rows = []
        removed = []
        for window_start, user_id in ready:
            acc = self._state.pop((window_start, user_id))
            removed.append(((window_start, user_id), acc))
            rows.append(self._build_row(window_start, user_id, acc))
        if self._state_path is not None and removed:
            try:
                self._commit()
            except StateStorageError:
                for key, acc in removed:
                    self._state[key] = acc
                raise
        return rows

    def _drain_session(self):
        """输出水位严格大于 session_end 的已确定会话，按
        (session_start, session_end, user_id) 排序，随后从状态中移除。"""
        gap = self._size_ms
        ready = []  # (session_start, session_end, user_id, accumulator)
        for user_id, sessions in self._sessions.items():
            for entry in sessions:
                start, max_event = entry[0], entry[1]
                if max_event + gap < self._watermark_ms:
                    ready.append((start, max_event + gap, user_id, entry[2:]))
        if not ready:
            return []
        ready.sort(key=lambda item: (item[0], item[1], _user_id_sort_key(item[2])))
        snapshot = {
            uid: [list(entry) for entry in entries]
            for uid, entries in self._sessions.items()
        }
        rows = []
        for start, end, user_id, acc in ready:
            entries = self._sessions[user_id]
            for idx, entry in enumerate(entries):
                if entry[0] == start:
                    del entries[idx]
                    break
            if not entries:
                del self._sessions[user_id]
            rows.append(self._build_session_row(start, end, user_id, acc))
        if self._state_path is not None:
            try:
                self._commit()
            except StateStorageError:
                self._sessions = snapshot
                raise
        return rows

    def _build_session_row(self, session_start, session_end, user_id, acc):
        row = {}
        for kind, name in self._columns:
            if kind == "user_id":
                row[name] = user_id
            elif kind == "session_start":
                row[name] = _format_iso8601(session_start)
            elif kind == "session_end":
                row[name] = _format_iso8601(session_end)
            else:  # 聚合列 sum/count/min/max
                row[name] = acc[_AGG_INDEX[kind]]
        return row

    def _build_row(self, window_start, user_id, acc):
        row = {}
        for kind, name in self._columns:
            if kind == "user_id":
                row[name] = user_id
            elif kind == "window_start":
                row[name] = _format_iso8601(window_start)
            elif kind == "window_end":
                row[name] = _format_iso8601(window_start + self._size_ms)
            else:  # 聚合列 sum/count/min/max
                row[name] = acc[_AGG_INDEX[kind]]
        return row

    # ------------------------------------------------------------------
    # Exactly-once 状态持久化（仅 state_path 模式下使用）
    # ------------------------------------------------------------------

    def _fingerprint(self):
        """查询指纹：同一 SQL 语义与 capacity 得到同一指纹。

        TUMBLE 沿用既有的 window_ms 形式，保证既有 TUMBLE 状态文件仍可
        恢复；HOP 以独立的 hop 描述记录 size 与 slide，SESSION 以独立的
        session 描述记录 gap，从而与 TUMBLE 以及不同参数的 HOP、SESSION
        相互区分。
        """
        fingerprint = {
            "columns": [[kind, name] for kind, name in self._columns],
            "capacity": self._capacity,
        }
        if self._window_kind == "tumble":
            fingerprint["window_ms"] = self._size_ms
        elif self._window_kind == "hop":
            fingerprint["hop"] = {
                "size_ms": self._size_ms,
                "slide_ms": self._slide_ms,
            }
        else:
            fingerprint["session"] = {"gap_ms": self._size_ms}
        return fingerprint

    def _serialize_state(self):
        doc = {
            "version": _STATE_VERSION,
            "fingerprint": self._fingerprint(),
            "watermark_ms": self._watermark_ms,
            "seen_ids": sorted(self._seen_ids),
        }
        if self._window_kind == "session":
            doc["sessions"] = [
                [user_id, entry[0], entry[1], entry[2], entry[3], entry[4], entry[5]]
                for user_id, sessions in sorted(
                    self._sessions.items(),
                    key=lambda item: _user_id_sort_key(item[0]),
                )
                for entry in sessions
            ]
        else:
            doc["windows"] = [
                [window_start, user_id, acc[0], acc[1], acc[2], acc[3]]
                for (window_start, user_id), acc in sorted(
                    self._state.items(),
                    key=lambda item: (item[0][0], _user_id_sort_key(item[0][1])),
                )
            ]
        return json.dumps(doc, ensure_ascii=False, sort_keys=True).encode("utf-8")

    def _commit(self):
        """把完整状态原子写入 state_path；失败抛 StateStorageError。"""
        payload = self._serialize_state()
        tmp_path = self._state_path + ".tmp"
        try:
            with open(tmp_path, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self._state_path)
        except OSError as exc:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise StateStorageError(
                "cannot persist state to %r: %s" % (self._state_path, exc)) from exc

    def _restore_or_initialize(self):
        """加载已有状态文件；不存在时以当前（空）状态创建。"""
        if os.path.exists(self._state_path):
            self._load_state()
        else:
            self._commit()

    def _load_state(self):
        path = self._state_path
        try:
            with open(path, "rb") as handle:
                raw = handle.read()
        except OSError as exc:
            raise StateStorageError(
                "cannot read state file %r: %s" % (path, exc)) from exc
        try:
            doc = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise StateStorageError("state file %r is corrupted" % path) from exc
        if not isinstance(doc, dict):
            raise StateStorageError("state file %r is corrupted" % path)
        state_key = "sessions" if self._window_kind == "session" else "windows"
        for key in ("version", "fingerprint", "watermark_ms", state_key, "seen_ids"):
            if key not in doc:
                raise StateStorageError(
                    "state file %r is corrupted: missing %r" % (path, key))
        if doc["version"] != _STATE_VERSION:
            raise StateStorageError(
                "state file %r has incompatible version %r" % (path, doc["version"]))
        if doc["fingerprint"] != self._fingerprint():
            raise StateStorageError(
                "state file %r does not match the given SQL and capacity" % path)
        watermark = doc["watermark_ms"]
        if watermark is not None and (
                isinstance(watermark, bool) or not isinstance(watermark, int)):
            raise StateStorageError("state file %r is corrupted: bad watermark" % path)
        if self._window_kind == "session":
            sessions = self._parse_state_sessions(doc["sessions"], path)
        else:
            state = self._parse_state_windows(doc["windows"], path)
        seen_ids = doc["seen_ids"]
        if not isinstance(seen_ids, list):
            raise StateStorageError("state file %r is corrupted: bad seen_ids" % path)
        seen = set()
        for record_id in seen_ids:
            if not isinstance(record_id, str) or not record_id:
                raise StateStorageError("state file %r is corrupted: bad record_id" % path)
            seen.add(record_id)
        self._watermark_ms = watermark
        if self._window_kind == "session":
            self._sessions = sessions
        else:
            self._state = state
        self._seen_ids = seen

    def _parse_state_windows(self, windows, path):
        if not isinstance(windows, list):
            raise StateStorageError("state file %r is corrupted: bad windows" % path)
        state = {}
        for entry in windows:
            if not isinstance(entry, list) or len(entry) not in (3, 6):
                raise StateStorageError("state file %r is corrupted: bad window" % path)
            window_start, user_id = entry[0], entry[1]
            if isinstance(window_start, bool) or not isinstance(window_start, int):
                raise StateStorageError("state file %r is corrupted: bad window" % path)
            _validate_state_user_id(user_id, path)
            if len(entry) == 3:
                # 旧版仅 SUM 布局：[start, user_id, sum]，count 视为 1，
                # min/max 无历史信息，取该窗口的唯一可用值 sum。
                total = entry[2]
                if isinstance(total, bool) or not isinstance(total, int):
                    raise StateStorageError(
                        "state file %r is corrupted: bad window" % path)
                acc = [total, 1, total, total]
            else:
                acc = _parse_state_acc(entry[2:], path)
            key = (window_start, user_id)
            if key in state:
                raise StateStorageError(
                    "state file %r is corrupted: duplicate window" % path)
            state[key] = acc
        return state

    def _parse_state_sessions(self, sessions_doc, path):
        if not isinstance(sessions_doc, list):
            raise StateStorageError("state file %r is corrupted: bad sessions" % path)
        sessions = {}
        for entry in sessions_doc:
            if not isinstance(entry, list) or len(entry) not in (4, 7):
                raise StateStorageError("state file %r is corrupted: bad session" % path)
            user_id, session_start, max_event = entry[0], entry[1], entry[2]
            _validate_state_user_id(user_id, path)
            for value in (session_start, max_event):
                if isinstance(value, bool) or not isinstance(value, int):
                    raise StateStorageError(
                        "state file %r is corrupted: bad session" % path)
            if session_start > max_event:
                raise StateStorageError(
                    "state file %r is corrupted: bad session" % path)
            if len(entry) == 4:
                # 旧版仅 SUM 布局：[user_id, start, max_event, sum]。
                total = entry[3]
                if isinstance(total, bool) or not isinstance(total, int):
                    raise StateStorageError(
                        "state file %r is corrupted: bad session" % path)
                acc = [total, 1, total, total]
            else:
                acc = _parse_state_acc(entry[3:], path)
            sessions.setdefault(user_id, []).append(
                [session_start, max_event] + acc)
        gap = self._size_ms
        for user_sessions in sessions.values():
            user_sessions.sort(key=lambda item: item[0])
            starts = [item[0] for item in user_sessions]
            if len(set(starts)) != len(starts):
                raise StateStorageError(
                    "state file %r is corrupted: duplicate session" % path)
            # 相邻会话的事件时间差必须大于 gap，否则摄入时早已合并。
            for previous, current in zip(user_sessions, user_sessions[1:]):
                if current[0] - previous[1] <= gap:
                    raise StateStorageError(
                        "state file %r is corrupted: overlapping sessions" % path)
        return sessions


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


def compile_query(sql, capacity=None, state_path=None):
    """编译限定语法的窗口聚合 SQL，返回 StreamQuery。

    支持的形态（关键字与标识符大小写不敏感，空白不影响语义）：

        SELECT user_id,
               [TUMBLE_START(event_time, INTERVAL n SECOND) [AS alias],]
               [TUMBLE_END(event_time, INTERVAL n SECOND) [AS alias],]
               agg(amount) [AS alias] ...
        FROM orders
        GROUP BY user_id, TUMBLE(event_time, INTERVAL n SECOND)

    其中 agg 为 SUM、COUNT、MIN、MAX 中的任意个，可任意排列、省略或
    使用 AS 别名，同一聚合也可按不同别名重复输出；缺省列名依次为
    sum_amount、count_amount、min_amount、max_amount。四类聚合无论是否
    出现在 SELECT 中都随每条记录共同原子更新，SELECT 只决定输出哪些列。

    或滑动窗口形态：

        SELECT user_id,
               [HOP_START(event_time, INTERVAL n SECOND, INTERVAL m SECOND) [AS alias],]
               [HOP_END(event_time, INTERVAL n SECOND, INTERVAL m SECOND) [AS alias],]
               agg(amount) [AS alias] ...
        FROM orders
        GROUP BY user_id, HOP(event_time, INTERVAL n SECOND, INTERVAL m SECOND)

    HOP 的 size（n）与 slide（m）均为正整数秒且 slide 不大于 size；
    一条记录落入所有起点为 slide 整数倍、且覆盖其事件时间的窗口，
    四类聚合分别参与每个覆盖窗口，分别累加到各
    (window_start, user_id) 聚合键。TUMBLE 与 HOP 不可混用，
    HOP_START/HOP_END 的参数必须与 GROUP BY 的 HOP 一致。

    会话窗口形态：

        SELECT user_id,
               [SESSION_START(event_time, INTERVAL n SECOND) [AS alias],]
               [SESSION_END(event_time, INTERVAL n SECOND) [AS alias],]
               agg(amount) [AS alias] ...
        FROM orders
        GROUP BY user_id, SESSION(event_time, INTERVAL n SECOND)

    SESSION 的 gap（n）为正整数秒；同一 user_id 内相邻事件时间差不超过
    gap 的记录归入同一会话，一条记录可连接前后两个会话并将其合并，
    四类聚合在合并时每条记录只贡献一次（SUM 加一份、COUNT 加一、
    MIN/MAX 并入一次）。session_start 为会话内最小事件时间，session_end
    为最大事件时间加 gap（不做 epoch 对齐）；水位严格大于 session_end
    后会话确定，drain 按 (session_start, session_end, user_id) 排序输出。
    SESSION 与 TUMBLE、HOP 不可混用，SESSION_START/SESSION_END 的
    间隔必须与 GROUP BY 的 SESSION 一致。

    聚合语义：SUM 累加 amount，COUNT 统计 amount 的条数，MIN/MAX 取
    最小、最大整数；未知聚合或聚合作用于 amount 之外的字段抛
    QuerySyntaxError。

    capacity 省略或为 None 时查询无界；给定正整数时表示查询可保留的
    最大未输出聚合键数（聚合键由窗口起点与 user_id 确定；SESSION 窗口
    的聚合键为合并后的 (user_id, 会话)）。记录的全部新聚合键都能被
    容量容纳时才被接收，任一超容量即整体背压拒绝
    （push 返回 "backpressured"）；drain() 移除已确定聚合键后释放
    对应容量。

    state_path 省略或为 None 时查询为纯内存模式，行为与既往一致。
    给定 state_path 时查询进入持久模式：每次成功变更把完整状态原子
    提交到该文件，使用同一 SQL、capacity 和 state_path 重新构造查询
    即可恢复未输出窗口的四类聚合（SUM/COUNT/MIN/MAX）、当前水位与
    去重信息。旧版仅 SUM 的状态文件仍可恢复（COUNT 视为 1，MIN/MAX
    取该窗口唯一可用值），随后继续共同维护四类聚合；新查询读取列集合
    不匹配的状态时抛 StateStorageError。持久模式下每条记录必须
    带非空字符串 record_id，已处理过的 record_id 返回 "duplicate"。
    state_path 为空字符串、非字符串、不可写，或状态文件损坏、版本
    不兼容、与 SQL 或 capacity 不一致时抛出 StateStorageError。
    """
    capacity = _validate_capacity(capacity)
    if not isinstance(sql, str):
        raise QuerySyntaxError("sql must be a string, got %s" % type(sql).__name__)
    if not sql.strip():
        raise QuerySyntaxError("sql must not be empty")
    columns, window = _Parser(_tokenize(sql)).parse()
    if state_path is None:
        return StreamQuery(columns, window, capacity)
    if not isinstance(state_path, str) or not state_path:
        raise StateStorageError(
            "state_path must be a non-empty string path, got %r" % (state_path,))
    query = StreamQuery(columns, window, capacity, state_path=state_path)
    query._restore_or_initialize()
    return query
