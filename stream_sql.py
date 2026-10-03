"""流式 SQL 计算引擎：事件时间翻滚窗口聚合（乱序水位推进、有界背压、Exactly-once 状态）。

公开入口：
    compile_query(sql, capacity=None, state_path=None) -> StreamQuery

StreamQuery:
    push(record)            -> "included" | "late" | "backpressured"
                               （持久模式下还可能是 "duplicate"）
    advance_watermark(ts)   -> None
    drain()                 -> list[dict]

异常：
    QuerySyntaxError          SQL 不合法
    QueryConfigurationError   capacity 等查询配置不合法
    InvalidRecordError        记录缺字段 / 类型不符 / 时间无法解析
    WatermarkRegressionError  水位回退
    StateStorageError         state_path 不可用或状态文件损坏 / 不兼容
"""

from __future__ import annotations

import json
import os
import re
import tempfile
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
    """state_path 不可写，或状态文件损坏、版本不兼容、与 SQL / capacity 不一致。"""


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


# ---------------------------------------------------------------------------
# 状态持久化（Exactly-once）
# ---------------------------------------------------------------------------

_STATE_VERSION = 1


def _validate_state_path(state_path):
    """state_path 省略或为 None 表示内存模式；否则必须是非空字符串路径。"""
    if state_path is None:
        return None
    if isinstance(state_path, bool) or not isinstance(state_path, str):
        raise StateStorageError(
            "state_path must be a non-empty string path, got %s" % type(state_path).__name__)
    if not state_path:
        raise StateStorageError("state_path must not be empty")
    return state_path


def _validate_payload(payload, path):
    """校验状态文件内容结构；损坏或版本不兼容统一抛 StateStorageError。"""
    def corrupted():
        return StateStorageError("state file %r is corrupted" % path)

    if not isinstance(payload, dict):
        raise corrupted()
    version = payload.get("version")
    if isinstance(version, bool) or not isinstance(version, int):
        raise corrupted()
    if version != _STATE_VERSION:
        raise StateStorageError(
            "state file %r has unsupported version %r" % (path, version))
    if not isinstance(payload.get("signature"), dict):
        raise corrupted()
    watermark = payload.get("watermark_ms")
    if watermark is not None and (isinstance(watermark, bool) or not isinstance(watermark, int)):
        raise corrupted()
    entries = payload.get("state")
    if not isinstance(entries, list):
        raise corrupted()
    for entry in entries:
        if not (isinstance(entry, list) and len(entry) == 3):
            raise corrupted()
        window_start, user_id, total = entry
        if isinstance(window_start, bool) or not isinstance(window_start, int):
            raise corrupted()
        if isinstance(user_id, bool) or not isinstance(user_id, (str, int)):
            raise corrupted()
        if isinstance(total, bool) or not isinstance(total, int):
            raise corrupted()
    seen_ids = payload.get("seen_ids")
    if not isinstance(seen_ids, list) or any(not isinstance(rid, str) or not rid for rid in seen_ids):
        raise corrupted()
    return payload


class _StateStore:
    """state_path 的原子读写：完整状态快照写入同目录临时文件后 os.replace 落盘。

    提交成功后只存在 state_path 本身，不留下日志或额外文件；中途失败
    清理临时文件，已提交状态保持不变。
    """

    def __init__(self, path):
        self._path = path

    def load(self):
        """读取并校验状态文件；文件不存在返回 None。"""
        if not os.path.exists(self._path):
            return None
        try:
            with open(self._path, "rb") as handle:
                raw = handle.read()
        except OSError as exc:
            raise StateStorageError(
                "cannot read state file %r: %s" % (self._path, exc)) from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise StateStorageError("state file %r is corrupted" % self._path) from exc
        return _validate_payload(payload, self._path)

    def ensure_writable(self):
        """确认已存在的状态文件可写（不修改内容）。"""
        try:
            with open(self._path, "ab"):
                pass
        except OSError as exc:
            raise StateStorageError(
                "state file %r is not writable: %s" % (self._path, exc)) from exc

    def commit(self, payload):
        """原子提交完整状态快照；失败抛 StateStorageError 且不留下部分更新。"""
        data = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        directory = os.path.dirname(os.path.abspath(self._path))
        try:
            fd, tmp_path = tempfile.mkstemp(prefix=".stream_sql-", dir=directory)
        except OSError as exc:
            raise StateStorageError(
                "cannot write state file %r: %s" % (self._path, exc)) from exc
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self._path)
        except OSError as exc:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise StateStorageError(
                "cannot write state file %r: %s" % (self._path, exc)) from exc


class StreamQuery:
    """由 compile_query 编译得到的可执行流式查询。"""

    def __init__(self, columns, window_ms, capacity=None, state_path=None):
        self._columns = tuple(columns)  # ((kind, output_name), ...)
        self._window_ms = window_ms
        self._capacity = capacity  # None 表示无界
        self._watermark_ms = None
        self._state = {}  # (window_start_ms, user_id) -> sum(amount)
        self._seen_ids = set()  # 已处理的 record_id（仅持久模式使用）
        self._store = None
        if state_path is not None:
            store = _StateStore(state_path)
            payload = store.load()
            if payload is None:
                # 初始化状态文件，同时验证路径可写。
                store.commit(self._serialize_state())
            else:
                if payload["signature"] != self._signature():
                    raise StateStorageError(
                        "state file %r does not match this SQL and capacity" % state_path)
                self._watermark_ms = payload["watermark_ms"]
                self._state = {
                    (window_start, user_id): total
                    for window_start, user_id, total in payload["state"]
                }
                self._seen_ids = set(payload["seen_ids"])
                store.ensure_writable()
            self._store = store

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

    @property
    def persistent(self):
        """是否启用状态持久化（构造时传入了 state_path）。"""
        return self._store is not None

    def _signature(self):
        """查询的确定性签名，用于校验状态文件与 SQL、capacity 是否一致。"""
        return {
            "columns": [[kind, name] for kind, name in self._columns],
            "window_ms": self._window_ms,
            "capacity": self._capacity,
        }

    def _serialize_state(self):
        entries = [
            [window_start, user_id, total]
            for (window_start, user_id), total in self._state.items()
        ]
        entries.sort(key=lambda item: (item[0], _user_id_sort_key(item[1])))
        return {
            "version": _STATE_VERSION,
            "signature": self._signature(),
            "watermark_ms": self._watermark_ms,
            "state": entries,
            "seen_ids": sorted(self._seen_ids),
        }

    def _snapshot(self):
        return (self._watermark_ms, dict(self._state), set(self._seen_ids))

    def _commit(self, snapshot):
        """持久模式下原子提交当前状态；失败时回滚内存状态并抛 StateStorageError。"""
        try:
            self._store.commit(self._serialize_state())
        except StateStorageError:
            self._watermark_ms, self._state, self._seen_ids = snapshot
            raise

    def push(self, record):
        """摄入一条记录，返回 "included"、"late"、"backpressured" 或 "duplicate"。

        迟到记录不改变聚合状态；因容量已满被背压拒绝的记录同样不改变
        聚合状态、不产生结果，也不影响水位。持久模式下记录必须携带非空
        字符串 record_id；已处理过的 record_id 再次出现时不改变任何状态，
        返回 "duplicate"（优先于 late 与 backpressured 判定）。
        """
        if not isinstance(record, Mapping):
            raise InvalidRecordError("record must be a mapping of field name to value")
        required = _SOURCE_FIELDS + (("record_id",) if self._store is not None else ())
        for field in required:
            if field not in record:
                raise InvalidRecordError("record is missing field %r" % field)
        user_id = _validate_user_id(record["user_id"])
        event_ms = _parse_timestamp(record["event_time"], InvalidRecordError)
        amount = _validate_amount(record["amount"])
        record_id = None
        if self._store is not None:
            record_id = record["record_id"]
            if not isinstance(record_id, str) or not record_id:
                raise InvalidRecordError("record_id must be a non-empty string")
            if record_id in self._seen_ids:
                return "duplicate"

        if self._watermark_ms is not None and event_ms < self._watermark_ms:
            return "late"
        window_start = event_ms - (event_ms % self._window_ms)
        key = (window_start, user_id)
        snapshot = self._snapshot() if self._store is not None else None
        if key not in self._state:
            if self._capacity is not None and len(self._state) >= self._capacity:
                return "backpressured"
            self._state[key] = 0
        self._state[key] += amount
        if record_id is not None:
            self._seen_ids.add(record_id)
        if snapshot is not None:
            self._commit(snapshot)
        return "included"

    def advance_watermark(self, timestamp):
        """显式推进水位；回退水位抛 WatermarkRegressionError。"""
        ms = _parse_timestamp(timestamp, InvalidRecordError)
        if self._watermark_ms is not None and ms < self._watermark_ms:
            raise WatermarkRegressionError(
                "watermark cannot regress: %r is before current watermark" % (timestamp,))
        snapshot = self._snapshot() if self._store is not None else None
        self._watermark_ms = ms
        if snapshot is not None:
            self._commit(snapshot)

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
        if not ready:
            return []
        snapshot = self._snapshot() if self._store is not None else None
        rows = []
        for window_start, user_id in ready:
            total = self._state.pop((window_start, user_id))
            rows.append(self._build_row(window_start, user_id, total))
        if snapshot is not None:
            self._commit(snapshot)
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


def compile_query(sql, capacity=None, state_path=None):
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

    state_path 省略或为 None 时查询为纯内存语义；给定非空字符串路径时
    启用 Exactly-once 状态：push、advance_watermark、drain 的每次状态
    变更都原子提交到该文件，使用同一 SQL、capacity 和 state_path 重新
    构造查询即可恢复未输出窗口、当前水位与去重信息。持久模式下每条
    记录必须携带非空字符串 record_id，已处理过的 record_id 再次被
    推送时返回 "duplicate" 且不改变状态。state_path 不可写、状态文件
    损坏、版本不兼容或与 SQL、capacity 不一致时抛 StateStorageError。
    """
    capacity = _validate_capacity(capacity)
    if not isinstance(sql, str):
        raise QuerySyntaxError("sql must be a string, got %s" % type(sql).__name__)
    if not sql.strip():
        raise QuerySyntaxError("sql must not be empty")
    columns, window_ms = _Parser(_tokenize(sql)).parse()
    state_path = _validate_state_path(state_path)
    return StreamQuery(columns, window_ms, capacity, state_path)
