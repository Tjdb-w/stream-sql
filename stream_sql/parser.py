"""受限 SQL 的解析。

支持的语法（关键字与标识符大小写不敏感，空白不影响语义）：

    SELECT <select_item> [, <select_item> ...]
    FROM orders
    GROUP BY user_id, TUMBLE(event_time, INTERVAL n SECOND)

select_item 只能是以下形式之一，可带可选别名（AS name 或裸 name）：

    user_id
    TUMBLE_START 或 TUMBLE_START(event_time, INTERVAL n SECOND)
    TUMBLE_END   或 TUMBLE_END(event_time, INTERVAL n SECOND)
    SUM(amount)

GROUP BY 两项顺序不限。n 必须是正整数秒。语句末尾允许一个分号。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

from .errors import QuerySyntaxError

# 输出列种类
COL_USER_ID = "user_id"
COL_WINDOW_START = "window_start"
COL_WINDOW_END = "window_end"
COL_SUM_AMOUNT = "sum_amount"

_SOURCE_FIELDS = {"user_id", "event_time", "amount"}

_TOKEN_RE = re.compile(
    r"""
    (?P<ws>\s+)
    | (?P<string>'(?:[^']|'')*')
    | (?P<number>\d+(?:\.\d+)?)
    | (?P<ident>[A-Za-z_][A-Za-z0-9_]*)
    | (?P<punct>[(),;*])
    """,
    re.VERBOSE,
)


@dataclass(frozen=True)
class SelectColumn:
    kind: str
    name: str  # 输出列名（别名或规范名）


@dataclass(frozen=True)
class QuerySpec:
    interval_seconds: int
    columns: Tuple[SelectColumn, ...]


@dataclass(frozen=True)
class _Token:
    kind: str  # "ident" | "number" | "string" | "punct"
    text: str


def _tokenize(sql: str) -> List[_Token]:
    tokens: List[_Token] = []
    pos = 0
    while pos < len(sql):
        match = _TOKEN_RE.match(sql, pos)
        if match is None:
            raise QuerySyntaxError(f"无法识别的字符: {sql[pos]!r}")
        pos = match.end()
        if match.lastgroup == "ws":
            continue
        tokens.append(_Token(match.lastgroup, match.group()))
    return tokens


class _Parser:
    def __init__(self, tokens: List[_Token]):
        self._tokens = tokens
        self._pos = 0

    def peek(self) -> Optional[_Token]:
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return None

    def next(self) -> _Token:
        token = self.peek()
        if token is None:
            raise QuerySyntaxError("语句意外结束")
        self._pos += 1
        return token

    def expect_keyword(self, word: str) -> None:
        token = self.next()
        if token.kind != "ident" or token.text.upper() != word:
            raise QuerySyntaxError(f"期望 {word}，实际为 {token.text!r}")

    def expect_punct(self, punct: str) -> None:
        token = self.next()
        if token.kind != "punct" or token.text != punct:
            raise QuerySyntaxError(f"期望 {punct!r}，实际为 {token.text!r}")

    def accept_punct(self, punct: str) -> bool:
        token = self.peek()
        if token is not None and token.kind == "punct" and token.text == punct:
            self._pos += 1
            return True
        return False

    def parse_ident(self) -> str:
        token = self.next()
        if token.kind != "ident":
            raise QuerySyntaxError(f"期望标识符，实际为 {token.text!r}")
        return token.text


def _check_source_field(name: str) -> str:
    lowered = name.lower()
    if lowered not in _SOURCE_FIELDS:
        raise QuerySyntaxError(f"不支持的字段: {name!r}")
    return lowered


def _parse_interval(parser: _Parser) -> int:
    """解析 INTERVAL n SECOND，返回正整数秒。"""
    parser.expect_keyword("INTERVAL")
    token = parser.next()
    if token.kind == "number":
        text = token.text
    elif token.kind == "string":
        text = token.text[1:-1].replace("''", "'")
    else:
        raise QuerySyntaxError("INTERVAL 后应为正整数秒数")
    if not re.fullmatch(r"\d+", text):
        raise QuerySyntaxError(f"窗口间隔不是正的秒数: {text!r}")
    value = int(text)
    if value <= 0:
        raise QuerySyntaxError("窗口间隔必须为正整数秒")
    parser.expect_keyword("SECOND")
    return value


def _parse_tumble_args(parser: _Parser, func: str) -> int:
    """解析 (event_time, INTERVAL n SECOND)，返回间隔秒数。"""
    parser.expect_punct("(")
    field = _check_source_field(parser.parse_ident())
    if field != "event_time":
        raise QuerySyntaxError(f"{func} 的第一个参数必须是 event_time")
    parser.expect_punct(",")
    interval = _parse_interval(parser)
    parser.expect_punct(")")
    return interval


def _parse_expr(parser: _Parser):
    """解析一个表达式，返回 ("field", name) 或 ("call", name, interval_or_None)。"""
    token = parser.next()
    if token.kind != "ident":
        raise QuerySyntaxError(f"不支持的表达式: {token.text!r}")
    name = token.text
    upper = name.upper()
    if parser.accept_punct("("):
        parser._pos -= 1  # 交还给 _parse_tumble_args / SUM 解析
        if upper in ("TUMBLE", "TUMBLE_START", "TUMBLE_END"):
            interval = _parse_tumble_args(parser, upper)
            return ("call", upper, interval)
        if upper == "SUM":
            parser.expect_punct("(")
            field = _check_source_field(parser.parse_ident())
            if field != "amount":
                raise QuerySyntaxError("SUM 只支持 amount")
            parser.expect_punct(")")
            return ("call", "SUM", None)
        raise QuerySyntaxError(f"不支持的函数: {name!r}")
    return ("field", name, None)


def _parse_select_item(parser: _Parser) -> Tuple[object, Optional[str]]:
    expr = _parse_expr(parser)
    alias: Optional[str] = None
    token = parser.peek()
    if token is not None and token.kind == "ident":
        if token.text.upper() == "AS":
            parser.next()
            alias = parser.parse_ident()
        elif token.text.upper() not in ("FROM", "GROUP"):
            alias = parser.parse_ident()
    return expr, alias


def _select_column(expr, alias: Optional[str]) -> SelectColumn:
    kind, name, _interval = expr
    if kind == "field":
        upper = name.upper()
        if upper == "USER_ID":
            return SelectColumn(COL_USER_ID, alias or "user_id")
        if upper == "TUMBLE_START":
            return SelectColumn(COL_WINDOW_START, alias or "TUMBLE_START")
        if upper == "TUMBLE_END":
            return SelectColumn(COL_WINDOW_END, alias or "TUMBLE_END")
        lowered = name.lower()
        if lowered in _SOURCE_FIELDS:
            raise QuerySyntaxError(f"SELECT 中不支持直接输出字段: {name!r}")
        raise QuerySyntaxError(f"不支持的字段: {name!r}")
    if name in ("TUMBLE_START", "TUMBLE_END"):
        kind_const = COL_WINDOW_START if name == "TUMBLE_START" else COL_WINDOW_END
        return SelectColumn(kind_const, alias or name)
    if name == "SUM":
        return SelectColumn(COL_SUM_AMOUNT, alias or "SUM(amount)")
    raise QuerySyntaxError(f"SELECT 中不支持的表达式: {name!r}")


def parse_query(sql: str) -> QuerySpec:
    if not isinstance(sql, str):
        raise QuerySyntaxError("SQL 必须是字符串")
    parser = _Parser(_tokenize(sql))

    parser.expect_keyword("SELECT")
    columns: List[SelectColumn] = []
    while True:
        expr, alias = _parse_select_item(parser)
        columns.append(_select_column(expr, alias))
        if not parser.accept_punct(","):
            break

    parser.expect_keyword("FROM")
    table = parser.parse_ident()
    if table.lower() != "orders":
        raise QuerySyntaxError(f"只支持从 orders 读取，实际为 {table!r}")

    parser.expect_keyword("GROUP")
    parser.expect_keyword("BY")
    group_exprs = [_parse_expr(parser)]
    while parser.accept_punct(","):
        group_exprs.append(_parse_expr(parser))

    parser.accept_punct(";")
    if parser.peek() is not None:
        raise QuerySyntaxError(f"语句末尾存在多余内容: {parser.peek().text!r}")

    seen_user_id = False
    interval_seconds: Optional[int] = None
    for kind, name, interval in group_exprs:
        if kind == "field" and name.upper() == "USER_ID" and not seen_user_id:
            seen_user_id = True
        elif kind == "call" and name == "TUMBLE" and interval_seconds is None:
            interval_seconds = interval
        else:
            raise QuerySyntaxError("GROUP BY 只支持 user_id 与 TUMBLE(event_time, INTERVAL n SECOND)")
    if not seen_user_id or interval_seconds is None:
        raise QuerySyntaxError("GROUP BY 必须包含 user_id 与 TUMBLE(event_time, INTERVAL n SECOND)")

    return QuerySpec(interval_seconds=interval_seconds, columns=tuple(columns))
