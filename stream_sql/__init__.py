"""Stream SQL：流式 SQL 计算引擎的公开入口。"""

from .engine import StreamQuery
from .errors import InvalidRecordError, QuerySyntaxError, WatermarkRegressionError
from .parser import parse_query

__all__ = [
    "compile_query",
    "StreamQuery",
    "QuerySyntaxError",
    "InvalidRecordError",
    "WatermarkRegressionError",
]


def compile_query(sql: str) -> StreamQuery:
    """编译一条受限的翻滚窗口聚合 SQL，返回可摄入记录的 StreamQuery。"""
    return StreamQuery(parse_query(sql))
