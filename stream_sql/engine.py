"""流式查询运行时：乱序水位推进的事件时间翻滚窗口聚合。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .errors import InvalidRecordError, WatermarkRegressionError
from .parser import (
    COL_SUM_AMOUNT,
    COL_USER_ID,
    COL_WINDOW_END,
    COL_WINDOW_START,
    QuerySpec,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _timestamp_to_ms(value: Any) -> int:
    """把 Unix 毫秒整数或带时区的 ISO 8601 字符串统一为 UTC 毫秒。"""
    if isinstance(value, bool):
        raise InvalidRecordError(f"时间值类型不支持: {type(value).__name__}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            raise InvalidRecordError(f"时间无法解析: {value!r}") from None
        if parsed.tzinfo is None:
            raise InvalidRecordError(f"时间缺少时区: {value!r}")
        parsed = parsed.astimezone(timezone.utc)
        delta = parsed - _EPOCH
        return (
            delta.days * 86_400_000
            + delta.seconds * 1_000
            + delta.microseconds // 1_000
        )
    raise InvalidRecordError(f"时间值类型不支持: {type(value).__name__}")


def _ms_to_iso_utc(ms: int) -> str:
    """把 UTC 毫秒格式化为 ISO 8601 UTC 字符串。"""
    moment = _EPOCH + timedelta(milliseconds=ms)
    if ms % 1000 == 0:
        return moment.strftime("%Y-%m-%dT%H:%M:%SZ")
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


class StreamQuery:
    """一条已编译的流式查询。

    - push(record)：摄入一条记录，返回 "included" 或 "late"。
    - advance_watermark(timestamp)：显式推进水位，水位达到窗口结束
      时刻的窗口随之确定。
    - drain()：取走自上次 drain 以来已确定的结果，按
      (window_start, window_end, user_id) 排序。
    """

    def __init__(self, spec: QuerySpec):
        self._spec = spec
        self._interval_ms = spec.interval_seconds * 1_000
        # (window_start_ms, user_id) -> 累计的 SUM(amount)
        self._open: Dict[Tuple[int, Any], int] = {}
        # 已确定、尚未被 drain 取走的 (window_start_ms, window_end_ms, user_id, total)
        self._ready: List[Tuple[int, int, Any, int]] = []
        self._watermark_ms: Optional[int] = None

    def push(self, record: Mapping[str, Any]) -> str:
        if not isinstance(record, Mapping):
            raise InvalidRecordError("记录必须是字段名到值的映射")
        for field in ("user_id", "event_time", "amount"):
            if field not in record:
                raise InvalidRecordError(f"记录缺少字段: {field!r}")

        user_id = record["user_id"]
        if isinstance(user_id, bool) or not isinstance(user_id, (str, int)):
            raise InvalidRecordError(f"user_id 类型不支持: {type(user_id).__name__}")
        amount = record["amount"]
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise InvalidRecordError(f"amount 必须是整数，实际为 {type(amount).__name__}")
        event_ms = _timestamp_to_ms(record["event_time"])

        if self._watermark_ms is not None and event_ms < self._watermark_ms:
            return "late"

        window_start = (event_ms // self._interval_ms) * self._interval_ms
        key = (window_start, user_id)
        self._open[key] = self._open.get(key, 0) + amount
        return "included"

    def advance_watermark(self, timestamp: Any) -> None:
        watermark_ms = _timestamp_to_ms(timestamp)
        if self._watermark_ms is not None and watermark_ms < self._watermark_ms:
            raise WatermarkRegressionError(
                f"水位不能回退: 当前 {self._watermark_ms}，收到 {watermark_ms}"
            )
        self._watermark_ms = watermark_ms
        finalized = [
            key
            for key in self._open
            if key[0] + self._interval_ms <= watermark_ms
        ]
        for window_start, user_id in finalized:
            total = self._open.pop((window_start, user_id))
            self._ready.append(
                (window_start, window_start + self._interval_ms, user_id, total)
            )

    def drain(self) -> List[Dict[str, Any]]:
        ready = sorted(self._ready, key=lambda row: (row[0], row[1], row[2]))
        self._ready = []
        return [self._build_row(row) for row in ready]

    def _build_row(self, row: Tuple[int, int, Any, int]) -> Dict[str, Any]:
        window_start, window_end, user_id, total = row
        output: Dict[str, Any] = {}
        for column in self._spec.columns:
            if column.kind == COL_USER_ID:
                output[column.name] = user_id
            elif column.kind == COL_WINDOW_START:
                output[column.name] = _ms_to_iso_utc(window_start)
            elif column.kind == COL_WINDOW_END:
                output[column.name] = _ms_to_iso_utc(window_end)
            elif column.kind == COL_SUM_AMOUNT:
                output[column.name] = total
        return output
