"""公开异常类型。"""


class QuerySyntaxError(ValueError):
    """SQL 不符合受支持的语法或语义时抛出。"""


class InvalidRecordError(ValueError):
    """记录缺少字段、字段类型不符或时间无法解析时抛出。"""


class WatermarkRegressionError(ValueError):
    """advance_watermark 试图回退水位时抛出。"""
