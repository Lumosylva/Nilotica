"""
事件去重扩展事件引擎。
"""
import time
from .engine import EventEngine, Event
from vnpy.trader.event import EVENT_LOG

class DedupEventEngine(EventEngine):
    """
    扩展事件引擎，支持对EVENT_LOG事件内容去重，10秒内相同内容只分发一次。
    """
    def __init__(self, interval: int = 1):
        super().__init__(interval)
        self._last_log_event = None
        self._last_log_time = 0

    def put(self, event: Event) -> None:
        # 只对 EVENT_LOG 做去重，10秒内相同内容不重复分发
        if event.type == EVENT_LOG:
            now = time.time()
            msg = getattr(event.data, "msg", None)
            if msg == self._last_log_event and now - self._last_log_time < 10:
                return  # 跳过重复日志
            self._last_log_event = msg
            self._last_log_time = now
        super().put(event) 