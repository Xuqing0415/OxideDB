import time
from threading import Lock


class TimestampAllocator:
    def __init__(self):
        self._lock = Lock()
        self._last_timestamp = int(time.time() * 1_000_000_000)

    def allocate(self) -> int:
        with self._lock:
            now = int(time.time() * 1_000_000_000)
            if now > self._last_timestamp:
                self._last_timestamp = now
            else:
                self._last_timestamp += 1
            return self._last_timestamp

    def get_current(self) -> int:
        with self._lock:
            return self._last_timestamp


_global_ts_allocator = TimestampAllocator()


def get_timestamp() -> int:
    return _global_ts_allocator.allocate()


def get_current_timestamp() -> int:
    return _global_ts_allocator.get_current()
