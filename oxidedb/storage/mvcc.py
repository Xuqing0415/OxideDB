from typing import Dict, List, Optional, Tuple
import bisect
from threading import RLock


class MVCCRecord:
    def __init__(self, value: bytes, timestamp: int, deleted: bool = False):
        self.value = value
        self.timestamp = timestamp
        self.deleted = deleted


class WriteRecord:
    def __init__(self, start_ts: int, commit_ts: int):
        self.start_ts = start_ts
        self.commit_ts = commit_ts


class MVCCStorage:
    def __init__(self):
        self._data: Dict[bytes, List[MVCCRecord]] = {}
        self._write_records: Dict[bytes, List[WriteRecord]] = {}
        self._write_intents: Dict[bytes, Dict[int, bytes]] = {}
        self._lock = RLock()

    def get(self, key: bytes, timestamp: int) -> Optional[bytes]:
        with self._lock:
            if key not in self._data:
                return None

            versions = self._data[key]
            idx = bisect.bisect_right(versions, timestamp, key=lambda r: r.timestamp) - 1

            if idx < 0:
                return None

            record = versions[idx]
            if record.deleted:
                return None
            return record.value

    def set(self, key: bytes, value: bytes, timestamp: int) -> None:
        with self._lock:
            if key not in self._data:
                self._data[key] = []

            versions = self._data[key]
            idx = bisect.bisect_right(versions, timestamp, key=lambda r: r.timestamp)

            if idx > 0 and versions[idx - 1].timestamp == timestamp:
                versions[idx - 1] = MVCCRecord(value, timestamp, False)
            else:
                bisect.insort(versions, MVCCRecord(value, timestamp, False), key=lambda r: r.timestamp)

    def delete(self, key: bytes, timestamp: int) -> None:
        with self._lock:
            if key not in self._data:
                self._data[key] = []

            versions = self._data[key]
            idx = bisect.bisect_right(versions, timestamp, key=lambda r: r.timestamp)

            if idx > 0 and versions[idx - 1].timestamp == timestamp:
                versions[idx - 1].deleted = True
            else:
                bisect.insort(versions, MVCCRecord(b"", timestamp, True), key=lambda r: r.timestamp)

    def scan(self, start_key: bytes, end_key: bytes, timestamp: int) -> List[Tuple[bytes, bytes]]:
        with self._lock:
            result = []
            for key in sorted(self._data.keys()):
                if start_key <= key < end_key:
                    value = self.get(key, timestamp)
                    if value is not None:
                        result.append((key, value))
            return result

    def get_latest_version(self, key: bytes) -> Optional[MVCCRecord]:
        with self._lock:
            if key not in self._data or not self._data[key]:
                return None
            return self._data[key][-1]
    
    def get_latest_write(self, key: bytes) -> Optional[Dict[str, int]]:
        with self._lock:
            if key not in self._write_records or not self._write_records[key]:
                return None
            latest = self._write_records[key][-1]
            return {"commit_ts": latest.commit_ts, "start_ts": latest.start_ts}
    
    def set_write_intent(self, key: bytes, value: bytes, start_ts: int) -> None:
        with self._lock:
            if key not in self._write_intents:
                self._write_intents[key] = {}
            self._write_intents[key][start_ts] = value
    
    def get_write_intent(self, key: bytes, start_ts: int) -> Optional[bytes]:
        with self._lock:
            if key not in self._write_intents:
                return None
            return self._write_intents[key].get(start_ts)
    
    def remove_write_intent(self, key: bytes, start_ts: int) -> None:
        with self._lock:
            if key in self._write_intents:
                self._write_intents[key].pop(start_ts, None)
                if not self._write_intents[key]:
                    del self._write_intents[key]
    
    def write_write_record(self, key: bytes, start_ts: int, commit_ts: int) -> None:
        with self._lock:
            if key not in self._write_records:
                self._write_records[key] = []
            
            records = self._write_records[key]
            idx = bisect.bisect_right(records, commit_ts, key=lambda r: r.commit_ts)
            
            if idx > 0 and records[idx - 1].commit_ts == commit_ts:
                records[idx - 1] = WriteRecord(start_ts, commit_ts)
            else:
                bisect.insort(records, WriteRecord(start_ts, commit_ts), key=lambda r: r.commit_ts)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._write_records.clear()
            self._write_intents.clear()