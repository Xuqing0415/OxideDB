import json
import os
import tempfile
from abc import ABC, abstractmethod
from typing import List, Optional
from .node import LogEntry


class RaftStorage(ABC):
    @abstractmethod
    def save_meta(self, current_term: int, voted_for: Optional[int]) -> None:
        pass

    @abstractmethod
    def load_meta(self) -> tuple:
        pass

    @abstractmethod
    def append_log_entry(self, entry: LogEntry) -> None:
        pass

    @abstractmethod
    def load_log(self) -> List[LogEntry]:
        pass

    @abstractmethod
    def clear(self) -> None:
        pass


class JSONFileStorage(RaftStorage):
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._meta_path = os.path.join(data_dir, "raft_meta.json")
        self._log_path = os.path.join(data_dir, "raft_log.json")

    def _atomic_write(self, path: str, data: dict) -> None:
        fd, temp_path = tempfile.mkstemp(dir=self._data_dir)
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(data, f)
            os.replace(temp_path, path)
        except Exception:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise

    def save_meta(self, current_term: int, voted_for: Optional[int]) -> None:
        data = {
            "current_term": current_term,
            "voted_for": voted_for
        }
        self._atomic_write(self._meta_path, data)

    def load_meta(self) -> tuple:
        if not os.path.exists(self._meta_path):
            return 0, None
        with open(self._meta_path, 'r') as f:
            data = json.load(f)
        return data.get("current_term", 0), data.get("voted_for", None)

    def append_log_entry(self, entry: LogEntry) -> None:
        if not os.path.exists(self._log_path):
            log_data = {"entries": []}
        else:
            with open(self._log_path, 'r') as f:
                log_data = json.load(f)
        
        log_data["entries"].append({
            "term": entry.term,
            "index": entry.index,
            "command": entry.command.decode('latin-1')
        })
        
        self._atomic_write(self._log_path, log_data)

    def load_log(self) -> List[LogEntry]:
        if not os.path.exists(self._log_path):
            return []
        with open(self._log_path, 'r') as f:
            log_data = json.load(f)
        
        entries = []
        for item in log_data.get("entries", []):
            entries.append(LogEntry(
                term=item["term"],
                index=item["index"],
                command=item["command"].encode('latin-1')
            ))
        return entries

    def clear(self) -> None:
        if os.path.exists(self._meta_path):
            os.remove(self._meta_path)
        if os.path.exists(self._log_path):
            os.remove(self._log_path)